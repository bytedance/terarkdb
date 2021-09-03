#pragma once

#include <inttypes.h>

#include <chrono>
#include <cstddef>
#include <string>

#include "rocksdb/env.h"
#include "rocksdb/terark_namespace.h"
#include "util/logging.h"
#define REPORT_DEBUG_STACKTRACE 1

#if REPORT_DEBUG_STACKTRACE
#include <boost/stacktrace.hpp>
#endif

namespace TERARKDB_NAMESPACE {
class HistReporterHandle {
 public:
  HistReporterHandle() = default;

  virtual Logger* GetLogger() { return nullptr; }
  virtual const char* GetTag() { return ""; }
  virtual const char* GetName() { return ""; }

  virtual ~HistReporterHandle() = default;

 public:
  virtual void AddRecord(size_t val) = 0;
};

class LatencyHistGuard {
 public:
  explicit LatencyHistGuard(HistReporterHandle* handle)
      : handle_(handle),
        begin_time_(std::chrono::high_resolution_clock::now()) {}

  ~LatencyHistGuard() {
    if (handle_ != nullptr) {
      auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - begin_time_)
                    .count();
      handle_->AddRecord(us);
    }
  }

 private:
  HistReporterHandle* handle_;
  std::chrono::high_resolution_clock::time_point begin_time_;
};

class LatencyHistLoggedGuard {
 public:
  explicit LatencyHistLoggedGuard(HistReporterHandle* handle,
                                  unsigned int threshold = 500 * 1000)
      : handle_(handle),
        begin_time_(std::chrono::high_resolution_clock::now()),
        log_threshold_us_(threshold) {
#if REPORT_DEBUG_STACKTRACE
    auto stacktrace = new boost::stacktrace::stacktrace();
    start_stacktrace_ = stacktrace;
#endif
  }

  ~LatencyHistLoggedGuard() {
    if (handle_ != nullptr) {
      auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - begin_time_)
                    .count();
      handle_->AddRecord(us);
      if (us > log_threshold_us_ && handle_->GetLogger() != nullptr) {
#if REPORT_DEBUG_STACKTRACE
        auto stacktrace =
            static_cast<boost::stacktrace::stacktrace*>(start_stacktrace_);
        start_stacktrace_ = nullptr;
        ROCKS_LOG_WARN(
            handle_->GetLogger(),
            "[name:%s] [tags:%s]: %" PRIu64 "us\n%s----------\n%s-----------\n",
            handle_->GetName(), handle_->GetTag(), static_cast<uint64_t>(us),
            boost::stacktrace::to_string(*stacktrace).c_str(),
            boost::stacktrace::to_string(boost::stacktrace::stacktrace())
                .c_str());
        delete stacktrace;
#else
        ROCKS_LOG_WARN(handle_->GetLogger(), "[%s]: %" PRIu64 "us\n",
                       handle_->GetTag(), static_cast<uint64_t>(us));
#endif
      }
    }
  }

 private:
  HistReporterHandle* handle_;
  std::chrono::high_resolution_clock::time_point begin_time_;
  unsigned int log_threshold_us_;
  Logger* logger_;
  const char* tag_;
  void* start_stacktrace_ = nullptr;
};

class CountReporterHandle {
 public:
  CountReporterHandle() = default;

  virtual ~CountReporterHandle() = default;

 public:
  virtual void AddCount(size_t val) = 0;
};

class MetricsReporterFactory {
 public:
  MetricsReporterFactory() = default;

  virtual ~MetricsReporterFactory() = default;

 public:
  virtual HistReporterHandle* BuildHistReporter(const std::string& name,
                                                const std::string& tags,
                                                Logger* log) = 0;

  virtual CountReporterHandle* BuildCountReporter(const std::string& name,
                                                  const std::string& tags,
                                                  Logger* log) = 0;
};
}  // namespace TERARKDB_NAMESPACE