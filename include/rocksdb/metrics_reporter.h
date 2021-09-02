#pragma once

#include <chrono>
#include <cstddef>
#include <string>

#include "rocksdb/env.h"
#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {
class HistReporterHandle {
 public:
  HistReporterHandle() = default;

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
  decltype(std::chrono::high_resolution_clock::now()) begin_time_;
};

class LatencyHistLoggedGuard {
 public:
  explicit LatencyHistLoggedGuard(HistReporterHandle* handle, Logger* logger,
                                  unsigned int threshold,
                                  const char* tag = "LatencyHistLoggedGuard")
      : handle_(handle),
        begin_time_(std::chrono::high_resolution_clock::now()),
        log_threshold_us_(threshold),
        logger_(logger),
        tag_(tag) {}

  ~LatencyHistLoggedGuard() {
    if (handle_ != nullptr) {
      auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - begin_time_)
                    .count();
      handle_->AddRecord(us);
      if (us > log_threshold_us_) {
        ROCKS_LOG_WARN(logger_, "[%s]: %" PRIu64 "", tag_,
                       static_cast<uint64_t>(us));
      }
    }
  }

 private:
  HistReporterHandle* handle_;
  decltype(std::chrono::high_resolution_clock::now()) begin_time_;
  Logger* logger_;
  unsigned int log_threshold_us_;
  const char* tag_;
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