#pragma once

#include <inttypes.h>

#include <chrono>
#include <cstddef>
#include <string>

#include "rocksdb/env.h"
#include "rocksdb/terark_namespace.h"
#include "util/logging.h"

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
                                  unsigned int threshold_us = 500 * 1000);
  ~LatencyHistLoggedGuard();

 private:
  HistReporterHandle* handle_;
  std::chrono::high_resolution_clock::time_point begin_time_;
  unsigned int log_threshold_us_;
  void* start_stacktrace_;
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