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
  explicit LatencyHistGuard(HistReporterHandle* handle, Env* env = nullptr)
      : handle_(handle),
        env_(handle == nullptr ? nullptr
                               : (env == nullptr ? Env::Default() : env)),
        begin_time_(env_ == nullptr ? 0 : env_->NowMicros()) {}

  ~LatencyHistGuard() {
    if (handle_ != nullptr) {
      auto us = env_->NowMicros() - begin_time_;
      handle_->AddRecord(us);
    }
  }

 private:
  HistReporterHandle* handle_;
  Env* const env_;
  uint64_t begin_time_;
};

class LatencyHistLoggedGuard {
 public:
  explicit LatencyHistLoggedGuard(HistReporterHandle* handle,
                                  unsigned int threshold_us = 500 * 1000,
                                  Env* env = nullptr);
  ~LatencyHistLoggedGuard();

 private:
  HistReporterHandle* handle_;
  Env* const env_;
  uint64_t begin_time_;
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