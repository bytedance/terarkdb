#pragma once

#include <inttypes.h>

#include <cstddef>
#include <string>

#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

class Env;
class Logger;

class HistReporterHandle {
 public:
  HistReporterHandle() = default;

  virtual Logger* GetLogger() { return nullptr; }
  virtual const char* GetTag() { return ""; }
  virtual const char* GetName() { return ""; }
  virtual Env* GetEnv() { return nullptr; }

  virtual ~HistReporterHandle() = default;

 public:
  virtual void AddRecord(size_t val) = 0;
};

class LatencyHistGuard {
 public:
  explicit LatencyHistGuard(HistReporterHandle* handle);

  ~LatencyHistGuard();

 private:
  HistReporterHandle* handle_;
  uint64_t begin_time_ns_;
};

class LatencyHistLoggedGuard {
 public:
  explicit LatencyHistLoggedGuard(HistReporterHandle* handle,
                                  uint64_t threshold_us = 500 * 1000);
  ~LatencyHistLoggedGuard();

 private:
  HistReporterHandle* handle_;
  uint64_t begin_time_ns_;
  uint64_t log_threshold_us_;
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
                                                Logger* log,
                                                Env* const env) = 0;

  virtual CountReporterHandle* BuildCountReporter(const std::string& name,
                                                  const std::string& tags,
                                                  Logger* log,
                                                  Env* const env) = 0;
};
}  // namespace TERARKDB_NAMESPACE