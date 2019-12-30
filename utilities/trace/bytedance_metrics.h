#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>

#include "port/port.h"
#include "rocksdb/slice.h"

namespace rocksdb {
class OperationTimerReporter {
 public:
  OperationTimerReporter(const std::string& name, const std::string& tags);

  ~OperationTimerReporter();

  static void InitNamespace(const std::string& metrics_namespace);

 private:
#ifdef TERARKDB_ENABLE_METRICS
  const std::string& name_;
  const std::string& tags_;
  decltype(std::chrono::high_resolution_clock::now()) begin_t_;
#endif
};

class QPSReporter {
 public:
  QPSReporter(const std::string& name, const std::string& tags);

  void AddCount(size_t n);

 private:
#ifdef TERARKDB_ENABLE_METRICS
  std::atomic<bool> reporter_lock_{false};

  const std::string& name_;
  const std::string& tags_;

  std::chrono::high_resolution_clock::time_point last_report_t_;
  size_t last_report_count_;

  char _padding_[CACHE_LINE_SIZE - 8 * 5];

  std::atomic<size_t> count_{0};
#endif
};
}  // namespace rocksdb