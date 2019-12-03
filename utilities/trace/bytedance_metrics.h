#pragma once

#include <chrono>

#include "rocksdb/slice.h"

namespace rocksdb {
class OperationTimerReporter {
 public:
  explicit OperationTimerReporter(const std::string& name,
                                  const std::string& tags);

  ~OperationTimerReporter();

  static void InitNamespace(const std::string& metrics_namespace);

 private:
  const std::string& name_;
  const std::string& tags_;
  decltype(std::chrono::high_resolution_clock::now()) begin_t_;
};
}  // namespace rocksdb