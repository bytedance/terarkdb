#include <atomic>
#include <iostream>
#include <mutex>

#include "bytedance_metrics.h"
#include "metrics.h"

namespace rocksdb {
static std::mutex metrics_mtx{};
static std::atomic<bool> metrics_init{false};
static const char default_namespace[] = "terarkdb.engine.stats";

OperationTimerReporter::OperationTimerReporter(const std::string& name,
                                               const std::string& tags)
    : name_(name),
      tags_(tags),
      begin_t_(std::chrono::high_resolution_clock::now()) {
  if (!metrics_init.load(std::memory_order_acquire)) {
    std::lock_guard<std::mutex> guard(metrics_mtx);
    if (!metrics_init.load(std::memory_order_relaxed)) {
      auto metricsConf = cpputil::metrics2::MetricCollectorConf();
      metricsConf.namespace_prefix = default_namespace;
      cpputil::metrics2::Metrics::init(metricsConf);
      metrics_init.store(true, std::memory_order_release);
    }
  }
}

OperationTimerReporter::~OperationTimerReporter() {
  auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - begin_t_)
                .count();
  cpputil::metrics2::Metrics::define_timer(name_);
  if (!tags_.empty()) {
    cpputil::metrics2::Metrics::emit_timer(name_, us, tags_);
  } else {
    cpputil::metrics2::Metrics::emit_timer(name_, us);
  }
}

void OperationTimerReporter::InitNamespace(
    const std::string& metrics_namespace) {
  if (!metrics_init.load(std::memory_order_acquire)) {
    std::lock_guard<std::mutex> guard(metrics_mtx);
    if (!metrics_init.load(std::memory_order_relaxed)) {
      auto metricsConf = cpputil::metrics2::MetricCollectorConf();
      metricsConf.namespace_prefix = metrics_namespace;
      cpputil::metrics2::Metrics::init(metricsConf);
      metrics_init.store(true, std::memory_order_release);
    }
  }
}
}  // namespace rocksdb