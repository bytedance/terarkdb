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
  OperationTimerReporter::InitNamespace(default_namespace);
}

OperationTimerReporter::~OperationTimerReporter() {
  auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - begin_t_)
                .count();
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

QPSReporter::QPSReporter(const std::string& name, const std::string& tags)
    : name_(name),
      tags_(tags),
      last_report_t_(std::chrono::high_resolution_clock::now()),
      last_report_count_(0) {
  OperationTimerReporter::InitNamespace(default_namespace);
}

void QPSReporter::AddCount(size_t n) {
  count_.fetch_add(n, std::memory_order_relaxed);
  if (!reporter_lock_.load(std::memory_order_relaxed)) {
    if (!reporter_lock_.exchange(true, std::memory_order_acquire)) {
      // I am reporter
      auto curr_t = std::chrono::high_resolution_clock::now();
      auto diff_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         curr_t - last_report_t_)
                         .count();
      if (diff_ms > 1000) {
        size_t curr_count = count_.load(std::memory_order_release);
        size_t qps = (curr_count - last_report_count_) /
                     (static_cast<double>(diff_ms) / 1000);

        if (!tags_.empty()) {
          cpputil::metrics2::Metrics::emit_store(name_, qps, tags_);
        } else {
          cpputil::metrics2::Metrics::emit_store(name_, qps);
        }

        last_report_t_ = curr_t;
        last_report_count_ = curr_count;
      }
      reporter_lock_.store(false, std::memory_order_release);
    }
  }
}

#else

OperationTimerReporter::OperationTimerReporter(const std::string& /*name*/,
                                               const std::string& /*tags*/) {}

OperationTimerReporter::~OperationTimerReporter() {}

void OperationTimerReporter::InitNamespace(
    const std::string& /*metrics_namespace*/) {}

QPSReporter::QPSReporter(const std::string& /*name*/,
                         const std::string& /*tags*/) {}

void QPSReporter::AddCount(size_t /*n*/) {}

#endif
}  // namespace rocksdb