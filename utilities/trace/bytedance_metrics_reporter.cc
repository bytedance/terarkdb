#include "bytedance_metrics_reporter.h"

#include <cassert>
#include <mutex>

#ifdef TERARKDB_ENABLE_METRICS
#include "metrics.h"
#endif

#include "util/logging.h"

namespace rocksdb {
static std::mutex metrics_mtx;
static std::atomic<bool> metrics_init{false};
static const char default_namespace[] = "terarkdb.engine.stats";

static int GetThreadID() {
  static std::mutex mutex;
  static std::vector<bool> pool;
  static __thread int id;

  struct ID {
    ID() {
      id = 0;
      std::lock_guard<std::mutex> guard(mutex);
      for (const auto& empty : pool) {
        if (empty) {
          pool[id] = false;
          return;
        }
        ++id;
      }
      pool.emplace_back();
    }

    ~ID() {
      std::lock_guard<std::mutex> guard(mutex);
      pool[id] = true;
    }
  };
  static thread_local ID _;

  if (id >= static_cast<int>(8192)) {
    return -1;
  }
  return id;
}

#ifdef TERARKDB_ENABLE_METRICS
void ByteDanceHistReporterHandle::AddRecord(size_t val) {
  auto* tls_stat_ptr = GetThreadLocalStats();
  if (tls_stat_ptr == nullptr) {
    return;
  }

  auto& tls_stat = *tls_stat_ptr;
  tls_stat.AppendRecord(val);

  auto curr_time = std::chrono::high_resolution_clock::now();
  auto diff_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     curr_time - tls_stat.last_report_time)
                     .count();

  if (diff_ms > 1000 && !merge_lock_.load(std::memory_order_relaxed) &&
      !merge_lock_.exchange(true, std::memory_order_acquire)) {
    stats_.Merge(tls_stat);

    diff_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  curr_time - stats_.last_report_time)
                  .count();
    if (diff_ms > 5000) {
      auto result = stats_.GetResult({0.50, 0.99, 0.999});
      stats_.Reset();
      stats_.last_report_time = curr_time;
      merge_lock_.store(false, std::memory_order_release);

      cpputil::metrics2::Metrics::emit_store(name_ + "_p50", result[0], tags_);
      cpputil::metrics2::Metrics::emit_store(name_ + "_p99", result[1], tags_);
      cpputil::metrics2::Metrics::emit_store(name_ + "_p999", result[2], tags_);

      diff_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    curr_time - last_log_time_)
                    .count();
      if (diff_ms > 10 * 60 * 1000) {
        ROCKS_LOG_INFO(log_, "name:%s P50, tags:%s, val:%zu", name_.c_str(),
                       tags_.c_str(), result[0]);
        ROCKS_LOG_INFO(log_, "name:%s P99, tags:%s, val:%zu", name_.c_str(),
                       tags_.c_str(), result[1]);
        ROCKS_LOG_INFO(log_, "name:%s P999, tags:%s, val:%zu", name_.c_str(),
                       tags_.c_str(), result[2]);
        last_log_time_ = curr_time;
      }
    } else {
      merge_lock_.store(false, std::memory_order_release);
    }

    tls_stat.Reset();
    tls_stat.last_report_time = curr_time;
  }
}
#else
void ByteDanceHistReporterHandle::AddRecord(size_t) {}
#endif

HistStats<>* ByteDanceHistReporterHandle::GetThreadLocalStats() {
  auto id = GetThreadID();
  if (id == -1) {
    return nullptr;
  }
  auto& s = stats_arr_[id];
  if (s == nullptr) {
    s = new HistStats<>;
  }
  return s;
}

#ifdef TERARKDB_ENABLE_METRICS
void ByteDanceCountReporterHandle::AddCount(size_t n) {
  count_.fetch_add(n, std::memory_order_relaxed);
  if (!reporter_lock_.load(std::memory_order_relaxed)) {
    if (!reporter_lock_.exchange(true, std::memory_order_acquire)) {
      auto curr_time = std::chrono::high_resolution_clock::now();
      auto diff_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         curr_time - last_report_time_)
                         .count();

      if (diff_ms > 1000) {
        size_t curr_count = count_.load(std::memory_order_relaxed);
        size_t qps = (curr_count - last_report_count_) /
                     (static_cast<double>(diff_ms) / 1000);
        cpputil::metrics2::Metrics::emit_store(name_, qps, tags_);

        last_report_time_ = curr_time;
        last_report_count_ = curr_count;

        diff_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      curr_time - last_log_time_)
                      .count();
        if (diff_ms > 10 * 60 * 1000) {
          ROCKS_LOG_INFO(log_, "name:%s, tags:%s, val:%zu", name_.c_str(),
                         tags_.c_str(), qps);
          last_log_time_ = curr_time;
        }
      }
      reporter_lock_.store(false, std::memory_order_release);
    }
  }
}
#else
void ByteDanceCountReporterHandle::AddCount(size_t) {}
#endif

ByteDanceMetricsReporterFactory::ByteDanceMetricsReporterFactory() {
  InitNamespace(default_namespace);
}

ByteDanceMetricsReporterFactory::ByteDanceMetricsReporterFactory(
    const std::string& ns) {
  InitNamespace(ns);
}

#ifdef TERARKDB_ENABLE_METRICS
void ByteDanceMetricsReporterFactory::InitNamespace(const std::string& ns) {
  if (!metrics_init.load(std::memory_order_acquire)) {
    std::lock_guard<std::mutex> guard(metrics_mtx);
    if (!metrics_init.load(std::memory_order_relaxed)) {
      auto metricsConf = cpputil::metrics2::MetricCollectorConf();
      metricsConf.namespace_prefix = ns;
      cpputil::metrics2::Metrics::init(metricsConf);
      metrics_init.store(true, std::memory_order_release);
    }
  }
}
#else
void ByteDanceMetricsReporterFactory::InitNamespace(const std::string&) {}
#endif

ByteDanceHistReporterHandle* ByteDanceMetricsReporterFactory::BuildHistReporter(
    const std::string& name, const std::string& tags, Logger* log) {
  std::lock_guard<std::mutex> guard(metrics_mtx);
  hist_reporters_.emplace_back(name, tags, log);
  return &hist_reporters_.back();
}

ByteDanceCountReporterHandle*
ByteDanceMetricsReporterFactory::BuildCountReporter(const std::string& name,
                                                    const std::string& tags,
                                                    Logger* log) {
  std::lock_guard<std::mutex> guard(metrics_mtx);
  count_reporters_.emplace_back(name, tags, log);
  return &count_reporters_.back();
}
}  // namespace rocksdb