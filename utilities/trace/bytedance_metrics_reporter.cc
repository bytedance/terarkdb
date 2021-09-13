#include "bytedance_metrics_reporter.h"

#include <cassert>
#include <mutex>
#ifdef WITH_BYTEDANCE_METRICS
#include "metrics.h"
#endif

#include "rocksdb/terark_namespace.h"
#include "util/logging.h"

namespace TERARKDB_NAMESPACE {

const int kNanosInMilli = 1000000;
static const char default_namespace[] = "terarkdb.engine.stats";

#ifdef WITH_BYTEDANCE_METRICS
static std::mutex metrics_mtx;
static std::atomic<bool> metrics_init{false};

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

void ByteDanceHistReporterHandle::AddRecord(size_t val) {
  auto* tls_stat_ptr = GetThreadLocalStats();
  if (tls_stat_ptr == nullptr) {
    return;
  }

  auto& tls_stat = *tls_stat_ptr;
  tls_stat.AppendRecord(val);

  auto curr_time_ns = env_->NowNanos();
  auto diff_ms = (curr_time_ns - tls_stat.last_report_time_ns_) / kNanosInMilli;

  if (diff_ms > 1000 && !merge_lock_.load(std::memory_order_relaxed) &&
      !merge_lock_.exchange(true, std::memory_order_acquire)) {
    stats_.Merge(tls_stat);

    diff_ms = (curr_time_ns - stats_.last_report_time_ns_) / kNanosInMilli;
    if (diff_ms >= 30000 /* 30 seconds */) {
      auto result = stats_.GetResult({0.50, 0.99, 0.999});
      stats_.Reset();
      stats_.last_report_time_ns_ = curr_time_ns;
      merge_lock_.store(false, std::memory_order_release);

      cpputil::metrics2::Metrics::emit_store(name_ + "_p50", result[0], tags_);
      cpputil::metrics2::Metrics::emit_store(name_ + "_p99", result[1], tags_);
      cpputil::metrics2::Metrics::emit_store(name_ + "_p999", result[2], tags_);
      cpputil::metrics2::Metrics::emit_store(name_ + "_avg", result[3], tags_);
      cpputil::metrics2::Metrics::emit_store(name_ + "_max", result[4], tags_);

      diff_ms = (curr_time_ns - last_log_time_ns_) / kNanosInMilli;
      InfoLogLevel log_level = InfoLogLevel::DEBUG_LEVEL;
      if (diff_ms > 10 * 60 * 1000 /* 10 minutes */) {
        log_level = InfoLogLevel::INFO_LEVEL;
        last_log_time_ns_ = curr_time_ns;
      }
      TERARKDB_NAMESPACE::Log(log_level, logger_,
                              "name:%s P50, tags:%s, val:%zu", name_.c_str(),
                              tags_.c_str(), result[0]);
      TERARKDB_NAMESPACE::Log(log_level, logger_,
                              "name:%s P99, tags:%s, val:%zu", name_.c_str(),
                              tags_.c_str(), result[1]);
      TERARKDB_NAMESPACE::Log(log_level, logger_,
                              "name:%s P999, tags:%s, val:%zu", name_.c_str(),
                              tags_.c_str(), result[2]);
      TERARKDB_NAMESPACE::Log(log_level, logger_,
                              "name:%s Avg, tags:%s, val:%zu", name_.c_str(),
                              tags_.c_str(), result[3]);
      TERARKDB_NAMESPACE::Log(log_level, logger_,
                              "name:%s Max, tags:%s, val:%zu", name_.c_str(),
                              tags_.c_str(), result[4]);
    } else {
      merge_lock_.store(false, std::memory_order_release);
    }

    tls_stat.Reset();
    tls_stat.last_report_time_ns_ = curr_time_ns;
  }
}

HistStats<>* ByteDanceHistReporterHandle::GetThreadLocalStats() {
  auto id = GetThreadID();
  if (id == -1) {
    return nullptr;
  }
  auto& s = stats_arr_[id];
  if (s == nullptr) {
    s = new HistStats<>(env_->NowNanos());
  }
  return s;
}

void ByteDanceCountReporterHandle::AddCount(size_t n) {
  count_.fetch_add(n, std::memory_order_relaxed);
  if (!reporter_lock_.load(std::memory_order_relaxed)) {
    if (!reporter_lock_.exchange(true, std::memory_order_acquire)) {
      auto curr_time_ns = env_->NowNanos();
      auto diff_ms = (curr_time_ns - last_report_time_ns_) / kNanosInMilli;
      if (diff_ms >= 30000 /* 30 seconds */) {
        size_t curr_count = count_.load(std::memory_order_relaxed);
        size_t qps = (curr_count - last_report_count_) /
                     (static_cast<double>(diff_ms) / 1000);
        cpputil::metrics2::Metrics::emit_store(name_, qps, tags_);

        last_report_time_ns_ = curr_time_ns;
        last_report_count_ = curr_count;

        diff_ms = (curr_time_ns - last_log_time_ns_) / kNanosInMilli;
        InfoLogLevel log_level = InfoLogLevel::DEBUG_LEVEL;
        if (diff_ms > 10 * 60 * 1000 /* 10 minutes */) {
          log_level = InfoLogLevel::INFO_LEVEL;
          last_log_time_ns_ = curr_time_ns;
        }
        TERARKDB_NAMESPACE::Log(log_level, logger_, "name:%s, tags:%s, val:%zu",
                                name_.c_str(), tags_.c_str(), qps);
      }
      reporter_lock_.store(false, std::memory_order_release);
    }
  }
}
#endif

ByteDanceMetricsReporterFactory::ByteDanceMetricsReporterFactory() {
  InitNamespace(default_namespace);
}

ByteDanceMetricsReporterFactory::ByteDanceMetricsReporterFactory(
    const std::string& ns) {
  InitNamespace(ns);
}

void ByteDanceMetricsReporterFactory::InitNamespace(const std::string& ns) {
#ifdef WITH_BYTEDANCE_METRICS
  if (!metrics_init.load(std::memory_order_acquire)) {
    std::lock_guard<std::mutex> guard(metrics_mtx);
    if (!metrics_init.load(std::memory_order_relaxed)) {
      auto metricsConf = cpputil::metrics2::MetricCollectorConf();
      metricsConf.namespace_prefix = ns;
      cpputil::metrics2::Metrics::init(metricsConf);
      metrics_init.store(true, std::memory_order_release);
    }
  }
#else
  (void)ns;
#endif
}

HistReporterHandle* ByteDanceMetricsReporterFactory::BuildHistReporter(
    const std::string& name, const std::string& tags, Logger* logger,
    Env* const env) {
#ifdef WITH_BYTEDANCE_METRICS
  std::lock_guard<std::mutex> guard(metrics_mtx);
  hist_reporters_.emplace_back(name, tags, logger, env);
  return &hist_reporters_.back();
#else
  (void)name, (void)tags, (void)logger, (void)env;
  return DummyHistReporterHandle();
#endif
}

CountReporterHandle* ByteDanceMetricsReporterFactory::BuildCountReporter(
    const std::string& name, const std::string& tags, Logger* logger,
    Env* const env) {
#ifdef WITH_BYTEDANCE_METRICS
  std::lock_guard<std::mutex> guard(metrics_mtx);
  count_reporters_.emplace_back(name, tags, logger, env);
  return &count_reporters_.back();
#else
  (void)name, (void)tags, (void)logger, (void)env;
  return DummyCountReporterHandle();
#endif
}
}  // namespace TERARKDB_NAMESPACE