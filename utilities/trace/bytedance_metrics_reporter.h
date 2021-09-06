#pragma once

#include <atomic>
#include <deque>

#include "rocksdb/env.h"
#include "rocksdb/metrics_reporter.h"
#include "rocksdb/terark_namespace.h"
#include "stats.h"
namespace TERARKDB_NAMESPACE {

#ifdef WITH_BYTEDANCE_METRICS
class ByteDanceHistReporterHandle : public HistReporterHandle {
 public:
  ByteDanceHistReporterHandle(const std::string& name, const std::string& tags,
                              Logger* log, Env* const env)
      : name_(name),
        tags_(tags),
        env_(env),
        last_log_time_ns_(env_->NowNanos()),
        log_(log),
        stats_(env_->NowNanos()) {}
  virtual Env* GetEnv() override { return env_; }
#else
  ByteDanceHistReporterHandle(const std::string& /*name*/,
                              const std::string& /*tags*/, Logger* /*log*/,
                              Env* const /*env*/) {}
#endif

  ~ByteDanceHistReporterHandle() override {
    for (auto* s : stats_arr_) {
      delete s;
    }
  }

 public:
  void AddRecord(size_t val) override;

  Logger* GetLogger() override {
#ifdef TERARKDB_ENABLE_METRICS
    return log_;
#else
    return nullptr;
#endif
  }
  const char* GetTag() {
#ifdef TERARKDB_ENABLE_METRICS
    return tags_.c_str();
#else
    return "";
#endif
  }
  const char* GetName() {
#ifdef TERARKDB_ENABLE_METRICS
    return name_.c_str();
#else
    return "";
#endif
  }

 private:
  enum {
    kMaxThreadNum = 8192,
  };
  const std::string& name_;
  const std::string& tags_;
  Env* env_;

  uint64_t last_log_time_ns_;
  Logger* log_;

  std::array<HistStats<>*, kMaxThreadNum> stats_arr_{};

  std::atomic<bool> merge_lock_{false};
  HistStats<> stats_;

  HistStats<>* GetThreadLocalStats();
};

class ByteDanceCountReporterHandle : public CountReporterHandle {
 public:
  ByteDanceCountReporterHandle(const std::string& name, const std::string& tags,
                               Logger* log, Env* const env)
      : name_(name),
        tags_(tags),
        env_(env),
        last_report_time_ns_(env_->NowNanos()),
        last_log_time_ns_(env_->NowNanos()),
        log_(log) {}
#else
  ByteDanceCountReporterHandle(const std::string& /*name*/,
                               const std::string& /*tags*/, Logger* /*log*/,
                               Env* const /*env*/) {}
#endif

  ~ByteDanceCountReporterHandle() override = default;

 public:
  void AddCount(size_t val) override;

 private:
  std::atomic<bool> reporter_lock_{false};

  const std::string& name_;
  const std::string& tags_;
  Env* const env_;
  uint64_t last_report_time_ns_;
  size_t last_report_count_ = 0;

  uint64_t last_log_time_ns_;
  Logger* log_;

  std::atomic<size_t> count_{0};
};
#endif

class ByteDanceMetricsReporterFactory : public MetricsReporterFactory {
 public:
  ByteDanceMetricsReporterFactory();

  explicit ByteDanceMetricsReporterFactory(const std::string& ns);

  ~ByteDanceMetricsReporterFactory() override = default;

 public:
  ByteDanceHistReporterHandle* BuildHistReporter(const std::string& name,
                                                 const std::string& tags,
                                                 Logger* log,
                                                 Env* const env) override;

  ByteDanceCountReporterHandle* BuildCountReporter(const std::string& name,
                                                   const std::string& tags,
                                                   Logger* log,
                                                   Env* const env) override;

 private:
#ifdef TERARKDB_ENABLE_METRICS
  std::deque<ByteDanceHistReporterHandle> hist_reporters_;
  std::deque<ByteDanceCountReporterHandle> count_reporters_;
#endif

  void InitNamespace(const std::string& ns);
};
}  // namespace TERARKDB_NAMESPACE
