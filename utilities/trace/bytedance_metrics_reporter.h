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
                              Logger* logger, Env* const env)
      : name_(name),
        tags_(tags),
        logger_(logger),
        env_(env),
        last_log_time_ns_(env_->NowNanos()),
        stats_(last_log_time_ns_) {}

  ~ByteDanceHistReporterHandle() override {
    for (auto* s : stats_arr_) {
      delete s;
    }
  }

 public:
  void AddRecord(size_t val) override;

  const char* GetName() override { return name_.c_str(); }
  const char* GetTag() override { return tags_.c_str(); }
  Logger* GetLogger() override { return logger_; }
  Env* GetEnv() override { return env_; }

 private:
  enum {
    kMaxThreadNum = 8192,
  };
  const std::string& name_;
  const std::string& tags_;

  Logger* logger_;
  Env* env_;
  uint64_t last_log_time_ns_;

  std::array<HistStats<>*, kMaxThreadNum> stats_arr_{};

  std::atomic<bool> merge_lock_{false};
  HistStats<> stats_;

  HistStats<>* GetThreadLocalStats();
};

class ByteDanceCountReporterHandle : public CountReporterHandle {
 public:
  ByteDanceCountReporterHandle(const std::string& name, const std::string& tags,
                               Logger* logger, Env* const env)
      : name_(name),
        tags_(tags),
        env_(env),
        last_report_time_ns_(env_->NowNanos()),
        last_log_time_ns_(env_->NowNanos()),
        logger_(logger) {}

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
  Logger* logger_;

  std::atomic<size_t> count_{0};
};
#endif

class ByteDanceMetricsReporterFactory : public MetricsReporterFactory {
 public:
  ByteDanceMetricsReporterFactory();

  explicit ByteDanceMetricsReporterFactory(const std::string& ns);

  ~ByteDanceMetricsReporterFactory() override = default;

 public:
  HistReporterHandle* BuildHistReporter(const std::string& name,
                                        const std::string& tags, Logger* logger,
                                        Env* const env) override;

  CountReporterHandle* BuildCountReporter(const std::string& name,
                                          const std::string& tags,
                                          Logger* logger,
                                          Env* const env) override;

 private:
#ifdef WITH_BYTEDANCE_METRICS
  std::deque<ByteDanceHistReporterHandle> hist_reporters_;
  std::deque<ByteDanceCountReporterHandle> count_reporters_;
#endif

  void InitNamespace(const std::string& ns);
};
}  // namespace TERARKDB_NAMESPACE