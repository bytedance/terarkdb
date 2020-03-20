#include "rocksdb/metrics_reporter.h"

#include <atomic>
#include <chrono>
#include <list>

#include "rocksdb/env.h"
#include "stats.h"

namespace rocksdb {
class ByteDanceHistReporterHandle : public HistReporterHandle {
 public:
#ifdef TERARKDB_ENABLE_METRICS
  ByteDanceHistReporterHandle(const std::string& name, const std::string& tags,
                              Logger* log)
      : name_(name),
        tags_(tags),
        last_log_time_(std::chrono::high_resolution_clock::now()),
        log_(log) {}
#else
  ByteDanceHistReporterHandle(const std::string& /*name*/,
                              const std::string& /*tags*/, Logger* /*log*/) {}
#endif

  ~ByteDanceHistReporterHandle() override {
#ifdef TERARKDB_ENABLE_METRICS
    for (auto* s : stats_arr_) {
      delete s;
    }
#endif
  }

 public:
  void AddRecord(size_t val) override;

 private:
#ifdef TERARKDB_ENABLE_METRICS
  enum {
    kMaxThreadNum = 8192,
  };

  const std::string& name_;
  const std::string& tags_;

  std::chrono::high_resolution_clock::time_point last_log_time_;
  Logger* log_;

  std::array<HistStats<>*, kMaxThreadNum> stats_arr_{};

  std::atomic<bool> merge_lock_{false};
  HistStats<> stats_;
#endif

  HistStats<>* GetThreadLocalStats();
};

class ByteDanceCountReporterHandle : public CountReporterHandle {
 public:
#ifdef TERARKDB_ENABLE_METRICS
  ByteDanceCountReporterHandle(const std::string& name, const std::string& tags,
                               Logger* log)
      : name_(name),
        tags_(tags),
        last_report_time_(std::chrono::high_resolution_clock::now()),
        last_log_time_(std::chrono::high_resolution_clock::now()),
        log_(log) {}
#else
  ByteDanceCountReporterHandle(const std::string& /*name*/,
                               const std::string& /*tags*/, Logger* /*log*/) {}
#endif

  ~ByteDanceCountReporterHandle() override = default;

 public:
  void AddCount(size_t val) override;

 private:
#ifdef TERARKDB_ENABLE_METRICS
  std::atomic<bool> reporter_lock_{false};

  const std::string& name_;
  const std::string& tags_;

  std::chrono::high_resolution_clock::time_point last_report_time_;
  size_t last_report_count_ = 0;

  std::chrono::high_resolution_clock::time_point last_log_time_;
  Logger* log_;

  char _padding_[64 /* x86 cache line size */ - 8 * 7];

  std::atomic<size_t> count_{0};
#endif
};

class ByteDanceMetricsReporterFactory : public MetricsReporterFactory {
 public:
  ByteDanceMetricsReporterFactory();

  ByteDanceMetricsReporterFactory(const std::string& ns);

  ~ByteDanceMetricsReporterFactory() override = default;

 public:
  ByteDanceHistReporterHandle* BuildHistReporter(const std::string& name,
                                                 const std::string& tags,
                                                 Logger* log) override;

  ByteDanceCountReporterHandle* BuildCountReporter(const std::string& name,
                                                   const std::string& tags,
                                                   Logger* log) override;

 private:
#ifdef TERARKDB_ENABLE_METRICS
  std::list<ByteDanceHistReporterHandle> hist_reporters_;
  std::list<ByteDanceCountReporterHandle> count_reporters_;
#endif

  void InitNamespace(const std::string& ns);
};
}  // namespace rocksdb