#pragma once

#include "rocksdb/terark_namespace.h"
#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(WITH_ZENFS)

#include <unordered_map>

#include "bytedance_metrics_reporter.h"
#include "rocksdb/env.h"
#include "third-party/zenfs/fs/metrics.h"

namespace ROCKSDB_NAMESPACE {

typedef ZenFSMetricsHistograms BDZenFSMetricsHistograms;
typedef ZenFSMetricsReporterType BDZenFSMetricsReporterType;

extern const std::unordered_map<uint32_t, std::pair<std::string, uint32_t>>
    BDZenFSHistMap;

struct NoLogger : public Logger {
 public:
  using Logger::Logv;
  virtual void Logv(const char* /*format*/, va_list /*ap*/) override {}
};

struct BDZenFSMetrics : public ZenFSMetrics {
 public:
  struct Reporter {
    void* handle_;
    BDZenFSMetricsReporterType type_;
    Reporter(void* handle = nullptr,
             uint32_t type = ZENFS_REPORTER_TYPE_WITHOUT_CHECK)
        : handle_(handle), type_((BDZenFSMetricsReporterType)type) {}
    Reporter(const Reporter& r) : handle_(r.handle_), type_(r.type_) {}
    ~Reporter() {}
    HistReporterHandle* GetHistReporterHandle() const {
      return reinterpret_cast<HistReporterHandle*>(handle_);
    }
    CountReporterHandle* GetCountReporterHandle() const {
      return reinterpret_cast<CountReporterHandle*>(handle_);
    }
  };

 private:
  std::string tag_;
  std::shared_ptr<Logger> logger_;
  std::shared_ptr<CurriedMetricsReporterFactory> factory_;
  std::unordered_map<BDZenFSMetricsHistograms, Reporter> reporter_map_;

 private:
  virtual void AddReporter_(uint32_t h, const Reporter& reporter) {
    auto hist = (BDZenFSMetricsHistograms)h;
    assert(reporter_map_.find(hist) == reporter_map_.end());
    reporter_map_[hist] = reporter;
  }

 public:
  virtual void AddReporter(uint32_t label, uint32_t type = 0) override;

  virtual void Report(uint32_t label, size_t value,
                      uint32_t type_check = 0) override;

  void* GetReporter(uint32_t label, uint32_t type_check = 0);

 public:
  virtual void ReportQPS(uint32_t label, size_t qps) override;

  virtual void ReportLatency(uint32_t label, size_t latency) override;

  virtual void ReportThroughput(uint32_t label, size_t throughput) override;

  virtual void ReportGeneral(uint32_t label, size_t value) override;

  virtual void ReportSnapshot(const ZenFSSnapshot& snapshot) override;

 public:
  BDZenFSMetrics(std::shared_ptr<MetricsReporterFactory> factory,
                 std::string tag, std::shared_ptr<Logger> logger = nullptr)
      : ZenFSMetrics(),
        tag_(tag),
        logger_(logger == nullptr ? std::make_shared<NoLogger>() : logger) {
    factory_ = std::make_shared<CurriedMetricsReporterFactory>(
        factory, logger_.get(), Env::Default());
    for (auto& h : BDZenFSHistMap)
      AddReporter(static_cast<uint32_t>(h.first),
                  static_cast<uint32_t>(h.second.second));
  }
  virtual ~BDZenFSMetrics() {}
};
}  // namespace ROCKSDB_NAMESPACE

#endif
