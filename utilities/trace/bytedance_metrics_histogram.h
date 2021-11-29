#pragma once

#include "metrics.h"

#include "rocksdb/env.h"
#include "bytedance_metrics_reporter.h"
#include <unordered_map>

#ifdef WITH_ZENFS
#include "third-party/zenfs/fs/metrics.h"
#endif

namespace ROCKSDB_NAMESPACE {
  
enum ZenFSMetricsHistograms : uint32_t {
  ZENFS_HISTOGRAM_ENUM_MIN, 

  ZENFS_FG_WRITE_LATENCY,
  ZENFS_BG_WRITE_LATENCY,
  
  ZENFS_READ_LATENCY,
  ZENFS_FG_SYNC_LATENCY,
  ZENFS_BG_SYNC_LATENCY,
  ZENFS_IO_ALLOC_WAL_LATENCY,
  ZENFS_IO_ALLOC_NON_WAL_LATENCY,
  ZENFS_IO_ALLOC_WAL_ACTUAL_LATENCY,
  ZENFS_IO_ALLOC_NON_WAL_ACTUAL_LATENCY,
  ZENFS_META_ALLOC_LATENCY,
  ZENFS_METADATA_SYNC_LATENCY,
  ZENFS_ROLL_LATENCY,

  ZENFS_WRITE_QPS,
  ZENFS_READ_QPS,
  ZENFS_SYNC_QPS,
  ZENFS_IO_ALLOC_QPS,
  ZENFS_META_ALLOC_QPS,
  ZENFS_ROLL_QPS,

  ZENFS_WRITE_THROUGHPUT,
  ZENFS_ROLL_THROUGHPUT,
  
  ZENFS_ACTIVE_ZONES,
  ZENFS_OPEN_ZONES,
  ZENFS_FREE_SPACE,
  ZENFS_USED_SPACE,
  ZENFS_RECLAIMABLE_SPACE,
  ZENFS_RESETABLE_ZONES,

  ZENFS_HISTOGRAM_ENUM_MAX,
};

enum BDZenFSMetricsReporterType : uint32_t {
  ZENFS_REPORTER_TYPE_WITHOUT_CHECK = 0,
  ZENFS_REPORTER_TYPE_GENERAL,
  ZENFS_REPORTER_TYPE_LATENCY,    
  ZENFS_REPORTER_TYPE_QPS,
  ZENFS_REPORTER_TYPE_THROUGHPUT
};

const std::unordered_map<BDZenFSMetricsHistograms, std::string> BDZenFSHistogramsNameMap = {
  {ZENFS_FG_WRITE_LATENCY, "zenfs_fg_write_latency"},
  {ZENFS_BG_WRITE_LATENCY, "zenfs_bg_write_latency"},
  {ZENFS_READ_LATENCY, "zenfs_read_latency"},
  {ZENFS_FG_SYNC_LATENCY, "fg_zenfs_sync_latency"},
  {ZENFS_BG_SYNC_LATENCY, "bg_zenfs_sync_latency"},
  {ZENFS_IO_ALLOC_WAL_LATENCY, "zenfs_io_alloc_wal_latency"},
  {ZENFS_IO_ALLOC_NON_WAL_LATENCY, "zenfs_io_alloc_non_wal_latency"},
  {ZENFS_IO_ALLOC_WAL_ACTUAL_LATENCY, "zenfs_io_alloc_wal_actual_latency"},
  {ZENFS_IO_ALLOC_NON_WAL_ACTUAL_LATENCY, "zenfs_io_alloc_non_wal_actual_latency"},
  {ZENFS_META_ALLOC_LATENCY, "rzenfs_meta_alloc_latency"},
  {ZENFS_METADATA_SYNC_LATENCY, "zenfs_metadata_sync_latency"},
  {ZENFS_ROLL_LATENCY, "zenfs_roll_latency"},
  {ZENFS_WRITE_QPS, "zenfs_write_qps"},
  {ZENFS_READ_QPS, "zenfs_read_qps"},
  {ZENFS_SYNC_QPS, "zenfs_sync_qps"},
  {ZENFS_IO_ALLOC_QPS, "zenfs_io_alloc_qps"},
  {ZENFS_META_ALLOC_QPS, "zenfs_meta_alloc_qps"},
  {ZENFS_ROLL_QPS, "zenfs_roll_qps"},
  {ZENFS_WRITE_THROUGHPUT, "rzenfs_write_throughput"},
  {ZENFS_ROLL_THROUGHPUT, "zenfs_roll_throughput"},
  {ZENFS_ACTIVE_ZONES, "zenfs_active_zones"},
  {ZENFS_OPEN_ZONES, "zenfs_open_zones"},
  {ZENFS_FREE_SPACE, "zenfs_free_space"},
  {ZENFS_USED_SPACE, "zenfs_used_space"},
  {ZENFS_RECLAIMABLE_SPACE, "zenfs_reclaimable_space"},
  {ZENFS_RESETABLE_ZONES, "zenfs_resetable_zones"}
};

const std::unordered_map<BDZenFSMetricsHistograms, BDZenFSMetricsReporterType> BDZenFSHistogramsTypeMap = {
  {ZENFS_FG_WRITE_LATENCY,            ZENFS_REPORTER_TYPE_LATENCY},
  {ZENFS_BG_WRITE_LATENCY,            ZENFS_REPORTER_TYPE_LATENCY},
  {ZENFS_READ_LATENCY,                ZENFS_REPORTER_TYPE_LATENCY},
  {ZENFS_FG_SYNC_LATENCY,             ZENFS_REPORTER_TYPE_LATENCY},
  {ZENFS_BG_SYNC_LATENCY,             ZENFS_REPORTER_TYPE_LATENCY},
  {ZENFS_IO_ALLOC_WAL_LATENCY,        ZENFS_REPORTER_TYPE_LATENCY},
  {ZENFS_IO_ALLOC_NON_WAL_LATENCY,    ZENFS_REPORTER_TYPE_LATENCY},
  {ZENFS_IO_ALLOC_WAL_ACTUAL_LATENCY, ZENFS_REPORTER_TYPE_LATENCY},
  {ZENFS_IO_ALLOC_NON_WAL_ACTUAL_LATENCY,   ZENFS_REPORTER_TYPE_LATENCY},
  {ZENFS_META_ALLOC_LATENCY,          ZENFS_REPORTER_TYPE_LATENCY},
  {ZENFS_METADATA_SYNC_LATENCY,       ZENFS_REPORTER_TYPE_LATENCY},
  {ZENFS_ROLL_LATENCY,                ZENFS_REPORTER_TYPE_LATENCY},
  {ZENFS_WRITE_QPS,                   ZENFS_REPORTER_TYPE_QPS},
  {ZENFS_READ_QPS,                    ZENFS_REPORTER_TYPE_QPS},
  {ZENFS_SYNC_QPS,                    ZENFS_REPORTER_TYPE_QPS},
  {ZENFS_IO_ALLOC_QPS,                ZENFS_REPORTER_TYPE_QPS},
  {ZENFS_META_ALLOC_QPS,              ZENFS_REPORTER_TYPE_QPS},
  {ZENFS_ROLL_QPS,                    ZENFS_REPORTER_TYPE_QPS},
  {ZENFS_WRITE_THROUGHPUT,            ZENFS_REPORTER_TYPE_THROUGHPUT},
  {ZENFS_ROLL_THROUGHPUT,             ZENFS_REPORTER_TYPE_THROUGHPUT},
  {ZENFS_ACTIVE_ZONES,                ZENFS_REPORTER_TYPE_GENERAL},
  {ZENFS_OPEN_ZONES,                  ZENFS_REPORTER_TYPE_GENERAL},
  {ZENFS_FREE_SPACE,                  ZENFS_REPORTER_TYPE_GENERAL},
  {ZENFS_USED_SPACE,                  ZENFS_REPORTER_TYPE_GENERAL},
  {ZENFS_RECLAIMABLE_SPACE,           ZENFS_REPORTER_TYPE_GENERAL},
  {ZENFS_RESETABLE_ZONES,             ZENFS_REPORTER_TYPE_GENERAL}
};

#ifdef WITH_ZENFS
struct BDZenFSMetrics : public ZenFSMetrics {
public:
  struct Reporter {
    void* handle_;
    BDZenFSMetricsReporterType type_;
    Reporter(void* handle = nullptr, BDZenFSMetricsReporterType type = ZENFS_REPORTER_TYPE_WITHOUT_CHECK) 
      : handle_(handle), type_(type) {}
    Reporter(const Reporter& r) : handle_(r.handle_), type_(r.type_) {}
    ~Reporter() {}
    HistReporterHandle* GetHistReporterHandle() const { return reinterpret_cast<HistReporterHandle*>(handle_); }
    CountReporterHandle* GetCountReporterHandle() const { return reinterpret_cast<CountReporterHandle*>(handle_); }
  };
private:
  std::string bytedance_tags_;
  std::shared_ptr<CurriedMetricsReporterFactory> factory_;
  std::unordered_map<BDZenFSMetricsHistograms, Reporter> reporter_map_;
private:
  virtual void AddReporter_(BDZenFSMetricsHistograms h, const Reporter& reporter) {
    assert(reporter_map_.find(h) == reporter_map_.end());
    reporter_map_[h] = reporter;
  }
  public:
  virtual void AddReporter(uint32_t label_uint, uint32_t type_uint = 0) override {
    BDZenFSMetricsHistograms label = static_cast<BDZenFSMetricsHistograms>(label_uint);
    BDZenFSMetricsReporterType type = static_cast<BDZenFSMetricsReporterType>(type_uint);
    assert(BDZenFSHistogramsNameMap.find(label) != BDZenFSHistogramsNameMap.end());
    const std::string& name = BDZenFSHistogramsNameMap.find(label)->second;
    switch (type) {
    case ZENFS_REPORTER_TYPE_GENERAL:
    case ZENFS_REPORTER_TYPE_LATENCY: {
      AddReporter_(label, 
        Reporter(factory_->BuildHistReporter(name, bytedance_tags_), type));
    } break;
    case ZENFS_REPORTER_TYPE_QPS:
    case ZENFS_REPORTER_TYPE_THROUGHPUT: {
      AddReporter_(label, 
        Reporter(factory_->BuildCountReporter(name, bytedance_tags_), type));
    } break;
    default: {
      assert(false);
      AddReporter_(label, Reporter(nullptr, type));
    }
    }
  }
  virtual void Report(uint32_t label, size_t value, uint32_t type_check = 0) override {
    auto p = reporter_map_.find(static_cast<BDZenFSMetricsHistograms>(label));
    assert ( p != reporter_map_.end());
    Reporter& r = p->second;
    if (type_check != 0) {
      assert(static_cast<BDZenFSMetricsReporterType>(type_check) == r.type_);
    }
    switch (r.type_) {
    case ZENFS_REPORTER_TYPE_GENERAL:
    case ZENFS_REPORTER_TYPE_LATENCY: {
      r.GetHistReporterHandle()->AddRecord(value);
    } break;
    case ZENFS_REPORTER_TYPE_QPS:
    case ZENFS_REPORTER_TYPE_THROUGHPUT: {
      r.GetCountReporterHandle()->AddCount(value);
    } break;
    default: {
      assert(false);
    }
    }
  }
  void* GetReporter(uint32_t label, uint32_t type_check = 0) {
    auto p = reporter_map_.find(static_cast<BDZenFSMetricsHistograms>(label));
    assert (p != reporter_map_.end());
    Reporter& r = p->second;
    if (type_check != 0) {
      assert(static_cast<BDZenFSMetricsReporterType>(type_check) == r.type_);
    }
    return r.handle_;
  }
public:
  virtual void ReportQPS(uint32_t label, size_t qps) override { 
    Report(label, qps, ZENFS_REPORTER_TYPE_QPS); 
  }
  virtual void ReportLatency(uint32_t label, size_t latency) override {
    Report(label, latency, ZENFS_REPORTER_TYPE_LATENCY);
  }
  virtual void ReportThroughput(uint32_t label, size_t throughput) override {
    Report(label, throughput, ZENFS_REPORTER_TYPE_THROUGHPUT);
  }
  virtual void ReportGeneral(uint32_t label, size_t value) override {
    Report(label, value, ZENFS_REPORTER_TYPE_GENERAL);
  }
 
 public:
  BDZenFSMetrics(std::shared_ptr<MetricsReporterFactory> factory, std::string bytedance_tags, std::shared_ptr<Logger> logger):
    ZenFSMetrics(),
    bytedance_tags_(bytedance_tags),
    factory_(new CurriedMetricsReporterFactory(factory, logger.get(), Env::Default())) {
      for (auto &h : BDZenFSHistogramsTypeMap) 
        AddReporter(static_cast<uint32_t>(h.first), static_cast<uint32_t>(h.second));
    }
  virtual ~BDZenFSMetrics() {}
};
#endif
}