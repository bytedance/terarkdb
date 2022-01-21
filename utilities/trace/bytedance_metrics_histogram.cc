#include "rocksdb/terark_namespace.h"
#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(WITH_ZENFS)

#include "bytedance_metrics_histogram.h"
#include "third-party/zenfs/fs/snapshot.h"
namespace ROCKSDB_NAMESPACE {

const uint32_t TYPE_LATENCY = ZENFS_REPORTER_TYPE_LATENCY;
const uint32_t TYPE_THROUGHPUT = ZENFS_REPORTER_TYPE_THROUGHPUT;
const uint32_t TYPE_QPS = ZENFS_REPORTER_TYPE_QPS;
const uint32_t TYPE_GENERAL = ZENFS_REPORTER_TYPE_GENERAL;

const std::unordered_map<uint32_t, std::pair<std::string, uint32_t>>
    BDZenFSHistMap = {
        {ZENFS_READ_LATENCY, {"zenfs_read_latency", TYPE_LATENCY}},
        {ZENFS_WRITE_LATENCY, {"zenfs_write_latency", TYPE_LATENCY}},
        {ZENFS_WAL_WRITE_LATENCY, {"zenfs_fg_write_latency", TYPE_LATENCY}},
        {ZENFS_NON_WAL_WRITE_LATENCY, {"zenfs_bg_write_latency", TYPE_LATENCY}},
        {ZENFS_SYNC_LATENCY, {"zenfs_sync_latency", TYPE_LATENCY}},
        {ZENFS_WAL_SYNC_LATENCY, {"fg_zenfs_sync_latency", TYPE_LATENCY}},
        {ZENFS_NON_WAL_SYNC_LATENCY, {"bg_zenfs_sync_latency", TYPE_LATENCY}},

        {ZENFS_IO_ALLOC_LATENCY, {"zenfs_io_alloc_latency", TYPE_LATENCY}},
        {ZENFS_WAL_IO_ALLOC_LATENCY,
         {"zenfs_io_alloc_wal_latency", TYPE_LATENCY}},
        {ZENFS_NON_WAL_IO_ALLOC_LATENCY,
         {"zenfs_io_alloc_non_wal_latency", TYPE_LATENCY}},

        {ZENFS_META_ALLOC_LATENCY, {"zenfs_meta_alloc_latency", TYPE_LATENCY}},
        {ZENFS_META_SYNC_LATENCY,
         {"zenfs_metadata_sync_latency", TYPE_LATENCY}},

        {ZENFS_ROLL_LATENCY, {"zenfs_roll_latency", TYPE_LATENCY}},
        {ZENFS_WRITE_QPS, {"zenfs_write_qps", TYPE_QPS}},
        {ZENFS_READ_QPS, {"zenfs_read_qps", TYPE_QPS}},
        {ZENFS_SYNC_QPS, {"zenfs_sync_qps", TYPE_QPS}},
        {ZENFS_IO_ALLOC_QPS, {"zenfs_io_alloc_qps", TYPE_QPS}},
        {ZENFS_META_ALLOC_QPS, {"zenfs_meta_alloc_qps", TYPE_QPS}},
        {ZENFS_ROLL_QPS, {"zenfs_roll_qps", TYPE_QPS}},
        {ZENFS_WRITE_THROUGHPUT, {"zenfs_write_throughput", TYPE_THROUGHPUT}},
        {ZENFS_ROLL_THROUGHPUT, {"zenfs_roll_throughput", TYPE_THROUGHPUT}},
        {ZENFS_FREE_SPACE_SIZE, {"zenfs_free_space", TYPE_GENERAL}},
        {ZENFS_USED_SPACE_SIZE, {"zenfs_used_space", TYPE_GENERAL}},
        {ZENFS_RECLAIMABLE_SPACE_SIZE,
         {"zenfs_reclaimable_space", TYPE_GENERAL}},
        {ZENFS_ACTIVE_ZONES_COUNT, {"zenfs_active_zones", TYPE_GENERAL}},
        {ZENFS_OPEN_ZONES_COUNT, {"zenfs_open_zones", TYPE_GENERAL}},
        {ZENFS_RESETABLE_ZONES_COUNT, {"zenfs_resetable_zones", TYPE_GENERAL}},
        {ZENFS_ZONE_WRITE_THROUGHPUT,
         {"zenfs_zone_write_throughput", TYPE_THROUGHPUT}}};

void BDZenFSMetrics::AddReporter(uint32_t label, uint32_t type) {
  assert(BDZenFSHistMap.find(label) != BDZenFSHistMap.end());
  const std::string& name = BDZenFSHistMap.find(label)->second.first;
  switch (type) {
    case TYPE_GENERAL:
    case TYPE_LATENCY: {
      AddReporter_(label,
                   Reporter(factory_->BuildHistReporter(name, tag_), type));
    } break;
    case TYPE_QPS:
    case TYPE_THROUGHPUT: {
      AddReporter_(label,
                   Reporter(factory_->BuildCountReporter(name, tag_), type));
    } break;
    default: {
      assert(false);
      AddReporter_(label, Reporter(nullptr, type));
    }
  }
}

void BDZenFSMetrics::Report(uint32_t label, size_t value, uint32_t type_check) {
  auto p = reporter_map_.find(static_cast<BDZenFSMetricsHistograms>(label));
  assert(p != reporter_map_.end());
  Reporter& r = p->second;
  if (type_check != 0) {
    assert(static_cast<BDZenFSMetricsReporterType>(type_check) == r.type_);
  }
  switch (r.type_) {
    case TYPE_GENERAL:
    case TYPE_LATENCY: {
      r.GetHistReporterHandle()->AddRecord(value);
    } break;
    case TYPE_QPS:
    case TYPE_THROUGHPUT: {
      r.GetCountReporterHandle()->AddCount(value);
    } break;
    default: {
      assert(false);
    }
  }
}

void* BDZenFSMetrics::GetReporter(uint32_t label, uint32_t type_check) {
  auto p = reporter_map_.find(static_cast<BDZenFSMetricsHistograms>(label));
  assert(p != reporter_map_.end());
  Reporter& r = p->second;
  if (type_check != 0) {
    assert(static_cast<BDZenFSMetricsReporterType>(type_check) == r.type_);
  }
  return r.handle_;
}

void BDZenFSMetrics::ReportQPS(uint32_t label, size_t qps) {
  Report(label, qps, TYPE_QPS);
}
void BDZenFSMetrics::ReportLatency(uint32_t label, size_t latency) {
  Report(label, latency, TYPE_LATENCY);
}
void BDZenFSMetrics::ReportThroughput(uint32_t label, size_t throughput) {
  Report(label, throughput, TYPE_THROUGHPUT);
}
void BDZenFSMetrics::ReportGeneral(uint32_t label, size_t value) {
  Report(label, value, TYPE_GENERAL);
}

void BDZenFSMetrics::ReportSnapshot(const ZenFSSnapshot& snapshot) {
  ReportGeneral(ZENFS_FREE_SPACE_SIZE, snapshot.zbd_.free_space >> 30);
  ReportGeneral(ZENFS_USED_SPACE_SIZE, snapshot.zbd_.used_space >> 30);
  ReportGeneral(ZENFS_RECLAIMABLE_SPACE_SIZE,
                snapshot.zbd_.reclaimable_space >> 30);
}

}  // namespace ROCKSDB_NAMESPACE

#endif
