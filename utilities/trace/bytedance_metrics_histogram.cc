#include "rocksdb/terark_namespace.h"
#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(WITH_ZENFS)

#include "bytedance_metrics_histogram.h"
#include "third-party/zenfs/fs/snapshot.h"
namespace ROCKSDB_NAMESPACE {

void BDZenFSMetrics::ReportSnapshot(const ZenFSSnapshot& snapshot, const ZenFSSnapshotOptions& options) {
  if (options.zbd_.get_free_space_)
    ReportGeneral(ZENFS_LABEL(FREE_SPACE,SIZE), snapshot.zbd_.GetFreeSpace() >> 30);
  if (options.zbd_.get_used_space_)
    ReportGeneral(ZENFS_LABEL(USED_SPACE,SIZE), snapshot.zbd_.GetUsedSpace() >> 30);
  if (options.zbd_.get_reclaimable_space_)
    ReportGeneral(ZENFS_LABEL(RECLAIMABLE_SPACE,SIZE), snapshot.zbd_.GetReclaimableSpace() >> 30);
  // and more
}
}

#endif
