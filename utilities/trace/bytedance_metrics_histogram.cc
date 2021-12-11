#include "rocksdb/terark_namespace.h"
#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(WITH_ZENFS)

#include "bytedance_metrics_histogram.h"
#include "third-party/zenfs/fs/snapshot.h"
namespace ROCKSDB_NAMESPACE {

void BDZenFSMetrics::ReportSnapshot(const ZenFSSnapshot& snapshot) {
    ReportGeneral(ZENFS_LABEL(FREE_SPACE,SIZE), snapshot.zbd_.free_space >> 30);
    ReportGeneral(ZENFS_LABEL(USED_SPACE,SIZE), snapshot.zbd_.used_space >> 30);
    ReportGeneral(ZENFS_LABEL(RECLAIMABLE_SPACE,SIZE), snapshot.zbd_.reclaimable_space >> 30);
}
}

#endif
