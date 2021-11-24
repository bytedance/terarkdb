#pragma once

#include "rocksdb/terark_namespace.h"
#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(WITH_ZENFS)

#include <string>
#include <vector>
#include "third-party/zenfs/fs/snapshot.h"
namespace ROCKSDB_NAMESPACE {

class BDZoneFileStat {
 public:
  uint64_t file_id;
  uint64_t size_in_zone;
  std::string filename;
};

class BDZoneStat {
public:
  uint64_t total_capacity;
  uint64_t write_position;
  uint64_t start_position;
  std::vector<BDZoneFileStat> files;
  BDZoneStat(const ZoneSnapshot& zs):
    total_capacity(zs.MaxCapacity()),
    write_position(zs.WritePosition()),
    start_position(zs.StartPosition()) {}
  ~BDZoneStat() {}
  uint64_t FakeID() { return start_position; }
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
