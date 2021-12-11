#pragma once

#include "rocksdb/terark_namespace.h"
#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(WITH_ZENFS)

#include <iostream>
#include <algorithm>
#include <string>
#include <sstream>
#include <vector>

#include "fs/snapshot.h"
namespace ROCKSDB_NAMESPACE {

class BDZoneFileStat {
 public:
  uint64_t file_id;
  uint64_t size_in_zone;
  std::string filename;
};

class BDZoneStat {
public:
  //uint64_t total_capacity;
  uint64_t free_capacity;
  uint64_t used_capacity;
  uint64_t reclaim_capacity;

  //uint64_t write_position;
  uint64_t start_position;
  
  std::vector<BDZoneFileStat> files;
  BDZoneStat(const ZoneSnapshot& z):
    //total_capacity(zs.MaxCapacity()),
    free_capacity(z.capacity),
    used_capacity(z.used_capacity),
    reclaim_capacity(z.max_capacity - z.used_capacity - z.capacity),
    //write_position(zs.WritePosition()),
    start_position(z.start) {}

  ~BDZoneStat() = default;

  double GarbageRate() const {
    uint64_t total = reclaim_capacity + used_capacity + free_capacity;
    return double(reclaim_capacity) / total;
  }

  uint64_t FakeID() const { return start_position; }

  std::string ToString() const {
    std::ostringstream msg;
    msg << "Free/Used/Reclaim Capacity = " << free_capacity << "/" << used_capacity << "/" << reclaim_capacity << ";"
        << "StartPosition = " << start_position << ";";
    return msg.str();
  }
};

class BDZenFSStat {
 public:
  ZenFSSnapshot snapshot_;

  std::vector<BDZoneStat> zone_stats_;

 public:
  BDZenFSStat() {}

  /*
  BDZenFSStat(ZenFSSnapshot&& snapshot, const ZenFSSnapshotOptions& options) {
    SetStat(snapshot, options);
  }
  */

  std::string ToString() const {
    std::ostringstream msg;
    for (const auto& zone_stat : zone_stats_) 
      msg << "\"" << zone_stat.FakeID() << "\":[" << zone_stat.ToString() << "]\n";
    return msg.str();
  }

  bool SetStat(ZenFSSnapshot& snapshot, const ZenFSSnapshotOptions& options) {
    snapshot_ = std::move(snapshot);

    // Store size of each file_id in each zone
    std::map<uint64_t, std::map<uint64_t, uint64_t>> sizes;
    // Store file_id to filename map
    std::map<uint64_t, std::string> filenames;
    
    for (auto& file : snapshot_.zone_files_) {
      uint64_t file_id = file.file_id;
      filenames[file_id] = file.filename;
      for (const ZoneExtentSnapshot& extent : file.extents) {
        sizes[extent.zone_start][file_id] += extent.length;
      }
    }

    for (const auto& zone : snapshot_.zones_) {
      zone_stats_.emplace_back(zone);
    }

    // sort by trash descending order
    std::sort(zone_stats_.begin(), zone_stats_.end(), [](const BDZoneStat &l, const BDZoneStat &r) {
      return l.free_capacity < r.free_capacity ||
            (l.free_capacity == r.free_capacity && l.used_capacity < r.used_capacity);
    });

    for (auto& bd_zone : zone_stats_) {
      std::map<uint64_t, uint64_t>& zone_files = sizes[bd_zone.FakeID()];
      
      for (auto& file : zone_files) {
        uint64_t file_id = file.first;
        uint64_t file_length = file.second;
        BDZoneFileStat file_stat;
        file_stat.file_id = file_id;
        file_stat.size_in_zone = file_length;
        file_stat.filename = filenames[file_id];
        bd_zone.files.emplace_back(std::move(file_stat));
      }
    }
    assert(zone_stats_.size() == snapshot_.zones_.size());
    return true;
  }
};


}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
