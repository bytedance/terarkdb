#pragma once

#include "rocksdb/terark_namespace.h"
#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(WITH_ZENFS)

#include <string>
#include <sstream>
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
  //uint64_t total_capacity;
  uint64_t free_capacity;
  uint64_t used_capacity;
  uint64_t reclaim_capacity;

  //uint64_t write_position;
  uint64_t start_position;
  
  
  std::vector<BDZoneFileStat> files;
  BDZoneStat(const ZoneSnapshot& zs):
    //total_capacity(zs.MaxCapacity()),
    free_capacity(zs.RemainingCapacity()),
    used_capacity(zs.UsedCapacity()),
    reclaim_capacity(zs.MaxCapacity() - zs.UsedCapacity()),
    //write_position(zs.WritePosition()),
    start_position(zs.StartPosition()) {}
  ~BDZoneStat() {}
  uint64_t FakeID() const { return start_position; }
  std::string ToString() const {
    std::ostringstream msg;
    msg << "Free/Used/Reclaim Capacity = " << free_capacity << "/" << used_capacity << "/" << reclaim_capacity << ";"
        << "StartPosition" << start_position << ";";
    return msg.str();
  }
};

struct BDZenFSStat {
 public:
  std::vector<BDZoneStat> zone_stats_;
 public:
  BDZenFSStat() {}
  BDZenFSStat(const ZenFSSnapshot& snapshot, const ZenFSSnapshotOptions& options) {
    SetStat(snapshot, options);
  }
  std::string ToString() const {
    std::ostringstream msg;
    for (const auto& zone_stat : zone_stats_) 
      msg << "\"" << zone_stat.FakeID() << "\":[" << zone_stat.ToString() << "]\n";
    return msg.str();
  }
  bool SetStat(const ZenFSSnapshot& snapshot, const ZenFSSnapshotOptions& options) {
    std::vector<BDZoneStat>& stat = zone_stats_;
    const std::vector<ZoneSnapshot>& zones = snapshot.zones_;
    const std::vector<ZoneFileSnapshot>& zone_files = snapshot.zone_files_;
    
    // Store size of each file_id in each zone
    std::map<uint64_t, std::map<uint64_t, uint64_t>> sizes;
    // Store file_id to filename map
    std::map<uint64_t, std::string> filenames;
    
    for (auto& file : zone_files) {
      uint64_t file_id = file.FileID();
      filenames[file_id] = file.Filename();
      for (const ZoneExtentSnapshot& extent : file.Extent()) 
        sizes[extent.ZoneID()][file_id] += extent.Length();
    }

    for (const auto& zone : zones) 
      stat.emplace_back(zone);
    // sort by trash descending order
    std::sort(stat.begin(), stat.end(), [](const BDZoneStat &l, const BDZoneStat &r) {
      return l.free_capacity < r.free_capacity ||
            (l.free_capacity == r.free_capacity && l.used_capacity < r.used_capacity);
    });

    for (auto& bd_zone : stat) {
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
    assert(stat.size() == zones.size());
    return 1;
  }
};


}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
