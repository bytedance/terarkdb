// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
// Copyright (c) 2021-present, Bytedance Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)

#include <errno.h>
#include <libzbd/zbd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace TERARKDB_NAMESPACE {

class ZonedBlockDevice;

struct zenfs_aio_ctx {
  struct iocb iocb;
  struct iocb *iocbs[1];
  io_context_t io_ctx;
  int inflight;
  int fd;
};

class Zone {
  ZonedBlockDevice *zbd_;

 public:
  explicit Zone(ZonedBlockDevice *zbd, struct zbd_zone *z);

  uint64_t start_;
  uint64_t capacity_; /* remaining capacity */
  uint64_t max_capacity_;
  uint64_t wp_;
  bool open_for_write_;
  Env::WriteLifeTimeHint lifetime_;
  std::atomic<long> used_capacity_;
  struct zenfs_aio_ctx wr_ctx;

  Status Reset();
  Status Finish();
  Status Close();

  Status Append(char *data, uint32_t size);
  bool IsUsed();
  bool IsFull();
  bool IsEmpty();
  uint64_t GetZoneNr();
  uint64_t GetCapacityLeft();

  void CloseWR(); /* Done writing */
};

class ZonedBlockDevice {
 private:
  std::string filename_;
  uint32_t block_sz_;
  uint32_t zone_sz_;
  uint32_t nr_zones_;
  std::vector<Zone *> io_zones;
  std::mutex io_zones_mtx;
  std::vector<Zone *> meta_zones;
  int read_f_;
  int read_direct_f_;
  int write_f_;
  time_t start_time_;
  std::shared_ptr<Logger> logger_;
  uint32_t finish_threshold_ = 0;

  std::atomic<long> active_io_zones_;
  std::atomic<long> open_io_zones_;
  std::condition_variable zone_resources_;
  std::mutex zone_resources_mtx_; /* Protects active/open io zones */

  unsigned int max_nr_active_io_zones_;
  unsigned int max_nr_open_io_zones_;

 public:
  explicit ZonedBlockDevice(std::string bdevname,
                            std::shared_ptr<Logger> logger);
  virtual ~ZonedBlockDevice();

  Status Open(bool readonly = false);

  Zone *GetIOZone(uint64_t offset);

  Zone *AllocateZone(Env::WriteLifeTimeHint lifetime);
  Zone *AllocateMetaZone();

  uint64_t GetFreeSpace();
  uint64_t GetUsedSpace();
  uint64_t GetReclaimableSpace();

  std::string GetFilename();
  uint32_t GetBlockSize();

  void ResetUnusedIOZones();
  void LogZoneStats();
  void LogZoneUsage();

  int GetReadFD() { return read_f_; }
  int GetReadDirectFD() { return read_direct_f_; }
  int GetWriteFD() { return write_f_; }

  uint32_t GetZoneSize() { return zone_sz_; }
  uint32_t GetNrZones() { return nr_zones_; }
  uint32_t GetMaxActiveZones() { return max_nr_active_io_zones_ + 1; };
  uint32_t GetMaxOpenZones() { return max_nr_open_io_zones_ + 1; };

  std::vector<Zone *> GetMetaZones() { return meta_zones; }

  void SetFinishTreshold(uint32_t threshold) { finish_threshold_ = threshold; }

  bool SetMaxActiveZones(uint32_t max_active) {
    if (max_active == 0) /* No limit */
      return true;
    if (max_active <= GetMaxActiveZones()) {
      max_nr_active_io_zones_ = max_active - 1;
      return true;
    } else {
      return false;
    }
  }
  
  bool SetMaxOpenZones(uint32_t max_open) {
    if (max_open == 0) /* No limit */
      return true;
    if (max_open <= GetMaxOpenZones()) {
      max_nr_open_io_zones_ = max_open - 1;
      return true;
    } else {
      return false;
    }
  }

  void NotifyIOZoneFull();
  void NotifyIOZoneClosed();

 private:
  std::string ErrorToString(int err);
};

}  // namespace TERARKDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)
