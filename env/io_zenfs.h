// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/file_system.h"
#include "zbd_zenfs.h"

namespace ROCKSDB_NAMESPACE {

class ZoneExtent {
 public:
  uint64_t start_;
  uint32_t length_;
  Zone* zone_;

  explicit ZoneExtent(uint64_t start, uint32_t length, Zone* zone);
  Status DecodeFrom(Slice* input);
  void EncodeTo(std::string* output);
};

class ZoneFile {
 protected:
  ZonedBlockDevice* zbd_;
  std::vector<ZoneExtent*> extents_;
  Zone* active_zone_;
  uint64_t extent_start_;
  uint64_t extent_filepos_;

  Env::WriteLifeTimeHint lifetime_;
  uint64_t fileSize;
  std::string filename_;
  uint64_t file_id_;

  uint32_t nr_synced_extents_;

 public:
  explicit ZoneFile(ZonedBlockDevice* zbd, std::string filename,
                    uint64_t file_id_);

  virtual ~ZoneFile();

  void CloseWR();
  Status Append(void* data, int data_size, int valid_size);
  Status SetWriteLifeTimeHint(Env::WriteLifeTimeHint lifetime);
  std::string GetFilename();
  void Rename(std::string name);
  uint64_t GetFileSize();
  void SetFileSize(uint64_t sz);

  uint32_t GetBlockSize() { return zbd_->GetBlockSize(); }
  std::vector<ZoneExtent*> GetExtents() { return extents_; }
  Env::WriteLifeTimeHint GetWriteLifeTimeHint() { return lifetime_; }

  Status PositionedRead(uint64_t offset, size_t n, Slice* result,
                          char* scratch, bool direct);
  ZoneExtent* GetExtent(uint64_t file_offset, uint64_t* dev_offset);
  void PushExtent();

  void EncodeTo(std::string* output, uint32_t extent_start);
  void EncodeUpdateTo(std::string* output) {
    EncodeTo(output, nr_synced_extents_);
  };
  void EncodeSnapshotTo(std::string* output) { EncodeTo(output, 0); };
  void MetadataSynced() { nr_synced_extents_ = extents_.size(); };

  Status DecodeFrom(Slice* input);
  Status MergeUpdate(ZoneFile* update);

  uint64_t GetID() { return file_id_; }
  size_t GetUniqueId(char* id, size_t max_size);
};

class ZonedWritableFile : public FSWritableFile {
 public:
  /* Interface for persisting metadata for files */
  class MetadataWriter {
   public:
    virtual ~MetadataWriter();
    virtual Status Persist(ZoneFile* zoneFile) = 0;
  };

  explicit ZonedWritableFile(ZonedBlockDevice* zbd, bool buffered,
                             ZoneFile* zoneFile,
                             MetadataWriter* metadata_writer = nullptr);
  virtual ~ZonedWritableFile();

  virtual Status Append(const Slice& data, const IOOptions& options,
                          IODebugContext* dbg) override;
  virtual Status Append(const Slice& data, const IOOptions& opts,
                          const DataVerificationInfo& /* verification_info */,
                          IODebugContext* dbg) override {
    return Append(data, opts, dbg);
  }
  virtual Status PositionedAppend(const Slice& data, uint64_t offset,
                                    const IOOptions& options,
                                    IODebugContext* dbg) override;
  virtual Status PositionedAppend(
      const Slice& data, uint64_t offset, const IOOptions& opts,
      const DataVerificationInfo& /* verification_info */,
      IODebugContext* dbg) override {
    return PositionedAppend(data, offset, opts, dbg);
  }
  virtual Status Truncate(uint64_t size, const IOOptions& options,
                            IODebugContext* dbg) override;
  virtual Status Close(const IOOptions& options,
                         IODebugContext* dbg) override;
  virtual Status Flush(const IOOptions& options,
                         IODebugContext* dbg) override;
  virtual Status Sync(const IOOptions& options, IODebugContext* dbg) override;
  virtual Status RangeSync(uint64_t offset, uint64_t nbytes,
                             const IOOptions& options,
                             IODebugContext* dbg) override;
  virtual Status Fsync(const IOOptions& options,
                         IODebugContext* dbg) override;
  bool use_direct_io() const override { return !buffered; }
  bool IsSyncThreadSafe() const override { return true; };
  size_t GetRequiredBufferAlignment() const override {
    return zoneFile_->GetBlockSize();
  }
  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override;

 private:
  Status BufferedWrite(const Slice& data);
  Status FlushBuffer();

  bool buffered;
  char* buffer;
  size_t buffer_sz;
  uint32_t block_sz;
  uint32_t buffer_pos;
  uint64_t wp;
  int write_temp;

  ZoneFile* zoneFile_;
  MetadataWriter* metadata_writer_;

  std::mutex buffer_mtx_;
};

class ZonedSequentialFile : public FSSequentialFile {
 private:
  ZoneFile* zoneFile_;
  uint64_t rp;
  bool direct_;

 public:
  explicit ZonedSequentialFile(ZoneFile* zoneFile, const FileOptions& file_opts)
      : zoneFile_(zoneFile), rp(0), direct_(file_opts.use_direct_reads) {}

  Status Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;
  Status PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override;
  Status Skip(uint64_t n);

  bool use_direct_io() const override { /*return target_->use_direct_io(); */
    return true;
  }

  size_t GetRequiredBufferAlignment() const override {
    return zoneFile_->GetBlockSize();
  }

  Status InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
    return Status::OK();
  }
};

class ZonedRandomAccessFile : public FSRandomAccessFile {
 private:
  ZoneFile* zoneFile_;
  bool direct_;

 public:
  explicit ZonedRandomAccessFile(ZoneFile* zoneFile,
                                 const FileOptions& file_opts)
      : zoneFile_(zoneFile), direct_(file_opts.use_direct_reads) {}

  Status Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  Status MultiRead(FSReadRequest* /*reqs*/, size_t /*num_reqs*/,
                     const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override {
    return Status::IOError("Not implemented");
  }

  Status Prefetch(uint64_t /*offset*/, size_t /*n*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return Status::OK();
  }

  bool use_direct_io() const override { return true; }

  size_t GetRequiredBufferAlignment() const override {
    return zoneFile_->GetBlockSize();
  }

  Status InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
    return Status::OK();
  }

  size_t GetUniqueId(char* id, size_t max_size) const override;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)

