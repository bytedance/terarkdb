// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
// Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "env/io_zenfs.h"
#include "env/zbd_zenfs.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace TERARKDB_NAMESPACE {

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)

class Superblock {
  uint32_t magic_ = 0;
  char uuid_[37] = {0};
  uint32_t sequence_ = 0;
  uint32_t version_ = 0;
  uint32_t flags_ = 0;
  uint32_t block_size_ = 0; /* in bytes */
  uint32_t zone_size_ = 0;  /* in blocks */
  uint32_t nr_zones_ = 0;
  char aux_fs_path_[256] = {0};
  uint32_t finish_treshold_ = 0;
  char reserved_[187] = {0};

 public:
  const uint32_t MAGIC = 0x5a454e46; /* ZENF */
  const uint32_t ENCODED_SIZE = 512;
  const uint32_t CURRENT_VERSION = 1;
  const uint32_t DEFAULT_FLAGS = 0;

  Superblock() {}

  /* Create a superblock for a filesystem covering the entire zoned block device
   */
  Superblock(ZonedBlockDevice* zbd, std::string aux_fs_path = "",
             uint32_t finish_threshold = 0) {
    std::string uuid = Env::Default()->GenerateUniqueId();
    int uuid_len =
        std::min(uuid.length(),
                 sizeof(uuid_) - 1); /* make sure uuid is nullterminated */
    memcpy((void*)uuid_, uuid.c_str(), uuid_len);
    magic_ = MAGIC;
    version_ = CURRENT_VERSION;
    flags_ = DEFAULT_FLAGS;
    finish_treshold_ = finish_threshold;

    block_size_ = zbd->GetBlockSize();
    zone_size_ = zbd->GetZoneSize() / block_size_;
    nr_zones_ = zbd->GetNrZones();

    assert(aux_fs_path.length() < sizeof(aux_fs_path_));
    strcpy(aux_fs_path_, aux_fs_path.c_str());
  }

  Status DecodeFrom(Slice* input) {
    if (input->size() != ENCODED_SIZE) {
      return Status::Corruption("ZenFS Superblock",
                                "Error: Superblock size missmatch");
    }

    GetFixed32(input, &magic_);
    memcpy(&uuid_, input->data(), sizeof(uuid_));
    input->remove_prefix(sizeof(uuid_));
    GetFixed32(input, &sequence_);
    GetFixed32(input, &version_);
    GetFixed32(input, &flags_);
    GetFixed32(input, &block_size_);
    GetFixed32(input, &zone_size_);
    GetFixed32(input, &nr_zones_);
    GetFixed32(input, &finish_treshold_);
    memcpy(&aux_fs_path_, input->data(), sizeof(aux_fs_path_));
    input->remove_prefix(sizeof(aux_fs_path_));
    memcpy(&reserved_, input->data(), sizeof(reserved_));
    input->remove_prefix(sizeof(reserved_));
    assert(input->size() == 0);

    if (magic_ != MAGIC)
      return Status::Corruption("ZenFS Superblock", "Error: Magic missmatch");
    if (version_ != CURRENT_VERSION)
      return Status::Corruption("ZenFS Superblock", "Error: Version missmatch");

    return Status::OK();
  }

  void EncodeTo(std::string* output) {
    sequence_++; /* Ensure that this superblock representation is unique */
    output->clear();
    PutFixed32(output, magic_);
    output->append(uuid_, sizeof(uuid_));
    PutFixed32(output, sequence_);
    PutFixed32(output, version_);
    PutFixed32(output, flags_);
    PutFixed32(output, block_size_);
    PutFixed32(output, zone_size_);
    PutFixed32(output, nr_zones_);
    PutFixed32(output, finish_treshold_);
    output->append(aux_fs_path_, sizeof(aux_fs_path_));
    output->append(reserved_, sizeof(reserved_));
    assert(output->length() == ENCODED_SIZE);
  }

  Status CompatibleWith(ZonedBlockDevice* zbd) {
    if (block_size_ != zbd->GetBlockSize())
      return Status::Corruption("ZenFS Superblock",
                                "Error: block size missmatch");
    if (zone_size_ != (zbd->GetZoneSize() / block_size_))
      return Status::Corruption("ZenFS Superblock",
                                "Error: zone size missmatch");
    if (nr_zones_ > zbd->GetNrZones())
      return Status::Corruption("ZenFS Superblock",
                                "Error: nr of zones missmatch");

    return Status::OK();
  }

  uint32_t GetSeq() { return sequence_; }
  std::string GetAuxFsPath() { return std::string(aux_fs_path_); }
  uint32_t GetFinishTreshold() { return finish_treshold_; }
  std::string GetUUID() { return std::string(uuid_); }
};

class ZenMetaLog {
  uint64_t read_pos_;
  Zone* zone_;
  ZonedBlockDevice* zbd_;
  size_t bs_;

  /* Every meta log record is prefixed with a CRC(32 bits) and record length (32
   * bits) */
  const size_t zMetaHeaderSize = sizeof(uint32_t) * 2;

 public:
  ZenMetaLog(ZonedBlockDevice* zbd, Zone* zone) {
    zbd_ = zbd;
    zone_ = zone;
    zone_->open_for_write_ = true;
    bs_ = zbd_->GetBlockSize();
    read_pos_ = zone->start_;
  }

  virtual ~ZenMetaLog() { zone_->open_for_write_ = false; }

  Status AddRecord(const Slice& slice);
  Status ReadRecord(Slice* record, std::string* scratch);

  Zone* GetZone() { return zone_; };

 private:
  Status Read(Slice* slice);
};

class ZenEnv : public EnvWrapper {
  ZonedBlockDevice* zbd_;
  std::map<std::string, ZoneFile*> files_;
  std::mutex files_mtx_;
  std::shared_ptr<Logger> logger_;
  std::atomic<uint64_t> next_file_id_;

  Zone* cur_meta_zone_ = nullptr;
  std::unique_ptr<ZenMetaLog> meta_log_;
  std::mutex metadata_sync_mtx_;
  std::unique_ptr<Superblock> superblock_;

  std::shared_ptr<Logger> GetLogger() { return logger_; }

  struct MetadataWriter : public ZonedWritableFile::MetadataWriter {
    ZenEnv* zenEnv;
    Status Persist(ZoneFile* zoneFile) {
      Debug(zenEnv->GetLogger(), "Syncing metadata for: %s",
            zoneFile->GetFilename().c_str());
      return zenEnv->SyncFileMetadata(zoneFile);
    }
  };

  MetadataWriter metadata_writer_;

  enum ZenFSTag : uint32_t {
    kCompleteFilesSnapshot = 1,
    kFileUpdate = 2,
    kFileDeletion = 3,
    kEndRecord = 4,
  };

  void LogFiles();
  void ClearFiles();
  Status WriteSnapshot(ZenMetaLog* meta_log);
  Status WriteEndRecord(ZenMetaLog* meta_log);
  Status RollMetaZone();
  Status PersistSnapshot(ZenMetaLog* meta_writer);
  Status PersistRecord(std::string record);
  Status SyncFileMetadata(ZoneFile* zoneFile);

  void EncodeSnapshotTo(std::string* output);
  void EncodeFileDeletionTo(ZoneFile* zoneFile, std::string* output);

  Status DecodeSnapshotFrom(Slice* input);
  Status DecodeFileUpdateFrom(Slice* slice);
  Status DecodeFileDeletionFrom(Slice* slice);

  Status RecoverFrom(ZenMetaLog* log);

  std::string ToAuxPath(std::string path) {
    return superblock_->GetAuxFsPath() + path;
  }

  std::string ToZenFSPath(std::string aux_path) {
    std::string path = aux_path;
    path.erase(0, superblock_->GetAuxFsPath().length());
    return path;
  }

  ZoneFile* GetFile(std::string fname);
  Status DeleteFile_Internal(std::string fname);

 public:
  explicit ZenEnv(ZonedBlockDevice* zbd, Env* env,
                  std::shared_ptr<Logger> logger);
  ~ZenEnv();

  Status Mount();
  Status MkFS(std::string aux_fs_path, uint32_t finish_threshold);

  virtual Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) override;

  virtual Status NewRandomAccessFile(
      const std::string& fname,
      std::unique_ptr<RandomAccessFile>* result,
      const EnvOptions& options) override;

  virtual Status NewWritableFile(const std::string& fname,
                                   std::unique_ptr<WritableFile>* result,
                                   const EnvOptions& options) override;

  virtual Status ReuseWritableFile(const std::string& fname,
                                     const std::string& old_fname,
                                     std::unique_ptr<WritableFile>* result,
                                     const EnvOptions& options) override;

  virtual Status FileExists(const std::string& fname) override;

  virtual Status GetChildren(const std::string& dir,
                               std::vector<std::string>* result) override;

  virtual Status DeleteFile(const std::string& fname) override;

  Status GetFileSize(const std::string& f, uint64_t* size) override;

  Status RenameFile(const std::string& f, const std::string& t) override;

  Status GetFreeSpace(const std::string& /*path*/, uint64_t* diskfree) override {
    *diskfree = zbd_->GetFreeSpace();
    return Status::OK();
  }

  // The directory structure is stored in the aux file system

  Status NewDirectory(const std::string& name,
                        std::unique_ptr<Directory>* result) override {
    Debug(logger_, "NewDirectory: %s to aux: %s\n", name.c_str(),
          ToAuxPath(name).c_str());
    return target()->NewDirectory(ToAuxPath(name), result);
  }

  Status CreateDir(const std::string& d) override {
    Debug(logger_, "CreatDir: %s to aux: %s\n", d.c_str(),
          ToAuxPath(d).c_str());
    return target()->CreateDir(ToAuxPath(d));
  }

  Status CreateDirIfMissing(const std::string& d) override {
    Debug(logger_, "CreatDirIfMissing: %s to aux: %s\n", d.c_str(),
          ToAuxPath(d).c_str());
    return target()->CreateDirIfMissing(ToAuxPath(d));
  }

  Status DeleteDir(const std::string& d) override {
    std::vector<std::string> children;
    Status s;

    Debug(logger_, "DeleteDir: %s aux: %s\n", d.c_str(), ToAuxPath(d).c_str());

    s = GetChildren(d, &children);
    if (children.size() != 0)
      return Status::IOError("Directory has children");

    return target()->DeleteDir(ToAuxPath(d));
  }

  // We might want to override these in the future
  Status GetAbsolutePath(const std::string& db_path,
                           std::string* output_path) override {
    return target()->GetAbsolutePath(db_path, output_path);
  }

  Status LockFile(const std::string& f, FileLock** l) override {
    return target()->LockFile(ToAuxPath(f), l);
  }

  Status UnlockFile(FileLock* l) override {
    return target()->UnlockFile(l);
  }

  Status GetTestDirectory(std::string* path) override {
    *path = "rocksdbtest";
    Debug(logger_, "GetTestDirectory: %s aux: %s\n", path->c_str(),
          ToAuxPath(*path).c_str());
    return target()->CreateDirIfMissing(ToAuxPath(*path));
  }

  Status NewLogger(const std::string& fname,
                     std::shared_ptr<Logger>* result) override {
    return target()->NewLogger(ToAuxPath(fname), result);
  }

  // Not supported (at least not yet)
  Status Truncate(const std::string& /*fname*/, size_t /*size*/) override {
    return Status::NotSupported("Truncate is not implemented in ZenFS");
  }

  virtual Status ReopenWritableFile(const std::string& fname,
                                    std::unique_ptr<WritableFile>* result,
                                    const EnvOptions& opts) {
    return target()->NewWritableFile(fname, result, opts);
  }

  virtual Status NewRandomRWFile(const std::string& /*fname*/,
                                 std::unique_ptr<RandomRWFile>* /*result*/,
                                 const EnvOptions& ) override {
    return Status::NotSupported("RandomRWFile is not implemented in ZenFS");
  }

  virtual Status NewMemoryMappedFileBuffer(
      const std::string& /*fname*/,
      std::unique_ptr<MemoryMappedFileBuffer>* /*result*/) override {
    return Status::NotSupported(
        "MemoryMappedFileBuffer is not implemented in ZenFS");
  }

  Status GetFileModificationTime(const std::string& /*fname*/,
                                   uint64_t* /*file_mtime*/) override {
    return Status::NotSupported(
        "GetFileModificationTime is not implemented in ZenFS");
  }

  virtual Status LinkFile(const std::string& /*src*/,
                            const std::string& /*target*/) {
    return Status::NotSupported("LinkFile is not supported in ZenFS");
  }

  virtual Status NumFileLinks(const std::string& /*fname*/,
                                uint64_t* /*count*/) {
    return Status::NotSupported(
        "Getting number of file links is not supported in ZenFS");
  }

  virtual Status AreFilesSame(const std::string& /*first*/,
                                const std::string& /*second*/, bool* /*res*/) {
    return Status::NotSupported("AreFilesSame is not supported in ZenFS");
  }
};
#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)

std::map<std::string, std::string> ListZenFileSystems();

} // namespace_TERARKDB_NAMESPACE
