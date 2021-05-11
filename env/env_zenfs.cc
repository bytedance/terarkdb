// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
// Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)

#include "env_zenfs.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <utility>
#include <vector>

#include "rocksdb/utilities/object_registry.h"
#include "util/crc32c.h"

#define DEFAULT_ZENV_LOG_PATH "/tmp/"

namespace TERARKDB_NAMESPACE {

Status Superblock::DecodeFrom(Slice* input) {
  if (input->size() != ENCODED_SIZE) {
    return Status::Corruption("ZenEnv Superblock",
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
  GetFixed32(input, &max_active_limit_);
  GetFixed32(input, &max_open_limit_);
  memcpy(&reserved_, input->data(), sizeof(reserved_));
  input->remove_prefix(sizeof(reserved_));
  assert(input->size() == 0);

  if (magic_ != MAGIC)
    return Status::Corruption("ZenEnv Superblock", "Error: Magic missmatch");
  if (version_ != CURRENT_VERSION)
    return Status::Corruption("ZenEnv Superblock", "Error: Version missmatch");

  return Status::OK();
}

void Superblock::EncodeTo(std::string* output) {
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
  PutFixed32(output, max_active_limit_);
  PutFixed32(output, max_open_limit_);
  output->append(reserved_, sizeof(reserved_));
  assert(output->length() == ENCODED_SIZE);
}

Status Superblock::CompatibleWith(ZonedBlockDevice* zbd) {
  if (block_size_ != zbd->GetBlockSize())
    return Status::Corruption("ZenEnv Superblock",
                              "Error: block size missmatch");
  if (zone_size_ != (zbd->GetZoneSize() / block_size_))
    return Status::Corruption("ZenEnv Superblock",
                              "Error: zone size missmatch");
  if (nr_zones_ > zbd->GetNrZones())
    return Status::Corruption("ZenEnv Superblock",
                              "Error: nr of zones missmatch");
  if (max_active_limit_ > zbd->GetMaxActiveZones())
    return Status::Corruption("ZenEnv Superblock",
                              "Error: active zone limit missmatch");
  if (max_open_limit_ > zbd->GetMaxOpenZones())
    return Status::Corruption("ZenEnv Superblock",
                              "Error: open zone limit missmatch");

  return Status::OK();
}

Status ZenMetaLog::AddRecord(const Slice& slice) {
  uint32_t record_sz = slice.size();
  const char* data = slice.data();
  size_t phys_sz;
  uint32_t crc = 0;
  char* buffer;
  int ret;
  Status s;

  phys_sz = record_sz + zMetaHeaderSize;

  if (phys_sz % bs_) phys_sz += bs_ - phys_sz % bs_;

  assert(data != nullptr);
  assert((phys_sz % bs_) == 0);

  ret = posix_memalign((void**)&buffer, bs_, phys_sz);
  if (ret) return Status::IOError("Failed to allocate memory");

  memset(buffer, 0, phys_sz);

  crc = crc32c::Extend(crc, (const char*)&record_sz, sizeof(uint32_t));
  crc = crc32c::Extend(crc, data, record_sz);
  crc = crc32c::Mask(crc);

  EncodeFixed32(buffer, crc);
  EncodeFixed32(buffer + sizeof(uint32_t), record_sz);
  memcpy(buffer + sizeof(uint32_t) * 2, data, record_sz);

  s = zone_->Append(buffer, phys_sz);

  free(buffer);
  return s;
}

Status ZenMetaLog::Read(Slice* slice) {
  int f = zbd_->GetReadFD();
  const char* data = slice->data();
  size_t read = 0;
  size_t to_read = slice->size();
  int ret;

  if (read_pos_ >= zone_->wp_) {
    // EOF
    slice->clear();
    return Status::OK();
  }

  if ((read_pos_ + to_read) > (zone_->start_ + zone_->max_capacity_)) {
    return Status::IOError("Read across zone");
  }

  while (read < to_read) {
    ret = pread(f, (void*)(data + read), to_read - read, read_pos_);

    if (ret == -1 && errno == EINTR) continue;
    if (ret < 0) return Status::IOError("Read failed");

    read += ret;
    read_pos_ += ret;
  }

  return Status::OK();
}

Status ZenMetaLog::ReadRecord(Slice* record, std::string* scratch) {
  Slice header;
  uint32_t record_sz = 0;
  uint32_t record_crc = 0;
  uint32_t actual_crc;
  Status s;

  scratch->clear();
  record->clear();

  scratch->append(zMetaHeaderSize, 0);
  header = Slice(scratch->c_str(), zMetaHeaderSize);

  s = Read(&header);
  if (!s.ok()) return s;

  // EOF?
  if (header.size() == 0) {
    record->clear();
    return Status::OK();
  }

  GetFixed32(&header, &record_crc);
  GetFixed32(&header, &record_sz);

  scratch->clear();
  scratch->append(record_sz, 0);

  *record = Slice(scratch->c_str(), record_sz);
  s = Read(record);
  if (!s.ok()) return s;

  actual_crc = crc32c::Value((const char*)&record_sz, sizeof(uint32_t));
  actual_crc = crc32c::Extend(actual_crc, record->data(), record->size());

  if (actual_crc != crc32c::Unmask(record_crc)) {
    return Status::IOError("Not a valid record");
  }

  /* Next record starts on a block boundary */
  if (read_pos_ % bs_) read_pos_ += bs_ - (read_pos_ % bs_);

  return Status::OK();
}

ZenEnv::ZenEnv(ZonedBlockDevice* zbd, Env* env, std::shared_ptr<Logger> logger)
    : EnvWrapper(env), zbd_(zbd), logger_(logger) {
  Info(logger_, "ZenEnv initializing");
  Info(logger_, "ZenEnv parameters: block device: %s",
       zbd_->GetFilename().c_str());

  Info(logger_, "ZenEnv initializing");
  next_file_id_ = 1;
  metadata_writer_.zenEnv = this;
}

ZenEnv::~ZenEnv() {
  Status s;
  Info(logger_, "ZenEnv shutting down");
  zbd_->LogZoneUsage();
  LogFiles();

  meta_log_.reset(nullptr);
  ClearFiles();
  delete zbd_;
}

void ZenEnv::LogFiles() {
  std::map<std::string, ZoneFile*>::iterator it;
  uint64_t total_size = 0;

  Info(logger_, "  Files:\n");
  for (it = files_.begin(); it != files_.end(); it++) {
    ZoneFile* zFile = it->second;
    std::vector<ZoneExtent*> extents = zFile->GetExtents();

    Info(logger_, "    %-45s sz: %lu lh: %d", it->first.c_str(),
         zFile->GetFileSize(), zFile->GetWriteLifeTimeHint());
    for (unsigned int i = 0; i < extents.size(); i++) {
      ZoneExtent* extent = extents[i];
      Info(logger_, "          Extent %u {start=0x%lx, zone=%u, len=%u} ", i,
           extent->start_,
           (uint32_t)(extent->zone_->start_ / zbd_->GetZoneSize()),
           extent->length_);

      total_size += extent->length_;
    }
  }
  Info(logger_, "Sum of all files: %lu MB of data \n",
       total_size / (1024 * 1024));
}

void ZenEnv::ClearFiles() {
  std::map<std::string, ZoneFile*>::iterator it;
  std::unique_lock<std::mutex> lk(&files_mtx_);
  for (it = files_.begin(); it != files_.end(); it++) delete it->second;
  files_.clear();
}

/* Assumes that files_mutex_ is held */
Status ZenEnv::WriteSnapshotLocked(ZenMetaLog* meta_log) {
  Status s;
  std::string snapshot;

  EncodeSnapshotTo(&snapshot);
  s = meta_log->AddRecord(snapshot);
  if (s.ok()) {
    for (auto it = files_.begin(); it != files_.end(); it++) {
      ZoneFile* zoneFile = it->second;
      zoneFile->MetadataSynced();
    }
  }
  return s;
}

Status ZenEnv::WriteEndRecord(ZenMetaLog* meta_log) {
  std::string endRecord;

  PutFixed32(&endRecord, kCompleteFilesSnapshot);
  return meta_log->AddRecord(endRecord);
}

/* Assumes the files_mtx_ is held */
Status ZenEnv::RollMetaZoneLocked() {
  ZenMetaLog* new_meta_log;
  Zone *new_meta_zone, *old_meta_zone;
  Status s;

  new_meta_zone = zbd_->AllocateMetaZone();
  if (!new_meta_zone) {
    assert(false);  // TMP
    Error(logger_, "Out of metadata zones, we should go to read only now.");
    return Status::NoSpace("Out of metadata zones");
  }

  Info(logger_, "Rolling to metazone %d\n", (int)new_meta_zone->GetZoneNr());
  new_meta_log = new ZenMetaLog(zbd_, new_meta_zone);

  old_meta_zone = meta_log_->GetZone();
  old_meta_zone->open_for_write_ = false;

  /* Write an end record and finish the meta data zone if there is space left */
  if (old_meta_zone->GetCapacityLeft()) WriteEndRecord(meta_log_.get());
  if (old_meta_zone->GetCapacityLeft()) old_meta_zone->Finish();

  meta_log_.reset(new_meta_log);

  std::string super_string;
  superblock_->EncodeTo(&super_string);

  s = meta_log_->AddRecord(super_string);
  if (!s.ok()) {
    Error(logger_,
          "Could not write super block when rolling to a new meta zone");
    return Status::IOError("Failed writing a new superblock");
  }

  s = WriteSnapshot(meta_log_.get());

  /* We've rolled successfully, we can reset the old zone now */
  if (s.ok()) old_meta_zone->Reset();

  return s;
}

Status ZenEnv::PersistSnapshot(ZenMetaLog* meta_writer) {
  Status s;

  files_mtx_.lock();
  metadata_sync_mtx_.lock();

  s = WriteSnapshotLocked(meta_writer);
  if (s == Status::NoSpace()) {
    Info(logger_, "Current meta zone full, rolling to next meta zone");
    s = RollMetaZoneLocked();
  }

  if (!s.ok()) {
    Error(logger_,
          "Failed persisting a snapshot, we should go to read only now!");
  }

  metadata_sync_mtx_.unlock();
  files_mtx_.unlock();

  return s;
}

Status ZenEnv::PersistRecord(std::string record) {
  Status s;

  metadata_sync_mtx_.lock();
  s = meta_log_->AddRecord(record);
  if (s == Status::NoSpace()) {
    Info(logger_, "Current meta zone full, rolling to next meta zone");
    s = RollMetaZoneLocked();
    /* After a successfull roll, a complete snapshot has been persisted
     * - no need to write the record update */
  }
  metadata_sync_mtx_.unlock();

  return s;
}

Status ZenEnv::SyncFileMetadata(ZoneFile* zoneFile) {
  std::string fileRecord;
  std::string output;

  Status s;

  files_mtx_.lock();

  PutFixed32(&output, kFileUpdate);
  zoneFile->EncodeUpdateTo(&fileRecord);
  PutLengthPrefixedSlice(&output, Slice(fileRecord));

  s = PersistRecord(output);
  if (s.ok()) zoneFile->MetadataSynced();

  files_mtx_.unlock();

  return s;
}

ZoneFile* ZenEnv::GetFile(std::string fname) {
  ZoneFile* zoneFile = nullptr;
  files_mtx_.lock();
  if (files_.find(fname) != files_.end()) {
    zoneFile = files_[fname];
  }
  files_mtx_.unlock();
  return zoneFile;
}

Status ZenEnv::DeleteFile_Internal(std::string fname) {
  ZoneFile* zoneFile = nullptr;
  Status s;

  zoneFile = GetFile(fname);
  files_mtx_.lock();
  if (zoneFile != nullptr) {
    std::string record;

    zoneFile = files_[fname];
    files_.erase(fname);

    EncodeFileDeletionTo(zoneFile, &record);
    s = PersistRecord(record);
    if (!s.ok()) {
      /* Failed to persist the delete, return to a consistent state */
      files_.insert(std::make_pair(fname.c_str(), zoneFile));
    } else {
      delete (zoneFile);
    }
  }
  files_mtx_.unlock();
  return s;
}

Status ZenEnv::NewSequentialFile(const std::string& fname,
                                  std::unique_ptr<SequentialFile>* result,
                                  const EnvOptions& options) {
  ZoneFile* zoneFile = GetFile(fname);

  Debug(logger_, "New sequential file: %s direct: %d\n", fname.c_str(),
        options.use_direct_reads);

  if (zoneFile == nullptr) {
    return Status::IOError("File does not exist!\n");
  }

  result->reset(new ZonedSequentialFile(zoneFile, options));
  return Status::OK();
}

Status ZenEnv::NewRandomAccessFile(const std::string& fname,
                                    std::unique_ptr<RandomAccessFile>* result,
                                    const EnvOptions& options) {
  ZoneFile* zoneFile = GetFile(fname);

  Debug(logger_, "New random access file: %s direct: %d\n", fname.c_str(),
        options.use_direct_reads);

  if (zoneFile == nullptr) {
    return target()->NewRandomAccessFile(ToAuxPath(fname), result);
  }

  result->reset(new ZonedRandomAccessFile(files_[fname], options));
  return Status::OK();
}

Status ZenEnv::NewWritableFile(const std::string& fname,
                                std::unique_ptr<WritableFile>* result,
                                const EnvOptions& options) {
  ZoneFile* zoneFile;
  Status s;

  Debug(logger_, "New writable file: %s direct: %d\n", fname.c_str(),
        options.use_direct_writes);

  if (GetFile(fname) != nullptr) {
    s = DeleteFile(fname);
    if (!s.ok()) return s;
  }

  zoneFile = new ZoneFile(zbd_, fname, next_file_id_++);

  files_mtx_.lock();
  files_.insert(std::make_pair(fname.c_str(), zoneFile));
  files_mtx_.unlock();

  result->reset(new ZonedWritableFile(zbd_, true, zoneFile, &metadata_writer_));

  return s;
}

Status ZenEnv::ReuseWritableFile(const std::string& fname,
                                  const std::string& old_fname,
                                  std::unique_ptr<WritableFile>* result,
                                  const EnvOptions& options) {
  Debug(logger_, "Reuse writable file: %s old name: %s\n", fname.c_str(),
        old_fname.c_str());

  if (GetFile(fname) == nullptr)
    return Status::NotFound("Old file does not exist");

  return NewWritableFile(fname, result, options);
}

Status ZenEnv::FileExists(const std::string& fname) {
  Debug(logger_, "FileExists: %s \n", fname.c_str());

  if (GetFile(fname) == nullptr) {
    return target()->FileExists(ToAuxPath(fname));
  } else {
    return Status::OK();
  }
}

Status ZenEnv::ReopenWritableFile(const std::string& fname,
                                   const FileOptions& options,
                                   std::unique_ptr<FSWritableFile>* result,
                                   IODebugContext* dbg) {
  Debug(logger_, "Reopen writable file: %s \n", fname.c_str());

  if (GetFile(fname) != nullptr)
    return NewWritableFile(fname, options, result, dbg);

  return target()->NewWritableFile(fname, options, result, dbg);
}
 

Status ZenEnv::GetChildren(const std::string& dir,
                            std::vector<std::string>* result) {
  std::map<std::string, ZoneFile*>::iterator it;
  std::vector<std::string> auxfiles;
  Status s;

  Debug(logger_, "GetChildren: %s \n", dir.c_str());

  target()->GetChildren(ToAuxPath(dir), &auxfiles);
  for (const auto f : auxfiles) {
    if (f != "." && f != "..") result->push_back(f);
  }

  files_mtx_.lock();
  for (it = files_.begin(); it != files_.end(); it++) {
    std::string fname = it->first;
    if (fname.rfind(dir, 0) == 0) {
      if (dir.back() == '/') {
        fname.erase(0, dir.length());
      } else {
        fname.erase(0, dir.length() + 1);
      }
      // Don't report grandchildren
      if (fname.find("/") == std::string::npos) {
        result->push_back(fname);
      }
    }
  }
  files_mtx_.unlock();

  return s;
}

Status ZenEnv::DeleteFile(const std::string& fname) {
  Status s;
  ZoneFile *zoneFile = GetFile(fname);
  Debug(logger_, "Delete file: %s \n", fname.c_str());

  if (zoneFile == nullptr) {
    return target()->DeleteFile(ToAuxPath(fname));
  }

  if (zoneFile->IsOpenForWR()) {
    s = Status::Busy("Cannot delete, file open for writing: ", fname.c_str());
  } else {
    s = DeleteFile_Internal(fname);
    zbd_->LogZoneStats();
  }

  return s;
}

Status ZenEnv::GetFileSize(const std::string& f, uint64_t* size) {
  ZoneFile* zoneFile;
  Status s;

  Debug(logger_, "GetFileSize: %s \n", f.c_str());

  files_mtx_.lock();
  if (files_.find(f) != files_.end()) {
    zoneFile = files_[f];
    *size = zoneFile->GetFileSize();
  } else {
    s = Status::IOError("file does not exist\n");
  }
  files_mtx_.unlock();

  return s;
}

Status ZenEnv::RenameFile(const std::string& f, const std::string& t) {
  ZoneFile* zoneFile;
  Status s;

  Debug(logger_, "Rename file: %s to : %s\n", f.c_str(), t.c_str());

  zoneFile = GetFile(f);
  if (zoneFile != nullptr) {
    s = DeleteFile(t);
    if (s.ok()) {
      files_mtx_.lock();
      files_.erase(f);
      zoneFile->Rename(t);
      files_.insert(std::make_pair(t, zoneFile));
      files_mtx_.unlock();

      s = SyncFileMetadata(zoneFile);
      if (!s.ok()) {
        /* Failed to persist the rename, roll back */
        files_mtx_.lock();
        files_.erase(t);
        zoneFile->Rename(f);
        files_.insert(std::make_pair(f, zoneFile));
        files_mtx_.unlock();
      }
    }
  } else {
    s = target()->RenameFile(ToAuxPath(f), ToAuxPath(t));
  }

  return s;
}

void ZenEnv::EncodeSnapshotTo(std::string* output) {
  std::map<std::string, ZoneFile*>::iterator it;
  std::string files_string;
  PutFixed32(output, kCompleteFilesSnapshot);
  for (it = files_.begin(); it != files_.end(); it++) {
    std::string file_string;
    ZoneFile* zFile = it->second;

    zFile->EncodeSnapshotTo(&file_string);
    PutLengthPrefixedSlice(&files_string, Slice(file_string));
  }
  PutLengthPrefixedSlice(output, Slice(files_string));
}

Status ZenEnv::DecodeFileUpdateFrom(Slice* slice) {
  ZoneFile* update = new ZoneFile(zbd_, "not_set", 0);
  uint64_t id;
  Status s;

  s = update->DecodeFrom(slice);
  if (!s.ok()) return s;

  id = update->GetID();
  if (id >= next_file_id_) next_file_id_ = id + 1;

  /* Check if this is an update to an existing file */
  for (auto it = files_.begin(); it != files_.end(); it++) {
    ZoneFile* zFile = it->second;
    if (id == zFile->GetID()) {
      std::string oldName = zFile->GetFilename();

      s = zFile->MergeUpdate(update);
      delete update;

      if (!s.ok()) return s;

      if (zFile->GetFilename() != oldName) {
        files_.erase(oldName);
        files_.insert(std::make_pair(zFile->GetFilename(), zFile));
      }

      return Status::OK();
    }
  }

  /* The update is a new file */
  assert(GetFile(update->GetFilename()) == nullptr);
  files_.insert(std::make_pair(update->GetFilename(), update));

  return Status::OK();
}

Status ZenEnv::DecodeSnapshotFrom(Slice* input) {
  Slice slice;

  assert(files_.size() == 0);

  while (GetLengthPrefixedSlice(input, &slice)) {
    ZoneFile* zoneFile = new ZoneFile(zbd_, "not_set", 0);
    Status s = zoneFile->DecodeFrom(&slice);
    if (!s.ok()) return s;

    files_.insert(std::make_pair(zoneFile->GetFilename(), zoneFile));
    if (zoneFile->GetID() >= next_file_id_)
      next_file_id_ = zoneFile->GetID() + 1;
  }

  return Status::OK();
}

void ZenEnv::EncodeFileDeletionTo(ZoneFile* zoneFile, std::string* output) {
  std::string file_string;

  PutFixed64(&file_string, zoneFile->GetID());
  PutLengthPrefixedSlice(&file_string, Slice(zoneFile->GetFilename()));

  PutFixed32(output, kFileDeletion);
  PutLengthPrefixedSlice(output, Slice(file_string));
}

Status ZenEnv::DecodeFileDeletionFrom(Slice* input) {
  uint64_t fileID;
  std::string fileName;
  Slice slice;

  if (!GetFixed64(input, &fileID))
    return Status::Corruption("Zone file deletion: file id missing");

  if (!GetLengthPrefixedSlice(input, &slice))
    return Status::Corruption("Zone file deletion: file name missing");

  fileName = slice.ToString();
  if (files_.find(fileName) == files_.end())
    return Status::Corruption("Zone file deletion: no such file");

  ZoneFile* zoneFile = files_[fileName];
  if (zoneFile->GetID() != fileID)
    return Status::Corruption("Zone file deletion: file ID missmatch");

  files_.erase(fileName);
  delete zoneFile;

  return Status::OK();
}

Status ZenEnv::RecoverFrom(ZenMetaLog* log) {
  bool at_least_one_snapshot = false;
  std::string scratch;
  uint32_t tag = 0;
  Slice record;
  Slice data;
  Status s;
  bool done = false;

  while (!done) {
    Status rs = log->ReadRecord(&record, &scratch);
    if (!rs.ok()) {
      Error(logger_, "Read recovery record failed with error: %s",
            rs.ToString().c_str());
      return Status::Corruption("ZenEnv", "Metadata corruption");
    }

    if (!GetFixed32(&record, &tag)) break;

    if (!GetLengthPrefixedSlice(&record, &data))
      return Status::Corruption("ZenEnv", "No recovery record data");

    switch (tag) {
      case kCompleteFilesSnapshot:
        ClearFiles();
        s = DecodeSnapshotFrom(&data);
        if (!s.ok()) {
          Warn(logger_, "Could not decode complete snapshot: %s",
               s.ToString().c_str());
          return s;
        }
        at_least_one_snapshot = true;
        break;

      case kFileUpdate:
        s = DecodeFileUpdateFrom(&data);
        if (!s.ok()) {
          Warn(logger_, "Could not decode file snapshot: %s",
               s.ToString().c_str());
          return s;
        }
        break;

      case kFileDeletion:
        s = DecodeFileDeletionFrom(&data);
        if (!s.ok()) {
          Warn(logger_, "Could not decode file deletion: %s",
               s.ToString().c_str());
          return s;
        }
        break;

      case kEndRecord:
        done = true;
        break;

      default:
        Warn(logger_, "Unexpected metadata record tag: %u", tag);
        return Status::Corruption("ZenEnv", "Unexpected tag");
    }
  }

  if (at_least_one_snapshot)
    return Status::OK();
  else
    return Status::NotFound("ZenEnv", "No snapshot found");
}

#define ZENV_URI_PATTERN "ZenEnv://"

Status ZenEnv::Mount(bool readonly) {
  std::vector<Zone*> metazones = zbd_->GetMetaZones();
  std::vector<std::unique_ptr<Superblock>> valid_superblocks;
  std::vector<std::unique_ptr<ZenMetaLog>> valid_logs;
  std::vector<Zone*> valid_zones;
  std::vector<std::pair<uint32_t, uint32_t>> seq_map;

  Status s;

  /* We need a minimum of two non-offline meta data zones */
  if (metazones.size() < 2) {
    Error(logger_,
          "Need at least two non-offline meta zones to open for write");
    return Status::NotSupported();
  }

  /* Find all valid superblocks */
  for (const auto z : metazones) {
    std::unique_ptr<ZenMetaLog> log;
    std::string scratch;
    Slice super_record;

    log.reset(new ZenMetaLog(zbd_, z));

    if (!log->ReadRecord(&super_record, &scratch).ok()) continue;

    if (super_record.size() == 0) continue;

    std::unique_ptr<Superblock> super_block;

    super_block.reset(new Superblock());
    s = super_block->DecodeFrom(&super_record);
    if (s.ok()) s = super_block->CompatibleWith(zbd_);
    if (!s.ok()) return s;

    Info(logger_, "Found OK superblock in zone %lu seq: %u\n", z->GetZoneNr(),
         super_block->GetSeq());

    seq_map.push_back(std::make_pair(super_block->GetSeq(), seq_map.size()));
    valid_superblocks.push_back(std::move(super_block));
    valid_logs.push_back(std::move(log));
    valid_zones.push_back(z);
  }

  if (!seq_map.size()) return Status::NotFound("No valid superblock found");

  /* Sort superblocks by descending sequence number */
  std::sort(seq_map.begin(), seq_map.end(),
            std::greater<std::pair<uint32_t, uint32_t>>());

  bool recovery_ok = false;
  unsigned int r = 0;

  for (const auto sm : seq_map) {
    uint32_t i = sm.second;
    std::string scratch;
    std::unique_ptr<ZenMetaLog> log = std::move(valid_logs[i]);

    s = RecoverFrom(log.get());
    if (!s.ok()) {
      if (s.IsNotFound()) {
        Warn(logger_,
             "Did not find a valid snapshot, trying next meta zone. Error: %s",
             s.ToString().c_str());
        continue;
      }

      Error(logger_, "Metadata corruption. Error: %s", s.ToString().c_str());
      return s;
    }

    r = i;
    recovery_ok = true;
    meta_log_ = std::move(log);
    break;
  }

  if (!recovery_ok) {
    return Status::IOError("Failed to mount filesystem");
  }

  Info(logger_, "Recovered from zone: %d", (int)valid_zones[r]->GetZoneNr());
  superblock_ = std::move(valid_superblocks[r]);
  zbd_->SetFinishTreshold(superblock_->GetFinishTreshold());
  zbd_->SetMaxActiveZones(superblock_->GetMaxActiveZoneLimit());
  zbd_->SetMaxOpenZones(superblock_->GetMaxOpenZoneLimit());

  s = target()->CreateDirIfMissing(superblock_->GetAuxFsPath());
  if (!s.ok()) {
    Error(logger_, "Failed to create aux filesystem directory.");
    return s;
  }

  /* Free up old metadata zones, to get ready to roll */
  for (const auto sm : seq_map) {
    uint32_t i = sm.second;
    /* Don't reset the current metadata zone */
    if (i != r) {
      /* Metadata zones are not marked as having valid data, so they can be
       * reset */
      valid_logs[i].reset();
    }
  }
  
  if (readonly) {
    Info(logger_, "Mounting READ ONLY");
  } else {
    files_mtx_.lock();
    s = RollMetaZoneLocked();
    if (!s.ok()) {
      files_mtx_.unlock();
      Error(logger_, "Failed to roll metadata zone.");
      return s;
    }
    files_mtx_.unlock();
  }

  Info(logger_, "Superblock sequence %d", (int)superblock_->GetSeq());
  Info(logger_, "Finish threshold %u", superblock_->GetFinishTreshold());
  Info(logger_, "Filesystem mount OK");
  
  if (!readonly) {
    Info(logger_, "Resetting unused IO Zones..");
    zbd_->ResetUnusedIOZones();
    Info(logger_, "  Done");
  }

  LogFiles();

  return Status::OK();
}

Status ZenEnv::MkFS(std::string aux_fs_path, uint32_t finish_threshold,
                    uint32_t max_open_limit, uint32_t max_active_limit) {
  std::vector<Zone*> metazones = zbd_->GetMetaZones();
  std::unique_ptr<ZenMetaLog> log;
  Zone* meta_zone = nullptr;
  Status s;

  /* TODO: check practical limits */

  if (max_open_limit > zbd_->GetMaxOpenZones()) {
    return Status::InvalidArgument(
        "Max open zone limit exceeds the device limit\n");
  }

  if (max_active_limit > zbd_->GetMaxActiveZones()) {
    return Status::InvalidArgument(
        "Max active zone limit exceeds the device limit\n");
  }

  if (max_active_limit < max_open_limit) {
    return Status::InvalidArgument(
        "Max open limit must be smaller than max active limit\n");
  }

  if (aux_fs_path.length() > 255) {
    return Status::InvalidArgument(
        "Aux filesystem path must be less than 256 bytes\n");
  }

  ClearFiles();
  zbd_->ResetUnusedIOZones();

  for (const auto mz : metazones) {
    if (mz->Reset().ok()) {
      if (!meta_zone) meta_zone = mz;
    } else {
      Warn(logger_, "Failed to reset meta zone\n");
    }
  }

  if (!meta_zone) {
    return Status::IOError("Not available meta zones\n");
  }

  log.reset(new ZenMetaLog(zbd_, meta_zone));

  Superblock* super = new Superblock(zbd_, aux_fs_path, finish_threshold);
  std::string super_string;
  super->EncodeTo(&super_string);

  s = log->AddRecord(super_string);
  if (!s.ok()) return s;

  /* Write an empty snapshot to make the metadata zone valid */
  s = PersistSnapshot(log.get());
  if (!s.ok()) {
    Error(logger_, "Failed to persist snapshot: %s", s.ToString().c_str());
    return Status::IOError("Failed persist snapshot");
  }

  Info(logger_, "Empty filesystem created");
  return Status::OK();
}

std::map<std::string, Env::WriteLifeTimeHint> ZenEnv::GetWriteLifeTimeHints() {
  std::map<std::string, Env::WriteLifeTimeHint> hint_map;

  for (auto it = files_.begin(); it != files_.end(); it++) {
    ZoneFile* zoneFile = it->second;
    std::string filename = it->first;
    hint_map.insert(std::make_pair(filename, zoneFile->GetWriteLifeTimeHint()));
  }

  return hint_map;
}

#ifndef NDEBUG
static std::string GetLogFilename(std::string bdev) {
  std::ostringstream ss;
  time_t t = time(0);
  struct tm* log_start = std::localtime(&t);
  char buf[40];

  std::strftime(buf, sizeof(buf), "%Y-%m-%d_%H:%M:%S.log", log_start);
  ss << DEFAULT_ZENV_LOG_PATH << std::string("zenfs_") << bdev << "_" << buf;

  return ss.str();
}
#endif

Status NewZenEnv(Env** env, const std::string& bdevname) {
  std::shared_ptr<Logger> logger;
  Status s;

#ifndef NDEBUG
  s = Env::Default()->NewLogger(GetLogFilename(bdevname), &logger);
  if (!s.ok()) {
    fprintf(stderr, "ZenEnv: Could not create logger");
  } else {
    logger->SetInfoLogLevel(DEBUG_LEVEL);
  }
#endif

  ZonedBlockDevice* zbd = new ZonedBlockDevice(bdevname, logger);
  Status zbd_status = zbd->Open();
  if (!zbd_status.ok()) {
    Error(logger, "Failed to open zoned block device: %s",
          zbd_status.ToString().c_str());
    return Status::IOError(zbd_status.ToString());
  }

  ZenEnv* zenEnv = new ZenEnv(zbd, Env::Default(), logger);
  s = zenEnv->Mount(false);
  if (!s.ok()) {
    delete zenEnv;
    return s;
  }

  *env = zenEnv;
  return Status::OK();
}

std::map<std::string, std::string> ListZenFileSystems() {
  std::map<std::string, std::string> zenFileSystems;
  DIR* dir = opendir("/sys/class/block");
  struct dirent* entry;

  while (NULL != (entry = readdir(dir))) {
    if (entry->d_type == DT_LNK) {
      std::string zbdName = std::string(entry->d_name);
      ZonedBlockDevice* zbd = new ZonedBlockDevice(zbdName, nullptr);
      Status zbd_status = zbd->Open(true);

      if (zbd_status.ok()) {
        std::vector<Zone*> metazones = zbd->GetMetaZones();
        std::string scratch;
        Slice super_record;
        Status s;

        for (const auto z : metazones) {
          Superblock super_block;
          std::unique_ptr<ZenMetaLog> log;
          log.reset(new ZenMetaLog(zbd, z));

          if (!log->ReadRecord(&super_record, &scratch).ok()) continue;
          s = super_block.DecodeFrom(&super_record);
          if (s.ok()) {
            /* Map the uuid to the device-mapped (i.g dm-linear) block device to
               avoid trying to mount the whole block device in case of a split
               device */
            if (zenFileSystems.find(super_block.GetUUID()) !=
                    zenFileSystems.end() &&
                zenFileSystems[super_block.GetUUID()].rfind("dm-", 0) == 0) {
              break;
            }
            zenFileSystems[super_block.GetUUID()] = zbdName;
            break;
          }
        }

        delete zbd;
        continue;
      }
    }
  }

  return zenFileSystems;
}

};  // namespace TERARKDB_NAMESPACE
#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)
