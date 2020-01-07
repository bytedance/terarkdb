//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "db/compacted_db_impl.h"

#include "db/db_impl.h"
#include "db/version_set.h"
#include "table/get_context.h"
#if !defined(_MSC_VER) && !defined(__APPLE__)
#include <sys/unistd.h>
#include <table/terark_zip_table.h>
#endif

#include <terark/valvec.hpp>

namespace rocksdb {

extern void MarkKeyMayExist(void* arg);
extern bool SaveValue(void* arg, const ParsedInternalKey& parsed_key,
                      const Slice& v, bool hit_and_return);

CompactedDBImpl::CompactedDBImpl(const DBOptions& options,
                                 const std::string& dbname)
    : DBImpl(options, dbname),
      cfd_(nullptr),
      version_(nullptr),
      user_comparator_(nullptr) {}

CompactedDBImpl::~CompactedDBImpl() {}

size_t CompactedDBImpl::FindFile(const Slice& key) {
  size_t right = files_.num_files - 1;
  return terark::lower_bound_ex_0(files_.files, right, key,
                                  TERARK_GET(.largest_key) + &ExtractUserKey,
                                  "" < *user_comparator_);
}

Status CompactedDBImpl::Get(const ReadOptions& options, ColumnFamilyHandle*,
                            const Slice& key, LazyBuffer* value) {
  GetContext get_context(user_comparator_, nullptr, nullptr, nullptr,
                         GetContext::kNotFound, key, value, nullptr, nullptr,
                         version_, nullptr, nullptr);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = kMaxSequenceNumber;
  }
  LookupKey lkey(key, snapshot);
  files_.files[FindFile(key)].fd.table_reader->Get(options, lkey.internal_key(),
                                                   &get_context, nullptr);
  if (get_context.State() == GetContext::kFound) {
    return value->fetch();
  } else if (get_context.State() == GetContext::kCorrupt) {
    return std::move(get_context).CorruptReason();
  }
  return Status::NotFound();
}

std::vector<Status> CompactedDBImpl::MultiGet(
    const ReadOptions& options, const std::vector<ColumnFamilyHandle*>&,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  autovector<TableReader*, 16> reader_list;
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = kMaxSequenceNumber;
  }
  for (const auto& key : keys) {
    const FdWithKeyRange& f = files_.files[FindFile(key)];
    if (user_comparator_->Compare(key, ExtractUserKey(f.smallest_key)) < 0) {
      reader_list.push_back(nullptr);
    } else {
      LookupKey lkey(key, snapshot);
      f.fd.table_reader->Prepare(lkey.internal_key());
      reader_list.push_back(f.fd.table_reader);
    }
  }
  std::vector<Status> statuses(keys.size(), Status::NotFound());
  values->resize(keys.size());
  int idx = 0;
  for (auto* r : reader_list) {
    if (r != nullptr) {
      std::string& value = (*values)[idx];
      LazyBuffer lazy_val(&value);
      GetContext get_context(user_comparator_, nullptr, nullptr, nullptr,
                             GetContext::kNotFound, keys[idx], &lazy_val,
                             nullptr, nullptr, version_, nullptr, nullptr);
      LookupKey lkey(keys[idx], kMaxSequenceNumber);
      r->Get(options, lkey.internal_key(), &get_context, nullptr);
      if (get_context.State() == GetContext::kFound) {
        statuses[idx] = std::move(lazy_val).dump(&value);
      } else if (get_context.State() == GetContext::kCorrupt) {
        statuses[idx] = std::move(get_context).CorruptReason();
      }
    }
    ++idx;
  }
  return statuses;
}

Status CompactedDBImpl::Init(const Options& options) {
  SuperVersionContext sv_context(/* create_superversion */ true);
  mutex_.Lock();
  ColumnFamilyDescriptor cf(kDefaultColumnFamilyName,
                            ColumnFamilyOptions(options));
  Status s = Recover({cf}, true /* read only */, false, true);
  if (s.ok()) {
    cfd_ =
        reinterpret_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily())->cfd();
    cfd_->InstallSuperVersion(&sv_context, &mutex_);
  }
  mutex_.Unlock();
  sv_context.Clean();
  if (!s.ok()) {
    return s;
  }
  NewThreadStatusCfInfo(cfd_);
  version_ = cfd_->GetSuperVersion()->current;
  user_comparator_ = cfd_->user_comparator();
  auto* vstorage = version_->storage_info();
  if (vstorage->num_non_empty_levels() == 0) {
    return Status::NotSupported("no file exists");
  }
  const LevelFilesBrief& l0 = vstorage->LevelFilesBrief(0);
  // L0 should not have files
  if (l0.num_files > 1) {
    return Status::NotSupported("L0 contain more than 1 file");
  }
  if (l0.num_files == 1) {
    if (vstorage->num_non_empty_levels() > 1) {
      return Status::NotSupported("Both L0 and other level contain files");
    }
    if (l0.files[0].file_metadata->prop.is_map_sst()) {
      return Status::NotSupported("L0 has read amp");
    }
    files_ = l0;
    return Status::OK();
  }

  for (int i = 1; i < vstorage->num_non_empty_levels() - 1; ++i) {
    if (vstorage->LevelFilesBrief(i).num_files > 0) {
      return Status::NotSupported("Other levels also contain files");
    }
  }

  int level = vstorage->num_non_empty_levels() - 1;
  for (auto f : vstorage->LevelFiles(level)) {
    if (f->prop.is_map_sst()) {
      return Status::NotSupported("Level has read amp");
    }
  }
  if (vstorage->LevelFilesBrief(level).num_files > 0) {
    files_ = vstorage->LevelFilesBrief(level);
    return Status::OK();
  }
  return Status::NotSupported("no file exists");
}

Status CompactedDBImpl::Open(const Options& options, const std::string& dbname,
                             DB** dbptr) {
  *dbptr = nullptr;
#if !defined(_MSC_VER) && !defined(__APPLE__)
  const char* terarkdb_localTempDir = getenv("TerarkZipTable_localTempDir");
  const char* terarkConfigString = getenv("TerarkConfigString");
  if (terarkdb_localTempDir || terarkConfigString) {
    if (terarkdb_localTempDir &&
        ::access(terarkdb_localTempDir, R_OK | W_OK) != 0) {
      return Status::InvalidArgument(
          "Must exists, and Permission ReadWrite is required on "
          "env TerarkZipTable_localTempDir",
          terarkdb_localTempDir);
    }
    if (!TerarkZipIsBlackListCF(kDefaultColumnFamilyName)) {
      const ColumnFamilyOptions& cf_options = options;
      const DBOptions& db_options = options;
      TerarkZipDBOptionsFromEnv(const_cast<DBOptions&>(db_options));
      TerarkZipCFOptionsFromEnv(const_cast<ColumnFamilyOptions&>(cf_options),
                                dbname);
      auto& factory = cf_options.table_factory;
      Status s = factory->SanitizeOptions(db_options, cf_options);
      if (!s.ok()) return s;
    }
  }
#endif
  if (options.max_open_files != -1) {
    return Status::InvalidArgument("require max_open_files = -1");
  }
  if (options.merge_operator.get() != nullptr) {
    return Status::InvalidArgument("merge operator is not supported");
  }
  DBOptions db_options(options);
  std::unique_ptr<CompactedDBImpl> db(new CompactedDBImpl(db_options, dbname));
  Status s = db->Init(options);
  if (s.ok()) {
    db->StartTimedTasks();
    ROCKS_LOG_INFO(db->immutable_db_options_.info_log,
                   "Opened the db as fully compacted mode");
    LogFlush(db->immutable_db_options_.info_log);
    *dbptr = db.release();
  }
  return s;
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
