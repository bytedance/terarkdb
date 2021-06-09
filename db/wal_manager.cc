//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/wal_manager.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include <algorithm>
#include <memory>
#include <set>
#include <vector>

#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/transaction_log_impl.h"
#include "db/write_batch_internal.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/terark_namespace.h"
#include "rocksdb/write_batch.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "utilities/util/valvec.hpp"

namespace TERARKDB_NAMESPACE {

#ifndef ROCKSDB_LITE

namespace {
struct CompareLogByPointer {
  bool operator()(const std::unique_ptr<LogFile>& a,
                  const std::unique_ptr<LogFile>& b) {
    LogFileImpl* a_impl = static_cast_with_check<LogFileImpl, LogFile>(a.get());
    LogFileImpl* b_impl = static_cast_with_check<LogFileImpl, LogFile>(b.get());
    return *a_impl < *b_impl;
  }
};
struct EqualLogByPointer {
  bool operator()(const std::unique_ptr<LogFile>& a,
                  const std::unique_ptr<LogFile>& b) {
    LogFileImpl* a_impl = static_cast_with_check<LogFileImpl, LogFile>(a.get());
    LogFileImpl* b_impl = static_cast_with_check<LogFileImpl, LogFile>(b.get());
    return a_impl->LogNumber() == b_impl->LogNumber();
  }
};
}  // namespace

void WalManager::AddLogNumber(const uint64_t number) {
  MutexLock l(&read_first_record_cache_mutex_);
  log_numbers_.emplace(number);
}

uint64_t WalManager::GetNextLogNumber(const uint64_t number) {
  MutexLock l(&read_first_record_cache_mutex_);
  auto it = log_numbers_.find(number);
  if (it != log_numbers_.end() && ++it != log_numbers_.end()) {
    return *it;
  } else {
    return 0;
  }
}

Status WalManager::DeleteFile(const std::string& fname, uint64_t number) {
  auto s = env_->DeleteFile(db_options_.wal_dir + "/" + fname);
  if (s.ok()) {
    MutexLock l(&read_first_record_cache_mutex_);
    read_first_record_cache_.erase(number);
    log_numbers_.erase(number);
  }
  return s;
}

Status WalManager::GetSortedWalFiles(VectorLogPtr& files, bool allow_empty) {
  // First get sorted files in db dir, then get sorted files from archived
  // dir, to avoid a race condition where a log file is moved to archived
  // dir in between.
  Status s;
  // list wal files in main db dir.
  VectorLogPtr logs;
  s = GetWalsOfType(db_options_.wal_dir, logs, kAliveLogFile, allow_empty);
  if (!s.ok()) {
    return s;
  }

  // Reproduce the race condition where a log file is moved
  // to archived dir, between these two sync points, used in
  // (DBTest,TransactionLogIteratorRace)
  TEST_SYNC_POINT("WalManager::GetSortedWalFiles:1");
  TEST_SYNC_POINT("WalManager::GetSortedWalFiles:2");

  files.clear();
  // list wal files in archive dir.
  std::string archivedir = ArchivalDirectory(db_options_.wal_dir);
  Status exists = env_->FileExists(archivedir);
  if (exists.ok()) {
    s = GetWalsOfType(archivedir, files, kArchivedLogFile, allow_empty);
    if (!s.ok()) {
      return s;
    }
  } else if (!exists.IsNotFound()) {
    assert(s.IsIOError());
    return s;
  }

  if (!files.empty()) {
    // is this necessary ?
    auto& latest_archived_log =
        *std::max_element(files.begin(), files.end(), CompareLogByPointer());
    ROCKS_LOG_INFO(db_options_.info_log, "Latest Archived log: %" PRIu64,
                   latest_archived_log->LogNumber());
  }

  using MoveIter = std::move_iterator<VectorLogPtr::iterator>;
  files.insert(files.end(), MoveIter(logs.begin()), MoveIter(logs.end()));
  logs.clear();
  std::sort(files.begin(), files.end(), CompareLogByPointer());
  // When the race condition happens, we could see the same log in both db dir
  // and archived dir. Simply ignore the one in db dir. Note that, if we read
  // archived dir first, we would have missed the log file.
  files.erase(std::unique(files.begin(), files.end(), EqualLogByPointer()),
              files.end());
  return s;
}

Status WalManager::GetUpdatesSince(
    SequenceNumber seq, std::unique_ptr<TransactionLogIterator>* iter,
    const TransactionLogIterator::ReadOptions& read_options,
    VersionSet* version_set) {
  //  Get all sorted Wal Files.
  //  Do binary search and open files and find the seq number.

  std::unique_ptr<VectorLogPtr> wal_files(new VectorLogPtr);
  Status s = GetSortedWalFiles(*wal_files, false /* allow_empty */);
  if (!s.ok()) {
    return s;
  }

  s = RetainProbableWalFiles(*wal_files, seq, version_set);
  if (!s.ok()) {
    return s;
  }
  iter->reset(new TransactionLogIteratorImpl(
      db_options_.wal_dir, &db_options_, read_options, env_options_, seq,
      std::move(wal_files), version_set, this, seq_per_batch_));
  return (*iter)->status();
}

// 1. Go through all archived files and
//    a. if ttl is enabled, delete outdated files
//    b. if archive size limit is enabled, delete empty files,
//        compute file number and size.
// 2. If size limit is enabled:
//    a. compute how many files should be deleted
//    b. get sorted non-empty archived logs
//    c. delete what should be deleted
void WalManager::PurgeObsoleteWALFiles() {
  const bool ttl_enabled = db_options_.wal_ttl_seconds > 0;
  const bool size_limit_enabled = db_options_.wal_size_limit_mb > 0;
  if (!ttl_enabled && !size_limit_enabled) {
    return;
  }
  if (purge_wal_files_running_) {
    return;
  }

  int64_t current_time;
  Status s = env_->GetCurrentTime(&current_time);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log, "Can't get current time: %s",
                    s.ToString().c_str());
    assert(false);
    return;
  }
  const uint64_t now_seconds = static_cast<uint64_t>(current_time);
  const uint64_t time_to_check = kDefaultIntervalToDeleteObsoleteWAL;

  if (purge_wal_files_last_run_ + time_to_check > now_seconds) {
    return;
  }
  purge_wal_files_last_run_ = now_seconds;
  purge_wal_files_running_ = true;
  PurgeObsoleteWALFilesImpl(now_seconds);
  purge_wal_files_running_ = false;
}

void WalManager::ArchiveWALFile(const std::string& fname, uint64_t number) {
  auto archived_log_name = ArchivedLogFileName(db_options_.wal_dir, number);
  // The sync point below is used in (DBTest,TransactionLogIteratorRace)
  TEST_SYNC_POINT("WalManager::PurgeObsoleteFiles:1");
  Status s = env_->RenameFile(fname, archived_log_name);
  // The sync point below is used in (DBTest,TransactionLogIteratorRace)
  TEST_SYNC_POINT("WalManager::PurgeObsoleteFiles:2");
  ROCKS_LOG_INFO(db_options_.info_log, "Move log file %s to %s -- %s\n",
                 fname.c_str(), archived_log_name.c_str(),
                 s.ToString().c_str());
}

Status WalManager::GetWalsOfType(const std::string& path,
                                 VectorLogPtr& log_files, WalFileType log_type,
                                 bool allow_empty) {
  std::vector<std::string> all_files;
  const Status status = env_->GetChildren(path, &all_files);
  if (!status.ok()) {
    return status;
  }
  log_files.reserve(all_files.size());
  for (const auto& f : all_files) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == kLogFile) {
      SequenceNumber sequence;
      Status s = ReadFirstRecord(log_type, number, &sequence);
      if (!s.ok()) {
        return s;
      }
      if (!allow_empty && sequence == 0) {
        // empty file
        continue;
      }

      // Reproduce the race condition where a log file is moved
      // to archived dir, between these two sync points, used in
      // (DBTest,TransactionLogIteratorRace)
      TEST_SYNC_POINT("WalManager::GetWalsOfType:1");
      TEST_SYNC_POINT("WalManager::GetWalsOfType:2");

      uint64_t size_bytes;
      s = env_->GetFileSize(LogFileName(path, number), &size_bytes);

      if (!s.ok() && env_->FileExists(LogFileName(path, number)).IsNotFound()) {
        if (log_type == kAliveLogFile) {
          // re-try in case the alive log file has been moved to archive.
          std::string archived_file = ArchivedLogFileName(path, number);
          s = env_->GetFileSize(archived_file, &size_bytes);
          if (!s.ok() && env_->FileExists(archived_file).IsNotFound()) {
            // oops, the file just got deleted from archived dir! move on
            s = Status::OK();
            continue;
          }
        } else if (log_type == kArchivedLogFile) {
          // oops, the file just got deleted from archived dir! move on
          s = Status::OK();
          continue;
        }
      }
      if (!s.ok()) {
        return s;
      }

      log_files.push_back(std::unique_ptr<LogFile>(
          new LogFileImpl(number, log_type, sequence, size_bytes)));
    }
  }
  return status;
}

Status WalManager::RetainProbableWalFiles(VectorLogPtr& all_logs,
                                          const SequenceNumber target,
                                          VersionSet* version_set) {
  if (all_logs.empty()) {
    return Status::OK();
  }
  size_t start_index =
      std::upper_bound(
          all_logs.begin(), all_logs.end(), target,
          [](SequenceNumber target, const std::unique_ptr<LogFile>& a) {
            return target < a->StartSequence();
          }) -
      all_logs.begin();
  if (start_index == 0) {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "Outdated target in log files. Min seq=%" PRIu64
             ", Expected seq=%" PRIu64 ", Last flushed seq=%" PRIu64 ".",
             all_logs.front()->StartSequence(), target,
             version_set->LastSequence());
    ROCKS_LOG_INFO(db_options_.info_log, "[WalManger] %s", buf);
  } else {
    // The last wal file is always included
    all_logs.erase(all_logs.begin(), all_logs.begin() + start_index - 1);
  }
  return Status::OK();
}

Status WalManager::ReadFirstRecord(const WalFileType type,
                                   const uint64_t number,
                                   SequenceNumber* sequence) {
  *sequence = 0;
  if (type != kAliveLogFile && type != kArchivedLogFile) {
    ROCKS_LOG_ERROR(db_options_.info_log, "[WalManger] Unknown file type %s",
                    ToString(type).c_str());
    return Status::NotSupported("File Type Not Known " + ToString(type));
  }
  {
    MutexLock l(&read_first_record_cache_mutex_);
    auto itr = read_first_record_cache_.find(number);
    if (itr != read_first_record_cache_.end()) {
      *sequence = itr->second;
      return Status::OK();
    }
  }
  Status s;
  if (type == kAliveLogFile) {
    std::string fname = LogFileName(db_options_.wal_dir, number);
    s = ReadFirstLine(fname, number, sequence);
    if (env_->FileExists(fname).ok() && !s.ok()) {
      // return any error that is not caused by non-existing file
      return s;
    }
  }

  if (type == kArchivedLogFile || !s.ok()) {
    //  check if the file got moved to archive.
    std::string archived_file =
        ArchivedLogFileName(db_options_.wal_dir, number);
    s = ReadFirstLine(archived_file, number, sequence);
    // maybe the file was deleted from archive dir. If that's the case, return
    // Status::OK(). The caller with identify this as empty file because
    // *sequence == 0
    if (!s.ok() && env_->FileExists(archived_file).IsNotFound()) {
      return Status::OK();
    }
  }

  if (s.ok() && *sequence != 0) {
    MutexLock l(&read_first_record_cache_mutex_);
    read_first_record_cache_.insert({number, *sequence});
  }
  return s;
}

// the function returns status.ok() and sequence == 0 if the file exists, but is
// empty
Status WalManager::ReadFirstLine(const std::string& fname,
                                 const uint64_t number,
                                 SequenceNumber* sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;

    Status* status;
    bool ignore_error;  // true if db_options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) override {
      ROCKS_LOG_WARN(info_log, "[WalManager] %s%s: dropping %d bytes; %s",
                     (this->ignore_error ? "(ignoring error) " : ""), fname,
                     static_cast<int>(bytes), s.ToString().c_str());
      if (this->status->ok()) {
        // only keep the first error
        *this->status = s;
      }
    }
  };

  std::unique_ptr<SequentialFile> file;
  Status status = env_->NewSequentialFile(
      fname, &file, env_->OptimizeForLogRead(env_options_));
  std::unique_ptr<SequentialFileReader> file_reader(
      new SequentialFileReader(std::move(file), fname));

  if (!status.ok()) {
    return status;
  }

  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = db_options_.info_log.get();
  reporter.fname = fname.c_str();
  reporter.status = &status;
  reporter.ignore_error = !db_options_.paranoid_checks;
  log::Reader reader(db_options_.info_log, std::move(file_reader), &reporter,
                     true /*checksum*/, number, false /* retry_after_eof */);
  std::string scratch;
  Slice record;

  if (reader.ReadRecord(&record, &scratch) &&
      (status.ok() || !db_options_.paranoid_checks)) {
    if (record.size() < WriteBatchInternal::kHeader) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      // TODO read record's till the first no corrupt entry?
    } else {
      WriteBatch batch;
      WriteBatchInternal::SetContents(&batch, record);
      *sequence = WriteBatchInternal::Sequence(&batch);
      return Status::OK();
    }
  }

  // ReadRecord returns false on EOF, which means that the log file is empty. we
  // return status.ok() in that case and set sequence number to 0
  *sequence = 0;
  return status;
}

void WalManager::PurgeObsoleteWALFilesImpl(uint64_t now_seconds) {
  bool const ttl_enabled = db_options_.wal_ttl_seconds > 0;
  bool const size_limit_enabled = db_options_.wal_size_limit_mb > 0;

  VectorLogPtr files;
  auto s = GetSortedWalFiles(files, true /* allow_empty */);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log, "GetSortedWalFiles fail: %s",
                    s.ToString().c_str());
    assert(false);
    return;
  }
  uint64_t total_log_file_size = 0;

  auto delete_file = [&](uint64_t number, const std::string& file_path) {
    s = env_->DeleteFile(file_path);
    if (!s.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log, "Can't delete file: %s: %s",
                     file_path.c_str(), s.ToString().c_str());
    } else {
      MutexLock l(&read_first_record_cache_mutex_);
      read_first_record_cache_.erase(number);
      log_numbers_.erase(number);
    }
  };

  SequenceNumber guard_seqno = guard_seqno_.load(std::memory_order_relaxed);
  for (size_t i = 0; i < files.size(); ++i) {
    auto& f = files[i];
    if (f->Type() == WalFileType::kAliveLogFile ||
        (guard_seqno <= kMaxSequenceNumber &&
         (i + 1 == files.size() ||
          guard_seqno < files[i + 1]->StartSequence()))) {
      files.resize(i);
      break;
    }
    uint64_t number = f->LogNumber();
    std::string file_path = ArchivedLogFileName(db_options_.wal_dir, number);

    if (ttl_enabled) {
      uint64_t file_m_time;
      s = env_->GetFileModificationTime(file_path, &file_m_time);
      if (!s.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log, "Can't get file mod time: %s: %s",
                       file_path.c_str(), s.ToString().c_str());
      } else if (now_seconds - file_m_time > db_options_.wal_ttl_seconds) {
        f.reset();
        delete_file(number, file_path);
      }
    }
    if (size_limit_enabled && f) {
      uint64_t file_size = f->SizeFileBytes();
      if (file_size > 0) {
        total_log_file_size += file_size;
      } else {
        f.reset();
        delete_file(number, file_path);
      }
    }
  }

  if (total_log_file_size == 0) {
    return;
  }

  if (total_log_file_size <= db_options_.wal_size_limit_mb * 1024 * 1024) {
    return;
  }
  int64_t overflow_size =
      total_log_file_size - db_options_.wal_size_limit_mb * 1024 * 1024;

  for (auto& f : files) {
    if (!f) {
      continue;
    }
    uint64_t number = f->LogNumber();
    std::string file_path = ArchivedLogFileName(db_options_.wal_dir, number);
    int64_t file_size = int64_t(f->SizeFileBytes());
    overflow_size -= file_size;
    delete_file(number, file_path);
    if (overflow_size <= 0) {
      break;
    }
  }
}

#endif  // ROCKSDB_LITE
}  // namespace TERARKDB_NAMESPACE
