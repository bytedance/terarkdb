//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/db_impl.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <inttypes.h>

#include <set>
#include <unordered_set>

#include "db/event_helpers.h"
#include "db/memtable_list.h"
#include "util/file_util.h"
#include "util/sst_file_manager_impl.h"
#include "utilities/util/valvec.hpp"

namespace rocksdb {

uint64_t DBImpl::MinLogNumberToKeep() {
  if (allow_2pc()) {
    return versions_->min_log_number_to_keep_2pc();
  } else {
    return versions_->MinLogNumberWithUnflushedData();
  }
}

uint64_t DBImpl::MinObsoleteSstNumberToKeep() {
  mutex_.AssertHeld();
  if (!pending_outputs_.empty()) {
    return *pending_outputs_.begin();
  }
  return std::numeric_limits<uint64_t>::max();
}

namespace {
bool CompareCandidateFile(const JobContext::CandidateFileInfo& first,
                          const JobContext::CandidateFileInfo& second) {
  int c = Slice(first.file_name).compare(second.file_name);
  if (c == 0) {
    c = Slice(*first.file_path).compare(*second.file_path);
  }
  return c > 0;
}
bool EqualCandidateFile(const JobContext::CandidateFileInfo& first,
                        const JobContext::CandidateFileInfo& second) {
  int c = Slice(first.file_name).compare(second.file_name);
  if (c == 0) {
    c = Slice(*first.file_path).compare(*second.file_path);
  }
  return c == 0;
}
void PushCandidateFile(
    std::vector<JobContext::CandidateFileInfo>& candidate_files,
    std::string& file, const Slice& info_log_name_prefix,
    const std::string* path_ptr) {
  uint64_t number;
  FileType type;
  // If we cannot parse the file name, we skip;
  if (ParseFileName(file, &number, info_log_name_prefix, &type)) {
    candidate_files.emplace_back(
        JobContext::CandidateFileInfo{std::move(file), path_ptr});
  }
};
};  // namespace

// * Returns the list of live files in 'file_live'
// If it's doing full scan:
// * Returns the list of all files in the filesystem in
// 'full_scan_candidate_files'.
// Otherwise, gets obsolete files from VersionSet.
// no_full_scan = true -- never do the full scan using GetChildren()
// force = false -- don't force the full scan, except every
//  mutable_db_options_.delete_obsolete_files_period_micros
// force = true -- force the full scan
void DBImpl::FindObsoleteFiles(JobContext* job_context, bool force,
                               bool no_full_scan) {
  mutex_.AssertHeld();

  // if deletion is disabled, do nothing
  if (disable_delete_obsolete_files_ > 0) {
    return;
  }

  bool doing_the_full_scan = false;

  // logic for figuring out if we're doing the full scan
  if (no_full_scan || delete_obsolete_files_lock_) {
    // do nothing
  } else if (force ||
             mutable_db_options_.delete_obsolete_files_period_micros == 0) {
    doing_the_full_scan = true;
  } else {
    const uint64_t now_micros = env_->NowMicros();
    if ((delete_obsolete_files_last_run_ +
         mutable_db_options_.delete_obsolete_files_period_micros) <
        now_micros) {
      doing_the_full_scan = true;
      delete_obsolete_files_last_run_ = now_micros;
    }
  }

  if (doing_the_full_scan) {
    InfoLogPrefix info_log_prefix(!immutable_db_options_.db_log_dir.empty(),
                                  dbname_);
    delete_obsolete_files_lock_ = true;

    // Note that if cf_paths is not specified in the ColumnFamilyOptions
    // of a particular column family, we use db_paths as the cf_paths
    // setting. Hence, there can be multiple duplicates of files from db_paths
    // in the following code. The duplicate are removed while identifying
    // unique files in PurgeObsoleteFiles.
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      for (auto& db_path : cfd->ioptions()->cf_paths) {
        job_context->PushPath(db_path.path);
      }
    }

    for (auto other : files_grabbed_for_purge_) {
      job_context->skip_candidate_files.insert(
          job_context->skip_candidate_files.end(), other->begin(),
          other->end());
    }
    for (const auto& purge_file_info : purge_queue_) {
      job_context->skip_candidate_files.emplace_back(purge_file_info.number);
    }
    candidate_file_listener_.emplace_front(&job_context->skip_candidate_files);
    auto candidate_file_listener_it = candidate_file_listener_.begin();

    mutex_.Unlock();

    for (auto& db_path : immutable_db_options_.db_paths) {
      job_context->PushPath(db_path.path);
    }

    for (const auto& path : job_context->paths) {
      // set of all files in the directory. We'll exclude files that are still
      // alive in the subsequent processings.
      std::vector<std::string> files;
      env_->GetChildren(path, &files);  // Ignore errors
      for (std::string& file : files) {
        // TODO(icanadi) clean up this mess to avoid having one-off "/" prefixes
        file.insert(file.begin(), '/');
        PushCandidateFile(job_context->full_scan_candidate_files, file,
                          info_log_prefix.prefix, &path);
      }
    }

    // Add log files in wal_dir & db_log_dir
    for (auto& path :
         {immutable_db_options_.wal_dir, immutable_db_options_.db_log_dir}) {
      if (!path.empty() && path != dbname_) {
        auto path_ptr = job_context->PushPath(path);
        std::vector<std::string> log_files;
        env_->GetChildren(path, &log_files);  // Ignore errors
        for (std::string& log_file : log_files) {
          log_file.insert(log_file.begin(), '/');
          PushCandidateFile(job_context->full_scan_candidate_files, log_file,
                            info_log_prefix.prefix, path_ptr);
        }
      }
    }

    mutex_.Lock();
    candidate_file_listener_.erase(candidate_file_listener_it);
  }

  // don't delete files that might be currently written to from compaction
  // threads
  // Since job_context->min_pending_output is set, until file scan finishes,
  // mutex_ cannot be released. Otherwise, we might see no min_pending_output
  // here but later find newer generated unfinalized files while scanning.
  if (!pending_outputs_.empty()) {
    job_context->min_pending_output = *pending_outputs_.begin();
  } else {
    // delete all of them
    job_context->min_pending_output = std::numeric_limits<uint64_t>::max();
  }

  // Get obsolete files.  This function will also update the list of
  // pending files in VersionSet().
  versions_->GetObsoleteFiles(&job_context->sst_delete_files,
                              &job_context->manifest_delete_files,
                              job_context->min_pending_output);

  // Mark the elements in job_context->sst_delete_files as grabbedForPurge
  // so that other threads calling FindObsoleteFiles with full_scan=true
  // will not add these files to candidate list for purge.
  for (const auto& sst_to_del : job_context->sst_delete_files) {
    job_context->files_grabbed_for_purge.emplace_back(
        sst_to_del.metadata->fd.GetNumber());
  }
  for (const auto& manifest_to_del : job_context->manifest_delete_files) {
    uint64_t number;
    FileType type;
    // If we cannot parse the file name, we skip;
    if (ParseFileName(manifest_to_del, &number, &type)) {
      assert(type == kDescriptorFile);
      job_context->files_grabbed_for_purge.emplace_back(number);
    } else {
      assert(false);
    }
  }

  // store the current filenum, lognum, etc
  job_context->manifest_file_number = versions_->manifest_file_number();
  job_context->pending_manifest_file_number =
      versions_->pending_manifest_file_number();
  job_context->log_number = MinLogNumberToKeep();
#ifndef NDEBUG
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "GetMinLogNumberToKeep %" PRIu64 "\n",
                 job_context->log_number);
  ROCKS_LOG_INFO(immutable_db_options_.info_log, "alive_log_file content: ");
  for (auto _log : alive_log_files_) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, " %" PRIu64, _log.number);
  }
  ROCKS_LOG_INFO(immutable_db_options_.info_log, "logs_ content: ");
  for (auto _log : logs_) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, " %" PRIu64, _log.number);
  }
#endif  // !NDEBUG
  job_context->prev_log_number = versions_->prev_log_number();

  // logs_ is empty when called during recovery, in which case there can't yet
  // be any tracked obsolete logs
  if (!alive_log_files_.empty() && !logs_.empty()) {
    uint64_t min_log_number = job_context->log_number;
    size_t num_alive_log_files = alive_log_files_.size();
    // find newly obsoleted log files
    while (alive_log_files_.begin()->number < min_log_number) {
      auto& earliest = *alive_log_files_.begin();
      if (immutable_db_options_.recycle_log_file_num >
          log_recycle_files_.size()) {
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "adding log %" PRIu64 " to recycle list\n",
                       earliest.number);
        log_recycle_files_.push_back(earliest.number);
      } else {
        job_context->log_delete_files.push_back(earliest.number);
        job_context->files_grabbed_for_purge.emplace_back(earliest.number);
      }
      if (job_context->size_log_to_delete == 0) {
        job_context->prev_total_log_size = total_log_size_;
        job_context->num_alive_log_files = num_alive_log_files;
      }
      job_context->size_log_to_delete += earliest.size;
      total_log_size_ -= earliest.size;
      if (two_write_queues_) {
        log_write_mutex_.Lock();
      }
      alive_log_files_.pop_front();
      if (two_write_queues_) {
        log_write_mutex_.Unlock();
      }
      // Current log should always stay alive since it can't have
      // number < MinLogNumber().
      assert(alive_log_files_.size());
    }
    while (!logs_.empty() && logs_.front().number < min_log_number) {
      auto& log = logs_.front();
      if (log.getting_synced) {
        log_sync_cv_.Wait();
        // logs_ could have changed while we were waiting.
        continue;
      }
      logs_to_free_.push_back(log.ReleaseWriter());
      {
        InstrumentedMutexLock wl(&log_write_mutex_);
        logs_.pop_front();
      }
    }
    // Current log cannot be obsolete.
    assert(!logs_.empty());
  }

  // We're just cleaning up for DB::Write().
  assert(job_context->logs_to_free.empty());
  job_context->logs_to_free = logs_to_free_;
  job_context->log_recycle_files.assign(log_recycle_files_.begin(),
                                        log_recycle_files_.end());
  if (job_context->HaveSomethingToDelete()) {
    // Ref all version
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (!cfd->initialized()) {
        continue;
      }
      cfd->ForEachVersionList(
          [](void* arg, Version* v) {
            v->Ref();
            static_cast<JobContext*>(arg)->version_ref.emplace_back(v);
          },
          job_context);
    }
    files_grabbed_for_purge_.emplace(&job_context->files_grabbed_for_purge);
    for (auto listener : candidate_file_listener_) {
      listener->insert(listener->end(),
                       job_context->files_grabbed_for_purge.begin(),
                       job_context->files_grabbed_for_purge.end());
    }
    job_context->doing_the_full_scan = doing_the_full_scan;
    ++pending_purge_obsolete_files_;
  } else if (doing_the_full_scan) {
    delete_obsolete_files_lock_ = false;
  }
  logs_to_free_.clear();
}

// Delete obsolete files and log status and information of file deletion
void DBImpl::DeleteObsoleteFileImpl(int job_id, const std::string& fname,
                                    const std::string& path_to_sync,
                                    FileType type, uint64_t number) {
  TEST_SYNC_POINT("DBImpl::DeleteObsoleteFileImpl:BeforeDeletion");
  Status file_deletion_status;
  if (type == kTableFile) {
    file_deletion_status =
        DeleteSSTFile(&immutable_db_options_, fname, path_to_sync);
  } else if (type == kLogFile) {
    if (!immutable_db_options_.env->FileExists(fname).IsNotFound()) {
      file_deletion_status =
          DeleteWalFile(&immutable_db_options_, fname, path_to_sync);
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "[JOB %d] WalFile %s delete succeed", job_id, fname);
    } else {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "[JOB %d] WalFile %s already deleted", job_id, fname);
    }
  } else {
    file_deletion_status = env_->DeleteFile(fname);
  }
  TEST_SYNC_POINT_CALLBACK("DBImpl::DeleteObsoleteFileImpl:AfterDeletion",
                           &file_deletion_status);
  if (file_deletion_status.ok()) {
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "[JOB %d] Delete %s type=%d #%" PRIu64 " -- %s\n", job_id,
                    fname.c_str(), type, number,
                    file_deletion_status.ToString().c_str());
  } else if (env_->FileExists(fname).IsNotFound()) {
    ROCKS_LOG_INFO(
        immutable_db_options_.info_log,
        "[JOB %d] Tried to delete a non-existing file %s type=%d #%" PRIu64
        " -- %s\n",
        job_id, fname.c_str(), type, number,
        file_deletion_status.ToString().c_str());
  } else {
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "[JOB %d] Failed to delete %s type=%d #%" PRIu64 " -- %s\n",
                    job_id, fname.c_str(), type, number,
                    file_deletion_status.ToString().c_str());
  }
  if (type == kTableFile) {
    EventHelpers::LogAndNotifyTableFileDeletion(
        &event_logger_, job_id, number, fname, file_deletion_status, GetName(),
        immutable_db_options_.listeners);
  }
}

// Diffs the files listed in filenames and those that do not
// belong to live files are possibly removed. Also, removes all the
// files in sst_delete_files and log_delete_files.
// It is not necessary to hold the mutex when invoking this method.
void DBImpl::PurgeObsoleteFiles(JobContext& state, bool schedule_only) {
  TEST_SYNC_POINT("DBImpl::PurgeObsoleteFiles:Begin");
  // we'd better have sth to delete
  assert(state.HaveSomethingToDelete());

  // FindObsoleteFiles() should've populated this so nonzero
  assert(state.manifest_file_number != 0);

  // Now, convert live list to an unordered map, WITHOUT mutex held;
  // set is slow.
  std::unordered_set<uint64_t> file_live;
  for (auto v : state.version_ref) {
    auto vstorage = v->storage_info();
    for (int i = -1; i < vstorage->num_levels(); ++i) {
      for (auto f : vstorage->LevelFiles(i)) {
        file_live.emplace(f->fd.GetNumber());
      }
    }
  }
  std::unordered_set<uint64_t> log_recycle_files_set(
      state.log_recycle_files.begin(), state.log_recycle_files.end());

  auto& candidate_files = state.full_scan_candidate_files;
  candidate_files.reserve(
      candidate_files.size() + state.sst_delete_files.size() +
      state.log_delete_files.size() + state.manifest_delete_files.size());
  // We may ignore the dbname when generating the file names.
  const char* kDumbDbName = "";
  for (auto& file : state.sst_delete_files) {
    if (file.metadata->prop.is_blob_wal()) {
      candidate_files.emplace_back(JobContext::CandidateFileInfo{
          LogFileName(kDumbDbName, file.metadata->fd.GetNumber()),
          state.PushPath(immutable_db_options_.wal_dir)});
    } else {
      candidate_files.emplace_back(JobContext::CandidateFileInfo{
          MakeTableFileName(kDumbDbName, file.metadata->fd.GetNumber()),
          state.PushPath(file.path)});
    }
    if (file.metadata->table_reader_handle) {
      table_cache_->Release(file.metadata->table_reader_handle);
    }
    file.DeleteMetadata();
  }

  // TODO some log already deleted by upper
  for (auto file_num : state.log_delete_files) {
    if (file_num > 0) {
      candidate_files.emplace_back(JobContext::CandidateFileInfo{
          LogFileName(kDumbDbName, file_num),
          state.PushPath(immutable_db_options_.wal_dir)});
    }
  }
  for (auto filename : state.manifest_delete_files) {
    PushCandidateFile(candidate_files, filename, Slice(),
                      state.PushPath(dbname_));
  }

  // dedup state.candidate_files so we don't try to delete the same
  // file twice
  terark::sort_a(candidate_files, CompareCandidateFile);
  candidate_files.resize(
      terark::unique_a(candidate_files, EqualCandidateFile));
  terark::sort_a(state.skip_candidate_files);

  if (state.prev_total_log_size > 0) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[JOB %d] Try to delete WAL files size %" PRIu64
                   ", prev total WAL file size %" PRIu64
                   ", number of live WAL files %" ROCKSDB_PRIszt ".\n",
                   state.job_id, state.size_log_to_delete,
                   state.prev_total_log_size, state.num_alive_log_files);
  }

  std::vector<std::string> old_info_log_files;
  InfoLogPrefix info_log_prefix(!immutable_db_options_.db_log_dir.empty(),
                                dbname_);

  // File numbers of most recent two OPTIONS file in candidate_files (found in
  // previos FindObsoleteFiles(full_scan=true))
  // At this point, there must not be any duplicate file numbers in
  // candidate_files.
  uint64_t optsfile_num1 = std::numeric_limits<uint64_t>::min();
  uint64_t optsfile_num2 = std::numeric_limits<uint64_t>::min();
  for (size_t i = 0; i < candidate_files.size();) {
    const auto& candidate_file = candidate_files[i];
    const std::string& fname = candidate_file.file_name;
    uint64_t number;
    FileType type;
    // 1. remove if we cannot parse
    // 2. remove if file number in skip_candidate_files
    if (!ParseFileName(fname, &number, info_log_prefix.prefix, &type) ||
        (type != kInfoLogFile && terark::binary_search_a(
                                     state.skip_candidate_files, number))) {
      // erase this candidate_files
      candidate_files[i] = std::move(candidate_files.back());
      candidate_files.pop_back();
      continue;
    }
    if (type == kOptionsFile) {
      if (number > optsfile_num1) {
        optsfile_num2 = optsfile_num1;
        optsfile_num1 = number;
      } else if (number > optsfile_num2) {
        optsfile_num2 = number;
      }
    }
    ++i;
  }

  for (const auto& candidate_file : candidate_files) {
    const std::string& to_delete = candidate_file.file_name;
    uint64_t number;
    FileType type;
    if (!ParseFileName(to_delete, &number, info_log_prefix.prefix, &type)) {
      assert(false);
      continue;
    }

    bool keep = true;
    switch (type) {
      case kLogFile:
        keep = ((number >= state.log_number) ||
                (number == state.prev_log_number) ||
                (file_live.find(number) != file_live.end()) ||
                (log_recycle_files_set.find(number) !=
                 log_recycle_files_set.end()));
#ifndef NDEBUG
        if ((number >= state.log_number) || (number == state.prev_log_number)) {
          ROCKS_LOG_INFO(immutable_db_options_.info_log,
                         "[JOB %d] Try to delete WAL Number %" PRIu64
                         ",  decision is keep, reason is number, %" PRIu64
                         " %" PRIu64 " %" PRIu64,
                         state.job_id, number, state.log_number,
                         state.prev_log_number);
        }
        if (file_live.find(number) != file_live.end()) {
          ROCKS_LOG_INFO(immutable_db_options_.info_log,
                         "[JOB %d] Try to delete WAL Number %" PRIu64
                         ",  decision is keep, reason is file_live",
                         state.job_id, number);
        }
        if (log_recycle_files_set.find(number) != log_recycle_files_set.end()) {
          ROCKS_LOG_INFO(immutable_db_options_.info_log,
                         "[JOB %d] Try to delete WAL Number %" PRIu64
                         ",  decision is keep, reason is recycle",
                         state.job_id, number);
        }
#endif  // !NDEBUG
        break;
      case kDescriptorFile:
        // Keep my manifest file, and any newer incarnations'
        // (can happen during manifest roll)
        keep = (number >= state.manifest_file_number);
        break;
      case kTableFile:
        // If the second condition is not there, this makes
        // DontDeletePendingOutputs fail
        keep = (file_live.find(number) != file_live.end()) ||
               number >= state.min_pending_output;
        break;
      case kTempFile:
        // Any temp files that are currently being written to must
        // be recorded in pending_outputs_, which is inserted into "live".
        // Also, SetCurrentFile creates a temp file when writing out new
        // manifest, which is equal to state.pending_manifest_file_number. We
        // should not delete that file
        //
        // TODO(yhchiang): carefully modify the third condition to safely
        //                 remove the temp options files.
        keep = (file_live.find(number) != file_live.end()) ||
               (number == state.pending_manifest_file_number) ||
               (to_delete.find(kOptionsFileNamePrefix) != std::string::npos);
        break;
      case kInfoLogFile:
        keep = true;
        if (number != 0) {
          old_info_log_files.push_back(to_delete);
        }
        break;
      case kOptionsFile:
        keep = (number >= optsfile_num2);
        TEST_SYNC_POINT_CALLBACK(
            "DBImpl::PurgeObsoleteFiles:CheckOptionsFiles:1",
            reinterpret_cast<void*>(&number));
        TEST_SYNC_POINT_CALLBACK(
            "DBImpl::PurgeObsoleteFiles:CheckOptionsFiles:2",
            reinterpret_cast<void*>(&keep));
        break;
      case kCurrentFile:
      case kSocketFile:
      case kDBLockFile:
      case kIdentityFile:
      case kMetaDatabase:
        keep = true;
        break;
    }

    if (keep) {
      continue;
    }

    std::string fname;
    std::string dir_to_sync;
    if (type == kTableFile) {
      // evict from cache
      TableCache::Evict(table_cache_.get(), number);
      fname = MakeTableFileName(*candidate_file.file_path, number);
      dir_to_sync = *candidate_file.file_path;
    } else {
      dir_to_sync =
          (type == kLogFile) ? immutable_db_options_.wal_dir : dbname_;
      fname = dir_to_sync +
              ((!dir_to_sync.empty() && dir_to_sync.back() == '/') ||
                       (!to_delete.empty() && to_delete.front() == '/')
                   ? ""
                   : "/") +
              to_delete;
    }
    if (type == kLogFile) {
      versions_->ReleaseWalMeta(number);
    }
#ifndef ROCKSDB_LITE
    if (type == kLogFile && (immutable_db_options_.wal_ttl_seconds > 0 ||
                             immutable_db_options_.wal_size_limit_mb > 0)) {
      wal_manager_.ArchiveWALFile(fname, number);
      continue;
    }
#endif  // !ROCKSDB_LITE

    Status file_deletion_status;
    if (schedule_only) {
      InstrumentedMutexLock guard_lock(&mutex_);
      SchedulePendingPurge(fname, dir_to_sync, type, number, state.job_id);
    } else {
      DeleteObsoleteFileImpl(state.job_id, fname, dir_to_sync, type, number);
    }
  }

  {
    // After purging obsolete files, remove them from files_grabbed_for_purge_.
    InstrumentedMutexLock guard_lock(&mutex_);
    files_grabbed_for_purge_.erase(&state.files_grabbed_for_purge);
  }

  // Delete old info log files.
  size_t old_info_log_file_count = old_info_log_files.size();
  if (old_info_log_file_count != 0 &&
      old_info_log_file_count >= immutable_db_options_.keep_log_file_num) {
    std::sort(old_info_log_files.begin(), old_info_log_files.end());
    size_t end =
        old_info_log_file_count - immutable_db_options_.keep_log_file_num;
    for (unsigned int i = 0; i <= end; i++) {
      std::string& to_delete = old_info_log_files.at(i);
      std::string full_path_to_delete =
          (immutable_db_options_.db_log_dir.empty()
               ? dbname_
               : immutable_db_options_.db_log_dir) +
          "/" + to_delete;
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "[JOB %d] Delete info log file %s\n", state.job_id,
                     full_path_to_delete.c_str());
      Status s = env_->DeleteFile(full_path_to_delete);
      if (!s.ok()) {
        if (env_->FileExists(full_path_to_delete).IsNotFound()) {
          ROCKS_LOG_INFO(
              immutable_db_options_.info_log,
              "[JOB %d] Tried to delete non-existing info log file %s FAILED "
              "-- %s\n",
              state.job_id, to_delete.c_str(), s.ToString().c_str());
        } else {
          ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                          "[JOB %d] Delete info log file %s FAILED -- %s\n",
                          state.job_id, to_delete.c_str(),
                          s.ToString().c_str());
        }
      }
    }
  }
#ifndef ROCKSDB_LITE
  if (!schedule_only) {
    wal_manager_.PurgeObsoleteWALFiles();
  }
#endif  // ROCKSDB_LITE
  LogFlush(immutable_db_options_.info_log);
  InstrumentedMutexLock l(&mutex_);
  if (state.doing_the_full_scan) {
    delete_obsolete_files_lock_ = false;
  }
  --pending_purge_obsolete_files_;
  assert(pending_purge_obsolete_files_ >= 0);
  if (pending_purge_obsolete_files_ == 0) {
    bg_cv_.SignalAll();
  }
  state.CleanVersionRef(&mutex_);
  TEST_SYNC_POINT("DBImpl::PurgeObsoleteFiles:End");
}

void DBImpl::DeleteObsoleteFiles() {
  mutex_.AssertHeld();
  JobContext job_context(next_job_id_.fetch_add(1));
  FindObsoleteFiles(&job_context, true);

  mutex_.Unlock();
  if (job_context.HaveSomethingToDelete()) {
    PurgeObsoleteFiles(job_context);
  }
  job_context.Clean(&mutex_);
  mutex_.Lock();
}

uint64_t FindMinPrepLogReferencedByMemTable(
    VersionSet* vset, const ColumnFamilyData* cfd_to_flush,
    const autovector<MemTable*>& memtables_to_flush) {
  uint64_t min_log = 0;

  // we must look through the memtables for two phase transactions
  // that have been committed but not yet flushed
  for (auto loop_cfd : *vset->GetColumnFamilySet()) {
    if (loop_cfd->IsDropped() || loop_cfd == cfd_to_flush) {
      continue;
    }

    auto log = loop_cfd->imm()->PrecomputeMinLogContainingPrepSection(
        memtables_to_flush);

    if (log > 0 && (min_log == 0 || log < min_log)) {
      min_log = log;
    }

    log = loop_cfd->mem()->GetMinLogContainingPrepSection();

    if (log > 0 && (min_log == 0 || log < min_log)) {
      min_log = log;
    }
  }

  return min_log;
}

uint64_t PrecomputeMinLogNumberToKeep(
    VersionSet* vset, const ColumnFamilyData& cfd_to_flush,
    autovector<VersionEdit*> edit_list,
    const autovector<MemTable*>& memtables_to_flush,
    LogsWithPrepTracker* prep_tracker) {
  assert(vset != nullptr);
  assert(prep_tracker != nullptr);
  // Calculate updated min_log_number_to_keep
  // Since the function should only be called in 2pc mode, log number in
  // the version edit should be sufficient.

  // Precompute the min log number containing unflushed data for the column
  // family being flushed (`cfd_to_flush`).
  uint64_t cf_min_log_number_to_keep = 0;
  for (auto& e : edit_list) {
    if (e->has_log_number()) {
      cf_min_log_number_to_keep =
          std::max(cf_min_log_number_to_keep, e->log_number());
    }
  }
  if (cf_min_log_number_to_keep == 0) {
    // No version edit contains information on log number. The log number
    // for this column family should stay the same as it is.
    cf_min_log_number_to_keep = cfd_to_flush.GetLogNumber();
  }

  // Get min log number containing unflushed data for other column families.
  uint64_t min_log_number_to_keep =
      vset->PreComputeMinLogNumberWithUnflushedData(&cfd_to_flush);
  if (cf_min_log_number_to_keep != 0) {
    min_log_number_to_keep =
        std::min(cf_min_log_number_to_keep, min_log_number_to_keep);
  }

  // if are 2pc we must consider logs containing prepared
  // sections of outstanding transactions.
  //
  // We must check min logs with outstanding prep before we check
  // logs references by memtables because a log referenced by the
  // first data structure could transition to the second under us.
  //
  // TODO: iterating over all column families under db mutex.
  // should find more optimal solution
  auto min_log_in_prep_heap =
      prep_tracker->FindMinLogContainingOutstandingPrep();

  if (min_log_in_prep_heap != 0 &&
      min_log_in_prep_heap < min_log_number_to_keep) {
    min_log_number_to_keep = min_log_in_prep_heap;
  }

  uint64_t min_log_refed_by_mem = FindMinPrepLogReferencedByMemTable(
      vset, &cfd_to_flush, memtables_to_flush);

  if (min_log_refed_by_mem != 0 &&
      min_log_refed_by_mem < min_log_number_to_keep) {
    min_log_number_to_keep = min_log_refed_by_mem;
  }
  return min_log_number_to_keep;
}

}  // namespace rocksdb
