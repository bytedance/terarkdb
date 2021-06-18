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

#include "db/builder.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "db/map_builder.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_updater.h"
#include "monitoring/thread_status_util.h"
#include "rocksdb/terark_namespace.h"
#include "util/sst_file_manager_impl.h"
#include "util/sync_point.h"

namespace TERARKDB_NAMESPACE {

bool DBImpl::EnoughRoomForCompaction(
    ColumnFamilyData* cfd, const std::vector<CompactionInputFiles>& inputs,
    bool* sfm_reserved_compact_space, LogBuffer* log_buffer) {
  // Check if we have enough room to do the compaction
  bool enough_room = true;
#ifndef ROCKSDB_LITE
  auto sfm = static_cast<SstFileManagerImpl*>(
      immutable_db_options_.sst_file_manager.get());
  if (sfm) {
    // Pass the current bg_error_ to SFM so it can decide what checks to
    // perform. If this DB instance hasn't seen any error yet, the SFM can be
    // optimistic and not do disk space checks
    enough_room =
        sfm->EnoughRoomForCompaction(cfd, inputs, error_handler_.GetBGError());
    if (enough_room) {
      *sfm_reserved_compact_space = true;
    }
  }
#else
  (void)cfd;
  (void)inputs;
  (void)sfm_reserved_compact_space;
#endif  // ROCKSDB_LITE
  if (!enough_room) {
    // Just in case tests want to change the value of enough_room
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::BackgroundCompaction():CancelledCompaction", &enough_room);
    ROCKS_LOG_BUFFER(log_buffer,
                     "Cancelled compaction because not enough room");
    RecordTick(stats_, COMPACTION_CANCELLED, 1);
  }
  return enough_room;
}

Status DBImpl::SyncClosedLogs(JobContext* job_context) {
  TEST_SYNC_POINT("DBImpl::SyncClosedLogs:Start");
  mutex_.AssertHeld();
  autovector<log::Writer*, 1> logs_to_sync;
  uint64_t current_log_number = logfile_number_;
  while (logs_.front().number < current_log_number &&
         logs_.front().getting_synced) {
    log_sync_cv_.Wait();
  }
  for (auto it = logs_.begin();
       it != logs_.end() && it->number < current_log_number; ++it) {
    auto& log = *it;
    assert(!log.getting_synced);
    log.getting_synced = true;
    logs_to_sync.push_back(log.writer);
  }

  Status s;
  if (!logs_to_sync.empty()) {
    mutex_.Unlock();

    for (log::Writer* log : logs_to_sync) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "[JOB %d] Syncing log #%" PRIu64, job_context->job_id,
                     log->get_log_number());
      s = log->file()->Sync(immutable_db_options_.use_fsync);
      if (!s.ok()) {
        break;
      }
    }
    if (s.ok()) {
      s = directories_.GetWalDir()->Fsync();
    }

    mutex_.Lock();

    // "number <= current_log_number - 1" is equivalent to
    // "number < current_log_number".
    MarkLogsSynced(current_log_number - 1, true, s);
    if (!s.ok()) {
      error_handler_.SetBGError(s, BackgroundErrorReason::kFlush);
      TEST_SYNC_POINT("DBImpl::SyncClosedLogs:Failed");
      return s;
    }
  }
  return s;
}

Status DBImpl::FlushMemTableToOutputFile(
    ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    bool* made_progress, JobContext* job_context,
    SuperVersionContext* superversion_context, LogBuffer* log_buffer,
    VersionEdit::ApplyCallback apply_callback) {
  mutex_.AssertHeld();

  SequenceNumber earliest_write_conflict_snapshot;
  std::vector<SequenceNumber> snapshot_seqs =
      snapshots_.GetAll(&earliest_write_conflict_snapshot);

  auto snapshot_checker = snapshot_checker_.get();
  if (use_custom_gc_ && snapshot_checker == nullptr) {
    snapshot_checker = DisableGCSnapshotChecker::Instance();
  }
  auto flushes = num_running_flushes() - 1;
  auto max_flushes = std::max(flushes, GetBGJobLimits().max_flushes - 1);
  assert(flushes >= 0 && max_flushes >= 0);
  double flush_load = -1. * flushes / max_flushes;
  FlushJob flush_job(
      dbname_, cfd, immutable_db_options_, mutable_cf_options,
      nullptr /* memtable_id */, env_options_for_compaction_, versions_.get(),
      &mutex_, &shutting_down_, snapshot_seqs, earliest_write_conflict_snapshot,
      snapshot_checker, job_context, log_buffer, directories_.GetDbDir(),
      GetDataDir(cfd, 0U),
      GetCompressionFlush(*cfd->ioptions(), mutable_cf_options), stats_,
      &event_logger_, mutable_cf_options.report_bg_io_stats,
      true /* sync_output_directory */, true /* write_manifest */, flush_load);

  TEST_SYNC_POINT("DBImpl::FlushMemTableToOutputFile:BeforePickMemtables");
  flush_job.PickMemTable();
  TEST_SYNC_POINT("DBImpl::FlushMemTableToOutputFile:AfterPickMemtables");

  if (apply_callback) {
    if (!flush_job.GetMemTables().empty()) {
      flush_job.GetMemTables().front()->GetEdits()->SetApplyCallback(
          apply_callback);
    } else {
      apply_callback(Status::OK());
    }
  }

#ifndef ROCKSDB_LITE
  // may temporarily unlock and lock the mutex.
  NotifyOnFlushBegin(cfd, mutable_cf_options, job_context->job_id);
#endif  // ROCKSDB_LITE

  Status s;
  if (logfile_number_ > 0 &&
      versions_->GetColumnFamilySet()->NumberOfColumnFamilies() > 1) {
    // If there are more than one column families, we need to make sure that
    // all the log files except the most recent one are synced. Otherwise if
    // the host crashes after flushing and before WAL is persistent, the
    // flushed SST may contain data from write batches whose updates to
    // other column families are missing.
    // SyncClosedLogs() may unlock and re-lock the db_mutex.
    s = SyncClosedLogs(job_context);
  } else {
    TEST_SYNC_POINT("DBImpl::SyncClosedLogs:Skip");
  }

  // Within flush_job.Run, rocksdb may call event listener to notify
  // file creation and deletion.
  //
  // Note that flush_job.Run will unlock and lock the db_mutex,
  // and EventListener callback will be called when the db_mutex
  // is unlocked by the current thread.
  if (s.ok()) {
    s = flush_job.Run(&logs_with_prep_tracker_);
  } else {
    flush_job.Cancel();
  }

  if (s.ok()) {
    InstallSuperVersionAndScheduleWork(cfd, superversion_context,
                                       mutable_cf_options);
    if (made_progress) {
      *made_progress = true;
    }
    VersionStorageInfo::LevelSummaryStorage tmp;
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Level summary: %s\n",
                     cfd->GetName().c_str(),
                     cfd->current()->storage_info()->LevelSummary(&tmp));
  }

  if (!s.ok() && !s.IsShutdownInProgress()) {
    Status new_bg_error = s;
    error_handler_.SetBGError(new_bg_error, BackgroundErrorReason::kFlush);
  }
  if (s.ok()) {
#ifndef ROCKSDB_LITE
    // may temporarily unlock and lock the mutex.
    NotifyOnFlushCompleted(cfd, flush_job.GetFileMetas(), mutable_cf_options,
                           job_context->job_id, flush_job.GetTableProperties());
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    if (sfm) {
      // Notify sst_file_manager that a new file was added
      for (auto file_meta : flush_job.GetFileMetas()) {
        std::string file_path = MakeTableFileName(
            cfd->ioptions()->cf_paths[0].path, file_meta.fd.GetNumber());
        sfm->OnAddFile(file_path);
        if (sfm->IsMaxAllowedSpaceReached()) {
          Status new_bg_error =
              Status::SpaceLimit("Max allowed space was reached");
          TEST_SYNC_POINT_CALLBACK(
              "DBImpl::FlushMemTableToOutputFile:MaxAllowedSpaceReached",
              &new_bg_error);
          error_handler_.SetBGError(new_bg_error,
                                    BackgroundErrorReason::kFlush);
        }
      }
    }
#endif  // ROCKSDB_LITE
  }
  if (flush_job.IsInstallTimeout()) {
    FlushRequest flush_req;
    GenerateFlushRequest({cfd}, &flush_req);
    SchedulePendingFlush(flush_req, FlushReason::kInstallTimeout);
  }
  return s;
}

Status DBImpl::FlushMemTablesToOutputFiles(
    const autovector<BGFlushArg>& bg_flush_args, bool* made_progress,
    JobContext* job_context, LogBuffer* log_buffer) {
  assert(!bg_flush_args.empty());
  if (immutable_db_options_.atomic_flush) {
    auto pending_outputs_inserted_elem =
        CaptureCurrentFileNumberInPendingOutputs();
    auto s = AtomicFlushMemTablesToOutputFiles(bg_flush_args, made_progress,
                                               job_context, log_buffer);
    ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);
    return s;
  }
  struct CleanPendingOutputs {
    std::list<uint64_t>::iterator pending_outputs_inserted_elem;
    DBImpl* self;
    size_t count;
  };
  VersionEdit::ApplyCallback apply_callback;
  apply_callback.callback = [](void* args, const Status&) {
    auto args_pack = static_cast<CleanPendingOutputs*>(args);
    assert(args_pack->count > 0);
    if (--args_pack->count == 0) {
      args_pack->self->ReleaseFileNumberFromPendingOutputs(
          args_pack->pending_outputs_inserted_elem);
      delete args_pack;
    }
  };
  apply_callback.args = new CleanPendingOutputs{
      CaptureCurrentFileNumberInPendingOutputs(), this, bg_flush_args.size()};

  Status status;
  for (auto& arg : bg_flush_args) {
    ColumnFamilyData* cfd = arg.cfd_;
    MutableCFOptions mutable_cf_options = *cfd->GetLatestMutableCFOptions();
    SuperVersionContext* superversion_context = arg.superversion_context_;
    Status s = FlushMemTableToOutputFile(cfd, mutable_cf_options, made_progress,
                                         job_context, superversion_context,
                                         log_buffer, apply_callback);
    if (!s.ok()) {
      status = s;
      if (!s.IsShutdownInProgress()) {
        // At this point, DB is not shutting down, nor is cfd dropped.
        // Something is wrong, thus we break out of the loop.
        break;
      }
    }
  }
  return status;
}

/*
 * Atomically flushes multiple column families.
 *
 * For each column family, all memtables with ID smaller than or equal to the
 * ID specified in bg_flush_args will be flushed. Only after all column
 * families finish flush will this function commit to MANIFEST. If any of the
 * column families are not flushed successfully, this function does not have
 * any side-effect on the state of the database.
 */
Status DBImpl::AtomicFlushMemTablesToOutputFiles(
    const autovector<BGFlushArg>& bg_flush_args, bool* made_progress,
    JobContext* job_context, LogBuffer* log_buffer) {
  mutex_.AssertHeld();

  autovector<ColumnFamilyData*> cfds;
  for (const auto& arg : bg_flush_args) {
    cfds.emplace_back(arg.cfd_);
  }

#ifndef NDEBUG
  for (const auto cfd : cfds) {
    assert(cfd->imm()->NumNotFlushed() != 0);
    assert(cfd->imm()->IsFlushPending());
  }
#endif /* !NDEBUG */

  SequenceNumber earliest_write_conflict_snapshot;
  std::vector<SequenceNumber> snapshot_seqs =
      snapshots_.GetAll(&earliest_write_conflict_snapshot);

  auto snapshot_checker = snapshot_checker_.get();
  if (use_custom_gc_ && snapshot_checker == nullptr) {
    snapshot_checker = DisableGCSnapshotChecker::Instance();
  }
  autovector<Directory*> distinct_output_dirs;
  std::vector<FlushJob> jobs;
  std::vector<MutableCFOptions> all_mutable_cf_options;
  int num_cfs = static_cast<int>(cfds.size());
  all_mutable_cf_options.reserve(num_cfs);
  auto flushes = num_running_flushes() + num_cfs - 2;
  auto max_flushes = std::max(flushes, GetBGJobLimits().max_flushes - 1);
  assert(flushes >= 0 && max_flushes >= 0);
  double flush_load = -1. * flushes / max_flushes;
  for (int i = 0; i < num_cfs; ++i) {
    auto cfd = cfds[i];
    Directory* data_dir = GetDataDir(cfd, 0U);

    // Add to distinct output directories if eligible. Use linear search. Since
    // the number of elements in the vector is not large, performance should be
    // tolerable.
    bool found = false;
    for (const auto dir : distinct_output_dirs) {
      if (dir == data_dir) {
        found = true;
        break;
      }
    }
    if (!found) {
      distinct_output_dirs.emplace_back(data_dir);
    }

    all_mutable_cf_options.emplace_back(*cfd->GetLatestMutableCFOptions());
    const MutableCFOptions& mutable_cf_options = all_mutable_cf_options.back();
    const uint64_t* max_memtable_id = &(bg_flush_args[i].max_memtable_id_);
    jobs.emplace_back(
        dbname_, cfds[i], immutable_db_options_, mutable_cf_options,
        max_memtable_id, env_options_for_compaction_, versions_.get(), &mutex_,
        &shutting_down_, snapshot_seqs, earliest_write_conflict_snapshot,
        snapshot_checker, job_context, log_buffer, directories_.GetDbDir(),
        data_dir, GetCompressionFlush(*cfd->ioptions(), mutable_cf_options),
        stats_, &event_logger_, mutable_cf_options.report_bg_io_stats,
        false /* sync_output_directory */, false /* write_manifest */,
        flush_load);
    jobs.back().PickMemTable();
  }

  Status s;
  assert(num_cfs == static_cast<int>(jobs.size()));

#ifndef ROCKSDB_LITE
  for (int i = 0; i != num_cfs; ++i) {
    const MutableCFOptions& mutable_cf_options = all_mutable_cf_options.at(i);
    // may temporarily unlock and lock the mutex.
    NotifyOnFlushBegin(cfds[i], mutable_cf_options, job_context->job_id);
  }
#endif /* !ROCKSDB_LITE */

  if (logfile_number_ > 0) {
    // TODO (yanqin) investigate whether we should sync the closed logs for
    // single column family case.
    s = SyncClosedLogs(job_context);
  }

  // exec_status stores the execution status of flush_jobs as
  // <bool /* executed */, Status /* status code */>
  autovector<std::pair<bool, Status>> exec_status;
  for (int i = 0; i != num_cfs; ++i) {
    // Initially all jobs are not executed, with status OK.
    exec_status.emplace_back(false, Status::OK());
  }

  if (s.ok()) {
    // TODO (yanqin): parallelize jobs with threads.
    for (int i = 1; i != num_cfs; ++i) {
      exec_status[i].second = jobs[i].Run(&logs_with_prep_tracker_);
      exec_status[i].first = true;
    }
    if (num_cfs > 1) {
      TEST_SYNC_POINT(
          "DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:1");
      TEST_SYNC_POINT(
          "DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:2");
    }
    exec_status[0].second = jobs[0].Run(&logs_with_prep_tracker_);
    exec_status[0].first = true;

    Status error_status;
    for (const auto& e : exec_status) {
      if (!e.second.ok()) {
        s = e.second;
        if (!e.second.IsShutdownInProgress()) {
          // If a flush job did not return OK, and the CF is not dropped, and
          // the DB is not shutting down, then we have to return this result to
          // caller later.
          error_status = e.second;
        }
      }
    }

    s = error_status.ok() ? s : error_status;
  }

  if (s.ok() || s.IsShutdownInProgress()) {
    // Sync on all distinct output directories.
    for (auto dir : distinct_output_dirs) {
      if (dir != nullptr) {
        s = dir->Fsync();
        if (!s.ok()) {
          break;
        }
      }
    }
  }

  if (s.ok()) {
    auto wait_to_install_func = [&]() {
      bool ready = true;
      for (size_t i = 0; i != cfds.size(); ++i) {
        const auto& mems = jobs[i].GetMemTables();
        if (cfds[i]->IsDropped()) {
          // If the column family is dropped, then do not wait.
          continue;
        } else if (!mems.empty() &&
                   cfds[i]->imm()->GetEarliestMemTableID() < mems[0]->GetID()) {
          // If a flush job needs to install the flush result for mems and
          // mems[0] is not the earliest memtable, it means another thread must
          // be installing flush results for the same column family, then the
          // current thread needs to wait.
          ready = false;
          break;
        } else if (mems.empty() && cfds[i]->imm()->GetEarliestMemTableID() <=
                                       bg_flush_args[i].max_memtable_id_) {
          // If a flush job does not need to install flush results, then it has
          // to wait until all memtables up to max_memtable_id_ (inclusive) are
          // installed.
          ready = false;
          break;
        }
      }
      return ready;
    };

    bool resuming_from_bg_err = error_handler_.IsDBStopped();
    while ((!error_handler_.IsDBStopped() ||
            error_handler_.GetRecoveryError().ok()) &&
           !wait_to_install_func()) {
      atomic_flush_install_cv_.Wait();
    }

    s = resuming_from_bg_err ? error_handler_.GetRecoveryError()
                             : error_handler_.GetBGError();
  }

  if (s.ok()) {
    autovector<ColumnFamilyData*> tmp_cfds;
    autovector<const autovector<MemTable*>*> mems_list;
    autovector<const MutableCFOptions*> mutable_cf_options_list;
    autovector<const FileMetaData*> tmp_file_meta;
    for (int i = 0; i != num_cfs; ++i) {
      const auto& mems = jobs[i].GetMemTables();
      if (!cfds[i]->IsDropped() && !mems.empty()) {
        tmp_cfds.emplace_back(cfds[i]);
        mems_list.emplace_back(&mems);
        mutable_cf_options_list.emplace_back(&all_mutable_cf_options[i]);
        tmp_file_meta.emplace_back(&jobs[i].GetFileMetas()[0]);
      }
    }

    s = InstallMemtableAtomicFlushResults(
        nullptr /* imm_lists */, tmp_cfds, mutable_cf_options_list, mems_list,
        versions_.get(), &mutex_, tmp_file_meta,
        &job_context->memtables_to_free, directories_.GetDbDir(), log_buffer);
  }

  if (s.ok() || s.IsShutdownInProgress()) {
    assert(num_cfs ==
           static_cast<int>(job_context->superversion_contexts.size()));
    for (int i = 0; i != num_cfs; ++i) {
      if (cfds[i]->IsDropped()) {
        continue;
      }
      InstallSuperVersionAndScheduleWork(cfds[i],
                                         &job_context->superversion_contexts[i],
                                         all_mutable_cf_options[i]);
      VersionStorageInfo::LevelSummaryStorage tmp;
      ROCKS_LOG_BUFFER(log_buffer, "[%s] Level summary: %s\n",
                       cfds[i]->GetName().c_str(),
                       cfds[i]->current()->storage_info()->LevelSummary(&tmp));
    }
    if (made_progress) {
      *made_progress = true;
    }
#ifndef ROCKSDB_LITE
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    for (int i = 0; i != num_cfs; ++i) {
      if (cfds[i]->IsDropped()) {
        continue;
      }
      NotifyOnFlushCompleted(cfds[i], jobs[i].GetFileMetas(),
                             all_mutable_cf_options[i], job_context->job_id,
                             jobs[i].GetTableProperties());
      if (sfm) {
        for (auto file_meta : jobs[i].GetFileMetas()) {
          std::string file_path = MakeTableFileName(
              cfds[i]->ioptions()->cf_paths[0].path, file_meta.fd.GetNumber());
          sfm->OnAddFile(file_path);
          if (sfm->IsMaxAllowedSpaceReached() &&
              error_handler_.GetBGError().ok()) {
            Status new_bg_error =
                Status::SpaceLimit("Max allowed space was reached");
            error_handler_.SetBGError(new_bg_error,
                                      BackgroundErrorReason::kFlush);
          }
        }
      }
    }
#endif  // ROCKSDB_LITE
  }

  // Need to undo atomic flush if something went wrong, i.e. s is not OK and
  // it is not because of CF drop.
  if (!s.ok() && !s.IsShutdownInProgress()) {
    // Have to cancel the flush jobs that have NOT executed because we need to
    // unref the versions.
    for (int i = 0; i != num_cfs; ++i) {
      if (!exec_status[i].first) {
        jobs[i].Cancel();
      }
    }
    for (int i = 0; i != num_cfs; ++i) {
      if (exec_status[i].first && exec_status[i].second.ok()) {
        auto& mems = jobs[i].GetMemTables();
        uint64_t file_number = jobs[i].GetFileMetas().empty()
                                   ? 0
                                   : jobs[i].GetFileMetas()[0].fd.GetNumber();
        cfds[i]->imm()->RollbackMemtableFlush(mems, file_number, s);
      }
    }
    Status new_bg_error = s;
    error_handler_.SetBGError(new_bg_error, BackgroundErrorReason::kFlush);
  }

  return s;
}

void DBImpl::NotifyOnFlushBegin(ColumnFamilyData* cfd,
                                const MutableCFOptions& mutable_cf_options,
                                int job_id) {
#ifndef ROCKSDB_LITE
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  bool triggered_writes_slowdown =
      (cfd->current()->storage_info()->NumLevelFiles(0) >=
       mutable_cf_options.level0_slowdown_writes_trigger);
  bool triggered_writes_stop =
      (cfd->current()->storage_info()->NumLevelFiles(0) >=
       mutable_cf_options.level0_stop_writes_trigger);
  // release lock while notifying events
  mutex_.Unlock();
  {
    FlushJobInfo info;
    info.cf_name = cfd->GetName();
    // TODO(yhchiang): make db_paths dynamic in case flush does not
    //                 go to L0 in the future.
    info.file_path = MakeTableFileName(cfd->ioptions()->cf_paths[0].path, 0);
    info.thread_id = env_->GetThreadID();
    info.job_id = job_id;
    info.triggered_writes_slowdown = triggered_writes_slowdown;
    info.triggered_writes_stop = triggered_writes_stop;
    info.smallest_seqno = 0;
    info.largest_seqno = 0;
    info.flush_reason = cfd->GetFlushReason();
    for (auto listener : immutable_db_options_.listeners) {
      listener->OnFlushBegin(this, info);
    }
  }
  mutex_.Lock();
// no need to signal bg_cv_ as it will be signaled at the end of the
// flush process.
#else
  (void)cfd;
  (void)file_meta;
  (void)mutable_cf_options;
  (void)job_id;
  (void)prop;
#endif  // ROCKSDB_LITE
}

void DBImpl::NotifyOnFlushCompleted(
    ColumnFamilyData* cfd, const std::vector<FileMetaData>& file_meta_vec,
    const MutableCFOptions& mutable_cf_options, int job_id,
    const std::vector<TableProperties>& prop_vec) {
#ifndef ROCKSDB_LITE
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  bool triggered_writes_slowdown =
      (cfd->current()->storage_info()->NumLevelFiles(0) >=
       mutable_cf_options.level0_slowdown_writes_trigger);
  bool triggered_writes_stop =
      (cfd->current()->storage_info()->NumLevelFiles(0) >=
       mutable_cf_options.level0_stop_writes_trigger);
  // release lock while notifying events
  mutex_.Unlock();
  {
    assert(file_meta_vec.size() == prop_vec.size());
    FlushJobInfo info;
    for (size_t i = 0; i < file_meta_vec.size(); ++i) {
      auto file_meta = &file_meta_vec[i];
      auto& prop = prop_vec[i];
      info.cf_name = cfd->GetName();
      // TODO(yhchiang): make db_paths dynamic in case flush does not
      //                 go to L0 in the future.
      info.file_path = MakeTableFileName(cfd->ioptions()->cf_paths[0].path,
                                         file_meta->fd.GetNumber());
      info.thread_id = env_->GetThreadID();
      info.job_id = job_id;
      info.triggered_writes_slowdown = triggered_writes_slowdown;
      info.triggered_writes_stop = triggered_writes_stop;
      info.smallest_seqno = file_meta->fd.smallest_seqno;
      info.largest_seqno = file_meta->fd.largest_seqno;
      info.table_properties = prop;
      info.flush_reason = cfd->GetFlushReason();
      for (auto listener : immutable_db_options_.listeners) {
        listener->OnFlushCompleted(this, info);
      }
    }
  }
  mutex_.Lock();
  // no need to signal bg_cv_ as it will be signaled at the end of the
  // flush process.
#else
  (void)cfd;
  (void)file_meta;
  (void)mutable_cf_options;
  (void)job_id;
  (void)prop;
#endif  // ROCKSDB_LITE
}

Status DBImpl::CompactRange(const CompactRangeOptions& options,
                            ColumnFamilyHandle* column_family,
                            const Slice* begin, const Slice* end) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  if (options.target_path_id >= cfd->ioptions()->cf_paths.size()) {
    return Status::InvalidArgument("Invalid target path ID");
  }

  bool exclusive = options.exclusive_manual_compaction;

  bool flush_needed = true;
  if (begin != nullptr && end != nullptr) {
    // TODO(ajkr): We could also optimize away the flush in certain cases where
    // one/both sides of the interval are unbounded. But it requires more
    // changes to RangesOverlapWithMemtables.
    Range range(*begin, *end);
    SuperVersion* super_version = cfd->GetReferencedSuperVersion(this);
    cfd->RangesOverlapWithMemtables({range}, super_version, &flush_needed);
    CleanupSuperVersion(super_version);
  }

  Status s;
  if (flush_needed) {
    FlushOptions fo;
    fo.allow_write_stall = options.allow_write_stall;
    if (immutable_db_options_.atomic_flush) {
      autovector<ColumnFamilyData*> cfds;
      mutex_.Lock();
      SelectColumnFamiliesForAtomicFlush(&cfds);
      mutex_.Unlock();
      s = AtomicFlushMemTables(cfds, fo, FlushReason::kManualCompaction,
                               false /* writes_stopped */);
    } else {
      s = FlushMemTable(cfd, fo, FlushReason::kManualCompaction,
                        false /* writes_stopped*/);
    }
    if (!s.ok()) {
      LogFlush(immutable_db_options_.info_log);
      return s;
    }
  }

  int max_level_with_files = 0;
  chash_set<uint64_t> files_being_compact;
  {
    InstrumentedMutexLock l(&mutex_);
    Version* base = cfd->current();
    for (int level = 1; level < base->storage_info()->num_non_empty_levels();
         level++) {
      if (base->storage_info()->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
    cfd->PrepareManualCompaction(*cfd->GetLatestMutableCFOptions(), begin, end,
                                 &files_being_compact);
  }

  int final_output_level = 0;
  if (cfd->ioptions()->compaction_style == kCompactionStyleUniversal &&
      cfd->NumberLevels() > 1 &&
      (!cfd->ioptions()->enable_lazy_compaction ||
       (begin == nullptr && end == nullptr))) {
    // Always compact all files together.
    final_output_level = cfd->NumberLevels() - 1;
    // if bottom most level is reserved
    if (immutable_db_options_.allow_ingest_behind) {
      final_output_level--;
    }
    s = RunManualCompaction(
        cfd, options.separation_type, ColumnFamilyData::kCompactAllLevels,
        final_output_level, options.target_path_id, options.max_subcompactions,
        begin, end, &files_being_compact, exclusive, false);
  } else {
    for (int level = 0; level <= max_level_with_files; level++) {
      int output_level;
      // in case the compaction is universal or if we're compacting the
      // bottom-most level, the output level will be the same as input one.
      // level 0 can never be the bottommost level (i.e. if all files are in
      // level 0, we will compact to level 1)
      if (cfd->ioptions()->compaction_style == kCompactionStyleUniversal) {
        output_level = level;
      } else if (level == max_level_with_files && level > 0) {
        if (options.bottommost_level_compaction ==
            BottommostLevelCompaction::kSkip) {
          // Skip bottommost level compaction
          continue;
        } else if (options.bottommost_level_compaction ==
                       BottommostLevelCompaction::kIfHaveCompactionFilter &&
                   cfd->ioptions()->compaction_filter == nullptr &&
                   cfd->ioptions()->compaction_filter_factory == nullptr) {
          // Skip bottommost level compaction since we don't have a compaction
          // filter
          continue;
        }
        output_level = level;
      } else {
        output_level = level + 1;
        if (cfd->ioptions()->compaction_style == kCompactionStyleLevel &&
            cfd->ioptions()->level_compaction_dynamic_level_bytes &&
            level == 0) {
          output_level = ColumnFamilyData::kCompactToBaseLevel;
        }
      }
      s = RunManualCompaction(cfd, options.separation_type, level, output_level,
                              options.target_path_id,
                              options.max_subcompactions, begin, end,
                              &files_being_compact, exclusive, false);
      if (!s.ok()) {
        break;
      }
      if (output_level == ColumnFamilyData::kCompactToBaseLevel) {
        final_output_level = cfd->NumberLevels() - 1;
      } else if (output_level > final_output_level) {
        final_output_level = output_level;
      }
      TEST_SYNC_POINT("DBImpl::RunManualCompaction()::1");
      TEST_SYNC_POINT("DBImpl::RunManualCompaction()::2");
      while (cfd->ioptions()->enable_lazy_compaction) {
        int bottommost_level;
        {
          InstrumentedMutexLock l(&mutex_);
          bottommost_level =
              cfd->current()->storage_info()->num_non_empty_levels() - 1;
        }
        if (max_level_with_files >= bottommost_level) {
          break;
        }
        do {
          ++max_level_with_files;
          s = RunManualCompaction(cfd, options.separation_type,
                                  max_level_with_files, max_level_with_files,
                                  options.target_path_id,
                                  options.max_subcompactions, begin, end,
                                  &files_being_compact, exclusive, false);
        } while (max_level_with_files < bottommost_level);
      }
    }
  }
  if (!s.ok()) {
    LogFlush(immutable_db_options_.info_log);
    return s;
  }

  if (options.change_level) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[RefitLevel] waiting for background threads to stop");
    s = PauseBackgroundWork();
    if (s.ok()) {
      s = ReFitLevel(cfd, final_output_level, options.target_level);
    }
    ContinueBackgroundWork();
  }
  LogFlush(immutable_db_options_.info_log);

  {
    InstrumentedMutexLock l(&mutex_);
    // an automatic compaction that has been scheduled might have been
    // preempted by the manual compactions. Need to schedule it back.
    MaybeScheduleFlushOrCompaction();
  }

  return s;
}

Status DBImpl::CompactFiles(const CompactionOptions& compact_options,
                            ColumnFamilyHandle* column_family,
                            const std::vector<std::string>& input_file_names,
                            const int output_level, const int output_path_id,
                            std::vector<std::string>* const output_file_names) {
#ifdef ROCKSDB_LITE
  (void)compact_options;
  (void)column_family;
  (void)input_file_names;
  (void)output_level;
  (void)output_path_id;
  (void)output_file_names;
  // not supported in lite version
  return Status::NotSupported("Not supported in ROCKSDB LITE");
#else
  if (column_family == nullptr) {
    return Status::InvalidArgument("ColumnFamilyHandle must be non-null.");
  }

  auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  assert(cfd);

  Status s;
  JobContext job_context(0, true);
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());

  // Perform CompactFiles
  TEST_SYNC_POINT("TestCompactFiles::IngestExternalFile2");
  {
    InstrumentedMutexLock l(&mutex_);

    // This call will unlock/lock the mutex to wait for current running
    // IngestExternalFile() calls to finish.
    WaitForIngestFile();

    // We need to get current after `WaitForIngestFile`, because
    // `IngestExternalFile` may add files that overlap with `input_file_names`
    auto* current = cfd->current();
    current->Ref();

    s = CompactFilesImpl(compact_options, cfd, current, input_file_names,
                         output_file_names, output_level, output_path_id,
                         &job_context, &log_buffer);

    current->Unref();
  }

  // Find and delete obsolete files
  {
    InstrumentedMutexLock l(&mutex_);
    // If !s.ok(), this means that Compaction failed. In that case, we want
    // to delete all obsolete files we might have created and we force
    // FindObsoleteFiles(). This is because job_context does not
    // catch all created files if compaction failed.
    FindObsoleteFiles(&job_context, !s.ok());
  }  // release the mutex

  // delete unnecessary files if any, this is done outside the mutex
  if (job_context.HaveSomethingToClean() ||
      job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
    // Have to flush the info logs before bg_compaction_scheduled_--
    // because if bg_flush_scheduled_ becomes 0 and the lock is
    // released, the deconstructor of DB can kick in and destroy all the
    // states of DB so info_log might not be available after that point.
    // It also applies to access other states that DB owns.
    log_buffer.FlushBufferToLog();
    if (job_context.HaveSomethingToDelete()) {
      // no mutex is locked here.  No need to Unlock() and Lock() here.
      PurgeObsoleteFiles(job_context);
    }
    job_context.Clean(&mutex_);
  }

  return s;
#endif  // ROCKSDB_LITE
}

#ifndef ROCKSDB_LITE
Status DBImpl::CompactFilesImpl(
    const CompactionOptions& compact_options, ColumnFamilyData* cfd,
    Version* version, const std::vector<std::string>& input_file_names,
    std::vector<std::string>* const output_file_names, const int output_level,
    int output_path_id, JobContext* job_context, LogBuffer* log_buffer) {
  mutex_.AssertHeld();

  if (shutting_down_.load(std::memory_order_acquire)) {
    return Status::ShutdownInProgress();
  }

  std::unordered_set<uint64_t> input_set;
  for (const auto& file_name : input_file_names) {
    input_set.insert(TableFileNameToNumber(file_name));
  }

  ColumnFamilyMetaData cf_meta;
  // TODO(yhchiang): can directly use version here if none of the
  // following functions call is pluggable to external developers.
  version->GetColumnFamilyMetaData(&cf_meta);

  if (output_path_id < 0) {
    if (cfd->ioptions()->cf_paths.size() == 1U) {
      output_path_id = 0;
    } else {
      return Status::NotSupported(
          "Automatic output path selection is not "
          "yet supported in CompactFiles()");
    }
  }

  Status s = cfd->compaction_picker()->SanitizeCompactionInputFiles(
      &input_set, cf_meta, output_level);
  if (!s.ok()) {
    return s;
  }

  std::vector<CompactionInputFiles> input_files;
  s = cfd->compaction_picker()->GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, version->storage_info(), compact_options);
  if (!s.ok()) {
    return s;
  }

  for (const auto& inputs : input_files) {
    if (cfd->compaction_picker()->AreFilesInCompaction(inputs.files)) {
      return Status::Aborted(
          "Some of the necessary compaction input "
          "files are already being compacted");
    }
  }
  bool sfm_reserved_compact_space = false;
  // First check if we have enough room to do the compaction
  bool enough_room = EnoughRoomForCompaction(
      cfd, input_files, &sfm_reserved_compact_space, log_buffer);

  if (!enough_room) {
    // m's vars will get set properly at the end of this function,
    // as long as status == CompactionTooLarge
    return Status::CompactionTooLarge();
  }

  // At this point, CompactFiles will be run.
  bg_compaction_scheduled_++;

  std::unique_ptr<Compaction> c;
  assert(cfd->compaction_picker());
  c.reset(cfd->compaction_picker()->CompactFiles(
      compact_options, input_files, output_level, version->storage_info(),
      *cfd->GetLatestMutableCFOptions(), output_path_id));
  // we already sanitized the set of input files and checked for conflicts
  // without releasing the lock, so we're guaranteed a compaction can be formed.
  assert(c != nullptr);

  c->SetInputVersion(version);

  SequenceNumber earliest_write_conflict_snapshot;
  std::vector<SequenceNumber> snapshot_seqs =
      snapshots_.GetAll(&earliest_write_conflict_snapshot);

  auto pending_outputs_inserted_elem =
      CaptureCurrentFileNumberInPendingOutputs();

  auto snapshot_checker = snapshot_checker_.get();
  if (use_custom_gc_ && snapshot_checker == nullptr) {
    snapshot_checker = DisableGCSnapshotChecker::Instance();
  }
  assert(is_snapshot_supported_ || snapshots_.empty());
  CompactionJob compaction_job(
      job_context->job_id, c.get(), immutable_db_options_,
      env_options_for_compaction_, versions_.get(), &shutting_down_,
      preserve_deletes_seqnum_.load(), log_buffer, directories_.GetDbDir(),
      GetDataDir(c->column_family_data(), c->output_path_id()), stats_, &mutex_,
      &error_handler_, snapshot_seqs, earliest_write_conflict_snapshot,
      snapshot_checker, table_cache_, &event_logger_,
      c->mutable_cf_options()->paranoid_file_checks,
      c->mutable_cf_options()->report_bg_io_stats, dbname_,
      nullptr /* compaction_job_stats */);
  // Here we pass a nullptr for CompactionJobStats because
  // CompactFiles does not trigger OnCompactionCompleted(),
  // which is the only place where CompactionJobStats is
  // returned.  The idea of not triggering OnCompationCompleted()
  // is that CompactFiles runs in the caller thread, so the user
  // should always know when it completes.  As a result, it makes
  // less sense to notify the users something they should already
  // know.
  //
  // In the future, if we would like to add CompactionJobStats
  // support for CompactFiles, we should have CompactFiles API
  // pass a pointer of CompactionJobStats as the out-value
  // instead of using EventListener.

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here.
  version->storage_info()->ComputeCompactionScore(*cfd->ioptions(),
                                                  *c->mutable_cf_options());
  uint32_t max_subcompactions =
      compact_options.max_subcompactions > 0
          ? compact_options.max_subcompactions
          : c->mutable_cf_options()->max_subcompactions;
  int sub_compaction_scheduled =
      compaction_job.Prepare(GetSubCompactionSlots(max_subcompactions));
  bg_compaction_scheduled_ += sub_compaction_scheduled;
  mutex_.Unlock();
  TEST_SYNC_POINT("CompactFilesImpl:0");
  TEST_SYNC_POINT("CompactFilesImpl:1");
  compaction_job.Run();
  TEST_SYNC_POINT("CompactFilesImpl:2");
  TEST_SYNC_POINT("CompactFilesImpl:3");
  mutex_.Lock();
  bg_compaction_scheduled_ -= sub_compaction_scheduled;
  Status status = compaction_job.Install(*c->mutable_cf_options());
  if (status.ok()) {
    InstallSuperVersionAndScheduleWork(
        c->column_family_data(), &job_context->superversion_contexts[0],
        *c->mutable_cf_options(), FlushReason::kManualCompaction);
  }
  c->ReleaseCompactionFiles(s);
#ifndef ROCKSDB_LITE
  // Need to make sure SstFileManager does its bookkeeping
  auto sfm = static_cast<SstFileManagerImpl*>(
      immutable_db_options_.sst_file_manager.get());
  if (sfm && sfm_reserved_compact_space) {
    sfm->OnCompactionCompletion(c.get());
  }
#endif  // ROCKSDB_LITE

  ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);

  if (status.ok()) {
    // Done
  } else if (status.IsShutdownInProgress()) {
    // Ignore compaction errors found during shutting down
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "[%s] [JOB %d] Compaction error: %s",
                   c->column_family_data()->GetName().c_str(),
                   job_context->job_id, status.ToString().c_str());
    error_handler_.SetBGError(status, BackgroundErrorReason::kCompaction);
  }

  if (output_file_names != nullptr) {
    for (const auto& newf : c->edit()->GetNewFiles()) {
      (*output_file_names)
          .push_back(TableFileName(c->immutable_cf_options()->cf_paths,
                                   newf.second.fd.GetNumber(),
                                   newf.second.fd.GetPathId()));
    }
  }

  c.reset();

  bg_compaction_scheduled_--;
  if (bg_compaction_scheduled_ == 0) {
    bg_cv_.SignalAll();
  }
  MaybeScheduleFlushOrCompaction();
  TEST_SYNC_POINT("CompactFilesImpl:End");

  return status;
}
#endif  // ROCKSDB_LITE

Status DBImpl::PauseBackgroundWork() {
  InstrumentedMutexLock guard_lock(&mutex_);
  bg_compaction_paused_++;
  while (bg_bottom_compaction_scheduled_ > 0 || bg_compaction_scheduled_ > 0 ||
         bg_flush_scheduled_ > 0) {
    bg_cv_.Wait();
  }
  bg_work_paused_++;
  return Status::OK();
}

Status DBImpl::ContinueBackgroundWork() {
  InstrumentedMutexLock guard_lock(&mutex_);
  if (bg_work_paused_ == 0) {
    return Status::InvalidArgument();
  }
  assert(bg_work_paused_ > 0);
  assert(bg_compaction_paused_ > 0);
  bg_compaction_paused_--;
  bg_work_paused_--;
  // It's sufficient to check just bg_work_paused_ here since
  // bg_work_paused_ is always no greater than bg_compaction_paused_
  if (bg_work_paused_ == 0) {
    MaybeScheduleFlushOrCompaction();
  }
  return Status::OK();
}

void DBImpl::NotifyOnCompactionBegin(ColumnFamilyData* cfd, Compaction* c,
                                     const Status& st,
                                     const CompactionJobStats& job_stats,
                                     int job_id) {
#ifndef ROCKSDB_LITE
  if (immutable_db_options_.listeners.empty()) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  Version* current = cfd->current();
  current->Ref();
  // release lock while notifying events
  mutex_.Unlock();
  TEST_SYNC_POINT("DBImpl::NotifyOnCompactionBegin::UnlockMutex");
  {
    CompactionJobInfo info;
    info.cf_name = cfd->GetName();
    info.status = st;
    info.thread_id = env_->GetThreadID();
    info.job_id = job_id;
    info.base_input_level = c->start_level();
    info.output_level = c->output_level();
    info.stats = job_stats;
    info.table_properties = c->GetOutputTableProperties();
    info.compaction_reason = c->compaction_reason();
    info.compression = c->output_compression();
    for (size_t i = 0; i < c->num_input_levels(); ++i) {
      for (const auto fmd : *c->inputs(i)) {
        auto fn = TableFileName(c->immutable_cf_options()->cf_paths,
                                fmd->fd.GetNumber(), fmd->fd.GetPathId());
        info.input_files.push_back(fn);
        if (info.table_properties.count(fn) == 0) {
          std::shared_ptr<const TableProperties> tp;
          auto s = current->GetTableProperties(&tp, fmd, &fn);
          if (s.ok()) {
            info.table_properties[fn] = tp;
          }
        }
      }
    }
    for (const auto& newf : c->edit()->GetNewFiles()) {
      info.output_files.push_back(TableFileName(
          c->immutable_cf_options()->cf_paths, newf.second.fd.GetNumber(),
          newf.second.fd.GetPathId()));
    }
    for (auto& listener : immutable_db_options_.listeners) {
      listener->OnCompactionBegin(this, info);
    }
  }
  mutex_.Lock();
  current->Unref();
#else
  (void)cfd;
  (void)c;
  (void)st;
  (void)job_stats;
  (void)job_id;
#endif  // ROCKSDB_LITE
}

void DBImpl::NotifyOnCompactionCompleted(
    ColumnFamilyData* cfd, Compaction* c, const Status& st,
    const CompactionJobStats& compaction_job_stats, const int job_id) {
#ifndef ROCKSDB_LITE
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  Version* current = cfd->current();
  current->Ref();
  // release lock while notifying events
  mutex_.Unlock();
  TEST_SYNC_POINT("DBImpl::NotifyOnCompactionCompleted::UnlockMutex");
  {
    CompactionJobInfo info;
    info.cf_id = cfd->GetID();
    info.cf_name = cfd->GetName();
    info.status = st;
    info.thread_id = env_->GetThreadID();
    info.job_id = job_id;
    info.base_input_level = c->start_level();
    info.output_level = c->output_level();
    info.stats = compaction_job_stats;
    info.table_properties = c->GetOutputTableProperties();
    info.compaction_reason = c->compaction_reason();
    info.compression = c->output_compression();
    info.transient_stat = c->transient_stat();
    for (size_t i = 0; i < c->num_input_levels(); ++i) {
      for (const auto fmd : *c->inputs(i)) {
        auto fn = TableFileName(c->immutable_cf_options()->cf_paths,
                                fmd->fd.GetNumber(), fmd->fd.GetPathId());
        info.input_files.push_back(fn);
        if (info.table_properties.count(fn) == 0) {
          std::shared_ptr<const TableProperties> tp;
          auto s = current->GetTableProperties(&tp, fmd, &fn);
          if (s.ok()) {
            info.table_properties[fn] = tp;
          }
        }
      }
    }
    for (const auto& newf : c->edit()->GetNewFiles()) {
      if (!c->IsNewOutputTable(newf.second.fd.GetNumber())) {
        continue;
      }
      info.output_files.push_back(TableFileName(
          c->immutable_cf_options()->cf_paths, newf.second.fd.GetNumber(),
          newf.second.fd.GetPathId()));
    }
    for (auto listener : immutable_db_options_.listeners) {
      listener->OnCompactionCompleted(this, info);
    }
  }
  mutex_.Lock();
  current->Unref();
  // no need to signal bg_cv_ as it will be signaled at the end of the
  // flush process.
#else
  (void)cfd;
  (void)c;
  (void)st;
  (void)compaction_job_stats;
  (void)job_id;
#endif  // ROCKSDB_LITE
}

// REQUIREMENT: block all background work by calling PauseBackgroundWork()
// before calling this function
Status DBImpl::ReFitLevel(ColumnFamilyData* cfd, int level, int target_level) {
  assert(level < cfd->NumberLevels());
  if (target_level >= cfd->NumberLevels()) {
    return Status::InvalidArgument("Target level exceeds number of levels");
  }

  SuperVersionContext sv_context(/* create_superversion */ true);

  Status status;

  InstrumentedMutexLock guard_lock(&mutex_);

  // only allow one thread refitting
  if (refitting_level_) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[ReFitLevel] another thread is refitting");
    return Status::NotSupported("another thread is refitting");
  }
  refitting_level_ = true;

  const MutableCFOptions mutable_cf_options = *cfd->GetLatestMutableCFOptions();
  // move to a smaller level
  int to_level = target_level;
  if (target_level < 0) {
    to_level = FindMinimumEmptyLevelFitting(cfd, mutable_cf_options, level);
  }

  auto* vstorage = cfd->current()->storage_info();
  if (to_level > level) {
    if (level == 0) {
      return Status::NotSupported(
          "Cannot change from level 0 to other levels.");
    }
    // Check levels are empty for a trivial move
    for (int l = level + 1; l <= to_level; l++) {
      if (vstorage->NumLevelFiles(l) > 0) {
        return Status::NotSupported(
            "Levels between source and target are not empty for a move.");
      }
    }
  }
  if (to_level != level) {
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "[%s] Before refitting:\n%s", cfd->GetName().c_str(),
                    cfd->current()->DebugString().data());

    VersionEdit edit;
    edit.SetColumnFamily(cfd->GetID());
    for (const auto& f : vstorage->LevelFiles(level)) {
      edit.DeleteFile(level, f->fd.GetNumber());
      edit.AddFile(to_level, f->fd.GetNumber(), f->fd.GetPathId(),
                   f->fd.GetFileSize(), f->smallest, f->largest,
                   f->fd.smallest_seqno, f->fd.largest_seqno,
                   f->marked_for_compaction, f->prop);
    }
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "[%s] Apply version edit:\n%s", cfd->GetName().c_str(),
                    edit.DebugString().data());

    status = versions_->LogAndApply(cfd, mutable_cf_options, &edit, &mutex_,
                                    directories_.GetDbDir());
    InstallSuperVersionAndScheduleWork(cfd, &sv_context, mutable_cf_options);

    ROCKS_LOG_DEBUG(immutable_db_options_.info_log, "[%s] LogAndApply: %s\n",
                    cfd->GetName().c_str(), status.ToString().data());

    if (status.ok()) {
      ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                      "[%s] After refitting:\n%s", cfd->GetName().c_str(),
                      cfd->current()->DebugString().data());
    }
  }

  sv_context.Clean();
  refitting_level_ = false;

  return status;
}

int DBImpl::NumberLevels(ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  return cfh->cfd()->NumberLevels();
}

int DBImpl::MaxMemCompactionLevel(ColumnFamilyHandle* /*column_family*/) {
  return 0;
}

int DBImpl::Level0StopWriteTrigger(ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  InstrumentedMutexLock l(&mutex_);
  return cfh->cfd()
      ->GetSuperVersion()
      ->mutable_cf_options.level0_stop_writes_trigger;
}

Status DBImpl::Flush(const FlushOptions& flush_options,
                     ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  ROCKS_LOG_INFO(immutable_db_options_.info_log, "[%s] Manual flush start.",
                 cfh->GetName().c_str());
  Status s;
  if (immutable_db_options_.atomic_flush) {
    s = AtomicFlushMemTables({cfh->cfd()}, flush_options,
                             FlushReason::kManualFlush);
  } else {
    s = FlushMemTable(cfh->cfd(), flush_options, FlushReason::kManualFlush);
  }

  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "[%s] Manual flush finished, status: %s\n",
                 cfh->GetName().c_str(), s.ToString().c_str());
  return s;
}

Status DBImpl::Flush(const FlushOptions& flush_options,
                     const std::vector<ColumnFamilyHandle*>& column_families) {
  Status s;
  if (!immutable_db_options_.atomic_flush) {
    for (auto cfh : column_families) {
      s = Flush(flush_options, cfh);
      if (!s.ok()) {
        break;
      }
    }
  } else {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Manual atomic flush start.\n"
                   "=====Column families:=====");
    for (auto cfh : column_families) {
      auto cfhi = static_cast<ColumnFamilyHandleImpl*>(cfh);
      ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s",
                     cfhi->GetName().c_str());
    }
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "=====End of column families list=====");
    autovector<ColumnFamilyData*> cfds;
    std::for_each(column_families.begin(), column_families.end(),
                  [&cfds](ColumnFamilyHandle* elem) {
                    auto cfh = static_cast<ColumnFamilyHandleImpl*>(elem);
                    cfds.emplace_back(cfh->cfd());
                  });
    s = AtomicFlushMemTables(cfds, flush_options, FlushReason::kManualFlush);
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Manual atomic flush finished, status: %s\n",
                   "=====Column families:=====", s.ToString().c_str());
    for (auto cfh : column_families) {
      auto cfhi = static_cast<ColumnFamilyHandleImpl*>(cfh);
      ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s",
                     cfhi->GetName().c_str());
    }
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "=====End of column families list=====");
  }
  return s;
}

Status DBImpl::RunManualCompaction(
    ColumnFamilyData* cfd, SeparationType separation_type, int input_level,
    int output_level, uint32_t output_path_id, uint32_t max_subcompactions,
    const Slice* begin, const Slice* end,
    const chash_set<uint64_t>* files_being_compact, bool exclusive,
    bool disallow_trivial_move) {
  assert(input_level == ColumnFamilyData::kCompactAllLevels ||
         input_level >= 0);

  InternalKey begin_storage, end_storage;
  CompactionArg* ca;

  bool scheduled = false;
  bool manual_conflict = false;
  bool enable_lazy_compaction = cfd->ioptions()->enable_lazy_compaction;
  ManualCompactionState manual;
  manual.cfd = cfd;
  manual.input_level = input_level;
  manual.output_level = output_level;
  manual.output_path_id = output_path_id;
  manual.done = false;
  manual.in_progress = false;
  manual.incomplete = false;
  manual.exclusive = exclusive;
  manual.disallow_trivial_move = disallow_trivial_move;
  // For universal compaction, we enforce every manual compaction to compact
  // all files.
  if (begin == nullptr ||
      (cfd->ioptions()->compaction_style == kCompactionStyleUniversal &&
       !enable_lazy_compaction)) {
    manual.begin = nullptr;
  } else {
    begin_storage.SetMinPossibleForUserKey(*begin);
    manual.begin = &begin_storage;
  }
  if (end == nullptr ||
      (cfd->ioptions()->compaction_style == kCompactionStyleUniversal &&
       !enable_lazy_compaction)) {
    manual.end = nullptr;
  } else {
    end_storage.SetMaxPossibleForUserKey(*end);
    manual.end = &end_storage;
  }

  TEST_SYNC_POINT("DBImpl::RunManualCompaction:0");
  TEST_SYNC_POINT("DBImpl::RunManualCompaction:1");
  InstrumentedMutexLock l(&mutex_);

  // When a manual compaction arrives, temporarily disable scheduling of
  // non-manual compactions and wait until the number of scheduled compaction
  // jobs drops to zero. This is needed to ensure that this manual compaction
  // can compact any range of keys/files.
  //
  // HasPendingManualCompaction() is true when at least one thread is inside
  // RunManualCompaction(), i.e. during that time no other compaction will
  // get scheduled (see MaybeScheduleFlushOrCompaction).
  //
  // Note that the following loop doesn't stop more that one thread calling
  // RunManualCompaction() from getting to the second while loop below.
  // However, only one of them will actually schedule compaction, while
  // others will wait on a condition variable until it completes.

  AddManualCompaction(&manual);
  TEST_SYNC_POINT_CALLBACK("DBImpl::RunManualCompaction:NotScheduled", &mutex_);
  if (exclusive) {
    while (bg_bottom_compaction_scheduled_ > 0 ||
           bg_compaction_scheduled_ > bg_garbage_collection_scheduled_) {
      TEST_SYNC_POINT("DBImpl::RunManualCompaction:WaitScheduled");
      ROCKS_LOG_INFO(
          immutable_db_options_.info_log,
          "[%s] Manual compaction waiting for all other scheduled background "
          "compactions to finish",
          cfd->GetName().c_str());
      bg_cv_.Wait();
    }
  }

  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "[%s] Manual compaction starting", cfd->GetName().c_str());

  // We don't check bg_error_ here, because if we get the error in compaction,
  // the compaction will set manual.status to bg_error_ and set manual.done to
  // true.
  while (!manual.done) {
    assert(HasPendingManualCompaction());
    manual_conflict = false;
    Compaction* compaction = nullptr;
    if (ShouldntRunManualCompaction(&manual) || manual.in_progress ||
        scheduled ||
        (((manual.manual_end = &manual.tmp_storage1) != nullptr) &&
         ((compaction = manual.cfd->CompactRange(
               *manual.cfd->GetLatestMutableCFOptions(), separation_type,
               manual.input_level, manual.output_level, manual.output_path_id,
               max_subcompactions, manual.begin, manual.end, &manual.manual_end,
               &manual_conflict, files_being_compact)) == nullptr &&
          manual_conflict))) {
      // exclusive manual compactions should not see a conflict during
      // CompactRange
      assert(!exclusive || !manual_conflict);
      // Running either this or some other manual compaction
      bg_cv_.Wait();
      if (scheduled && manual.incomplete) {
        assert(!manual.in_progress);
        scheduled = false;
        manual.incomplete = false;
      }
    } else if (!scheduled) {
      if (compaction == nullptr) {
        manual.done = true;
        bg_cv_.SignalAll();
        continue;
      }
      ca = new CompactionArg;
      ca->db = this;
      ca->prepicked_compaction = new PrepickedCompaction;
      ca->prepicked_compaction->manual_compaction_state = &manual;
      ca->prepicked_compaction->compaction = compaction;
      manual.incomplete = false;
      bg_compaction_scheduled_++;
      env_->Schedule(&DBImpl::BGWorkCompaction, ca, Env::Priority::LOW, this,
                     &DBImpl::UnscheduleCallback);
      scheduled = true;
    }
  }

  assert(!manual.in_progress);
  assert(HasPendingManualCompaction());
  RemoveManualCompaction(&manual);
  bg_cv_.SignalAll();
  return manual.status;
}

void DBImpl::GenerateFlushRequest(const autovector<ColumnFamilyData*>& cfds,
                                  FlushRequest* req) {
  assert(req != nullptr);
  req->reserve(cfds.size());
  for (const auto cfd : cfds) {
    if (nullptr == cfd) {
      // cfd may be null, see DBImpl::ScheduleFlushes
      continue;
    }
    uint64_t max_memtable_id = cfd->imm()->GetLatestMemTableID();
    req->emplace_back(cfd, max_memtable_id);
  }
}

Status DBImpl::FlushMemTable(ColumnFamilyData* cfd,
                             const FlushOptions& flush_options,
                             FlushReason flush_reason, bool writes_stopped) {
  Status s;
  uint64_t flush_memtable_id = 0;
  if (!flush_options.allow_write_stall) {
    bool flush_needed = true;
    s = WaitUntilFlushWouldNotStallWrites(cfd, &flush_needed);
    TEST_SYNC_POINT("DBImpl::FlushMemTable:StallWaitDone");
    if (!s.ok() || !flush_needed) {
      return s;
    }
  }
  FlushRequest flush_req;
  {
    WriteContext context(immutable_db_options_.info_log.get());
    InstrumentedMutexLock guard_lock(&mutex_);

    WriteThread::Writer w;
    if (!writes_stopped) {
      write_thread_.EnterUnbatched(&w, &mutex_);
    }

    if (!cfd->mem()->IsEmpty() || !cached_recoverable_state_empty_.load()) {
      s = SwitchMemtable(cfd, &context);
    }

    if (s.ok()) {
      if (cfd->imm()->NumNotFlushed() != 0 || !cfd->mem()->IsEmpty() ||
          !cached_recoverable_state_empty_.load()) {
        flush_memtable_id = cfd->imm()->GetLatestMemTableID();
        flush_req.emplace_back(cfd, flush_memtable_id);
      }
    }

    if (s.ok() && !flush_req.empty()) {
      for (auto& elem : flush_req) {
        ColumnFamilyData* loop_cfd = elem.first;
        loop_cfd->imm()->FlushRequested();
      }
      SchedulePendingFlush(flush_req, flush_reason);
      MaybeScheduleFlushOrCompaction();
    }

    if (!writes_stopped) {
      write_thread_.ExitUnbatched(&w);
    }
  }

  if (s.ok() && flush_options.wait) {
    autovector<ColumnFamilyData*> cfds;
    autovector<const uint64_t*> flush_memtable_ids;
    for (auto& iter : flush_req) {
      cfds.push_back(iter.first);
      flush_memtable_ids.push_back(&(iter.second));
    }
    s = WaitForFlushMemTables(cfds, flush_memtable_ids,
                              (flush_reason == FlushReason::kErrorRecovery));
  }
  TEST_SYNC_POINT("FlushMemTableFinished");
  return s;
}

// Flush all elments in 'column_family_datas'
// and atomically record the result to the MANIFEST.
Status DBImpl::AtomicFlushMemTables(
    const autovector<ColumnFamilyData*>& column_family_datas,
    const FlushOptions& flush_options, FlushReason flush_reason,
    bool writes_stopped) {
  Status s;
  if (!flush_options.allow_write_stall) {
    int num_cfs_to_flush = 0;
    for (auto cfd : column_family_datas) {
      bool flush_needed = true;
      s = WaitUntilFlushWouldNotStallWrites(cfd, &flush_needed);
      if (!s.ok()) {
        return s;
      } else if (flush_needed) {
        ++num_cfs_to_flush;
      }
    }
    if (0 == num_cfs_to_flush) {
      return s;
    }
  }
  FlushRequest flush_req;
  autovector<ColumnFamilyData*> cfds;
  {
    WriteContext context(immutable_db_options_.info_log.get());
    InstrumentedMutexLock guard_lock(&mutex_);

    WriteThread::Writer w;
    if (!writes_stopped) {
      write_thread_.EnterUnbatched(&w, &mutex_);
    }

    for (auto cfd : column_family_datas) {
      if (cfd->IsDropped()) {
        continue;
      }
      if (cfd->imm()->NumNotFlushed() != 0 || !cfd->mem()->IsEmpty() ||
          !cached_recoverable_state_empty_.load()) {
        cfds.emplace_back(cfd);
      }
    }
    for (auto cfd : cfds) {
      if (cfd->mem()->IsEmpty() && cached_recoverable_state_empty_.load()) {
        continue;
      }
      cfd->Ref();
      s = SwitchMemtable(cfd, &context);
      cfd->Unref();
      if (!s.ok()) {
        break;
      }
    }
    if (s.ok()) {
      AssignAtomicFlushSeq(cfds);
      for (auto cfd : cfds) {
        cfd->imm()->FlushRequested();
      }
      GenerateFlushRequest(cfds, &flush_req);
      SchedulePendingFlush(flush_req, flush_reason);
      MaybeScheduleFlushOrCompaction();
    }

    if (!writes_stopped) {
      write_thread_.ExitUnbatched(&w);
    }
  }
  TEST_SYNC_POINT("DBImpl::AtomicFlushMemTables:AfterScheduleFlush");

  if (s.ok() && flush_options.wait) {
    autovector<const uint64_t*> flush_memtable_ids;
    for (auto& iter : flush_req) {
      flush_memtable_ids.push_back(&(iter.second));
    }
    s = WaitForFlushMemTables(cfds, flush_memtable_ids,
                              (flush_reason == FlushReason::kErrorRecovery));
  }
  return s;
}

// Calling FlushMemTable(), whether from DB::Flush() or from Backup Engine, can
// cause write stall, for example if one memtable is being flushed already.
// This method tries to avoid write stall (similar to CompactRange() behavior)
// it emulates how the SuperVersion / LSM would change if flush happens, checks
// it against various constrains and delays flush if it'd cause write stall.
// Called should check status and flush_needed to see if flush already happened.
Status DBImpl::WaitUntilFlushWouldNotStallWrites(ColumnFamilyData* cfd,
                                                 bool* flush_needed) {
  {
    *flush_needed = true;
    InstrumentedMutexLock l(&mutex_);
    uint64_t orig_active_memtable_id = cfd->mem()->GetID();
    WriteStallCondition write_stall_condition = WriteStallCondition::kNormal;
    do {
      if (write_stall_condition != WriteStallCondition::kNormal) {
        // Same error handling as user writes: Don't wait if there's a
        // background error, even if it's a soft error. We might wait here
        // indefinitely as the pending flushes/compactions may never finish
        // successfully, resulting in the stall condition lasting indefinitely
        if (error_handler_.IsBGWorkStopped()) {
          return error_handler_.GetBGError();
        }

        TEST_SYNC_POINT("DBImpl::WaitUntilFlushWouldNotStallWrites:StallWait");
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "[%s] WaitUntilFlushWouldNotStallWrites"
                       " waiting on stall conditions to clear",
                       cfd->GetName().c_str());
        bg_cv_.Wait();
      }
      if (cfd->IsDropped() || shutting_down_.load(std::memory_order_acquire)) {
        return Status::ShutdownInProgress();
      }

      uint64_t earliest_memtable_id =
          std::min(cfd->mem()->GetID(), cfd->imm()->GetEarliestMemTableID());
      if (earliest_memtable_id > orig_active_memtable_id) {
        // We waited so long that the memtable we were originally waiting on was
        // flushed.
        *flush_needed = false;
        return Status::OK();
      }

      const auto& mutable_cf_options = *cfd->GetLatestMutableCFOptions();
      const auto* vstorage = cfd->current()->storage_info();

      // Skip stalling check if we're below auto-flush and auto-compaction
      // triggers. If it stalled in these conditions, that'd mean the stall
      // triggers are so low that stalling is needed for any background work. In
      // that case we shouldn't wait since background work won't be scheduled.
      if (cfd->imm()->NumNotFlushed() <
              cfd->ioptions()->min_write_buffer_number_to_merge &&
          vstorage->l0_delay_trigger_count() <
              mutable_cf_options.level0_file_num_compaction_trigger) {
        break;
      }

      // check whether one extra immutable memtable or an extra L0 file would
      // cause write stalling mode to be entered. It could still enter stall
      // mode due to pending compaction bytes, but that's less common
      write_stall_condition =
          ColumnFamilyData::GetWriteStallConditionAndCause(
              cfd->imm()->NumNotFlushed() + 1,
              vstorage->l0_delay_trigger_count() + 1,
              int(vstorage->read_amplification()) + 1,
              vstorage->estimated_compaction_needed_bytes(),
              cfd->ioptions()->num_levels, mutable_cf_options)
              .first;
    } while (write_stall_condition != WriteStallCondition::kNormal);
  }
  return Status::OK();
}

// Wait for memtables to be flushed for multiple column families.
// let N = cfds.size()
// for i in [0, N),
//  1) if flush_memtable_ids[i] is not null, then the memtables with lower IDs
//     have to be flushed for THIS column family;
//  2) if flush_memtable_ids[i] is null, then all memtables in THIS column
//     family have to be flushed.
// Finish waiting when ALL column families finish flushing memtables.
// resuming_from_bg_err indicates whether the caller is trying to resume from
// background error or in normal processing.
Status DBImpl::WaitForFlushMemTables(
    const autovector<ColumnFamilyData*>& cfds,
    const autovector<const uint64_t*>& flush_memtable_ids,
    bool resuming_from_bg_err) {
  int num = static_cast<int>(cfds.size());
  // Wait until the compaction completes
  InstrumentedMutexLock l(&mutex_);
  // If the caller is trying to resume from bg error, then
  // error_handler_.IsDBStopped() is true.
  while (resuming_from_bg_err || !error_handler_.IsDBStopped()) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      return Status::ShutdownInProgress();
    }
    // If an error has occurred during resumption, then no need to wait.
    if (!error_handler_.GetRecoveryError().ok()) {
      break;
    }
    // Number of column families that have been dropped.
    int num_dropped = 0;
    // Number of column families that have finished flush.
    int num_finished = 0;
    for (int i = 0; i < num; ++i) {
      if (cfds[i]->IsDropped()) {
        ++num_dropped;
      } else if (cfds[i]->imm()->NumNotFlushed() == 0 ||
                 (flush_memtable_ids[i] != nullptr &&
                  cfds[i]->imm()->GetEarliestMemTableID() >
                      *flush_memtable_ids[i])) {
        ++num_finished;
      }
    }
    if (1 == num_dropped && 1 == num) {
      return Status::InvalidArgument("Cannot flush a dropped CF");
    }
    // Column families involved in this flush request have either been dropped
    // or finished flush. Then it's time to finish waiting.
    if (num_dropped + num_finished == num) {
      break;
    }
    bg_cv_.Wait();
  }
  Status s;
  // If not resuming from bg error, and an error has caused the DB to stop,
  // then report the bg error to caller.
  if (!resuming_from_bg_err && error_handler_.IsDBStopped()) {
    s = error_handler_.GetBGError();
  }
  return s;
}

Status DBImpl::EnableAutoCompaction(
    const std::vector<ColumnFamilyHandle*>& column_family_handles) {
  Status s;
  for (auto cf_ptr : column_family_handles) {
    Status status =
        this->SetOptions(cf_ptr, {{"disable_auto_compactions", "false"}});
    if (!status.ok()) {
      s = status;
    }
  }

  return s;
}

void DBImpl::MaybeScheduleFlushOrCompaction() {
  mutex_.AssertHeld();
  if (!opened_successfully_) {
    // Compaction may introduce data race to DB open
    return;
  }
  if (bg_work_paused_ > 0) {
    // we paused the background work
    return;
  } else if (error_handler_.IsBGWorkStopped() &&
             !error_handler_.IsRecoveryInProgress()) {
    // There has been a hard error and this call is not part of the recovery
    // sequence. Bail out here so we don't get into an endless loop of
    // scheduling BG work which will again call this function
    return;
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
    return;
  }
  auto bg_job_limits = GetBGJobLimits();
  bool is_flush_pool_empty =
      env_->GetBackgroundThreads(Env::Priority::HIGH) == 0;
  while (!is_flush_pool_empty && unscheduled_flushes_ > 0 &&
         bg_flush_scheduled_ < bg_job_limits.max_flushes) {
    bg_flush_scheduled_++;
    env_->Schedule(&DBImpl::BGWorkFlush, this, Env::Priority::HIGH, this);
  }

  // special case -- if high-pri (flush) thread pool is empty, then schedule
  // flushes in low-pri (compaction) thread pool.
  if (is_flush_pool_empty) {
    while (unscheduled_flushes_ > 0 &&
           bg_flush_scheduled_ + bg_compaction_scheduled_ <
               bg_job_limits.max_flushes) {
      bg_flush_scheduled_++;
      env_->Schedule(&DBImpl::BGWorkFlush, this, Env::Priority::LOW, this);
    }
  }

  if (bg_compaction_paused_ > 0) {
    // we paused the background compaction
    return;
  } else if (error_handler_.IsBGWorkStopped()) {
    // Compaction is not part of the recovery sequence from a hard error. We
    // might get here because recovery might do a flush and install a new
    // super version, which will try to schedule pending compactions. Bail
    // out here and let the higher level recovery handle compactions
    return;
  }

  bool gc_banned = false;
  TEST_SYNC_POINT_CALLBACK("CompactionJob::ProcessGarbageCollection::Start",
                           &gc_banned);
  while (!gc_banned &&
         bg_garbage_collection_scheduled_ <
             bg_job_limits.max_garbage_collections &&
         unscheduled_garbage_collections_ > 0) {
    CompactionArg* ca = new CompactionArg;
    ca->db = this;
    ca->prepicked_compaction = nullptr;
    bg_compaction_scheduled_++;
    bg_garbage_collection_scheduled_++;
    unscheduled_garbage_collections_--;
    env_->Schedule(&DBImpl::BGWorkGarbageCollection, ca, Env::Priority::LOW,
                   this, &DBImpl::UnscheduleCallback);
  }

  if (HasExclusiveManualCompaction()) {
    // only manual compactions are allowed to run. don't schedule automatic
    // compactions
    TEST_SYNC_POINT("DBImpl::MaybeScheduleFlushOrCompaction:Conflict");
    return;
  }

  while (bg_compaction_scheduled_ < bg_job_limits.max_compactions &&
         unscheduled_compactions_ > 0) {
    CompactionArg* ca = new CompactionArg;
    ca->db = this;
    ca->prepicked_compaction = nullptr;
    bg_compaction_scheduled_++;
    unscheduled_compactions_--;
    env_->Schedule(&DBImpl::BGWorkCompaction, ca, Env::Priority::LOW, this,
                   &DBImpl::UnscheduleCallback);
  }
}

DBImpl::BGJobLimits DBImpl::GetBGJobLimits() const {
  mutex_.AssertHeld();
  bool need_speedup_compaction = write_controller_.NeedSpeedupCompaction();
  for (auto cfd : *versions_->column_family_set_) {
    need_speedup_compaction |=
        cfd->current()->storage_info()->has_space_amplification();
  }
  return GetBGJobLimits(immutable_db_options_.max_background_flushes,
                        mutable_db_options_.max_background_compactions,
                        mutable_db_options_.max_background_garbage_collections,
                        mutable_db_options_.max_background_jobs,
                        need_speedup_compaction);
}

DBImpl::BGJobLimits DBImpl::GetBGJobLimits(
    int max_background_flushes, int max_background_compactions,
    int max_background_garbage_collections, int max_background_jobs,
    bool parallelize_compactions) {
  BGJobLimits res;
  if (max_background_flushes == -1 && max_background_compactions == -1 &&
      max_background_garbage_collections == -1) {
    // for our first stab implementing max_background_jobs, simply allocate a
    // quarter of the threads to flushes.
    res.max_flushes = std::max(1, max_background_jobs / 4);
    res.max_compactions = std::max(2, max_background_jobs - res.max_flushes);
    res.max_garbage_collections = std::max(1, res.max_compactions / 2);
  } else {
    // compatibility code in case users haven't migrated to max_background_jobs,
    // which automatically computes flush/compaction limits
    res.max_flushes = std::max(1, max_background_flushes);
    res.max_compactions = std::max(2, max_background_compactions);
    res.max_garbage_collections =
        std::max(1, max_background_garbage_collections);
    res.max_garbage_collections =
        std::min(res.max_garbage_collections, res.max_compactions - 1);
  }
  if (!parallelize_compactions) {
    // throttle background compactions until we deem necessary
    res.max_compactions = 1;
  }
  return res;
}

int DBImpl::GetSubCompactionSlots(uint32_t max_subcompactions) {
  mutex_.AssertHeld();
  auto bg_job_limits =
      GetBGJobLimits(immutable_db_options_.max_background_flushes,
                     mutable_db_options_.max_background_compactions,
                     mutable_db_options_.max_background_garbage_collections,
                     mutable_db_options_.max_background_jobs,
                     true /* parallelize_compactions */);
  int slots = bg_job_limits.max_compactions - bg_compaction_scheduled_ - 1;
  // max_subcompactions == 0 ? slots : min(max_subcompactions - 1, slots)
  return (int)std::min(max_subcompactions - 1, uint32_t(std::max(0, slots)));
}

void DBImpl::AddToCompactionQueue(ColumnFamilyData* cfd) {
  assert(!cfd->queued_for_compaction());
  cfd->Ref();
  compaction_queue_.push_back(cfd);
  cfd->set_queued_for_compaction(true);
}

ColumnFamilyData* DBImpl::PopFirstFromCompactionQueue() {
  assert(!compaction_queue_.empty());
  auto max_iter = compaction_queue_.begin();
  double max_load = (*max_iter)->current()->GetCompactionLoad();
  for (auto it = std::next(max_iter); it != compaction_queue_.end(); ++it) {
    double tmp_load = (*it)->current()->GetCompactionLoad();
    if (max_load < tmp_load) {
      max_load = tmp_load;
      max_iter = it;
    }
  }
  auto cfd = *max_iter;
  compaction_queue_.erase(max_iter);
  assert(cfd->queued_for_compaction());
  cfd->set_queued_for_compaction(false);
  return cfd;
}

void DBImpl::AddToGarbageCollectionQueue(ColumnFamilyData* cfd) {
  assert(!cfd->queued_for_garbage_collection());
  cfd->Ref();
  garbage_collection_queue_.push_back(cfd);
  cfd->set_queued_for_garbage_collection(true);
}

ColumnFamilyData* DBImpl::PopFirstFromGarbageCollectionQueue() {
  assert(!garbage_collection_queue_.empty());
  auto max_iter = garbage_collection_queue_.begin();
  double max_load = (*max_iter)->current()->GetGarbageCollectionLoad();
  for (auto it = std::next(max_iter); it != garbage_collection_queue_.end();
       ++it) {
    double tmp_load = (*it)->current()->GetGarbageCollectionLoad();
    if (max_load < tmp_load) {
      max_load = tmp_load;
      max_iter = it;
    }
  }
  auto cfd = *max_iter;
  garbage_collection_queue_.erase(max_iter);
  assert(cfd->queued_for_garbage_collection());
  cfd->set_queued_for_garbage_collection(false);
  return cfd;
}

DBImpl::FlushRequest DBImpl::PopFirstFromFlushQueue() {
  assert(!flush_queue_.empty());
  FlushRequest flush_req = flush_queue_.front();
  assert(unscheduled_flushes_ >= static_cast<int>(flush_req.size()));
  unscheduled_flushes_ -= static_cast<int>(flush_req.size());
  flush_queue_.pop_front();
  // TODO: need to unset flush reason?
  return flush_req;
}

void DBImpl::SchedulePendingFlush(const FlushRequest& flush_req,
                                  FlushReason flush_reason) {
  if (flush_req.empty()) {
    return;
  }
  for (auto& iter : flush_req) {
    ColumnFamilyData* cfd = iter.first;
    cfd->Ref();
    cfd->SetFlushReason(flush_reason);
    cfd->inc_queued_for_flush();
  }
  unscheduled_flushes_ += static_cast<int>(flush_req.size());
  flush_queue_.push_back(flush_req);
}

void DBImpl::SchedulePendingCompaction(ColumnFamilyData* cfd) {
  if (!cfd->queued_for_compaction() && cfd->NeedsCompaction()) {
    AddToCompactionQueue(cfd);
    ++unscheduled_compactions_;
  }
}

void DBImpl::SchedulePendingGarbageCollection(ColumnFamilyData* cfd) {
  if (!cfd->queued_for_garbage_collection() && cfd->NeedsGarbageCollection()) {
    AddToGarbageCollectionQueue(cfd);
    ++unscheduled_garbage_collections_;
  }
}

void DBImpl::SchedulePendingPurge(const std::string& fname,
                                  const std::string& dir_to_sync, FileType type,
                                  uint64_t number, int job_id) {
  mutex_.AssertHeld();
  PurgeFileInfo file_info(fname, dir_to_sync, type, number, job_id);
  purge_queue_.push_back(std::move(file_info));

  for (auto listener : candidate_file_listener_) {
    listener->emplace_back(number);
  }
}

void DBImpl::BGWorkFlush(void* db) {
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::HIGH);
  TEST_SYNC_POINT("DBImpl::BGWorkFlush");
  reinterpret_cast<DBImpl*>(db)->BackgroundCallFlush();
  TEST_SYNC_POINT("DBImpl::BGWorkFlush:done");
}

void DBImpl::BGWorkCompaction(void* arg) {
  CompactionArg ca = *(reinterpret_cast<CompactionArg*>(arg));
  delete reinterpret_cast<CompactionArg*>(arg);
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::LOW);
  TEST_SYNC_POINT("DBImpl::BGWorkCompaction");
  auto prepicked_compaction =
      static_cast<PrepickedCompaction*>(ca.prepicked_compaction);
  reinterpret_cast<DBImpl*>(ca.db)->BackgroundCallCompaction(
      prepicked_compaction, Env::Priority::LOW);
  delete prepicked_compaction;
}

void DBImpl::BGWorkGarbageCollection(void* arg) {
  CompactionArg ca = *(reinterpret_cast<CompactionArg*>(arg));
  delete reinterpret_cast<CompactionArg*>(arg);
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::LOW);
  TEST_SYNC_POINT("DBImpl::BGWorkGarbageCollection");
  reinterpret_cast<DBImpl*>(ca.db)->BackgroundCallGarbageCollection();
}

void DBImpl::BGWorkBottomCompaction(void* arg) {
  CompactionArg ca = *(static_cast<CompactionArg*>(arg));
  delete static_cast<CompactionArg*>(arg);
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::BOTTOM);
  TEST_SYNC_POINT("DBImpl::BGWorkBottomCompaction");
  auto* prepicked_compaction = ca.prepicked_compaction;
  assert(prepicked_compaction && prepicked_compaction->compaction &&
         !prepicked_compaction->manual_compaction_state);
  ca.db->BackgroundCallCompaction(prepicked_compaction, Env::Priority::BOTTOM);
  delete prepicked_compaction;
}

void DBImpl::BGWorkPurge(void* db) {
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::HIGH);
  TEST_SYNC_POINT("DBImpl::BGWorkPurge:start");
  reinterpret_cast<DBImpl*>(db)->BackgroundCallPurge();
  TEST_SYNC_POINT("DBImpl::BGWorkPurge:end");
}

void DBImpl::UnscheduleCallback(void* arg) {
  CompactionArg ca = *(reinterpret_cast<CompactionArg*>(arg));
  delete reinterpret_cast<CompactionArg*>(arg);
  if (ca.prepicked_compaction != nullptr) {
    if (ca.prepicked_compaction->compaction != nullptr) {
      delete ca.prepicked_compaction->compaction;
    }
    delete ca.prepicked_compaction;
  }
  TEST_SYNC_POINT("DBImpl::UnscheduleCallback");
}

Status DBImpl::BackgroundFlush(bool* made_progress, JobContext* job_context,
                               LogBuffer* log_buffer, FlushReason* reason) {
  mutex_.AssertHeld();

  Status status;
  *reason = FlushReason::kOthers;
  // If BG work is stopped due to an error, but a recovery is in progress,
  // that means this flush is part of the recovery. So allow it to go through
  if (!error_handler_.IsBGWorkStopped()) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      status = Status::ShutdownInProgress();
    }
  } else if (!error_handler_.IsRecoveryInProgress()) {
    status = error_handler_.GetBGError();
  }

  if (!status.ok()) {
    return status;
  }

  autovector<BGFlushArg> bg_flush_args;
  std::vector<SuperVersionContext>& superversion_contexts =
      job_context->superversion_contexts;
  while (!flush_queue_.empty()) {
    // This cfd is already referenced
    const FlushRequest& flush_req = PopFirstFromFlushQueue();
    superversion_contexts.clear();
    superversion_contexts.reserve(flush_req.size());

    for (const auto& iter : flush_req) {
      ColumnFamilyData* cfd = iter.first;
      if (cfd->IsDropped() || !cfd->imm()->IsFlushPending()) {
        // can't flush this CF, try next one
        cfd->dec_queued_for_flush();
        if (cfd->Unref()) {
          delete cfd;
        }
        continue;
      }
      superversion_contexts.emplace_back(SuperVersionContext(true));
      bg_flush_args.emplace_back(cfd, iter.second,
                                 &(superversion_contexts.back()));
    }
    if (!bg_flush_args.empty()) {
      break;
    }
  }

  if (!bg_flush_args.empty()) {
    auto bg_job_limits = GetBGJobLimits();
    for (const auto& arg : bg_flush_args) {
      ColumnFamilyData* cfd = arg.cfd_;
      ROCKS_LOG_BUFFER(
          log_buffer,
          "Calling FlushMemTableToOutputFile with column "
          "family [%s], flush slots available %d, compaction slots available "
          "%d, "
          "flush slots scheduled %d, compaction slots scheduled %d",
          cfd->GetName().c_str(), bg_job_limits.max_flushes,
          bg_job_limits.max_compactions, bg_flush_scheduled_,
          bg_compaction_scheduled_);
    }
    status = FlushMemTablesToOutputFiles(bg_flush_args, made_progress,
                                         job_context, log_buffer);
    // All the CFDs in the FlushReq must have the same flush reason, so just
    // grab the first one
    *reason = bg_flush_args[0].cfd_->GetFlushReason();
    for (auto& arg : bg_flush_args) {
      ColumnFamilyData* cfd = arg.cfd_;
      cfd->dec_queued_for_flush();
      if (cfd->Unref()) {
        delete cfd;
        arg.cfd_ = nullptr;
      }
    }
  }
  return status;
}

void DBImpl::BackgroundCallFlush() {
  bool made_progress = false;
  JobContext job_context(next_job_id_.fetch_add(1), true);

  TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:start");

  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());
  {
    InstrumentedMutexLock l(&mutex_);

#ifndef ROCKSDB_LITE
    if (!memtable_info_queue_.empty() && !memtable_info_queue_lock_) {
      memtable_info_queue_lock_ = true;
      autovector<std::pair<ColumnFamilyData*, MemTableInfo>> queue;
      for (auto& item : memtable_info_queue_) {
        queue.emplace_back(std::move(item));
      }
      memtable_info_queue_.clear();
      mutex_.Unlock();
      for (auto& item : queue) {
        NotifyOnMemTableSealed(item.first, item.second);
      }
      mutex_.Lock();
      memtable_info_queue_lock_ = false;
      for (auto& item : queue) {
        if (item.first->Unref()) {
          delete item.first;
        }
      }
    }
#endif  // ROCKSDB_LITE
    FillLogWriterPool();

    assert(bg_flush_scheduled_);
    num_running_flushes_++;
    FlushReason reason;

    Status s =
        BackgroundFlush(&made_progress, &job_context, &log_buffer, &reason);
    if (!s.ok() && !s.IsShutdownInProgress() &&
        reason != FlushReason::kErrorRecovery) {
      // Wait a little bit before retrying background flush in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed flushes for the duration of
      // the problem.
      uint64_t error_cnt =
          default_cf_internal_stats_->BumpAndGetBackgroundErrorCount();
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      mutex_.Unlock();
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Waiting after background flush error: %s"
                      "Accumulated background error counts: %" PRIu64,
                      s.ToString().c_str(), error_cnt);
      log_buffer.FlushBufferToLog();
      LogFlush(immutable_db_options_.info_log);
      env_->SleepForMicroseconds(1000000);
      mutex_.Lock();
    }

    TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:FlushFinish:0");

    // If flush failed, we want to delete all temporary files that we might have
    // created. Thus, we force full scan in FindObsoleteFiles()
    FindObsoleteFiles(&job_context, !s.ok() && !s.IsShutdownInProgress());
    // delete unnecessary files if any, this is done outside the mutex
    if (job_context.HaveSomethingToClean() ||
        job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
      mutex_.Unlock();
      TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:FilesFound");
      // Have to flush the info logs before bg_flush_scheduled_--
      // because if bg_flush_scheduled_ becomes 0 and the lock is
      // released, the deconstructor of DB can kick in and destroy all the
      // states of DB so info_log might not be available after that point.
      // It also applies to access other states that DB owns.
      log_buffer.FlushBufferToLog();
      if (job_context.HaveSomethingToDelete()) {
        PurgeObsoleteFiles(job_context);
      }
      job_context.Clean(&mutex_);
      mutex_.Lock();
    }
    TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:ContextCleanedUp");

    assert(num_running_flushes_ > 0);
    num_running_flushes_--;
    bg_flush_scheduled_--;
    // See if there's more work to be done
    MaybeScheduleFlushOrCompaction();
    atomic_flush_install_cv_.SignalAll();
    bg_cv_.SignalAll();
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be dealloacated and referencing them
    // will cause trouble.
  }
}

void DBImpl::BackgroundCallCompaction(PrepickedCompaction* prepicked_compaction,
                                      Env::Priority bg_thread_pri) {
  bool made_progress = false;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  TEST_SYNC_POINT("BackgroundCallCompaction:0");
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());
  {
    InstrumentedMutexLock l(&mutex_);

    // This call will unlock/lock the mutex to wait for current running
    // IngestExternalFile() calls to finish.
    WaitForIngestFile();

    num_running_compactions_++;

    auto pending_outputs_inserted_elem =
        CaptureCurrentFileNumberInPendingOutputs();

    assert((bg_thread_pri == Env::Priority::BOTTOM &&
            bg_bottom_compaction_scheduled_) ||
           (bg_thread_pri == Env::Priority::LOW && bg_compaction_scheduled_));
    Status s = BackgroundCompaction(&made_progress, &job_context, &log_buffer,
                                    prepicked_compaction);
    TEST_SYNC_POINT("BackgroundCallCompaction:1");
    if (!s.ok() && !s.IsShutdownInProgress()) {
      // Wait a little bit before retrying background compaction in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed compactions for the duration of
      // the problem.
      uint64_t error_cnt =
          default_cf_internal_stats_->BumpAndGetBackgroundErrorCount();
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      mutex_.Unlock();
      log_buffer.FlushBufferToLog();
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Waiting after background compaction error: %s, "
                      "Accumulated background error counts: %" PRIu64,
                      s.ToString().c_str(), error_cnt);
      LogFlush(immutable_db_options_.info_log);
      env_->SleepForMicroseconds(1000000);
      mutex_.Lock();
    }

    ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);

    // If compaction failed, we want to delete all temporary files that we might
    // have created (they might not be all recorded in job_context in case of a
    // failure). Thus, we force full scan in FindObsoleteFiles()
    FindObsoleteFiles(&job_context, !s.ok() && !s.IsShutdownInProgress());
    TEST_SYNC_POINT("DBImpl::BackgroundCallCompaction:FoundObsoleteFiles");

    // delete unnecessary files if any, this is done outside the mutex
    if (job_context.HaveSomethingToClean() ||
        job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
      mutex_.Unlock();
      // Have to flush the info logs before bg_compaction_scheduled_--
      // because if bg_flush_scheduled_ becomes 0 and the lock is
      // released, the deconstructor of DB can kick in and destroy all the
      // states of DB so info_log might not be available after that point.
      // It also applies to access other states that DB owns.
      log_buffer.FlushBufferToLog();
      if (job_context.HaveSomethingToDelete()) {
        PurgeObsoleteFiles(job_context);
        TEST_SYNC_POINT("DBImpl::BackgroundCallCompaction:PurgedObsoleteFiles");
      }
      job_context.Clean(&mutex_);
      mutex_.Lock();
    }

    assert(num_running_compactions_ > 0);
    num_running_compactions_--;
    if (bg_thread_pri == Env::Priority::LOW) {
      bg_compaction_scheduled_--;
    } else {
      assert(bg_thread_pri == Env::Priority::BOTTOM);
      bg_bottom_compaction_scheduled_--;
    }

    versions_->GetColumnFamilySet()->FreeDeadColumnFamilies();

    // See if there's more work to be done
    MaybeScheduleFlushOrCompaction();
    if (made_progress ||
        (bg_compaction_scheduled_ == 0 &&
         bg_bottom_compaction_scheduled_ == 0) ||
        HasPendingManualCompaction() || unscheduled_compactions_ == 0) {
      // signal if
      // * made_progress -- need to wakeup DelayWrite
      // * bg_{bottom,}_compaction_scheduled_ == 0 -- need to wakeup ~DBImpl
      // * HasPendingManualCompaction -- need to wakeup RunManualCompaction
      // If none of this is true, there is no need to signal since nobody is
      // waiting for it
      bg_cv_.SignalAll();
    }
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be dealloacated and referencing them
    // will cause trouble.
  }
}

void DBImpl::BackgroundCallGarbageCollection() {
  bool made_progress = false;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  TEST_SYNC_POINT("BackgroundCallGarbageCollection:0");
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());
  {
    InstrumentedMutexLock l(&mutex_);

    // This call will unlock/lock the mutex to wait for current running
    // IngestExternalFile() calls to finish.
    WaitForIngestFile();

    num_running_garbage_collections_++;

    auto pending_outputs_inserted_elem =
        CaptureCurrentFileNumberInPendingOutputs();

    assert(bg_garbage_collection_scheduled_);
    Status s =
        BackgroundGarbageCollection(&made_progress, &job_context, &log_buffer);
    TEST_SYNC_POINT("BackgroundCallGarbageCollection:1");
    if (!s.ok() && !s.IsShutdownInProgress()) {
      // Wait a little bit before retrying background garbage collection in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed garbage collections for the duration of
      // the problem.
      uint64_t error_cnt =
          default_cf_internal_stats_->BumpAndGetBackgroundErrorCount();
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      mutex_.Unlock();
      log_buffer.FlushBufferToLog();
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Waiting after background garbage collection error: %s, "
                      "Accumulated background error counts: %" PRIu64,
                      s.ToString().c_str(), error_cnt);
      LogFlush(immutable_db_options_.info_log);
      env_->SleepForMicroseconds(1000000);
      mutex_.Lock();
    }

    ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);

    // If garbage collection failed, we want to delete all temporary files that
    // we might have created (they might not be all recorded in job_context in
    // case of a failure). Thus, we force full scan in FindObsoleteFiles()
    FindObsoleteFiles(&job_context, !s.ok() && !s.IsShutdownInProgress());
    TEST_SYNC_POINT(
        "DBImpl::BackgroundCallGarbageCollection:FoundObsoleteFiles");

    // delete unnecessary files if any, this is done outside the mutex
    if (job_context.HaveSomethingToClean() ||
        job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
      mutex_.Unlock();
      // Have to flush the info logs before bg_garbage_collection_scheduled_--
      // because if bg_flush_scheduled_ becomes 0 and the lock is
      // released, the deconstructor of DB can kick in and destroy all the
      // states of DB so info_log might not be available after that point.
      // It also applies to access other states that DB owns.
      log_buffer.FlushBufferToLog();
      if (job_context.HaveSomethingToDelete()) {
        PurgeObsoleteFiles(job_context);
        TEST_SYNC_POINT(
            "DBImpl::BackgroundCallGarbageCollection:PurgedObsoleteFiles");
      }
      job_context.Clean(&mutex_);
      mutex_.Lock();
    }

    assert(num_running_garbage_collections_ > 0);
    num_running_garbage_collections_--;
    bg_compaction_scheduled_--;
    bg_garbage_collection_scheduled_--;

    versions_->GetColumnFamilySet()->FreeDeadColumnFamilies();

    // See if there's more work to be done
    MaybeScheduleFlushOrCompaction();
    if (made_progress || bg_garbage_collection_scheduled_ == 0 ||
        unscheduled_garbage_collections_ == 0) {
      // signal if
      // * made_progress -- need to wakeup DelayWrite
      // * bg_garbage_collection_scheduled_ == 0 -- need to wakeup ~DBImpl
      // If none of this is true, there is no need to signal since nobody is
      // waiting for it
      bg_cv_.SignalAll();
    }
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be dealloacated and referencing them
    // will cause trouble.
  }
}

Status DBImpl::BackgroundCompaction(bool* made_progress,
                                    JobContext* job_context,
                                    LogBuffer* log_buffer,
                                    PrepickedCompaction* prepicked_compaction) {
  ManualCompactionState* manual_compaction =
      prepicked_compaction == nullptr
          ? nullptr
          : prepicked_compaction->manual_compaction_state;
  *made_progress = false;
  mutex_.AssertHeld();
  TEST_SYNC_POINT("DBImpl::BackgroundCompaction:Start");

  bool is_manual = (manual_compaction != nullptr);
  std::unique_ptr<Compaction> c;
  if (prepicked_compaction != nullptr &&
      prepicked_compaction->compaction != nullptr) {
    c.reset(prepicked_compaction->compaction);
  }
  bool is_prepicked = is_manual || c;

  // (manual_compaction->in_progress == false);
  bool trivial_move_disallowed =
      is_manual && manual_compaction->disallow_trivial_move;

  CompactionJobStats compaction_job_stats;
  Status status;
  if (!error_handler_.IsBGWorkStopped()) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      status = Status::ShutdownInProgress();
    }
  } else {
    status = error_handler_.GetBGError();
    // If we get here, it means a hard error happened after this compaction
    // was scheduled by MaybeScheduleFlushOrCompaction(), but before it got
    // a chance to execute. Since we didn't pop a cfd from the compaction
    // queue, increment unscheduled_compactions_
    unscheduled_compactions_++;
  }

  if (!status.ok()) {
    if (is_manual) {
      manual_compaction->status = status;
      manual_compaction->done = true;
      manual_compaction->in_progress = false;
      manual_compaction = nullptr;
    }
    if (c) {
      c->ReleaseCompactionFiles(status);
      c.reset();
    }
    return status;
  }

  if (is_manual) {
    // another thread cannot pick up the same work
    manual_compaction->in_progress = true;
  }

  // InternalKey manual_end_storage;
  // InternalKey* manual_end = &manual_end_storage;
  bool sfm_reserved_compact_space = false;
  bool enable_lazy_compaction = false;
  std::string cf_name = "(null)";
  if (is_manual) {
    ManualCompactionState* m = manual_compaction;
    assert(m->in_progress);
    enable_lazy_compaction = m->cfd->ioptions()->enable_lazy_compaction;
    cf_name = m->cfd->GetName();
    if (!c) {
      m->done = true;
      m->manual_end = nullptr;
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] Manual compaction from level-%d from %s .. "
                       "%s; nothing to do\n",
                       m->cfd->GetName().c_str(), m->input_level,
                       (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
                       (m->end ? m->end->DebugString().c_str() : "(end)"));
    } else {
      // First check if we have enough room to do the compaction
      bool enough_room = EnoughRoomForCompaction(
          m->cfd, *(c->inputs()), &sfm_reserved_compact_space, log_buffer);

      if (!enough_room) {
        // Then don't do the compaction
        c->ReleaseCompactionFiles(status);
        c.reset();
        // m's vars will get set properly at the end of this function,
        // as long as status == CompactionTooLarge
        status = Status::CompactionTooLarge();
      } else {
        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Manual compaction from level-%d to level-%d from %s .. "
            "%s; will stop at %s\n",
            m->cfd->GetName().c_str(), m->input_level, c->output_level(),
            (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
            (m->end ? m->end->DebugString().c_str() : "(end)"),
            ((m->done || m->manual_end == nullptr)
                 ? "(end)"
                 : m->manual_end->DebugString().c_str()));
      }
    }
  } else if (!is_prepicked && !compaction_queue_.empty()) {
    if (HasExclusiveManualCompaction()) {
      // Can't compact right now, but try again later
      TEST_SYNC_POINT("DBImpl::BackgroundCompaction()::Conflict");

      // Stay in the compaction queue.
      unscheduled_compactions_++;

      return Status::OK();
    }

    // cfd is referenced here
    auto cfd = PopFirstFromCompactionQueue();
    cf_name = cfd->GetName();
    // We unreference here because the following code will take a Ref() on
    // this cfd if it is going to use it (Compaction class holds a
    // reference).
    // This will all happen under a mutex so we don't have to be afraid of
    // somebody else deleting it.
    if (cfd->Unref()) {
      delete cfd;
      // This was the last reference of the column family, so no need to
      // compact.
      return Status::OK();
    }
    enable_lazy_compaction = cfd->ioptions()->enable_lazy_compaction;

    // Pick up latest mutable CF Options and use it throughout the
    // compaction job
    // Compaction makes a copy of the latest MutableCFOptions. It should be used
    // throughout the compaction procedure to make sure consistency. It will
    // eventually be installed into SuperVersion
    auto* mutable_cf_options = cfd->GetLatestMutableCFOptions();
    if (!mutable_cf_options->disable_auto_compactions && !cfd->IsDropped()) {
      // NOTE: try to avoid unnecessary copy of MutableCFOptions if
      // compaction is not necessary. Need to make sure mutex is held
      // until we make a copy in the following code
      TEST_SYNC_POINT("DBImpl::BackgroundCompaction():BeforePickCompaction");
      c.reset(cfd->PickCompaction(*mutable_cf_options, snapshots_.GetAll(),
                                  log_buffer));
      TEST_SYNC_POINT("DBImpl::BackgroundCompaction():AfterPickCompaction");

      if (c != nullptr) {
        bool enough_room = EnoughRoomForCompaction(
            cfd, *(c->inputs()), &sfm_reserved_compact_space, log_buffer);

        if (!enough_room) {
          // Then don't do the compaction
          c->ReleaseCompactionFiles(status);
          c->column_family_data()
              ->current()
              ->storage_info()
              ->ComputeCompactionScore(*(c->immutable_cf_options()),
                                       *(c->mutable_cf_options()));
          AddToCompactionQueue(cfd);
          ++unscheduled_compactions_;

          c.reset();
          // Don't need to sleep here, because BackgroundCallCompaction
          // will sleep if !s.ok()
          status = Status::CompactionTooLarge();
        } else {
          // update statistics
          MeasureTime(stats_, NUM_FILES_IN_SINGLE_COMPACTION,
                      c->inputs(0)->size());
          // There are three things that can change compaction score:
          // 1) When flush or compaction finish. This case is covered by
          // InstallSuperVersionAndScheduleWork
          // 2) When MutableCFOptions changes. This case is also covered by
          // InstallSuperVersionAndScheduleWork, because this is when the new
          // options take effect.
          // 3) When we Pick a new compaction, we "remove" those files being
          // compacted from the calculation, which then influences compaction
          // score. Here we check if we need the new compaction even without the
          // files that are currently being compacted. If we need another
          // compaction, we might be able to execute it in parallel, so we add
          // it to the queue and schedule a new thread.
          if (cfd->NeedsCompaction()) {
            // Yes, we need more compactions!
            AddToCompactionQueue(cfd);
            ++unscheduled_compactions_;
            MaybeScheduleFlushOrCompaction();
          }
        }
      }
    }
  }

  if (!c) {
    // Nothing to do
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Compaction nothing to do",
                     cf_name.c_str());
  } else if (!trivial_move_disallowed && c->IsTrivialMove()) {
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:TrivialMove");
    // Instrument for event update
    // TODO(yhchiang): add op details for showing trivial-move.
    ThreadStatusUtil::SetColumnFamily(
        c->column_family_data(), c->column_family_data()->ioptions()->env,
        immutable_db_options_.enable_thread_tracking);
    ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION);

    compaction_job_stats.num_input_files = c->num_input_files(0);

    NotifyOnCompactionBegin(c->column_family_data(), c.get(), status,
                            compaction_job_stats, job_context->job_id);

    // Move files to next level
    int32_t moved_files = 0;
    int64_t moved_bytes = 0;
    for (unsigned int l = 0; l < c->num_input_levels(); l++) {
      if (c->level(l) == c->output_level()) {
        continue;
      }
      for (size_t i = 0; i < c->num_input_files(l); i++) {
        FileMetaData* f = c->input(l, i);
        c->edit()->DeleteFile(c->level(l), f->fd.GetNumber());
        c->edit()->AddFile(
            c->output_level(), f->fd.GetNumber(), f->fd.GetPathId(),
            f->fd.GetFileSize(), f->smallest, f->largest, f->fd.smallest_seqno,
            f->fd.largest_seqno, f->marked_for_compaction, f->prop);
        // Make EventListenerTest.OnSingleDBCompactionTest happy
        c->AddOutputTableFileNumber(f->fd.GetNumber());

        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Moving #%" PRIu64 " to level-%d %" PRIu64 " bytes\n",
            c->column_family_data()->GetName().c_str(), f->fd.GetNumber(),
            c->output_level(), f->fd.GetFileSize());
        ++moved_files;
        moved_bytes += f->fd.GetFileSize();
      }
    }

    status = versions_->LogAndApply(c->column_family_data(),
                                    *c->mutable_cf_options(), c->edit(),
                                    &mutex_, directories_.GetDbDir());
    // Use latest MutableCFOptions
    InstallSuperVersionAndScheduleWork(
        c->column_family_data(), &job_context->superversion_contexts[0],
        *c->mutable_cf_options(), FlushReason::kAutoCompaction);

    VersionStorageInfo::LevelSummaryStorage tmp;
    c->column_family_data()->internal_stats()->IncBytesMoved(c->output_level(),
                                                             moved_bytes);
    {
      event_logger_.LogToBuffer(log_buffer)
          << "job" << job_context->job_id << "event"
          << "trivial_move"
          << "destination_level" << c->output_level() << "files" << moved_files
          << "total_files_size" << moved_bytes;
    }
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] Moved #%d files to level-%d %" PRIu64 " bytes %s: %s\n",
        c->column_family_data()->GetName().c_str(), moved_files,
        c->output_level(), moved_bytes, status.ToString().c_str(),
        c->column_family_data()->current()->storage_info()->LevelSummary(&tmp));
    *made_progress = true;

    // Clear Instrument
    ThreadStatusUtil::ResetThreadStatus();
  } else if (!is_prepicked && c->output_level() > 0 &&
             c->output_level() ==
                 c->column_family_data()
                     ->current()
                     ->storage_info()
                     ->MaxOutputLevel(
                         immutable_db_options_.allow_ingest_behind) &&
             env_->GetBackgroundThreads(Env::Priority::BOTTOM) > 0) {
    // Forward compactions involving last level to the bottom pool if it exists,
    // such that compactions unlikely to contribute to write stalls can be
    // delayed or deprioritized.
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:ForwardToBottomPriPool");
    CompactionArg* ca = new CompactionArg;
    ca->db = this;
    ca->prepicked_compaction = new PrepickedCompaction;
    ca->prepicked_compaction->compaction = c.release();
    ca->prepicked_compaction->manual_compaction_state = nullptr;
    ++bg_bottom_compaction_scheduled_;
    env_->Schedule(&DBImpl::BGWorkBottomCompaction, ca, Env::Priority::BOTTOM,
                   this, &DBImpl::UnscheduleCallback);
  } else {
    // Normal compactions that needs to control the quantity of workers.
    int output_level __attribute__((__unused__));
    output_level = c->output_level();
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:NonTrivial",
                             &output_level);
    SequenceNumber earliest_write_conflict_snapshot;
    std::vector<SequenceNumber> snapshot_seqs =
        snapshots_.GetAll(&earliest_write_conflict_snapshot);

    auto snapshot_checker = snapshot_checker_.get();
    if (use_custom_gc_ && snapshot_checker == nullptr) {
      snapshot_checker = DisableGCSnapshotChecker::Instance();
    }
    assert(is_snapshot_supported_ || snapshots_.empty());
    CompactionJob compaction_job(
        job_context->job_id, c.get(), immutable_db_options_,
        env_options_for_compaction_, versions_.get(), &shutting_down_,
        preserve_deletes_seqnum_.load(), log_buffer, directories_.GetDbDir(),
        GetDataDir(c->column_family_data(), c->output_path_id()), stats_,
        &mutex_, &error_handler_, snapshot_seqs,
        earliest_write_conflict_snapshot, snapshot_checker, table_cache_,
        &event_logger_, c->mutable_cf_options()->paranoid_file_checks,
        c->mutable_cf_options()->report_bg_io_stats, dbname_,
        &compaction_job_stats);
    int sub_compaction_scheduled =
        compaction_job.Prepare(GetSubCompactionSlots(c->max_subcompactions()));
    bg_compaction_scheduled_ += sub_compaction_scheduled;

    NotifyOnCompactionBegin(c->column_family_data(), c.get(), status,
                            compaction_job_stats, job_context->job_id);

    mutex_.Unlock();
    compaction_job.Run();
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:NonTrivial:AfterRun");
    mutex_.Lock();
    bg_compaction_scheduled_ -= sub_compaction_scheduled;
    status = compaction_job.Install(*c->mutable_cf_options());
    if (status.ok()) {
      InstallSuperVersionAndScheduleWork(
          c->column_family_data(), &job_context->superversion_contexts[0],
          *c->mutable_cf_options(), FlushReason::kAutoCompaction);
    }
    *made_progress = true;
  }
  if (c != nullptr) {
    c->ReleaseCompactionFiles(status);
    *made_progress = true;

#ifndef ROCKSDB_LITE
    // Need to make sure SstFileManager does its bookkeeping
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    if (sfm && sfm_reserved_compact_space) {
      sfm->OnCompactionCompletion(c.get());
    }
#endif  // ROCKSDB_LITE

    NotifyOnCompactionCompleted(c->column_family_data(), c.get(), status,
                                compaction_job_stats, job_context->job_id);
  }

  if (status.ok() || status.IsCompactionTooLarge()) {
    // Done
  } else if (status.IsShutdownInProgress()) {
    // Ignore compaction errors found during shutting down
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log, "Compaction error: %s",
                   status.ToString().c_str());
    error_handler_.SetBGError(status, BackgroundErrorReason::kCompaction);
    if (c != nullptr && !is_manual && !error_handler_.IsBGWorkStopped()) {
      // Put this cfd back in the compaction queue so we can retry after some
      // time
      auto cfd = c->column_family_data();
      assert(cfd != nullptr);
      // Since this compaction failed, we need to recompute the score so it
      // takes the original input files into account
      c->column_family_data()
          ->current()
          ->storage_info()
          ->ComputeCompactionScore(*(c->immutable_cf_options()),
                                   *(c->mutable_cf_options()));
      if (!cfd->queued_for_compaction()) {
        AddToCompactionQueue(cfd);
        ++unscheduled_compactions_;
      }
    }
  }
  // this will unref its input_version and column_family_data
  c.reset();

  if (is_manual) {
    ManualCompactionState* m = manual_compaction;
    if (!status.ok()) {
      m->status = status;
      m->done = true;
    }
    // For universal compaction:
    //   Because universal compaction always happens at level 0, so one
    //   compaction will pick up all overlapped files. No files will be
    //   filtered out due to size limit and left for a successive compaction.
    //   So we can safely conclude the current compaction.
    //
    //   Also note that, if we don't stop here, then the current compaction
    //   writes a new file back to level 0, which will be used in successive
    //   compaction. Hence the manual compaction will never finish.
    //
    // Stop the compaction if manual_end points to nullptr -- this means
    // that we compacted the whole range. manual_end should always point
    // to nullptr in case of universal compaction
    if (m->manual_end == nullptr && !enable_lazy_compaction) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      // Universal and FIFO compactions should always compact the whole range
      if (m->cfd->ioptions()->compaction_style == kCompactionStyleUniversal ||
          enable_lazy_compaction) {
        // do nothing
      } else {
        assert(m->cfd->ioptions()->compaction_style !=
                   kCompactionStyleUniversal ||
               m->cfd->ioptions()->num_levels > 1);
        m->tmp_storage = *m->manual_end;
        m->begin = &m->tmp_storage;
      }
      m->incomplete = true;
    }
    m->in_progress = false;  // not being processed anymore
  }
  TEST_SYNC_POINT("DBImpl::BackgroundCompaction:Finish");
  return status;
}

Status DBImpl::BackgroundGarbageCollection(bool* made_progress,
                                           JobContext* job_context,
                                           LogBuffer* log_buffer) {
  *made_progress = false;
  mutex_.AssertHeld();
  TEST_SYNC_POINT("DBImpl::BackgroundGarbageCollection:Start");

  std::unique_ptr<Compaction> c;
  CompactionJobStats garbage_collection_job_stats;
  Status status;
  if (!error_handler_.IsBGWorkStopped()) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      status = Status::ShutdownInProgress();
    }
  } else {
    status = error_handler_.GetBGError();
    // If we get here, it means a hard error happened after this garbage
    // collections was scheduled by MaybeScheduleFlushOrCompaction(), but before
    // it got a chance to execute. Since we didn't pop a cfd from the garbage
    // collections queue, increment unscheduled_garbage_collections_
    unscheduled_garbage_collections_++;
  }

  if (!status.ok()) {
    return status;
  }

  bool sfm_reserved_compact_space = false;
  std::string cf_name = "(null)";
  if (!garbage_collection_queue_.empty()) {
    // cfd is referenced here
    auto cfd = PopFirstFromGarbageCollectionQueue();
    cf_name = cfd->GetName();
    // We unreference here because the following code will take a Ref() on
    // this cfd if it is going to use it (GarbageCollection class holds a
    // reference).
    // This will all happen under a mutex so we don't have to be afraid of
    // somebody else deleting it.
    if (cfd->Unref()) {
      delete cfd;
      // This was the last reference of the column family, so no need to
      // compact.
      return Status::OK();
    }

    // Pick up latest mutable CF Options and use it throughout the
    // garbage collection job
    // GarbageCollection makes a copy of the latest MutableCFOptions. It should
    // be used throughout the garbage collection procedure to make sure
    // consistency. It will eventually be installed into SuperVersion
    auto* mutable_cf_options = cfd->GetLatestMutableCFOptions();
    if (!mutable_cf_options->disable_auto_compactions && !cfd->IsDropped()) {
      // NOTE: try to avoid unnecessary copy of MutableCFOptions if
      // garbage collection is not necessary. Need to make sure mutex is held
      // until we make a copy in the following code
      TEST_SYNC_POINT(
          "DBImpl::BackgroundGarbageCollection():BeforePickGarbageCollection");
      c.reset(cfd->PickGarbageCollection(*mutable_cf_options, log_buffer));
      TEST_SYNC_POINT(
          "DBImpl::BackgroundGarbageCollection():AfterPickGarbageCollection");

      if (c != nullptr) {
        bool enough_room = EnoughRoomForCompaction(
            cfd, *(c->inputs()), &sfm_reserved_compact_space, log_buffer);

        if (!enough_room) {
          // Then don't do the garbage collection
          c->ReleaseCompactionFiles(status);
          AddToGarbageCollectionQueue(cfd);
          ++unscheduled_garbage_collections_;

          c.reset();
          // Don't need to sleep here, because BackgroundCallGarbageCollection
          // will sleep if !s.ok()
          status = Status::CompactionTooLarge();
        } else {
          // update statistics
          MeasureTime(stats_, NUM_FILES_IN_SINGLE_COMPACTION,
                      c->inputs(0)->size());
          bool maybe_schedule = false;
          if (cfd->NeedsGarbageCollection()) {
            // Yes, we need more garbage collections!
            AddToGarbageCollectionQueue(cfd);
            ++unscheduled_garbage_collections_;
            maybe_schedule = true;
          }
          if (!cfd->queued_for_compaction() && cfd->NeedsCompaction()) {
            // Yes, we need more compactions!
            AddToCompactionQueue(cfd);
            ++unscheduled_compactions_;
            maybe_schedule = true;
          }
          if (maybe_schedule) {
            MaybeScheduleFlushOrCompaction();
          }
        }
      }
    }
  }

  if (!c) {
    // Nothing to do
    ROCKS_LOG_BUFFER(log_buffer, "[%s] GarbageCollection nothing to do",
                     cf_name.c_str());
  } else {
    int output_level __attribute__((__unused__));
    output_level = c->output_level();
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundGarbageCollection:NonTrivial",
                             &output_level);
    SequenceNumber earliest_write_conflict_snapshot = kMaxSequenceNumber;
    std::vector<SequenceNumber> snapshot_seqs;
    auto snapshot_checker = DisableGCSnapshotChecker::Instance();

    CompactionJob garbage_collection_job(
        job_context->job_id, c.get(), immutable_db_options_,
        env_options_for_compaction_, versions_.get(), &shutting_down_,
        preserve_deletes_seqnum_.load(), log_buffer, directories_.GetDbDir(),
        GetDataDir(c->column_family_data(), c->output_path_id()), stats_,
        &mutex_, &error_handler_, snapshot_seqs,
        earliest_write_conflict_snapshot, snapshot_checker, table_cache_,
        &event_logger_, c->mutable_cf_options()->paranoid_file_checks,
        c->mutable_cf_options()->report_bg_io_stats, dbname_,
        &garbage_collection_job_stats);
    garbage_collection_job.Prepare(0 /* sub_compaction_slots */);
    NotifyOnCompactionBegin(c->column_family_data(), c.get(), status,
                            garbage_collection_job_stats, job_context->job_id);

    mutex_.Unlock();
    garbage_collection_job.Run();
    TEST_SYNC_POINT("DBImpl::BackgroundGarbageCollection:NonTrivial:AfterRun");
    mutex_.Lock();
    status = garbage_collection_job.Install(*c->mutable_cf_options());
    if (status.ok()) {
      InstallSuperVersionAndScheduleWork(
          c->column_family_data(), &job_context->superversion_contexts[0],
          *c->mutable_cf_options(), FlushReason::kAutoCompaction);
    }
    *made_progress = true;
  }
  if (c != nullptr) {
    c->ReleaseCompactionFiles(status);
    *made_progress = true;

#ifndef ROCKSDB_LITE
    // Need to make sure SstFileManager does its bookkeeping
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    if (sfm && sfm_reserved_compact_space) {
      sfm->OnCompactionCompletion(c.get());
    }
#endif  // ROCKSDB_LITE

    NotifyOnCompactionCompleted(c->column_family_data(), c.get(), status,
                                garbage_collection_job_stats,
                                job_context->job_id);
  }

  if (status.ok() || status.IsCompactionTooLarge()) {
    // Done
  } else if (status.IsShutdownInProgress()) {
    // Ignore garbage collection errors found during shutting down
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "GarbageCollection error: %s", status.ToString().c_str());
    error_handler_.SetBGError(status, BackgroundErrorReason::kCompaction);
    if (c != nullptr && !error_handler_.IsBGWorkStopped()) {
      // Put this cfd back in the garbage collection queue so we can retry after
      // some time
      auto cfd = c->column_family_data();
      assert(cfd != nullptr);

      if (!cfd->queued_for_garbage_collection()) {
        AddToGarbageCollectionQueue(cfd);
        ++unscheduled_garbage_collections_;
      }
    }
  }
  // this will unref its input_version and column_family_data
  c.reset();

  TEST_SYNC_POINT("DBImpl::BackgroundGarbageCollection:Finish");
  return status;
}

bool DBImpl::HasPendingManualCompaction() {
  return (!manual_compaction_dequeue_.empty());
}

void DBImpl::AddManualCompaction(DBImpl::ManualCompactionState* m) {
  manual_compaction_dequeue_.push_back(m);
}

void DBImpl::RemoveManualCompaction(DBImpl::ManualCompactionState* m) {
  // Remove from queue
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  while (it != manual_compaction_dequeue_.end()) {
    if (m == (*it)) {
      it = manual_compaction_dequeue_.erase(it);
      return;
    }
    it++;
  }
  assert(false);
}

bool DBImpl::ShouldntRunManualCompaction(ManualCompactionState* m) {
  if (num_running_ingest_file_ > 0) {
    // We need to wait for other IngestExternalFile() calls to finish
    // before running a manual compaction.
    return true;
  }
  if (m->exclusive) {
    return (bg_bottom_compaction_scheduled_ > 0 ||
            bg_compaction_scheduled_ > bg_garbage_collection_scheduled_);
  }
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  bool seen = false;
  while (it != manual_compaction_dequeue_.end()) {
    if (m == (*it)) {
      it++;
      seen = true;
      continue;
    } else if (MCOverlap(m, (*it)) && (!seen && !(*it)->in_progress)) {
      // Consider the other manual compaction *it, conflicts if:
      // overlaps with m
      // and (*it) is ahead in the queue and is not yet in progress
      return true;
    }
    it++;
  }
  return false;
}

bool DBImpl::HaveManualCompaction(ColumnFamilyData* cfd) {
  // Remove from priority queue
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  while (it != manual_compaction_dequeue_.end()) {
    if ((*it)->exclusive) {
      return true;
    }
    if ((cfd == (*it)->cfd) && (!((*it)->in_progress || (*it)->done))) {
      // Allow automatic compaction if manual compaction is
      // in progress
      return true;
    }
    it++;
  }
  return false;
}

bool DBImpl::HasExclusiveManualCompaction() {
  // Remove from priority queue
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  while (it != manual_compaction_dequeue_.end()) {
    if ((*it)->exclusive) {
      return true;
    }
    it++;
  }
  return false;
}

bool DBImpl::MCOverlap(ManualCompactionState* m, ManualCompactionState* m1) {
  if ((m->exclusive) || (m1->exclusive)) {
    return true;
  }
  if (m->cfd != m1->cfd) {
    return false;
  }
  return true;
}

// SuperVersionContext gets created and destructed outside of the lock --
// we use this conveniently to:
// * malloc one SuperVersion() outside of the lock -- new_superversion
// * delete SuperVersion()s outside of the lock -- superversions_to_free
//
// However, if InstallSuperVersionAndScheduleWork() gets called twice with the
// same sv_context, we can't reuse the SuperVersion() that got
// malloced because
// first call already used it. In that rare case, we take a hit and create a
// new SuperVersion() inside of the mutex. We do similar thing
// for superversion_to_free

void DBImpl::InstallSuperVersionAndScheduleWork(
    ColumnFamilyData* cfd, SuperVersionContext* sv_context,
    const MutableCFOptions& mutable_cf_options,
    FlushReason /* flush_reason */) {
  // TODO(yanqin) investigate if 'flush_reason' can be removed since it's not
  // used.
  mutex_.AssertHeld();

  // Update max_total_in_memory_state_
  size_t old_memtable_size = 0;
  auto* old_sv = cfd->GetSuperVersion();
  if (old_sv) {
    old_memtable_size = old_sv->mutable_cf_options.write_buffer_size *
                        old_sv->mutable_cf_options.max_write_buffer_number;
  }

  // this branch is unlikely to step in
  if (UNLIKELY(sv_context->new_superversion == nullptr)) {
    sv_context->NewSuperVersion();
  }
  cfd->InstallSuperVersion(sv_context, &mutex_, mutable_cf_options);

  // Whenever we install new SuperVersion, we might need to issue new flushes or
  // compactions.
  SchedulePendingCompaction(cfd);
  SchedulePendingGarbageCollection(cfd);
  MaybeScheduleFlushOrCompaction();

  // Update max_total_in_memory_state_
  max_total_in_memory_state_ = max_total_in_memory_state_ - old_memtable_size +
                               mutable_cf_options.write_buffer_size *
                                   mutable_cf_options.max_write_buffer_number;
}

void DBImpl::SetSnapshotChecker(SnapshotChecker* snapshot_checker) {
  InstrumentedMutexLock l(&mutex_);
  // snapshot_checker_ should only set once. If we need to set it multiple
  // times, we need to make sure the old one is not deleted while it is still
  // using by a compaction job.
  assert(!snapshot_checker_);
  snapshot_checker_.reset(snapshot_checker);
}
}  // namespace TERARKDB_NAMESPACE
