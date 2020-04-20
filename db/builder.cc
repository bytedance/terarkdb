//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include <algorithm>
#include <deque>
#include <vector>

#include "db/compaction_iterator.h"
#include "db/dbformat.h"
#include "db/event_helpers.h"
#include "db/internal_stats.h"
#include "db/merge_helper.h"
#include "db/range_del_aggregator.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block_based_table_builder.h"
#include "table/format.h"
#include "table/internal_iterator.h"
#include "util/c_style_callback.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"

namespace rocksdb {

class TableFactory;

TableBuilder* NewTableBuilder(
    const ImmutableCFOptions& ioptions, const MutableCFOptions& moptions,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, const std::string& column_family_name,
    WritableFileWriter* file, const CompressionType compression_type,
    const CompressionOptions& compression_opts, int level,
    double compaction_load, const std::string* compression_dict,
    bool skip_filters, uint64_t creation_time, uint64_t oldest_key_time,
    SstPurpose sst_purpose) {
  assert((column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         column_family_name.empty());
  return ioptions.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, internal_comparator,
                          int_tbl_prop_collector_factories, compression_type,
                          compression_opts, compression_dict, skip_filters,
                          column_family_name, level, compaction_load,
                          creation_time, oldest_key_time, sst_purpose),
      column_family_id, file);
}

Status BuildTable(
    const std::string& dbname, VersionSet* versions_, Env* env,
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, const EnvOptions& env_options,
    TableCache* table_cache,
    InternalIterator* (*get_input_iter_callback)(void*, Arena&),
    void* get_input_iter_arg,
    std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>> (
        *get_range_del_iters_callback)(void*),
    void* get_range_del_iters_arg, FileMetaData* meta,
    std::vector<FileMetaData>* blob_meta,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, const std::string& column_family_name,
    std::vector<SequenceNumber> snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    SnapshotChecker* snapshot_checker, const CompressionType compression,
    const CompressionOptions& compression_opts, bool paranoid_file_checks,
    InternalStats* internal_stats, TableFileCreationReason reason,
    EventLogger* event_logger, int job_id, const Env::IOPriority io_priority,
    TableProperties* table_properties, int level, double compaction_load,
    const uint64_t creation_time, const uint64_t oldest_key_time,
    Env::WriteLifeTimeHint write_hint) {
  assert((column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         column_family_name.empty());
  // Reports the IOStats for flush for every following bytes.
  const size_t kReportFlushIOStatsEvery = 1048576;
  Status s;
  meta->fd.file_size = 0;
  Arena arena;
  ScopedArenaIterator iter(get_input_iter_callback(get_input_iter_arg, arena));
  iter->SeekToFirst();
  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
      new CompactionRangeDelAggregator(&internal_comparator, snapshots));
  for (auto& range_del_iter :
       get_range_del_iters_callback(get_range_del_iters_arg)) {
    range_del_agg->AddTombstones(std::move(range_del_iter));
  }

  std::string fname = TableFileName(ioptions.cf_paths, meta->fd.GetNumber(),
                                    meta->fd.GetPathId());
#ifndef ROCKSDB_LITE
  EventHelpers::NotifyTableFileCreationStarted(
      ioptions.listeners, dbname, column_family_name, fname, job_id, reason);
#endif  // !ROCKSDB_LITE
  TableProperties tp;

  if (iter->Valid() || !range_del_agg->IsEmpty()) {
    TableBuilder* builder;
    std::unique_ptr<WritableFileWriter> file_writer;
    {
      std::unique_ptr<WritableFile> file;
#ifndef NDEBUG
      bool use_direct_writes = env_options.use_direct_writes;
      TEST_SYNC_POINT_CALLBACK("BuildTable:create_file", &use_direct_writes);
#endif  // !NDEBUG
      s = NewWritableFile(env, fname, &file, env_options);
      if (!s.ok()) {
        EventHelpers::LogAndNotifyTableFileCreationFinished(
            event_logger, ioptions.listeners, dbname, column_family_name, fname,
            job_id, meta->fd, tp, reason, s);
        return s;
      }
      file->SetIOPriority(io_priority);
      file->SetWriteLifeTimeHint(write_hint);

      file_writer.reset(new WritableFileWriter(std::move(file), fname,
                                               env_options, ioptions.statistics,
                                               ioptions.listeners));
      builder = NewTableBuilder(
          ioptions, mutable_cf_options, internal_comparator,
          int_tbl_prop_collector_factories, column_family_id,
          column_family_name, file_writer.get(), compression, compression_opts,
          level, compaction_load, nullptr /* compression_dict */,
          false /* skip_filters */, creation_time, oldest_key_time);
    }

    MergeHelper merge(env, internal_comparator.user_comparator(),
                      ioptions.merge_operator, nullptr, ioptions.info_log,
                      true /* internal key corruption is not ok */,
                      snapshots.empty() ? 0 : snapshots.back(),
                      snapshot_checker);

    struct BuilderSeparateHelper : public SeparateHelper {
      std::vector<Dependence> dependence;
      std::vector<FileMetaData>* output = nullptr;
      std::string fname;
      std::unique_ptr<WritableFileWriter> file_writer;
      std::unique_ptr<TableBuilder> builder;
      FileMetaData* current_output = nullptr;
      Status (*trans_to_separate_callback)(void* args, const Slice& key,
                                           LazyBuffer& value);
      void* trans_to_separate_callback_args;

      Status TransToSeparate(const Slice& key, LazyBuffer& value) override {
        if (trans_to_separate_callback == nullptr) {
          return Status::NotSupported();
        }
        return trans_to_separate_callback(trans_to_separate_callback_args, key,
                                          value);
      }

      void TransToCombined(const Slice& /*user_key*/, uint64_t /*sequence*/,
                           LazyBuffer& /*value*/) const override {
        assert(false);
      }
    } separate_helper;

    auto finish_output_blob_sst = [&] {
      Status status;
      TableBuilder* blob_builder = separate_helper.builder.get();
      FileMetaData* blob_meta = separate_helper.current_output;
      blob_meta->prop.num_entries = blob_builder->NumEntries();
      blob_meta->prop.purpose = kEssenceSst;
      blob_meta->prop.flags |= TablePropertyCache::kNoRangeDeletions;
      status = blob_builder->Finish(&blob_meta->prop, nullptr);
      blob_meta->marked_for_compaction = blob_builder->NeedCompact();
      TableProperties tp;
      if (status.ok()) {
        blob_meta->fd.file_size = blob_builder->FileSize();
        tp = blob_builder->GetTableProperties();
        StopWatch sw(env, ioptions.statistics, TABLE_SYNC_MICROS);
        status = separate_helper.file_writer->Sync(ioptions.use_fsync);
      }
      if (status.ok()) {
        status = separate_helper.file_writer->Close();
      }
      separate_helper.file_writer.reset();
      EventHelpers::LogAndNotifyTableFileCreationFinished(
          event_logger, ioptions.listeners, dbname, column_family_name,
          separate_helper.fname, -1, blob_meta->fd, tp,
          TableFileCreationReason::kFlush, status);

      separate_helper.builder.reset();
      return s;
    };

    auto trans_to_separate = [&](const Slice& key, LazyBuffer& value) {
      assert(value.file_number() == uint64_t(-1));
      Status status;
      TableBuilder* blob_builder = separate_helper.builder.get();
      FileMetaData* blob_meta = separate_helper.current_output;
      if (blob_builder != nullptr &&
          blob_builder->FileSize() > mutable_cf_options.target_file_size_base) {
        status = finish_output_blob_sst();
        blob_builder = nullptr;
      }
      if (status.ok() && blob_builder == nullptr) {
        std::unique_ptr<WritableFile> blob_file;
#ifndef NDEBUG
        bool use_direct_writes = env_options.use_direct_writes;
        TEST_SYNC_POINT_CALLBACK("BuildTable:create_file", &use_direct_writes);
#endif  // !NDEBUG
        separate_helper.output->emplace_back();
        blob_meta = separate_helper.current_output =
            &separate_helper.output->back();
        blob_meta->fd =
            FileDescriptor(versions_->NewFileNumber(), meta->fd.GetPathId(), 0);
        separate_helper.fname = TableFileName(
            ioptions.cf_paths, blob_meta->fd.GetNumber(), meta->fd.GetPathId());
        status = NewWritableFile(env, separate_helper.fname, &blob_file,
                                 env_options);
        if (!status.ok()) {
          EventHelpers::LogAndNotifyTableFileCreationFinished(
              event_logger, ioptions.listeners, dbname, column_family_name,
              fname, job_id, blob_meta->fd, TableProperties(), reason, status);
          return status;
        }
        blob_file->SetIOPriority(io_priority);
        blob_file->SetWriteLifeTimeHint(write_hint);

        separate_helper.file_writer.reset(
            new WritableFileWriter(std::move(blob_file), fname, env_options,
                                   ioptions.statistics, ioptions.listeners));
        separate_helper.builder.reset(NewTableBuilder(
            ioptions, mutable_cf_options, internal_comparator,
            int_tbl_prop_collector_factories, column_family_id,
            column_family_name, separate_helper.file_writer.get(), compression,
            compression_opts, -1 /* level */, 0 /* compaction_load */, nullptr,
            true));
        blob_builder = separate_helper.builder.get();
      }
      if (status.ok()) {
        status = blob_builder->Add(key, value);
      }
      if (status.ok()) {
        blob_meta->UpdateBoundaries(key, GetInternalKeySeqno(key));
        uint64_t file_number = blob_meta->fd.GetNumber();
        value.reset(SeparateHelper::EncodeFileNumber(file_number), true,
                    file_number);
        auto& dependence = separate_helper.dependence;
        if (dependence.empty() ||
            dependence.back().file_number != file_number) {
          assert(dependence.empty() ||
                 file_number > dependence.back().file_number);
          dependence.emplace_back(Dependence{file_number, 1});
        } else {
          ++dependence.back().entry_count;
        }
      }
      return status;
    };

    separate_helper.output = blob_meta;
    BlobConfig blob_config = mutable_cf_options.get_blob_config();
    if (ioptions.table_factory->IsBuilderNeedSecondPass()) {
      blob_config.blob_size = size_t(-1);
    } else {
      separate_helper.trans_to_separate_callback =
          c_style_callback(trans_to_separate);
      separate_helper.trans_to_separate_callback_args = &trans_to_separate;
    }

    CompactionIterator c_iter(
        iter.get(), &separate_helper, nullptr,
        internal_comparator.user_comparator(), &merge, kMaxSequenceNumber,
        &snapshots, earliest_write_conflict_snapshot, snapshot_checker, env,
        ShouldReportDetailedTime(env, ioptions.statistics),
        true /* internal key corruption is not ok */, range_del_agg.get(),
        nullptr, blob_config);

    struct SecondPassIterStorage {
      std::unique_ptr<CompactionRangeDelAggregator> range_del_agg;
      ScopedArenaIterator iter;
      std::aligned_storage<sizeof(MergeHelper), alignof(MergeHelper)>::type
          merge;

      ~SecondPassIterStorage() {
        if (iter.get() != nullptr) {
          range_del_agg.reset();
          iter.set(nullptr);
          auto merge_ptr = reinterpret_cast<MergeHelper*>(&merge);
          merge_ptr->~MergeHelper();
        }
      }
    } second_pass_iter_storage;

    auto make_compaction_iterator = [&] {
      second_pass_iter_storage.range_del_agg.reset(
          new CompactionRangeDelAggregator(&internal_comparator, snapshots));
      for (auto& range_del_iter :
           get_range_del_iters_callback(get_range_del_iters_arg)) {
        second_pass_iter_storage.range_del_agg->AddTombstones(
            std::move(range_del_iter));
      }
      second_pass_iter_storage.iter = ScopedArenaIterator(
          get_input_iter_callback(get_input_iter_arg, arena));
      auto merge_ptr = new (&second_pass_iter_storage.merge) MergeHelper(
          env, internal_comparator.user_comparator(), ioptions.merge_operator,
          nullptr, ioptions.info_log,
          true /* internal key corruption is not ok */,
          snapshots.empty() ? 0 : snapshots.back(), snapshot_checker);
      return new CompactionIterator(
          second_pass_iter_storage.iter.get(), &separate_helper, nullptr,
          internal_comparator.user_comparator(), merge_ptr, kMaxSequenceNumber,
          &snapshots, earliest_write_conflict_snapshot, snapshot_checker, env,
          false /* report_detailed_time */,
          true /* internal key corruption is not ok */, range_del_agg.get());
    };
    std::unique_ptr<InternalIterator> second_pass_iter(NewCompactionIterator(
        c_style_callback(make_compaction_iterator), &make_compaction_iterator));

    builder->SetSecondPassIterator(second_pass_iter.get());
    c_iter.SeekToFirst();
    for (; s.ok() && c_iter.Valid(); c_iter.Next()) {
      s = builder->Add(c_iter.key(), c_iter.value());
      meta->UpdateBoundaries(c_iter.key(), c_iter.ikey().sequence);

      // TODO(noetzli): Update stats after flush, too.
      if (io_priority == Env::IO_HIGH &&
          IOSTATS(bytes_written) >= kReportFlushIOStatsEvery) {
        ThreadStatusUtil::SetThreadOperationProperty(
            ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
      }
    }

    auto range_del_it = range_del_agg->NewIterator();
    for (range_del_it->SeekToFirst(); s.ok() && range_del_it->Valid();
         range_del_it->Next()) {
      auto tombstone = range_del_it->Tombstone();
      auto kv = tombstone.Serialize();
      s = builder->AddTombstone(kv.first.Encode(), LazyBuffer(kv.second));
      meta->UpdateBoundariesForRange(kv.first, tombstone.SerializeEndKey(),
                                     tombstone.seq_, internal_comparator);
    }

    // Finish and check for builder errors
    tp = builder->GetTableProperties();
    bool empty = builder->NumEntries() == 0 && tp.num_range_deletions == 0;
    if (s.ok()) {
      s = c_iter.status();
    }
    if (s.ok() && separate_helper.builder) {
      s = finish_output_blob_sst();
    }
    if (!s.ok() || empty) {
      builder->Abandon();
    } else {
      meta->prop.dependence = std::move(separate_helper.dependence);
      auto shrinked_snapshots = meta->ShrinkSnapshot(snapshots);
      s = builder->Finish(&meta->prop, &shrinked_snapshots);
      meta->prop.num_deletions = tp.num_deletions;
      meta->prop.raw_key_size = tp.raw_key_size;
      meta->prop.raw_value_size = tp.raw_value_size;
      meta->prop.flags |= tp.num_range_deletions > 0
                              ? 0
                              : TablePropertyCache::kNoRangeDeletions;
      meta->prop.flags |=
          tp.snapshots.empty() ? 0 : TablePropertyCache::kHasSnapshots;
    }

    if (s.ok() && !empty) {
      uint64_t file_size = builder->FileSize();
      meta->fd.file_size = file_size;
      meta->marked_for_compaction = builder->NeedCompact();
      meta->prop.num_entries = builder->NumEntries();
      assert(meta->fd.GetFileSize() > 0);
      // refresh now that builder is finished
      tp = builder->GetTableProperties();
      if (table_properties) {
        *table_properties = tp;
      }
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok() && !empty) {
      StopWatch sw(env, ioptions.statistics, TABLE_SYNC_MICROS);
      s = file_writer->Sync(ioptions.use_fsync);
    }
    if (s.ok() && !empty) {
      s = file_writer->Close();
    }

    if (s.ok() && !empty) {
      // this sst has no depend ...
      DependenceMap empty_dependence_map;
      assert(!meta->prop.is_map_sst());
      // Verify that the table is usable
      // We set for_compaction to false and don't OptimizeForCompactionTableRead
      // here because this is a special case after we finish the table building
      // No matter whether use_direct_io_for_flush_and_compaction is true,
      // we will regrad this verification as user reads since the goal is
      // to cache it here for further user reads
      std::unique_ptr<InternalIterator> it(table_cache->NewIterator(
          ReadOptions(), env_options, internal_comparator, *meta,
          empty_dependence_map, nullptr /* range_del_agg */,
          mutable_cf_options.prefix_extractor.get(), nullptr,
          (internal_stats == nullptr) ? nullptr
                                      : internal_stats->GetFileReadHist(0),
          false /* for_compaction */, nullptr /* arena */,
          false /* skip_filter */, level));
      s = it->status();
      if (s.ok() && paranoid_file_checks) {
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
        }
        s = it->status();
      }
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (!s.ok() || meta->fd.GetFileSize() == 0) {
    env->DeleteFile(fname);
  }

  // Output to event logger and fire events.
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      event_logger, ioptions.listeners, dbname, column_family_name, fname,
      job_id, meta->fd, tp, reason, s);

  return s;
}

}  // namespace rocksdb
