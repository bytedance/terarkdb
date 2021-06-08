//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction_job.h"

#include "table/iterator_wrapper.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include <algorithm>

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

#ifdef BOOSTLIB
#include <boost/fiber/all.hpp>
#endif

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

#include <functional>
#include <memory>
#include <set>
#include <thread>
#include <utility>
#include <vector>

#include "db/builder.h"
#include "db/db_impl.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "db/map_builder.h"
#include "db/memtable.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/range_del_aggregator.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/terark_namespace.h"
#include "table/block_based_table_factory.h"
#include "table/get_context.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "util/async_task.h"
#include "util/c_style_callback.h"
#include "util/chash_set.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/file_util.h"
#include "util/filename.h"
#include "util/heap.h"
#include "util/log_buffer.h"
#include "util/logging.h"
#include "util/random.h"
#include "util/sst_file_manager_impl.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "utilities/util/valvec.hpp"

namespace TERARKDB_NAMESPACE {

const char* GetCompactionReasonString(CompactionReason compaction_reason) {
  switch (compaction_reason) {
    case CompactionReason::kUnknown:
      return "Unknown";
    case CompactionReason::kLevelL0FilesNum:
      return "LevelL0FilesNum";
    case CompactionReason::kLevelMaxLevelSize:
      return "LevelMaxLevelSize";
    case CompactionReason::kUniversalSizeAmplification:
      return "UniversalSizeAmplification";
    case CompactionReason::kUniversalSizeRatio:
      return "UniversalSizeRatio";
    case CompactionReason::kUniversalSortedRunNum:
      return "UniversalSortedRunNum";
    case CompactionReason::kFIFOMaxSize:
      return "FIFOMaxSize";
    case CompactionReason::kFIFOReduceNumFiles:
      return "FIFOReduceNumFiles";
    case CompactionReason::kFIFOTtl:
      return "FIFOTtl";
    case CompactionReason::kManualCompaction:
      return "ManualCompaction";
    case CompactionReason::kFilesMarkedForCompaction:
      return "FilesMarkedForCompaction";
    case CompactionReason::kBottommostFiles:
      return "BottommostFiles";
    case CompactionReason::kFlush:
      return "Flush";
    case CompactionReason::kExternalSstIngestion:
      return "ExternalSstIngestion";
    case CompactionReason::kCompositeAmplification:
      return "CompositeAmplification";
    case CompactionReason::kTrivialMoveLevel:
      return "TrivialMoveLevel";
    case CompactionReason::kGarbageCollection:
      return "GarbageCollection";
    case CompactionReason::kRangeDeletion:
      return "RangeDeletion";
    case CompactionReason::kNumOfReasons:
      // fall through
    default:
      assert(false);
      return "Invalid";
  }
}

// Maintains state for each sub-compaction
struct CompactionJob::SubcompactionState {
  const Compaction* compaction;
  std::unique_ptr<CompactionIterator> c_iter;

  // The boundaries of the key-range this compaction is interested in. No two
  // subcompactions may have overlapping key-ranges.
  // 'start' is inclusive, 'end' is exclusive, and nullptr means unbounded
  const Slice *start, *end;

  // actual range for this subcompaction
  InternalKey actual_start, actual_end;

  // The return status of this subcompaction
  Status status;

  // Files produced by this subcompaction
  struct Output {
    FileMetaData meta;

    // Same as user_collected_properties["User.Collected.Transient.Stat"].
    // Should be transient and do not save to SST, but the code is too
    // complex, so we DO SAVE it to SST, to avoid errors
    // std::string  stat_one;
    bool finished;
    std::shared_ptr<const TableProperties> table_properties;
  };
  std::string stat_all;

  // State kept for output being generated
  std::vector<Output> outputs;
  std::unique_ptr<WritableFileWriter> outfile;
  std::unique_ptr<TableBuilder> builder;
  Output* current_output() {
    if (outputs.empty()) {
      // This subcompaction's outptut could be empty if compaction was aborted
      // before this subcompaction had a chance to generate any output files.
      // When subcompactions are executed sequentially this is more likely and
      // will be particulalry likely for the later subcompactions to be empty.
      // Once they are run in parallel however it should be much rarer.
      return nullptr;
    } else {
      return &outputs.back();
    }
  }

  std::vector<Output> blob_outputs;
  std::unique_ptr<WritableFileWriter> blob_outfile;
  std::unique_ptr<TableBuilder> blob_builder;
  Output* current_blob_output() {
    if (blob_outputs.empty()) {
      return nullptr;
    } else {
      return &blob_outputs.back();
    }
  }

  uint64_t current_output_file_size;

  // State during the subcompaction
  uint64_t total_bytes;
  uint64_t num_input_records;
  uint64_t num_output_records;
  CompactionJobStats compaction_job_stats;
  uint64_t approx_size;
  // An index that used to speed up ShouldStopBefore().
  size_t grandparent_index = 0;
  // The number of bytes overlapping between the current output and
  // grandparent files used in ShouldStopBefore().
  uint64_t overlapped_bytes = 0;
  // A flag determine whether the key has been seen in ShouldStopBefore()
  bool seen_key = false;
  std::string compression_dict;

  SubcompactionState(Compaction* c, const Slice* _start, const Slice* _end,
                     uint64_t size = 0)
      : compaction(c),
        start(_start),
        end(_end),
        outfile(nullptr),
        builder(nullptr),
        current_output_file_size(0),
        total_bytes(0),
        num_input_records(0),
        num_output_records(0),
        approx_size(size),
        grandparent_index(0),
        overlapped_bytes(0),
        seen_key(false),
        compression_dict() {
    assert(compaction != nullptr);
  }

  SubcompactionState(SubcompactionState&& o) { *this = std::move(o); }

  SubcompactionState& operator=(SubcompactionState&& o) {
    compaction = std::move(o.compaction);
    start = std::move(o.start);
    end = std::move(o.end);
    actual_start = std::move(o.actual_start);
    actual_end = std::move(o.actual_end);
    status = std::move(o.status);
    outputs = std::move(o.outputs);
    outfile = std::move(o.outfile);
    builder = std::move(o.builder);
    blob_outputs = std::move(o.blob_outputs);
    blob_outfile = std::move(o.blob_outfile);
    blob_builder = std::move(o.blob_builder);
    current_output_file_size = std::move(o.current_output_file_size);
    total_bytes = std::move(o.total_bytes);
    num_input_records = std::move(o.num_input_records);
    num_output_records = std::move(o.num_output_records);
    compaction_job_stats = std::move(o.compaction_job_stats);
    approx_size = std::move(o.approx_size);
    grandparent_index = std::move(o.grandparent_index);
    overlapped_bytes = std::move(o.overlapped_bytes);
    seen_key = std::move(o.seen_key);
    compression_dict = std::move(o.compression_dict);
    return *this;
  }

  // Because member unique_ptrs do not have these.
  SubcompactionState(const SubcompactionState&) = delete;

  SubcompactionState& operator=(const SubcompactionState&) = delete;

  // Returns true if we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key, uint64_t curr_file_size) {
    const InternalKeyComparator* icmp =
        &compaction->column_family_data()->internal_comparator();
    const std::vector<FileMetaData*>& grandparents = compaction->grandparents();

    // Scan to find earliest grandparent file that contains key.
    while (grandparent_index < grandparents.size() &&
           icmp->Compare(internal_key,
                         grandparents[grandparent_index]->largest.Encode()) >
               0) {
      if (seen_key) {
        overlapped_bytes += grandparents[grandparent_index]->fd.GetFileSize();
      }
      assert(grandparent_index + 1 >= grandparents.size() ||
             icmp->Compare(grandparents[grandparent_index]->largest,
                           grandparents[grandparent_index + 1]->smallest) <= 0);
      grandparent_index++;
    }
    seen_key = true;

    if (overlapped_bytes + curr_file_size >
        compaction->max_compaction_bytes()) {
      // Too much overlap for current output; start new output
      return true;
    }

    return false;
  }

  struct RebuildBlobsInfo {
    chash_set<uint64_t> blobs;
    uint64_t pop_count;
  };
  struct BlobRefInfo {
    uint64_t file_number;
    FileMetaData* meta;
    uint64_t ref_bytes;
    operator uint64_t() const { return file_number; }
  };

  struct InputBlobInfo {
    std::vector<BlobRefInfo> blobs;
    uint64_t input_bytes;
  };
  Status GetRebuildNeededBlobs(const CompactionJob* job,
                               RebuildBlobsInfo* output) {
    assert(output != nullptr);
    chash_set<uint64_t>& rebuild_blob_set = output->blobs;
    output->pop_count = 0;

    auto mutable_cf_options = compaction->mutable_cf_options();
    auto& score_map = compaction->current_blob_overlap_scores();
    auto ucmp = compaction->immutable_cf_options()->user_comparator;
    size_t target_overlap = mutable_cf_options->max_dependence_blob_overlap;
    uint64_t max_compaction_bytes =
        compaction->mutable_cf_options()->max_compaction_bytes;

    auto user_cmp = [ucmp](const InternalKey& k1, const InternalKey& k2) {
      return ucmp->Compare(k1.user_key(), k2.user_key());
    };

    // get all blob of inputs, and sort it
    InputBlobInfo input_blob_info;
    auto s = GetSortedInputBlobs(job, &input_blob_info);
    auto& blobs = input_blob_info.blobs;
    if (!s.ok() || blobs.empty()) {
      return s;
    }
    if (job->separation_type() == kCompactionForceRebuildBlob) {
      // when need_rebuild_blobs.empty() == true, means rebuild all blobs
      for (auto& info : blobs) {
        rebuild_blob_set.insert(info.file_number);
      }
      return s;
    }

    // get all blobs in target overlaped ranges, store those files in a min-heap
    // based on its overlap score, and summary all blobs compaction bytes
    auto blob_end_cmp = [user_cmp](BlobRefInfo* b1, BlobRefInfo* b2) {
      return user_cmp(b1->meta->largest, b2->meta->largest) > 0;
    };
    auto end_queue = make_heap<BlobRefInfo*>(blob_end_cmp);

    struct TargetRangeBlobItem {
      uint64_t file_number;
      uint64_t ref_bytes;
      double score;
    };
    auto blob_overlapscore_cmp = [score_map](const TargetRangeBlobItem& b1,
                                             const TargetRangeBlobItem& b2) {
      return b1.score > b2.score;
    };
    auto target_range_blob_heap =
        make_heap<TargetRangeBlobItem>(blob_overlapscore_cmp);

    uint64_t total_compaction_bytes = input_blob_info.input_bytes;
    if (compaction->partial_compaction() &&
        compaction->max_output_file_size() != 0) {
      total_compaction_bytes =
          std::min(total_compaction_bytes, compaction->max_output_file_size());
    }

    auto start = blobs[0].meta->smallest;
    end_queue.push(&blobs[0]);
    std::vector<InternalKey> prev_range;
    for (size_t i = 0; i < blobs.size() && !end_queue.empty();) {
      bool valid_range =
          prev_range.empty() ||
          !(user_cmp(prev_range[0], start) <= 0 &&
            user_cmp(prev_range[1], end_queue.top()->meta->largest) >= 0);
      bool find_target_range =
          end_queue.size() >= target_overlap && valid_range;
      if (find_target_range) {
        if (prev_range.empty()) {
          prev_range.push_back(start);
          prev_range.push_back(end_queue.top()->meta->largest);
        } else {
          prev_range[0] = start;
          prev_range[1] = end_queue.top()->meta->largest;
        }

        // collect blobs
        for (auto& b : end_queue.data()) {
          auto ib = rebuild_blob_set.emplace(b->file_number);
          if (ib.second) {
            total_compaction_bytes += b->ref_bytes;
            auto find = score_map.find(b->file_number);
            assert(find != score_map.end());
            double global_score = 1;
            if (find != score_map.end()) {
              global_score = find->second;
            }
            target_range_blob_heap.push(TargetRangeBlobItem{
                b->file_number, b->ref_bytes, global_score / b->ref_bytes});
          }
        }
      }

      if (i + 1 < blobs.size() &&
          (user_cmp(blobs[i + 1].meta->smallest,
                    end_queue.top()->meta->largest) < 0 ||
           end_queue.size() == 1)) {
        if (user_cmp(blobs[i + 1].meta->smallest,
                     end_queue.top()->meta->largest) >= 0) {
          end_queue.pop();
        }
        i++;
        start = blobs[i].meta->smallest;
        end_queue.push(&blobs[i]);
      } else {
        start = end_queue.top()->meta->largest;
        end_queue.pop();
      }
    }
    // pop smaller scores if total_compaction_bytes > max_compaction_bytes
    // remain one blob al least.

    size_t target_blob_file_size = MaxBlobSize(
        *mutable_cf_options, compaction->immutable_cf_options()->num_levels,
        compaction->immutable_cf_options()->compaction_style);

    uint64_t target_compaction_bytes =
        std::max(max_compaction_bytes,
                 input_blob_info.input_bytes + target_blob_file_size);
    while (total_compaction_bytes > target_compaction_bytes &&
           target_range_blob_heap.size() > 1) {
      total_compaction_bytes -= target_range_blob_heap.top().ref_bytes;
      rebuild_blob_set.erase(target_range_blob_heap.top().file_number);
      target_range_blob_heap.pop();
      ++output->pop_count;
    }
    TEST_SYNC_POINT_CALLBACK("Compaction::GetRebuildNeededBlobs::End",
                             &rebuild_blob_set);
    return Status::OK();
  }

 private:
  Status GetSortedInputBlobs(const CompactionJob* job, InputBlobInfo* output) {
    assert(output != nullptr);
    output->blobs.clear();
    output->input_bytes = 0;

    auto ucmp = compaction->immutable_cf_options()->user_comparator;
    auto& icmp = compaction->immutable_cf_options()->internal_comparator;

    // get sst in input range
    chash_set<uint64_t> input_sst;
    auto& dependence_map =
        compaction->input_version()->storage_info()->dependence_map();
    auto table_cache = compaction->column_family_data()->table_cache();
    auto create_iter = [&](const FileMetaData* file_metadata,
                           const DependenceMap& depend_map, Arena* arena,
                           TableReader** table_reader_ptr) {
      return table_cache->NewIterator(
          ReadOptions(), job->env_options_, icmp, *file_metadata, depend_map,
          nullptr, compaction->mutable_cf_options()->prefix_extractor.get(),
          table_reader_ptr, nullptr, false, arena, true, -1);
    };
    InternalKey internal_begin, internal_end;
    if (start != nullptr) {
      internal_begin.SetMinPossibleForUserKey(*start);
    }
    if (end != nullptr) {
      internal_end.SetMaxPossibleForUserKey(*end);
    }
    MapSstElement element;
    for (auto& level_files : *compaction->inputs()) {
      if (level_files.empty()) {
        continue;
      }
      Arena arena;
      ScopedArenaIterator iter(NewMapElementIterator(
          level_files.files.data(), level_files.files.size(), &icmp,
          &create_iter, c_style_callback(create_iter), &arena));
      for (level_files.level == 0 || start == nullptr
               ? iter->SeekToFirst()
               : iter->Seek(internal_begin.Encode());
           iter->Valid(); iter->Next()) {
        LazyBuffer value = iter->value();
        if (!value.fetch().ok() ||
            !element.Decode(iter->key(), value.slice())) {
          // TODO log error
          break;
        }
        if (level_files.level == 0 && start != nullptr &&
            icmp.Compare(element.largest_key, internal_begin.Encode()) < 0) {
          continue;
        }
        if (end != nullptr &&
            ucmp->Compare(ExtractUserKey(element.smallest_key), *end) > 0) {
          if (level_files.level == 0) {
            continue;
          } else {
            break;
          }
        }

        for (auto& link : element.link) {
          input_sst.emplace(link.file_number);
        }
      }
    }
    InternalKey smallest, largest;
    auto get_offset = [&](FileMetaData* meta, const InternalKey& ik,
                          uint64_t* offset_ptr) {
      TableReader* table_reader_ptr = meta->fd.table_reader;
      if (icmp.Compare(ik, meta->smallest) <= 0) {
        *offset_ptr = 0;
      } else if (icmp.Compare(meta->largest, ik) <= 0) {
        *offset_ptr = meta->fd.GetFileSize();
      } else if (table_reader_ptr != nullptr) {
        *offset_ptr = table_reader_ptr->ApproximateOffsetOf(ik.Encode());
      } else {
        TableCache* table_cache =
            compaction->column_family_data()->table_cache();
        Cache::Handle* handle = nullptr;
        auto s = table_cache->FindTable(
            job->env_options_, icmp, meta->fd, &handle,
            compaction->mutable_cf_options()->prefix_extractor.get());
        if (!s.ok()) {
          return s;
        }
        table_reader_ptr = table_cache->GetTableReaderFromHandle(handle);
        *offset_ptr = table_reader_ptr->ApproximateOffsetOf(ik.Encode());
        table_cache->ReleaseHandle(handle);
      }
      return Status::OK();
    };
    chash_map<uint64_t, std::pair<FileMetaData*, uint64_t>> blob_map;
    // collect and sort blob_map of sst
    for (auto& fn : input_sst) {
      auto find = dependence_map.find(fn);
      if (find == dependence_map.end()) {
        // TODO ???
        continue;
      }
      auto file = find->second;
      uint64_t start_offset = 0, end_offset = file->fd.GetFileSize();
      if (start != nullptr) {
        auto s = get_offset(file, internal_begin, &start_offset);
        if (!s.ok()) {
          return s;
        }
      }
      if (end != nullptr) {
        auto s = get_offset(file, internal_end, &end_offset);
        if (!s.ok()) {
          return s;
        }
      }
      assert(start_offset <= end_offset);
      output->input_bytes += std::max(start_offset, end_offset) - start_offset;
      for (auto& pair : file->prop.dependence) {
        find = dependence_map.find(pair.file_number);
        if (find == dependence_map.end() || find->second->is_gc_forbidden()) {
          continue;
        }
        auto ib =
            blob_map.emplace(find->second->fd.GetNumber(),
                             std::make_pair(find->second, pair.entry_count));
        if (!ib.second) {
          ib.first->second.second += pair.entry_count;
        }
      }
    }
    output->blobs.reserve(blob_map.size());
    for (auto& pair : blob_map) {
      auto meta = pair.second.first;
      uint64_t total_bytes = meta->fd.GetFileSize();
      uint64_t ref_bytes =
          total_bytes * pair.second.second / meta->prop.num_entries;
      output->blobs.emplace_back(BlobRefInfo{pair.first, meta, ref_bytes});
    }
    std::sort(output->blobs.begin(), output->blobs.end(),
              [&](const BlobRefInfo& b1, const BlobRefInfo& b2) {
                int c = ucmp->Compare(b1.meta->smallest.user_key(),
                                      b2.meta->smallest.user_key());
                if (c == 0) {
                  c = ucmp->Compare(b1.meta->largest.user_key(),
                                    b2.meta->largest.user_key());
                }
                return c < 0;
              });
    return Status::OK();
  }
};

// Maintains state for the entire compaction
struct CompactionJob::CompactionState {
  Compaction* const compaction;

  // REQUIRED: subcompaction states are stored in order of increasing
  // key-range
  std::vector<SubcompactionState> sub_compact_states;
  Status status;

  uint64_t total_bytes;
  uint64_t num_input_records;
  uint64_t num_output_records;

  explicit CompactionState(Compaction* c)
      : compaction(c),
        total_bytes(0),
        num_input_records(0),
        num_output_records(0) {}

  size_t NumOutputFiles() {
    size_t total = 0;
    for (auto& s : sub_compact_states) {
      total += s.outputs.size();
      total += s.blob_outputs.size();
    }
    return total;
  }

  Slice SmallestUserKey() {
    for (const auto& sub_compact_state : sub_compact_states) {
      if (sub_compact_state.status.ok() && !sub_compact_state.outputs.empty() &&
          sub_compact_state.outputs[0].finished) {
        return sub_compact_state.outputs[0].meta.smallest.user_key();
      }
    }
    // If there is no finished output, return an empty slice.
    return Slice(nullptr, 0);
  }

  Slice LargestUserKey() {
    for (auto it = sub_compact_states.rbegin(); it < sub_compact_states.rend();
         ++it) {
      if (it->status.ok() && !it->outputs.empty() &&
          it->current_output()->finished) {
        assert(it->current_output() != nullptr);
        return it->current_output()->meta.largest.user_key();
      }
    }
    // If there is no finished output, return an empty slice.
    return Slice(nullptr, 0);
  }
};

SeparationType CompactionJob::separation_type() const {
  return compact_->compaction->separation_type();
}

void CompactionJob::AggregateStatistics() {
  for (SubcompactionState& sc : compact_->sub_compact_states) {
    compact_->total_bytes += sc.total_bytes;
    compact_->num_input_records += sc.num_input_records;
    compact_->num_output_records += sc.num_output_records;
  }
  if (compaction_job_stats_) {
    for (SubcompactionState& sc : compact_->sub_compact_states) {
      compaction_job_stats_->Add(sc.compaction_job_stats);
    }
  }
}

CompactionJob::CompactionJob(
    int job_id, Compaction* compaction, const ImmutableDBOptions& db_options,
    const EnvOptions env_options, VersionSet* versions,
    const std::atomic<bool>* shutting_down,
    const SequenceNumber preserve_deletes_seqnum, LogBuffer* log_buffer,
    Directory* db_directory, Directory* output_directory, Statistics* stats,
    InstrumentedMutex* db_mutex, ErrorHandler* db_error_handler,
    std::vector<SequenceNumber> existing_snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    const SnapshotChecker* snapshot_checker, std::shared_ptr<Cache> table_cache,
    EventLogger* event_logger, bool paranoid_file_checks, bool measure_io_stats,
    const std::string& dbname, CompactionJobStats* compaction_job_stats)
    : job_id_(job_id),
      compact_(new CompactionState(compaction)),
      compaction_job_stats_(compaction_job_stats),
      compaction_stats_(compaction->compaction_reason(), 1),
      dbname_(dbname),
      db_options_(db_options),
      env_options_(env_options),
      env_(db_options.env),
      env_options_for_read_(
          env_->OptimizeForCompactionTableRead(env_options, db_options_)),
      versions_(versions),
      shutting_down_(shutting_down),
      preserve_deletes_seqnum_(preserve_deletes_seqnum),
      log_buffer_(log_buffer),
      db_directory_(db_directory),
      output_directory_(output_directory),
      stats_(stats),
      db_mutex_(db_mutex),
      db_error_handler_(db_error_handler),
      existing_snapshots_(std::move(existing_snapshots)),
      earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
      snapshot_checker_(snapshot_checker),
      table_cache_(std::move(table_cache)),
      event_logger_(event_logger),
      bottommost_level_(false),
      paranoid_file_checks_(paranoid_file_checks),
      measure_io_stats_(measure_io_stats),
      write_hint_(Env::WLTH_NOT_SET) {
  assert(log_buffer_ != nullptr);
  const auto* cfd = compact_->compaction->column_family_data();
  ThreadStatusUtil::SetColumnFamily(cfd, cfd->ioptions()->env,
                                    db_options_.enable_thread_tracking);
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION);
  ReportStartedCompaction(compaction);
}

CompactionJob::~CompactionJob() {
  assert(compact_ == nullptr);
  ThreadStatusUtil::ResetThreadStatus();
}

void CompactionJob::ReportStartedCompaction(Compaction* compaction) {
  const auto* cfd = compact_->compaction->column_family_data();
  ThreadStatusUtil::SetColumnFamily(cfd, cfd->ioptions()->env,
                                    db_options_.enable_thread_tracking);

  ThreadStatusUtil::SetThreadOperationProperty(ThreadStatus::COMPACTION_JOB_ID,
                                               job_id_);

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_INPUT_OUTPUT_LEVEL,
      (static_cast<uint64_t>(compact_->compaction->start_level()) << 32) +
          compact_->compaction->output_level());

  // In the current design, a CompactionJob is always created
  // for non-trivial compaction.
  assert(compaction->IsTrivialMove() == false ||
         compaction->is_manual_compaction() == true);

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_PROP_FLAGS, compaction->is_manual_compaction());

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_TOTAL_INPUT_BYTES,
      compaction->CalculateTotalInputSize());

  IOSTATS_RESET(bytes_written);
  IOSTATS_RESET(bytes_read);
  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_WRITTEN, 0);
  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_READ, 0);

  // Set the thread operation after operation properties
  // to ensure GetThreadList() can always show them all together.
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION);

  if (compaction_job_stats_) {
    compaction_job_stats_->is_manual_compaction =
        compaction->is_manual_compaction();
  }
}

int CompactionJob::Prepare(int sub_compaction_slots) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_PREPARE);
  // Generate file_levels_ for compaction berfore making Iterator
  auto* c = compact_->compaction;
  assert(c->column_family_data() != nullptr);
  assert(c->column_family_data()->current()->storage_info()->NumLevelFiles(
             compact_->compaction->level()) > 0);
  write_hint_ =
      c->column_family_data()->CalculateSSTWriteHint(c->output_level());
  // Is this compaction producing files at the bottommost level?
  bottommost_level_ = c->bottommost_level();

  if (c->compaction_type() != kMapCompaction && !c->input_range().empty()) {
    auto& input_range = c->input_range();
    size_t n =
        std::min({uint32_t(sub_compaction_slots + 1),
                  uint32_t(input_range.size()), c->max_subcompactions()});
    boundaries_.resize(n * 2);
    auto uc = c->column_family_data()->user_comparator();
    if (n < input_range.size()) {
      std::nth_element(input_range.begin(), input_range.begin() + n,
                       input_range.end(), TERARK_CMP(weight, >));
      input_range.resize(n);
      std::sort(input_range.begin(), input_range.end(),
                TERARK_FIELD(start) < *uc);
    }
    for (size_t i = 0; i < n; ++i) {
      Slice* start = &boundaries_[i * 2];
      Slice* end = &boundaries_[i * 2 + 1];
      *start = input_range[i].start;
      *end = input_range[i].limit;
      assert(input_range[i].include_start);
      if (uc->Compare(*start, c->GetSmallestUserKey()) == 0) {
        start = nullptr;
      }
      if (input_range[i].include_limit) {
        assert(uc->Compare(*end, c->GetLargestUserKey()) == 0);
        end = nullptr;
      }
      compact_->sub_compact_states.emplace_back(c, start, end);
    }
  } else if (c->ShouldFormSubcompactions()) {
    const uint64_t start_micros = env_->NowMicros();
    GenSubcompactionBoundaries(sub_compaction_slots + 1);
    MeasureTime(stats_, SUBCOMPACTION_SETUP_TIME,
                env_->NowMicros() - start_micros);

    assert(sizes_.size() == boundaries_.size() + 1);

    for (size_t i = 0; i <= boundaries_.size(); i++) {
      Slice* start = i == 0 ? nullptr : &boundaries_[i - 1];
      Slice* end = i == boundaries_.size() ? nullptr : &boundaries_[i];
      compact_->sub_compact_states.emplace_back(c, start, end, sizes_[i]);
    }
    MeasureTime(stats_, NUM_SUBCOMPACTIONS_SCHEDULED,
                compact_->sub_compact_states.size());
  } else {
    compact_->sub_compact_states.emplace_back(c, nullptr, nullptr);
  }
  assert(!compact_->sub_compact_states.empty());
  return static_cast<int>(compact_->sub_compact_states.size() - 1);
}

struct RangeWithSize {
  Range range;
  uint64_t size;

  RangeWithSize(const Slice& a, const Slice& b, uint64_t s = 0)
      : range(a, b), size(s) {}
};

// Generates a histogram representing potential divisions of key ranges from
// the input. It adds the starting and/or ending keys of certain input files
// to the working set and then finds the approximate size of data in between
// each consecutive pair of slices. Then it divides these ranges into
// consecutive groups such that each group has a similar size.
void CompactionJob::GenSubcompactionBoundaries(int max_usable_threads) {
  auto* c = compact_->compaction;
  auto* cfd = c->column_family_data();
  const Comparator* cfd_comparator = cfd->user_comparator();
  std::vector<Slice> bounds;
  int start_lvl = c->start_level();
  int out_lvl = c->output_level();

  // Add the starting and/or ending key of certain input files as a potential
  // boundary
  for (size_t lvl_idx = 0; lvl_idx < c->num_input_levels(); lvl_idx++) {
    int lvl = c->level(lvl_idx);
    if (lvl >= start_lvl && lvl <= out_lvl) {
      const LevelFilesBrief* flevel = c->input_levels(lvl_idx);
      size_t num_files = flevel->num_files;

      if (num_files == 0) {
        continue;
      }

      if (lvl == 0) {
        // For level 0 add the starting and ending key of each file since the
        // files may have greatly differing key ranges (not range-partitioned)
        for (size_t i = 0; i < num_files; i++) {
          bounds.emplace_back(flevel->files[i].smallest_key);
          bounds.emplace_back(flevel->files[i].largest_key);
        }
      } else {
        // For all other levels add the smallest/largest key in the level to
        // encompass the range covered by that level
        bounds.emplace_back(flevel->files[0].smallest_key);
        bounds.emplace_back(flevel->files[num_files - 1].largest_key);
        if (lvl == out_lvl) {
          // For the last level include the starting keys of all files since
          // the last level is the largest and probably has the widest key
          // range. Since it's range partitioned, the ending key of one file
          // and the starting key of the next are very close (or identical).
          for (size_t i = 1; i < num_files; i++) {
            bounds.emplace_back(flevel->files[i].smallest_key);
          }
        }
        for (size_t i = 0; i < num_files; i++) {
          if (flevel->files[i].file_metadata->prop.is_map_sst()) {
            auto& dependence_map =
                c->input_version()->storage_info()->dependence_map();
            for (auto& dependence :
                 flevel->files[i].file_metadata->prop.dependence) {
              auto find = dependence_map.find(dependence.file_number);
              if (find == dependence_map.end()) {
                assert(false);
                continue;
              }
              bounds.emplace_back(find->second->smallest.Encode());
              bounds.emplace_back(find->second->largest.Encode());
            }
          }
        }
      }
    }
  }
  terark::sort_a(bounds, &ExtractUserKey < *cfd_comparator);

  // Remove duplicated entries from bounds
  // bounds.resize(terark::unique_a(bounds, &ExtractUserKey ==
  // *cfd_comparator));
  bounds.resize(terark::unique_a(bounds, &ExtractUserKey == *cfd_comparator));

  // Combine consecutive pairs of boundaries into ranges with an approximate
  // size of data covered by keys in that range
  uint64_t sum = 0;
  std::vector<RangeWithSize> ranges;
  // Get input version from CompactionState since it's already referenced
  // earlier in SetInputVersioCompaction::SetInputVersion and will not change
  // when db_mutex_ is released below
  auto* v = compact_->compaction->input_version();
  for (auto it = bounds.begin();;) {
    const Slice a = *it;
    it++;

    if (it == bounds.end()) {
      break;
    }

    const Slice b = *it;

    // ApproximateSize could potentially create table reader iterator to seek
    // to the index block and may incur I/O cost in the process. Unlock db
    // mutex to reduce contention
    db_mutex_->Unlock();
    uint64_t size = versions_->ApproximateSize(v, a, b, start_lvl, out_lvl + 1);
    db_mutex_->Lock();
    ranges.emplace_back(a, b, size);
    sum += size;
  }

  // Group the ranges into subcompactions
  const double min_file_fill_percent = 4.0 / 5;
  int base_level = v->storage_info()->base_level();
  uint64_t max_output_files = static_cast<uint64_t>(std::ceil(
      sum / min_file_fill_percent /
      MaxFileSizeForLevel(
          *(c->mutable_cf_options()), out_lvl,
          c->immutable_cf_options()->compaction_style, base_level,
          c->immutable_cf_options()->level_compaction_dynamic_level_bytes)));
  int subcompactions =
      std::min({max_usable_threads, static_cast<int>(ranges.size()),
                static_cast<int>(c->max_subcompactions()),
                static_cast<int>(max_output_files)});

  if (subcompactions > 1) {
    double mean = sum * 1.0 / subcompactions;
    // Greedily add ranges to the subcompaction until the sum of the ranges'
    // sizes becomes >= the expected mean size of a subcompaction
    sum = 0;
    for (size_t i = 0; i < ranges.size() - 1; i++) {
      sum += ranges[i].size;
      if (subcompactions == 1) {
        // If there's only one left to schedule then it goes to the end so no
        // need to put an end boundary
        continue;
      }
      if (sum >= mean) {
        boundaries_.emplace_back(ExtractUserKey(ranges[i].range.limit));
        sizes_.emplace_back(sum);
        subcompactions--;
        sum = 0;
      }
    }
    sizes_.emplace_back(sum + ranges.back().size);
  } else {
    // Only one range so its size is the total sum of sizes computed above
    sizes_.emplace_back(sum);
  }
}

static std::shared_ptr<CompactionDispatcher> GetCmdLineDispatcher() {
  const char* cmdline = getenv("TerarkDB_compactionWorkerCommandLine");
  if (cmdline) {
#ifdef WITH_TERARK_ZIP
    return NewCommandLineCompactionDispatcher(cmdline);
#endif
  }
  return {};
}

Status CompactionJob::Run() {
  TEST_SYNC_POINT("CompactionJob::Run():OuterStart");
#ifdef WITH_TERARK_ZIP
  assert(!IsCompactionWorkerNode());
#endif
  ColumnFamilyData* cfd = compact_->compaction->column_family_data();
  CompactionDispatcher* dispatcher = cfd->ioptions()->compaction_dispatcher;
  Compaction* c = compact_->compaction;
  if (!dispatcher) {
    static std::shared_ptr<CompactionDispatcher> command_line_dispatcher(
        GetCmdLineDispatcher());
    dispatcher = command_line_dispatcher.get();
  }
  if (!dispatcher || c->compaction_type() != kKeyValueCompaction) {
    return RunSelf();
  }
  Status s;
  const ImmutableCFOptions* iopt = c->immutable_cf_options();
  CompactionWorkerContext context;
  context.user_comparator = iopt->user_comparator->Name();
  if (iopt->merge_operator != nullptr) {
    context.merge_operator = iopt->merge_operator->Name();
    s = iopt->merge_operator->Serialize(&context.merge_operator_data.data);
    if (s.IsNotSupported()) {
      return RunSelf();
    } else if (!s.ok()) {
      return s;
    }
  }
  if (iopt->value_meta_extractor_factory != nullptr) {
    context.value_meta_extractor_factory =
        iopt->value_meta_extractor_factory->Name();
    s = iopt->value_meta_extractor_factory->Serialize(
        &context.value_meta_extractor_factory_options.data);
    if (s.IsNotSupported()) {
      return RunSelf();
    } else if (!s.ok()) {
      return s;
    }
  }
  context.compaction_filter_context.is_full_compaction =
      c->is_full_compaction();
  context.compaction_filter_context.is_manual_compaction =
      c->is_manual_compaction();
  context.compaction_filter_context.column_family_id = cfd->GetID();
  context.compaction_filter_context.smallest_user_key =
      compact_->SmallestUserKey();
  context.compaction_filter_context.smallest_user_key =
      compact_->LargestUserKey();
  if (auto cf = iopt->compaction_filter) {
    s = cf->Serialize(&context.compaction_filter_data.data);
    if (s.IsNotSupported()) {
      return RunSelf();
    } else if (!s.ok()) {
      return s;
    }
    context.compaction_filter = cf->Name();
  } else if (auto factory = iopt->compaction_filter_factory) {
    s = factory->Serialize(&context.compaction_filter_data.data);
    if (s.IsNotSupported()) {
      return RunSelf();
    } else if (!s.ok()) {
      return s;
    }
    context.compaction_filter_factory = factory->Name();
  }
  context.blob_config = c->mutable_cf_options()->get_blob_config();
  context.separation_type = c->separation_type();
  context.table_factory = iopt->table_factory->Name();
  s = iopt->table_factory->GetOptionString(&context.table_factory_options,
                                           "\n");
  if (s.IsNotSupported()) {
    return RunSelf();
  } else if (!s.ok()) {
    return s;
  }
  context.bloom_locality = iopt->bloom_locality;
  for (auto& p : iopt->cf_paths) {
    context.cf_paths.push_back(p.path);
  }
  if (auto pe = c->mutable_cf_options()->prefix_extractor.get()) {
    context.prefix_extractor = pe->Name();
    context.prefix_extractor_options = pe->GetOptionString();
  }
  context.last_sequence = versions_->LastSequence();
  context.earliest_write_conflict_snapshot = earliest_write_conflict_snapshot_;
  context.preserve_deletes_seqnum = preserve_deletes_seqnum_;
  auto& dependence_map = c->input_version()->storage_info()->dependence_map();
  for (auto& pair : dependence_map) {
    context.file_metadata.emplace_back(pair.first, *pair.second);
  }
  for (auto& files : *c->inputs()) {
    for (auto& f : files.files) {
      if (dependence_map.count(f->fd.GetNumber()) == 0) {
        context.file_metadata.emplace_back(f->fd.GetNumber(), *f);
      }
      context.inputs.emplace_back(files.level, f->fd.GetNumber());
    }
  }
  context.cf_name = cfd->GetName();
  context.target_file_size = c->max_output_file_size();
  context.compression = c->output_compression();
  context.compression_opts = c->output_compression_opts();
  context.existing_snapshots = existing_snapshots_;
  context.smallest_user_key = c->GetSmallestUserKey();
  context.largest_user_key = c->GetLargestUserKey();
  context.level = c->level();
  context.output_level = c->output_level();
  context.number_levels = iopt->num_levels;
  context.skip_filters =
      c->mutable_cf_options()->optimize_filters_for_hits && bottommost_level_;
  context.bottommost_level = c->bottommost_level();
  context.allow_ingest_behind = iopt->allow_ingest_behind;
  context.preserve_deletes = iopt->preserve_deletes;
  TablePropertiesCollectorFactory::Context collector_context;
  collector_context.column_family_id =
      context.compaction_filter_context.column_family_id;
  collector_context.smallest_user_key = context.smallest_user_key;
  collector_context.largest_user_key = context.largest_user_key;
  for (auto& collector :
       *cfd->int_tbl_prop_collector_factories(*c->mutable_cf_options())) {
    std::string param;
    if (collector->NeedSerialize()) {
      collector->Serialize(&param, collector_context);
    }
    context.int_tbl_prop_collector_factories.push_back(
        {collector->Name(), {std::move(param)}});
  }
  std::vector<std::function<CompactionWorkerResult()>> results_fn;
  for (const auto& state : compact_->sub_compact_states) {
    if (state.start != nullptr) {
      context.has_start = true;
      context.start = *state.start;
    } else {
      context.has_start = false;
      context.start.clear();
    }
    if (state.end != nullptr) {
      context.has_end = true;
      context.end = *state.end;
    } else {
      context.has_end = false;
      context.end.clear();
    }
    results_fn.emplace_back(dispatcher->StartCompaction(context));
  }
  Status status = Status::Corruption();
  for (size_t i = 0; i < compact_->sub_compact_states.size(); ++i) {
    auto& sub_compact = compact_->sub_compact_states[i];
    CompactionWorkerResult result;
#if defined(NDEBUG)
    try {
#endif
      result = results_fn[i]();
      sub_compact.status = std::move(result.status);
      s = sub_compact.status;
      if (s.ok()) {
        sub_compact.stat_all = std::move(result.stat_all);
        for (auto& file_info : result.files) {
          uint64_t file_number = versions_->NewFileNumber();
          std::string fname = TableFileName(cfd->ioptions()->cf_paths,
                                            file_number, c->output_path_id());
          env_->RenameFile(file_info.file_name, fname);
          sub_compact.outputs.emplace_back();
          auto& output = sub_compact.outputs.back();
          output.meta.fd = FileDescriptor(
              file_number, c->output_path_id(), file_info.file_size,
              file_info.smallest_seqno, file_info.largest_seqno);
          output.meta.smallest = std::move(file_info.smallest);
          output.meta.largest = std::move(file_info.largest);
          output.meta.marked_for_compaction = file_info.marked_for_compaction;
          // output.stat_one = std::move(file_info.stat_one);
          std::unique_ptr<TERARKDB_NAMESPACE::RandomAccessFile> file;
          s = env_->NewRandomAccessFile(fname, &file, env_options_);
          if (!s.ok()) {
            break;
          }
          std::unique_ptr<TERARKDB_NAMESPACE::RandomAccessFileReader>
              file_reader(new TERARKDB_NAMESPACE::RandomAccessFileReader(
                  std::move(file), fname, env_));
          std::unique_ptr<TERARKDB_NAMESPACE::TableReader> reader;
          TableReaderOptions table_reader_options(
              *c->immutable_cf_options(),
              c->mutable_cf_options()->prefix_extractor.get(), env_options_,
              c->immutable_cf_options()->internal_comparator);
          s = c->immutable_cf_options()->table_factory->NewTableReader(
              table_reader_options, std::move(file_reader),
              output.meta.fd.file_size, &reader, false);
          if (!s.ok()) {
            break;
          }
          output.table_properties = reader->GetTableProperties();
          auto tp = output.table_properties.get();
          output.meta.prop.num_entries = tp->num_entries;
          output.meta.prop.num_deletions = tp->num_deletions;
          output.meta.prop.raw_key_size = tp->raw_key_size;
          output.meta.prop.raw_value_size = tp->raw_value_size;
          output.meta.prop.flags |= tp->num_range_deletions > 0
                                        ? 0
                                        : TablePropertyCache::kNoRangeDeletions;
          output.meta.prop.flags |=
              tp->snapshots.empty() ? 0 : TablePropertyCache::kHasSnapshots;
          output.meta.prop.purpose = tp->purpose;
          output.meta.prop.max_read_amp = tp->max_read_amp;
          output.meta.prop.read_amp = tp->read_amp;
          output.meta.prop.dependence = tp->dependence;
          output.meta.prop.inheritance =
              InheritanceTreeToSet(tp->inheritance_tree);
          if (iopt->ttl_extractor_factory != nullptr) {
            GetCompactionTimePoint(
                tp->user_collected_properties,
                &output.meta.prop.earliest_time_begin_compact,
                &output.meta.prop.latest_time_end_compact);
            ROCKS_LOG_INFO(
                db_options_.info_log,
                "CompactionOutput earliest_time_begin_compact = %" PRIu64
                ", latest_time_end_compact = %" PRIu64,
                output.meta.prop.earliest_time_begin_compact,
                output.meta.prop.latest_time_end_compact);
          }
          output.finished = true;
          c->AddOutputTableFileNumber(file_number);
        }
        if (s.ok()) {
          sub_compact.actual_start = std::move(result.actual_start);
          sub_compact.actual_end = std::move(result.actual_end);
        }
      }
      if (s.ok()) {
        status = Status::OK();
      } else {
        ROCKS_LOG_ERROR(
            db_options_.info_log,
            "[%s] [JOB %d] remote sub_compact failed with status = %s",
            sub_compact.compaction->column_family_data()->GetName().c_str(),
            job_id_, s.ToString().c_str());
        LogFlush(db_options_.info_log);
        if (!status.ok()) {
          status = s;
        }
      }
#if defined(NDEBUG)
    } catch (const std::exception& ex) {
      ROCKS_LOG_ERROR(
          db_options_.info_log,
          "[%s] [JOB %d] remote sub_compact failed with exception = %s",
          sub_compact.compaction->column_family_data()->GetName().c_str(),
          job_id_, ex.what());
      LogFlush(db_options_.info_log);
      if (!status.ok()) {
        status = Status::Corruption("remote sub_compact failed with exception",
                                    ex.what());
      }
    }
#endif
  }
  if (status.ok()) {
    status = VerifyFiles();
  }
  return status;
}

Status CompactionJob::RunSelf() {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_RUN);
  TEST_SYNC_POINT("CompactionJob::Run():Start");
  log_buffer_->FlushBufferToLog();
  LogCompaction();

  const size_t num_threads = compact_->sub_compact_states.size();
  assert(num_threads > 0);
  const uint64_t start_micros = env_->NowMicros();

  if (compact_->compaction->compaction_type() != kMapCompaction) {
    // map compact don't need multithreads
    std::vector<ProcessArg> vec_process_arg(num_threads - 1);
    for (size_t i = 0; i < num_threads - 1; i++) {
      vec_process_arg[i].job = this;
      vec_process_arg[i].task_id = int(i);
      vec_process_arg[i].future = vec_process_arg[i].finished.get_future();
      env_->Schedule(&CompactionJob::CallProcessCompaction, &vec_process_arg[i],
                     TERARKDB_NAMESPACE::Env::LOW, this, nullptr);
    }
    ProcessCompaction(&compact_->sub_compact_states.back());
    for (auto& arg : vec_process_arg) {
      arg.future.wait();
    }
  } else {
    assert(num_threads == 1);
  }

  compaction_stats_.micros = env_->NowMicros() - start_micros;
  MeasureTime(stats_, COMPACTION_TIME, compaction_stats_.micros);
  TEST_SYNC_POINT("CompactionJob::Run:BeforeVerify");

  // Check if any thread encountered an error during execution
  Status status;
  for (const auto& state : compact_->sub_compact_states) {
    if (!state.status.ok()) {
      status = state.status;
      break;
    }
  }

  if (status.ok() && output_directory_) {
    status = output_directory_->Fsync();
  }

  if (status.ok()) {
    status = VerifyFiles();
  }

  // Finish up all book-keeping to unify the subcompaction results
  AggregateStatistics();
  UpdateCompactionStats();
  RecordCompactionIOStats();
  LogFlush(db_options_.info_log);
  TEST_SYNC_POINT("CompactionJob::Run():End");

  compact_->status = status;
  return status;
}

Status CompactionJob::VerifyFiles() {
  std::vector<port::Thread> thread_pool;
  std::vector<const FileMetaData*> files_meta;
  for (const auto& state : compact_->sub_compact_states) {
    for (const auto& output : state.outputs) {
      files_meta.emplace_back(&output.meta);
    }
    for (const auto& output : state.blob_outputs) {
      files_meta.emplace_back(&output.meta);
    }
  }
  if (files_meta.empty()) {
    return Status::OK();
  }
  ColumnFamilyData* cfd = compact_->compaction->column_family_data();
  auto prefix_extractor =
      compact_->compaction->mutable_cf_options()->prefix_extractor.get();
  std::atomic<size_t> next_file_meta_idx(0);

  auto verify_table = [&]() {
    while (true) {
      size_t file_idx = next_file_meta_idx.fetch_add(1);
      if (file_idx >= files_meta.size()) {
        return Status::OK();
      }
      // Use empty depend files to disable map or link sst forward calls.
      // depend files will build in InstallCompactionResults
      DependenceMap empty_dependence_map;
      // Verify that the table is usable
      // We set for_compaction to false and don't OptimizeForCompactionTableRead
      // here because this is a special case after we finish the table building
      // No matter whether use_direct_io_for_flush_and_compaction is true, we
      // will regard this verification as user reads since the goal is to cache
      // it here for further user reads
      auto output_level = compact_->compaction->output_level();
      InternalIterator* iter = cfd->table_cache()->NewIterator(
          ReadOptions(), env_options_, cfd->internal_comparator(),
          *files_meta[file_idx], empty_dependence_map,
          nullptr /* range_del_agg */, prefix_extractor, nullptr,
          output_level == -1
              ? nullptr
              : cfd->internal_stats()->GetFileReadHist(output_level),
          false, nullptr /* arena */, false /* skip_filters */, output_level);
      auto s = iter->status();

      if (s.ok() && paranoid_file_checks_) {
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        }
        s = iter->status();
      }

      delete iter;

      if (!s.ok()) {
        return s;
      }
    }
  };
  size_t thread_count =
      std::min(files_meta.size(), compact_->sub_compact_states.size());
  std::vector<std::unique_ptr<AsyncTask<Status>>> vec_task;
  if (thread_count > 1) {
    vec_task.resize(thread_count - 1);
    for (size_t i = 0; i < thread_count - 1; ++i) {
      vec_task[i] = std::unique_ptr<AsyncTask<Status>>(
          new AsyncTask<Status>(verify_table));
      env_->Schedule(c_style_callback(*(vec_task[i])), vec_task[i].get());
    }
  }
  Status s = verify_table();
  for (auto& task : vec_task) {
    s.ok() ? s = task->get() : task->get();
  }
  return s;
}

Status CompactionJob::Install(const MutableCFOptions& mutable_cf_options) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_INSTALL);
  db_mutex_->AssertHeld();
  Status status = compact_->status;
  ColumnFamilyData* cfd = compact_->compaction->column_family_data();
  if (compact_->compaction->output_level() != -1) {
    cfd->internal_stats()->AddCompactionStats(
        compact_->compaction->output_level(), compaction_stats_);
  }

  if (status.ok()) {
    status = InstallCompactionResults(mutable_cf_options);
  }
  VersionStorageInfo::LevelSummaryStorage tmp;
  auto vstorage = cfd->current()->storage_info();
  const auto& stats = compaction_stats_;

  double read_write_amp = 0.0;
  double write_amp = 0.0;
  double bytes_read_per_sec = 0;
  double bytes_written_per_sec = 0;

  if (stats.bytes_read_non_output_levels > 0) {
    read_write_amp = (stats.bytes_written + stats.bytes_read_output_level +
                      stats.bytes_read_non_output_levels) /
                     static_cast<double>(stats.bytes_read_non_output_levels);
    write_amp = stats.bytes_written /
                static_cast<double>(stats.bytes_read_non_output_levels);
  }
  if (stats.micros > 0) {
    bytes_read_per_sec =
        (stats.bytes_read_non_output_levels + stats.bytes_read_output_level) /
        static_cast<double>(stats.micros);
    bytes_written_per_sec =
        stats.bytes_written / static_cast<double>(stats.micros);
  }

  ROCKS_LOG_BUFFER(
      log_buffer_,
      "[%s] compacted to: %s, MB/sec: %.1f rd, %.1f wr, level %d, "
      "files in(%d, %d) out(%d) "
      "MB in(%.1f, %.1f) out(%.1f), read-write-amplify(%.1f) "
      "write-amplify(%.1f) %s, records in: %" PRIu64
      ", records dropped: %" PRIu64 " output_compression: %s\n",
      cfd->GetName().c_str(), vstorage->LevelSummary(&tmp), bytes_read_per_sec,
      bytes_written_per_sec, compact_->compaction->output_level(),
      stats.num_input_files_in_non_output_levels,
      stats.num_input_files_in_output_level, stats.num_output_files,
      stats.bytes_read_non_output_levels / 1048576.0,
      stats.bytes_read_output_level / 1048576.0,
      stats.bytes_written / 1048576.0, read_write_amp, write_amp,
      status.ToString().c_str(), stats.num_input_records,
      stats.num_dropped_records,
      CompressionTypeToString(compact_->compaction->output_compression())
          .c_str());

  UpdateCompactionJobStats(stats);

  auto stream = event_logger_->LogToBuffer(log_buffer_);
  stream << "job" << job_id_ << "event"
         << "compaction_finished"
         << "compaction_time_micros" << compaction_stats_.micros
         << "output_level" << compact_->compaction->output_level()
         << "num_output_files" << compact_->NumOutputFiles()
         << "total_output_size" << compact_->total_bytes << "num_input_records"
         << compact_->num_input_records << "num_output_records"
         << compact_->num_output_records << "num_subcompactions"
         << compact_->sub_compact_states.size() << "output_compression"
         << CompressionTypeToString(compact_->compaction->output_compression());

  if (compaction_job_stats_ != nullptr) {
    stream << "num_single_delete_mismatches"
           << compaction_job_stats_->num_single_del_mismatch;
    stream << "num_single_delete_fallthrough"
           << compaction_job_stats_->num_single_del_fallthru;
  }

  if (measure_io_stats_ && compaction_job_stats_ != nullptr) {
    stream << "file_write_nanos" << compaction_job_stats_->file_write_nanos;
    stream << "file_range_sync_nanos"
           << compaction_job_stats_->file_range_sync_nanos;
    stream << "file_fsync_nanos" << compaction_job_stats_->file_fsync_nanos;
    stream << "file_prepare_write_nanos"
           << compaction_job_stats_->file_prepare_write_nanos;
  }

  stream << "lsm_state";
  stream.StartArray();
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    if (vstorage->LevelFiles(level).size() == 1 &&
        vstorage->LevelFiles(level).front()->prop.is_map_sst()) {
      stream << std::to_string(
          vstorage->LevelFiles(level).front()->prop.num_entries);
    } else {
      stream << vstorage->NumLevelFiles(level);
    }
  }
  stream.EndArray();

  CleanupCompaction();
  return status;
}

void CompactionJob::ProcessCompaction(SubcompactionState* sub_compact) {
  // SetThreadSched(kSchedIdle);
  switch (sub_compact->compaction->compaction_type()) {
    case kKeyValueCompaction:
      ProcessKeyValueCompaction(sub_compact);
      break;
    case kMapCompaction:
      assert(false);
      break;
    case kGarbageCollection:
      ProcessGarbageCollection(sub_compact);
      break;
    default:
      assert(false);
      break;
  }
  // SetThreadSched(kSchedOther);
}

void CompactionJob::ProcessKeyValueCompaction(SubcompactionState* sub_compact) {
  assert(sub_compact != nullptr);
  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
  CompactionRangeDelAggregator range_del_agg(&cfd->internal_comparator(),
                                             existing_snapshots_);

  // Although the v2 aggregator is what the level iterator(s) know about,
  // the AddTombstones calls will be propagated down to the v1 aggregator.
  std::unique_ptr<InternalIterator> input(versions_->MakeInputIterator(
      sub_compact->compaction, &range_del_agg, env_options_for_read_));

  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_PROCESS_KV);

  // I/O measurement variables
  PerfLevel prev_perf_level = PerfLevel::kEnableTime;
  const uint64_t kRecordStatsEvery = 1000;
  uint64_t prev_write_nanos = 0;
  uint64_t prev_fsync_nanos = 0;
  uint64_t prev_range_sync_nanos = 0;
  uint64_t prev_prepare_write_nanos = 0;
  if (measure_io_stats_) {
    prev_perf_level = GetPerfLevel();
    SetPerfLevel(PerfLevel::kEnableTime);
    prev_write_nanos = IOSTATS(write_nanos);
    prev_fsync_nanos = IOSTATS(fsync_nanos);
    prev_range_sync_nanos = IOSTATS(range_sync_nanos);
    prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
  }

  const MutableCFOptions* mutable_cf_options =
      sub_compact->compaction->mutable_cf_options();

  // To build compression dictionary, we sample the first output file, assuming
  // it'll reach the maximum length. We optionally pass these samples through
  // zstd's dictionary trainer, or just use them directly. Then, the dictionary
  // is used for compressing subsequent output files in the same subcompaction.
  const bool kUseZstdTrainer =
      sub_compact->compaction->output_compression_opts().zstd_max_train_bytes >
      0;
  const size_t kSampleBytes =
      kUseZstdTrainer
          ? sub_compact->compaction->output_compression_opts()
                .zstd_max_train_bytes
          : sub_compact->compaction->output_compression_opts().max_dict_bytes;
  const int kSampleLenShift = 6;  // 2^6 = 64-byte samples
  std::set<size_t> sample_begin_offsets;
  if (bottommost_level_ && kSampleBytes > 0) {
    const size_t kMaxSamples = kSampleBytes >> kSampleLenShift;
    const size_t kOutFileLen = static_cast<size_t>(MaxFileSizeForLevel(
        *mutable_cf_options, std::max(compact_->compaction->output_level(), 0),
        cfd->ioptions()->compaction_style,
        compact_->compaction->GetInputBaseLevel(),
        cfd->ioptions()->level_compaction_dynamic_level_bytes));
    if (kOutFileLen != port::kMaxSizet) {
      const size_t kOutFileNumSamples = kOutFileLen >> kSampleLenShift;
      Random64 generator{Random::GetTLSInstance()->Next()};
      for (size_t i = 0; i < kMaxSamples; ++i) {
        sample_begin_offsets.insert(
            static_cast<size_t>(generator.Uniform(kOutFileNumSamples))
            << kSampleLenShift);
      }
    }
  }

  auto compaction_filter = cfd->ioptions()->compaction_filter;
  std::unique_ptr<CompactionFilter> compaction_filter_from_factory = nullptr;
  if (compaction_filter == nullptr) {
    compaction_filter_from_factory =
        sub_compact->compaction->CreateCompactionFilter();
    compaction_filter = compaction_filter_from_factory.get();
  }
  MergeHelper merge(
      env_, cfd->user_comparator(), cfd->ioptions()->merge_operator,
      compaction_filter, db_options_.info_log.get(),
      false /* internal key corruption is expected */,
      existing_snapshots_.empty() ? 0 : existing_snapshots_.back(),
      snapshot_checker_, compact_->compaction->level(),
      db_options_.statistics.get(), shutting_down_);

  struct BuilderSeparateHelper : public SeparateHelper {
    SeparateHelper* separate_helper = nullptr;
    std::unique_ptr<ValueExtractor> value_meta_extractor;
    Status (*trans_to_separate_callback)(void* args, const Slice& key,
                                         LazyBuffer& value) = nullptr;
    void* trans_to_separate_callback_args = nullptr;

    Status TransToSeparate(const Slice& internal_key, LazyBuffer& value,
                           const Slice& meta, bool is_merge,
                           bool is_index) override {
      return SeparateHelper::TransToSeparate(
          internal_key, value, value.file_number(), meta, is_merge, is_index,
          value_meta_extractor.get());
    }

    Status TransToSeparate(const Slice& key, LazyBuffer& value) override {
      if (trans_to_separate_callback == nullptr) {
        return Status::NotSupported();
      }
      return trans_to_separate_callback(trans_to_separate_callback_args, key,
                                        value);
    }

    LazyBuffer TransToCombined(const Slice& user_key, uint64_t sequence,
                               const LazyBuffer& value) const override {
      return separate_helper->TransToCombined(user_key, sequence, value);
    }
  } separate_helper;
  if (compact_->compaction->immutable_cf_options()
          ->value_meta_extractor_factory != nullptr) {
    ValueExtractorContext context = {cfd->GetID()};
    separate_helper.value_meta_extractor =
        compact_->compaction->immutable_cf_options()
            ->value_meta_extractor_factory->CreateValueExtractor(context);
  }

  size_t target_blob_file_size =
      MaxBlobSize(*mutable_cf_options, cfd->ioptions()->num_levels,
                  cfd->ioptions()->compaction_style);

  auto trans_to_separate = [&](const Slice& key, LazyBuffer& value) {
    Status s;
    TableBuilder* blob_builder = sub_compact->blob_builder.get();
    FileMetaData* blob_meta = &sub_compact->current_blob_output()->meta;
    if (blob_builder != nullptr &&
        blob_builder->FileSize() > target_blob_file_size) {
      s = FinishCompactionOutputBlob(s, sub_compact, {});
      blob_builder = nullptr;
    }
    if (s.ok() && blob_builder == nullptr) {
      s = OpenCompactionOutputBlob(sub_compact);
      blob_builder = sub_compact->blob_builder.get();
      blob_meta = &sub_compact->current_blob_output()->meta;
    }
    if (s.ok()) {
      s = blob_builder->Add(key, value);
    }
    if (s.ok()) {
      blob_meta->UpdateBoundaries(key, GetInternalKeySeqno(key));
      s = SeparateHelper::TransToSeparate(
          key, value, blob_meta->fd.GetNumber(), Slice(),
          GetInternalKeyType(key) == kTypeMerge, false,
          separate_helper.value_meta_extractor.get());
    }
    return s;
  };

  separate_helper.separate_helper = sub_compact->compaction->input_version();
  if (!sub_compact->compaction->immutable_cf_options()
           ->table_factory->IsBuilderNeedSecondPass()) {
    separate_helper.trans_to_separate_callback =
        c_style_callback(trans_to_separate);
    separate_helper.trans_to_separate_callback_args = &trans_to_separate;
  }

  TEST_SYNC_POINT("CompactionJob::Run():Inprogress");

  const Slice* start = sub_compact->start;
  const Slice* end = sub_compact->end;
  if (start != nullptr) {
    sub_compact->actual_start.SetMinPossibleForUserKey(*start);
    input->Seek(sub_compact->actual_start.Encode());
  } else {
    sub_compact->actual_start.SetMinPossibleForUserKey(
        sub_compact->compaction->GetSmallestUserKey());
    input->SeekToFirst();
  }

  SubcompactionState::RebuildBlobsInfo rebuild_blobs_info;
  Status status = sub_compact->GetRebuildNeededBlobs(this, &rebuild_blobs_info);
  if (!status.ok()) {
    ROCKS_LOG_ERROR(
        db_options_.info_log,
        "[%s] [JOB %d] GetRebuildNeededBlobs Fail, status = %s",
        compact_->compaction->column_family_data()->GetName().c_str(), job_id_,
        status.ToString().c_str());
    // GetRebuildNeededBlobs fail, we can continue compaction
    rebuild_blobs_info.blobs.clear();
    rebuild_blobs_info.pop_count = 0;
    status = Status::OK();
  }

  sub_compact->c_iter.reset(new CompactionIterator(
      input.get(), &separate_helper, end, cfd->user_comparator(), &merge,
      versions_->LastSequence(), &existing_snapshots_,
      earliest_write_conflict_snapshot_, snapshot_checker_, env_,
      ShouldReportDetailedTime(env_, stats_), false, &range_del_agg,
      sub_compact->compaction, mutable_cf_options->get_blob_config(),
      compaction_filter, shutting_down_, preserve_deletes_seqnum_,
      &rebuild_blobs_info.blobs));
  auto c_iter = sub_compact->c_iter.get();
  c_iter->SeekToFirst();

  struct SecondPassIterStorage {
    std::aligned_storage<sizeof(CompactionRangeDelAggregator),
                         alignof(CompactionRangeDelAggregator)>::type
        range_del_agg;
    std::unique_ptr<CompactionFilter> compaction_filter_holder;
    const CompactionFilter* compaction_filter;
    std::aligned_storage<sizeof(MergeHelper), alignof(MergeHelper)>::type merge;
    std::unique_ptr<InternalIterator> input;

    ~SecondPassIterStorage() {
      if (input) {
        input.reset();
        auto merge_ptr = reinterpret_cast<MergeHelper*>(&merge);
        merge_ptr->~MergeHelper();
        compaction_filter_holder.reset();
        auto range_del_agg_ptr =
            reinterpret_cast<CompactionRangeDelAggregator*>(&range_del_agg);
        range_del_agg_ptr->~CompactionRangeDelAggregator();
      }
    }
  } second_pass_iter_storage;

  auto make_compaction_iterator = [&] {
    auto range_del_agg_ptr = new (&second_pass_iter_storage.range_del_agg)
        CompactionRangeDelAggregator(&cfd->internal_comparator(),
                                     existing_snapshots_);
    second_pass_iter_storage.compaction_filter =
        cfd->ioptions()->compaction_filter;
    if (second_pass_iter_storage.compaction_filter == nullptr) {
      second_pass_iter_storage.compaction_filter_holder =
          sub_compact->compaction->CreateCompactionFilter();
      second_pass_iter_storage.compaction_filter =
          second_pass_iter_storage.compaction_filter_holder.get();
    }
    auto merge_ptr = new (&second_pass_iter_storage.merge) MergeHelper(
        env_, cfd->user_comparator(), cfd->ioptions()->merge_operator,
        second_pass_iter_storage.compaction_filter, db_options_.info_log.get(),
        false /* internal key corruption is expected */,
        existing_snapshots_.empty() ? 0 : existing_snapshots_.back(),
        snapshot_checker_, compact_->compaction->level(),
        db_options_.statistics.get(), shutting_down_);
    second_pass_iter_storage.input.reset(versions_->MakeInputIterator(
        sub_compact->compaction, range_del_agg_ptr, env_options_for_read_));
    return new CompactionIterator(
        second_pass_iter_storage.input.get(), &separate_helper, end,
        cfd->user_comparator(), merge_ptr, versions_->LastSequence(),
        &existing_snapshots_, earliest_write_conflict_snapshot_,
        snapshot_checker_, env_, false, false, range_del_agg_ptr,
        sub_compact->compaction, mutable_cf_options->get_blob_config(),
        second_pass_iter_storage.compaction_filter, shutting_down_,
        preserve_deletes_seqnum_, &rebuild_blobs_info.blobs);
  };
  std::unique_ptr<InternalIterator> second_pass_iter(
      NewCompactionIterator(c_style_callback(make_compaction_iterator),
                            &make_compaction_iterator, start));
  if (c_iter->Valid() && sub_compact->compaction->output_level() != 0) {
    // ShouldStopBefore() maintains state based on keys processed so far. The
    // compaction loop always calls it on the "next" key, thus won't tell it the
    // first key. So we do that here.
    sub_compact->ShouldStopBefore(c_iter->key(),
                                  sub_compact->current_output_file_size);
  }
  const auto& c_iter_stats = c_iter->iter_stats();
  auto sample_begin_offset_iter = sample_begin_offsets.cbegin();
  // data_begin_offset and dict_sample_data are only valid while generating
  // dictionary from the first output file.
  size_t data_begin_offset = 0;
  std::string dict_sample_data;
  // single_output don't need sample
  if (!sub_compact->compaction->partial_compaction()) {
    dict_sample_data.reserve(kSampleBytes);
  }
  std::unordered_map<uint64_t, uint64_t> dependence;

  size_t yield_count = 0;
  while (status.ok() && !cfd->IsDropped() && c_iter->Valid()) {
    // Invariant: c_iter.status() is guaranteed to be OK if c_iter->Valid()
    // returns true.
    const Slice& key = c_iter->key();
    const LazyBuffer& value = c_iter->value();
    if (c_iter->ikey().type == kTypeValueIndex ||
        c_iter->ikey().type == kTypeMergeIndex) {
      assert(value.file_number() != uint64_t(-1));
      auto ib = dependence.emplace(value.file_number(), 1);
      if (!ib.second) {
        ++ib.first->second;
      }
    }

    assert(end == nullptr ||
           cfd->user_comparator()->Compare(c_iter->user_key(), *end) < 0);
    if (c_iter_stats.num_input_records % kRecordStatsEvery ==
        kRecordStatsEvery - 1) {
      RecordDroppedKeys(c_iter_stats, &sub_compact->compaction_job_stats);
      c_iter->ResetRecordCounts();
      RecordCompactionIOStats();
    }

    // Open output file if necessary
    if (sub_compact->builder == nullptr) {
      status = OpenCompactionOutputFile(sub_compact);
      if (!status.ok()) {
        break;
      }
      if ((compaction_filter == nullptr ||
           compaction_filter->IsStableChangeValue()) &&
          (cfd->ioptions()->merge_operator == nullptr ||
           cfd->ioptions()->merge_operator->IsStableMerge())) {
        sub_compact->builder->SetSecondPassIterator(second_pass_iter.get());
      }
    }
    assert(sub_compact->builder != nullptr);
    assert(sub_compact->current_output() != nullptr);
    status = sub_compact->builder->Add(key, value);
    if (!status.ok()) {
      break;
    }
    sub_compact->current_output_file_size = sub_compact->builder->FileSize();
    sub_compact->current_output()->meta.UpdateBoundaries(
        key, c_iter->ikey().sequence);
    sub_compact->num_output_records++;

    // partial_compaction always output single sst, don't need sample
    if (!sub_compact->compaction->partial_compaction() &&
        sub_compact->outputs.size() == 1) {  // first output file
      // Check if this key/value overlaps any sample intervals; if so, appends
      // overlapping portions to the dictionary.
      status = value.fetch();
      if (!status.ok()) {
        break;
      }
      for (const auto& data_elmt : {key, value.slice()}) {
        size_t data_end_offset = data_begin_offset + data_elmt.size();
        while (sample_begin_offset_iter != sample_begin_offsets.cend() &&
               *sample_begin_offset_iter < data_end_offset) {
          size_t sample_end_offset =
              *sample_begin_offset_iter + (1 << kSampleLenShift);
          // Invariant: Because we advance sample iterator while processing the
          // data_elmt containing the sample's last byte, the current sample
          // cannot end before the current data_elmt.
          assert(data_begin_offset < sample_end_offset);

          size_t data_elmt_copy_offset, data_elmt_copy_len;
          if (*sample_begin_offset_iter <= data_begin_offset) {
            // The sample starts before data_elmt starts, so take bytes starting
            // at the beginning of data_elmt.
            data_elmt_copy_offset = 0;
          } else {
            // data_elmt starts before the sample starts, so take bytes starting
            // at the below offset into data_elmt.
            data_elmt_copy_offset =
                *sample_begin_offset_iter - data_begin_offset;
          }
          if (sample_end_offset <= data_end_offset) {
            // The sample ends before data_elmt ends, so take as many bytes as
            // needed.
            data_elmt_copy_len =
                sample_end_offset - (data_begin_offset + data_elmt_copy_offset);
          } else {
            // data_elmt ends before the sample ends, so take all remaining
            // bytes in data_elmt.
            data_elmt_copy_len =
                data_end_offset - (data_begin_offset + data_elmt_copy_offset);
          }
          dict_sample_data.append(&data_elmt.data()[data_elmt_copy_offset],
                                  data_elmt_copy_len);
          if (sample_end_offset > data_end_offset) {
            // Didn't finish sample. Try to finish it with the next data_elmt.
            break;
          }
          // Next sample may require bytes from same data_elmt.
          sample_begin_offset_iter++;
        }
        data_begin_offset = data_end_offset;
      }
    }

    // Close output file if it is big enough. Two possibilities determine it's
    // time to close it: (1) the current key should be this file's last key, (2)
    // the next key should not be in this file.
    //
    // TODO(aekmekji): determine if file should be closed earlier than this
    // during subcompactions (i.e. if output size, estimated by input size, is
    // going to be 1.2MB and max_output_file_size = 1MB, prefer to have 0.6MB
    // and 0.6MB instead of 1MB and 0.2MB)
    bool output_file_ended = false;
    Status input_status;
    if (sub_compact->compaction->max_output_file_size() != 0 &&
        sub_compact->current_output_file_size >=
            sub_compact->compaction->max_output_file_size()) {
      // (1) this key terminates the file. For historical reasons, the iterator
      // status before advancing will be given to FinishCompactionOutputFile().
      input_status = input->status();
      output_file_ended = true;
    }
    c_iter->Next();
    if (!output_file_ended && c_iter->Valid() &&
        sub_compact->compaction->max_output_file_size() != 0 &&
        sub_compact->ShouldStopBefore(c_iter->key(),
                                      sub_compact->current_output_file_size) &&
        sub_compact->builder != nullptr) {
      // (2) this key belongs to the next file. For historical reasons, the
      // iterator status after advancing will be given to
      // FinishCompactionOutputFile().
      input_status = input->status();
      output_file_ended = true;
    }
    const Slice* next_key = nullptr;
    if (output_file_ended) {
      assert(sub_compact->compaction->max_output_file_size() != 0);
      if (c_iter->Valid()) {
        next_key = &c_iter->key();
      }
      // compaction_picker use user_key boundary, single user_key in multi
      // sst will make picker pick one or more unnecessary sst file(s) ???
      if (next_key != nullptr &&
          cfd->user_comparator()->Compare(
              ExtractUserKey(*next_key),
              sub_compact->outputs.back().meta.largest.user_key()) == 0) {
        output_file_ended = false;
      }
    }
    if (output_file_ended) {
      CompactionIterationStats range_del_out_stats;
      status = FinishCompactionOutputFile(input_status, sub_compact,
                                          &range_del_agg, &range_del_out_stats,
                                          dependence, next_key);
      dependence.clear();
      RecordDroppedKeys(range_del_out_stats,
                        &sub_compact->compaction_job_stats);
      if (sub_compact->compaction->partial_compaction()) {
        if (next_key != nullptr) {
          sub_compact->actual_end.SetMinPossibleForUserKey(
              ExtractUserKey(*next_key));
        }
        break;
      }
      sub_compact->overlapped_bytes = 0;
      if (sub_compact->outputs.size() == 1) {
        // Use samples from first output file to create dictionary for
        // compression of subsequent files.
        if (kUseZstdTrainer) {
          sub_compact->compression_dict = ZSTD_TrainDictionary(
              dict_sample_data, kSampleLenShift,
              sub_compact->compaction->output_compression_opts()
                  .max_dict_bytes);
        } else {
          sub_compact->compression_dict = std::move(dict_sample_data);
        }
      }
    }
  }

  sub_compact->num_input_records = c_iter_stats.num_input_records;
  sub_compact->compaction_job_stats.num_input_deletion_records =
      c_iter_stats.num_input_deletion_records;
  sub_compact->compaction_job_stats.num_corrupt_keys =
      c_iter_stats.num_input_corrupt_records;
  sub_compact->compaction_job_stats.num_single_del_fallthru =
      c_iter_stats.num_single_del_fallthru;
  sub_compact->compaction_job_stats.num_single_del_mismatch =
      c_iter_stats.num_single_del_mismatch;
  sub_compact->compaction_job_stats.total_input_raw_key_bytes +=
      c_iter_stats.total_input_raw_key_bytes;
  sub_compact->compaction_job_stats.total_input_raw_value_bytes +=
      c_iter_stats.total_input_raw_value_bytes;

  RecordTick(stats_, FILTER_OPERATION_TOTAL_TIME,
             c_iter_stats.total_filter_time);
  RecordDroppedKeys(c_iter_stats, &sub_compact->compaction_job_stats);
  RecordCompactionIOStats();

  if (status.ok() &&
      (shutting_down_->load(std::memory_order_relaxed) || cfd->IsDropped())) {
    status = Status::ShutdownInProgress(
        "Database shutdown or Column family drop during compaction");
  }
  if (status.ok()) {
    status = input->status();
  }
  if (status.ok()) {
    status = c_iter->status();
  }

  if (status.ok() && sub_compact->builder == nullptr &&
      sub_compact->outputs.size() == 0 && !range_del_agg.IsEmpty()) {
    // handle subcompaction containing only range deletions
    status = OpenCompactionOutputFile(sub_compact);
  }

  // Call FinishCompactionOutputFile() even if status is not ok: it needs to
  // close the output file.
  if (sub_compact->builder != nullptr) {
    CompactionIterationStats range_del_out_stats;
    Status s = FinishCompactionOutputFile(status, sub_compact, &range_del_agg,
                                          &range_del_out_stats, dependence);
    dependence.clear();
    if (status.ok()) {
      status = s;
    }
    RecordDroppedKeys(range_del_out_stats, &sub_compact->compaction_job_stats);
  }
  if (sub_compact->blob_builder != nullptr) {
    Status s = FinishCompactionOutputBlob(status, sub_compact, {});
    if (status.ok()) {
      status = s;
    }
  }

  if (!rebuild_blobs_info.blobs.empty()) {
    ROCKS_LOG_INFO(
        db_options_.info_log,
        "[%s] [JOB %d] Compaction auto rebuild %d(%d) blobs into %d new blobs",
        compact_->compaction->column_family_data()->GetName().c_str(), job_id_,
        rebuild_blobs_info.blobs.size(),
        rebuild_blobs_info.blobs.size() + rebuild_blobs_info.pop_count,
        sub_compact->blob_outputs.size());
  }

  if (measure_io_stats_) {
    sub_compact->compaction_job_stats.file_write_nanos +=
        IOSTATS(write_nanos) - prev_write_nanos;
    sub_compact->compaction_job_stats.file_fsync_nanos +=
        IOSTATS(fsync_nanos) - prev_fsync_nanos;
    sub_compact->compaction_job_stats.file_range_sync_nanos +=
        IOSTATS(range_sync_nanos) - prev_range_sync_nanos;
    sub_compact->compaction_job_stats.file_prepare_write_nanos +=
        IOSTATS(prepare_write_nanos) - prev_prepare_write_nanos;
    if (prev_perf_level != PerfLevel::kEnableTime) {
      SetPerfLevel(prev_perf_level);
    }
  }
  if (auto filt = compaction_filter) {
    ReapMatureAction(filt, &sub_compact->stat_all);
  }
  if (auto filt = second_pass_iter_storage.compaction_filter) {
    EraseFutureAction(filt);
  }

  sub_compact->c_iter.reset();
  input.reset();
  sub_compact->status = status;
}  // namespace TERARKDB_NAMESPACE

void CompactionJob::ProcessGarbageCollection(SubcompactionState* sub_compact) {
  assert(sub_compact != nullptr);
  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();

  std::unique_ptr<InternalIterator> input(versions_->MakeInputIterator(
      sub_compact->compaction, nullptr, env_options_for_read_));

  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_PROCESS_KV);

  // I/O measurement variables
  PerfLevel prev_perf_level = PerfLevel::kEnableTime;
  uint64_t prev_write_nanos = 0;
  uint64_t prev_fsync_nanos = 0;
  uint64_t prev_range_sync_nanos = 0;
  uint64_t prev_prepare_write_nanos = 0;
  if (measure_io_stats_) {
    prev_perf_level = GetPerfLevel();
    SetPerfLevel(PerfLevel::kEnableTime);
    prev_write_nanos = IOSTATS(write_nanos);
    prev_fsync_nanos = IOSTATS(fsync_nanos);
    prev_range_sync_nanos = IOSTATS(range_sync_nanos);
    prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
  }

  assert(sub_compact->start == nullptr);
  assert(sub_compact->end == nullptr);

  input->SeekToFirst();

  Arena arena;
  std::unordered_map<Slice, uint64_t, SliceHasher> conflict_map;
  std::mutex conflict_map_mutex;

  auto create_iter = [&](Arena* /* arena */) {
    return versions_->MakeInputIterator(sub_compact->compaction, nullptr,
                                        env_options_for_read_);
  };
  auto filter_conflict = [&](const Slice& ikey, const LazyBuffer& value) {
    std::lock_guard<std::mutex> lock(conflict_map_mutex);
    auto find = conflict_map.find(ikey);
    return find != conflict_map.end() && find->second != value.file_number();
  };

  LazyInternalIteratorWrapper second_pass_iter(
      c_style_callback(create_iter), &create_iter,
      c_style_callback(filter_conflict), &filter_conflict, nullptr /* arena */,
      shutting_down_);

  Status status = OpenCompactionOutputBlob(sub_compact);
  if (!status.ok()) {
    return;
  }
  sub_compact->blob_builder->SetSecondPassIterator(&second_pass_iter);

  Version* input_version = sub_compact->compaction->input_version();
  auto& dependence_map = input_version->storage_info()->dependence_map();
  auto& comp = cfd->internal_comparator();
  std::string last_key;
  uint64_t last_file_number = uint64_t(-1);
  IterKey iter_key;
  ParsedInternalKey ikey;
  struct {
    uint64_t input = 0;
    uint64_t garbage_type = 0;
    uint64_t get_not_found = 0;
    uint64_t file_number_mismatch = 0;
  } counter;
  std::vector<std::pair<uint64_t, FileMetaData*>> blob_meta_cache;
  assert(!sub_compact->compaction->inputs()->empty());
  blob_meta_cache.reserve(sub_compact->compaction->inputs()->front().size());
  while (status.ok() && !cfd->IsDropped() && input->Valid()) {
    ++counter.input;
    Slice curr_key = input->key();
    uint64_t curr_file_number = uint64_t(-1);
    if (!ParseInternalKey(curr_key, &ikey)) {
      status =
          Status::Corruption("ProcessGarbageCollection invalid InternalKey");
      break;
    }
    uint64_t blob_file_number = input->value().file_number();
    FileMetaData* blob_meta;
    auto find_cache = std::find_if(
        blob_meta_cache.begin(), blob_meta_cache.end(),
        [blob_file_number](const std::pair<uint64_t, FileMetaData*>& pair) {
          return pair.first == blob_file_number;
        });
    if (find_cache != blob_meta_cache.end()) {
      blob_meta = find_cache->second;
    } else {
      auto find_dependence_map = dependence_map.find(blob_file_number);
      if (find_dependence_map == dependence_map.end()) {
        status =
            Status::Corruption("ProcessGarbageCollection internal error !");
        break;
      }
      blob_meta = find_dependence_map->second;
      blob_meta_cache.emplace_back(blob_file_number, blob_meta);
      assert(blob_meta->fd.GetNumber() == blob_file_number);
    }
    do {
      if (ikey.type != kTypeValue && ikey.type != kTypeMerge) {
        ++counter.garbage_type;
        break;
      }
      iter_key.SetInternalKey(ikey.user_key, ikey.sequence, kValueTypeForSeek);
      Status s;
      ValueType type = kTypeDeletion;
      SequenceNumber seq = kMaxSequenceNumber;
      LazyBuffer value;
      input_version->GetKey(ikey.user_key, iter_key.GetInternalKey(), &s, &type,
                            &seq, &value, *blob_meta);
      if (s.IsNotFound()) {
        ++counter.get_not_found;
        break;
      } else if (!s.ok()) {
        status = std::move(s);
        break;
      } else if (seq != ikey.sequence ||
                 (type != kTypeValueIndex && type != kTypeMergeIndex)) {
        ++counter.get_not_found;
        break;
      }
      status = value.fetch();
      if (!status.ok()) {
        break;
      }
      uint64_t file_number = SeparateHelper::DecodeFileNumber(value.slice());
      auto find = dependence_map.find(file_number);
      if (find == dependence_map.end()) {
        status = Status::Corruption("Separate value dependence missing");
        break;
      }
      value = input->value();
      if (find->second->fd.GetNumber() != value.file_number()) {
        ++counter.file_number_mismatch;
        break;
      }
      curr_file_number = value.file_number();

      assert(sub_compact->blob_builder != nullptr);
      assert(sub_compact->current_blob_output() != nullptr);
      status = sub_compact->blob_builder->Add(curr_key, value);
      if (!status.ok()) {
        break;
      }
      sub_compact->current_blob_output()->meta.UpdateBoundaries(curr_key,
                                                                ikey.sequence);
      sub_compact->num_output_records++;
    } while (false);

    if (counter.input > 1 && comp.Compare(curr_key, last_key) == 0 &&
        (last_file_number & curr_file_number) != uint64_t(-1)) {
      assert(last_file_number == uint64_t(-1) ||
             curr_file_number == uint64_t(-1));
      uint64_t valid_file_number = last_file_number & curr_file_number;
      auto pinned_key = ArenaPinSlice(curr_key, &arena);
      std::lock_guard<std::mutex> lock(conflict_map_mutex);
      conflict_map.emplace(pinned_key, valid_file_number);
    }
    last_key.assign(curr_key.data(), curr_key.size());
    last_file_number = curr_file_number;

    input->Next();
  }

  if (status.ok() &&
      (shutting_down_->load(std::memory_order_relaxed) || cfd->IsDropped())) {
    status = Status::ShutdownInProgress(
        "Database shutdown or Column family drop during compaction");
  }
  if (status.ok()) {
    status = input->status();
  }
  std::vector<uint64_t> inheritance_tree;
  size_t inheritance_tree_pruge_count = 0;
  if (status.ok()) {
    status = BuildInheritanceTree(
        *sub_compact->compaction->inputs(), dependence_map, input_version,
        &inheritance_tree, &inheritance_tree_pruge_count);
  }
  Status s = FinishCompactionOutputBlob(status, sub_compact, inheritance_tree);
  if (status.ok()) {
    status = s;
  }
  if (status.ok()) {
    auto& meta = sub_compact->blob_outputs.front().meta;
    auto& inputs = *sub_compact->compaction->inputs();
    assert(inputs.size() == 1 && inputs.front().level == -1);
    auto& files = inputs.front().files;
    ROCKS_LOG_INFO(
        db_options_.info_log,
        "[%s] [JOB %d] Table #%" PRIu64 " GC: %" PRIu64
        " inputs from %zd files. %" PRIu64
        " clear, %.2f%% estimation: [ %" PRIu64 " garbage type, %" PRIu64
        " get not found, %" PRIu64
        " file number mismatch ], inheritance tree: %" PRIu64 " -> %" PRIu64,
        cfd->GetName().c_str(), job_id_, meta.fd.GetNumber(), counter.input,
        files.size(), counter.input - meta.prop.num_entries,
        sub_compact->compaction->num_antiquation() * 100. / counter.input,
        counter.garbage_type, counter.get_not_found,
        counter.file_number_mismatch,
        meta.prop.inheritance.size() + inheritance_tree_pruge_count,
        meta.prop.inheritance.size());
    if ((std::find_if(files.begin(), files.end(),
                      [](FileMetaData* f) {
                        return f->marked_for_compaction;
                      }) == files.end() &&
         files.size() == 1 && counter.input == meta.prop.num_entries) ||
        meta.prop.num_entries == 0) {
      ROCKS_LOG_INFO(db_options_.info_log,
                     "[%s] [JOB %d] Table #%" PRIu64
                     " GC purge %s records, dropped",
                     cfd->GetName().c_str(), job_id_, meta.fd.GetNumber(),
                     meta.prop.num_entries == 0 ? "whole" : "0");
      std::string fname = TableFileName(
          sub_compact->compaction->immutable_cf_options()->cf_paths,
          meta.fd.GetNumber(), meta.fd.GetPathId());
      env_->DeleteFile(fname);
      sub_compact->blob_outputs.clear();
    }
  }

  if (measure_io_stats_) {
    sub_compact->compaction_job_stats.file_write_nanos +=
        IOSTATS(write_nanos) - prev_write_nanos;
    sub_compact->compaction_job_stats.file_fsync_nanos +=
        IOSTATS(fsync_nanos) - prev_fsync_nanos;
    sub_compact->compaction_job_stats.file_range_sync_nanos +=
        IOSTATS(range_sync_nanos) - prev_range_sync_nanos;
    sub_compact->compaction_job_stats.file_prepare_write_nanos +=
        IOSTATS(prepare_write_nanos) - prev_prepare_write_nanos;
    if (prev_perf_level != PerfLevel::kEnableTime) {
      SetPerfLevel(prev_perf_level);
    }
  }

  input.reset();
  sub_compact->status = status;
}

void CompactionJob::RecordDroppedKeys(
    const CompactionIterationStats& c_iter_stats,
    CompactionJobStats* compaction_job_stats) {
  if (c_iter_stats.num_record_drop_user > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_USER,
               c_iter_stats.num_record_drop_user);
  }
  if (c_iter_stats.num_record_drop_hidden > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_NEWER_ENTRY,
               c_iter_stats.num_record_drop_hidden);
    if (compaction_job_stats) {
      compaction_job_stats->num_records_replaced +=
          c_iter_stats.num_record_drop_hidden;
    }
  }
  if (c_iter_stats.num_record_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_OBSOLETE,
               c_iter_stats.num_record_drop_obsolete);
    if (compaction_job_stats) {
      compaction_job_stats->num_expired_deletion_records +=
          c_iter_stats.num_record_drop_obsolete;
    }
  }
  if (c_iter_stats.num_record_drop_range_del > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_RANGE_DEL,
               c_iter_stats.num_record_drop_range_del);
  }
  if (c_iter_stats.num_range_del_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_RANGE_DEL_DROP_OBSOLETE,
               c_iter_stats.num_range_del_drop_obsolete);
  }
  if (c_iter_stats.num_optimized_del_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE,
               c_iter_stats.num_optimized_del_drop_obsolete);
  }
}

Status CompactionJob::FinishCompactionOutputFile(
    const Status& input_status, SubcompactionState* sub_compact,
    CompactionRangeDelAggregator* range_del_agg,
    CompactionIterationStats* range_del_out_stats,
    const std::unordered_map<uint64_t, uint64_t>& dependence,
    const Slice* next_table_min_key /* = nullptr */) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_SYNC_FILE);
  assert(sub_compact != nullptr);
  assert(sub_compact->outfile);
  assert(sub_compact->builder != nullptr);
  assert(sub_compact->current_output() != nullptr);

  uint64_t output_number = sub_compact->current_output()->meta.fd.GetNumber();
  assert(output_number != 0);

  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
  const Comparator* ucmp = cfd->user_comparator();

  // Check for iterator errors
  Status s = input_status;
  auto meta = &sub_compact->current_output()->meta;
  assert(meta != nullptr);
  if (s.ok() && range_del_agg != nullptr && !range_del_agg->IsEmpty()) {
    Slice lower_bound_guard, upper_bound_guard;
    std::string smallest_user_key;
    const Slice *lower_bound, *upper_bound;
    if (sub_compact->outputs.size() == 1) {
      // For the first output table, include range tombstones before the min key
      // but after the subcompaction boundary.
      lower_bound = sub_compact->start;
    } else if (meta->smallest.size() > 0) {
      smallest_user_key = meta->smallest.user_key().ToString(false /*hex*/);
      lower_bound_guard = Slice(smallest_user_key);
      lower_bound = &lower_bound_guard;
    } else {
      lower_bound = nullptr;
    }
    if (next_table_min_key != nullptr) {
      upper_bound_guard = ExtractUserKey(*next_table_min_key);
      // CompactionIterator will be invalid when arrive sub_compact->end
      assert(sub_compact->end == nullptr ||
             ucmp->Compare(upper_bound_guard, *sub_compact->end) < 0);
      // We would not split an user_key in to multi SST
      assert(meta->largest.size() == 0 ||
             ucmp->Compare(meta->largest.user_key(), upper_bound_guard) != 0);
      upper_bound = &upper_bound_guard;
    } else {
      upper_bound = sub_compact->end;
    }
    auto earliest_snapshot = kMaxSequenceNumber;
    if (existing_snapshots_.size() > 0) {
      earliest_snapshot = existing_snapshots_[0];
    }
    InternalKey smallest_candidate;
    InternalKey largest_candidate;
    auto it = range_del_agg->NewIterator();
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      auto tombstone = it->Tombstone();
      if (lower_bound != nullptr &&
          ucmp->Compare(tombstone.end_key_, *lower_bound) <= 0) {
        continue;
      }
      if (upper_bound != nullptr &&
          ucmp->Compare(tombstone.start_key_, *upper_bound) >= 0) {
        break;
      }
      if (bottommost_level_ && tombstone.seq_ <= earliest_snapshot) {
        // TODO(andrewkr): tombstones that span multiple output files are
        // counted for each compaction output file, so lots of double counting.
        range_del_out_stats->num_range_del_drop_obsolete++;
        range_del_out_stats->num_record_drop_obsolete++;
        continue;
      }
      if (lower_bound != nullptr &&
          ucmp->Compare(tombstone.start_key_, *lower_bound) < 0) {
        tombstone.start_key_ = *lower_bound;
        smallest_candidate.Set(*lower_bound, tombstone.seq_,
                               kTypeRangeDeletion);
      } else {
        smallest_candidate.Set(tombstone.start_key_, tombstone.seq_,
                               kTypeRangeDeletion);
      }
      if (upper_bound != nullptr &&
          ucmp->Compare(tombstone.end_key_, *upper_bound) > 0) {
        tombstone.end_key_ = *upper_bound;
        largest_candidate.Set(*upper_bound, kMaxSequenceNumber,
                              kTypeRangeDeletion);
      } else {
        largest_candidate.Set(tombstone.end_key_, kMaxSequenceNumber,
                              kTypeRangeDeletion);
      }
      assert(lower_bound == nullptr ||
             ucmp->Compare(*lower_bound, tombstone.start_key_) <= 0);
      assert(lower_bound == nullptr ||
             ucmp->Compare(*lower_bound, tombstone.end_key_) < 0);
      assert(upper_bound == nullptr ||
             ucmp->Compare(*upper_bound, tombstone.start_key_) > 0);
      assert(upper_bound == nullptr ||
             ucmp->Compare(*upper_bound, tombstone.end_key_) >= 0);
      if (ucmp->Compare(tombstone.start_key_, tombstone.end_key_) >= 0) {
        continue;
      }

      auto kv = tombstone.Serialize();
      s = sub_compact->builder->AddTombstone(kv.first.Encode(),
                                             LazyBuffer(kv.second));
      if (!s.ok()) {
        break;
      }
      meta->UpdateBoundariesForRange(smallest_candidate, largest_candidate,
                                     tombstone.seq_,
                                     cfd->internal_comparator());
    }
  }
  if (s.ok()) {
    meta->marked_for_compaction = sub_compact->builder->NeedCompact();
    meta->prop.num_entries = sub_compact->builder->NumEntries();
    for (auto& pair : dependence) {
      meta->prop.dependence.emplace_back(Dependence{pair.first, pair.second});
    }
    std::sort(meta->prop.dependence.begin(), meta->prop.dependence.end(),
              TERARK_CMP(file_number, <));

    auto shrinked_snapshots = meta->ShrinkSnapshot(existing_snapshots_);
    s = sub_compact->builder->Finish(&meta->prop, &shrinked_snapshots);
  } else {
    sub_compact->builder->Abandon();
  }
  const uint64_t current_bytes = sub_compact->builder->FileSize();
  if (s.ok()) {
    meta->fd.file_size = current_bytes;
  }
  sub_compact->current_output()->finished = true;
  sub_compact->total_bytes += current_bytes;

  // Finish and check for file errors
  if (s.ok()) {
    StopWatch sw(env_, stats_, COMPACTION_OUTFILE_SYNC_MICROS);
    s = sub_compact->outfile->Sync(db_options_.use_fsync);
  }
  if (s.ok()) {
    s = sub_compact->outfile->Close();
  }
  sub_compact->outfile.reset();

  TableProperties tp;
  if (s.ok()) {
    tp = sub_compact->builder->GetTableProperties();
    meta->prop.num_deletions = tp.num_deletions;
    meta->prop.raw_key_size = tp.raw_key_size;
    meta->prop.raw_value_size = tp.raw_value_size;
    meta->prop.flags |=
        tp.num_range_deletions > 0 ? 0 : TablePropertyCache::kNoRangeDeletions;
    meta->prop.flags |=
        tp.snapshots.empty() ? 0 : TablePropertyCache::kHasSnapshots;

    if (compact_->compaction->immutable_cf_options()->ttl_extractor_factory !=
        nullptr) {
      GetCompactionTimePoint(tp.user_collected_properties,
                             &meta->prop.earliest_time_begin_compact,
                             &meta->prop.latest_time_end_compact);
      ROCKS_LOG_INFO(db_options_.info_log,
                     "CompactionOutput earliest_time_begin_compact = %" PRIu64
                     ", latest_time_end_compact = %" PRIu64,
                     meta->prop.earliest_time_begin_compact,
                     meta->prop.latest_time_end_compact);
    }
  }

  if (s.ok() && tp.num_entries == 0 && tp.num_range_deletions == 0) {
    assert(meta->prop.num_entries == tp.num_entries);
    // If there is nothing to output, no necessary to generate a sst file.
    // This happens when the output level is bottom level, at the same time
    // the sub_compact output nothing.
    std::string fname =
        TableFileName(sub_compact->compaction->immutable_cf_options()->cf_paths,
                      meta->fd.GetNumber(), meta->fd.GetPathId());
    env_->DeleteFile(fname);

    // Also need to remove the file from outputs, or it will be added to the
    // VersionEdit.
    assert(!sub_compact->outputs.empty());
    sub_compact->outputs.pop_back();
    meta = nullptr;
  }

  if (s.ok() && (tp.num_entries > 0 || tp.num_range_deletions > 0)) {
    // Output to event logger and fire events.
    sub_compact->current_output()->table_properties =
        std::make_shared<TableProperties>(tp);
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Generated table #%" PRIu64 ": %" PRIu64
                   " keys, %" PRIu64 " bytes%s",
                   cfd->GetName().c_str(), job_id_, output_number,
                   meta->prop.num_entries, current_bytes,
                   meta->marked_for_compaction ? " (need compaction)" : "");
  }
  std::string fname;
  FileDescriptor output_fd;
  if (meta != nullptr) {
    fname =
        TableFileName(sub_compact->compaction->immutable_cf_options()->cf_paths,
                      meta->fd.GetNumber(), meta->fd.GetPathId());
    output_fd = meta->fd;
  } else {
    fname = "(nil)";
  }
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      event_logger_, cfd->ioptions()->listeners, dbname_, cfd->GetName(), fname,
      job_id_, output_fd, tp, TableFileCreationReason::kCompaction, s);

#ifndef ROCKSDB_LITE
  // Report new file to SstFileManagerImpl
  auto sfm =
      static_cast<SstFileManagerImpl*>(db_options_.sst_file_manager.get());
  if (sfm && meta != nullptr && meta->fd.GetPathId() == 0) {
    auto fn =
        TableFileName(sub_compact->compaction->immutable_cf_options()->cf_paths,
                      meta->fd.GetNumber(), meta->fd.GetPathId());
    sfm->OnAddFile(fn);
    if (sfm->IsMaxAllowedSpaceReached()) {
      // TODO(ajkr): should we return OK() if max space was reached by the final
      // compaction output file (similarly to how flush works when full)?
      s = Status::SpaceLimit("Max allowed space was reached");
      TEST_SYNC_POINT(
          "CompactionJob::FinishCompactionOutputFile:"
          "MaxAllowedSpaceReached");
      InstrumentedMutexLock l(db_mutex_);
      db_error_handler_->SetBGError(s, BackgroundErrorReason::kCompaction);
    }
  }
#endif

  sub_compact->builder.reset();
  sub_compact->current_output_file_size = 0;
  return s;
}

Status CompactionJob::FinishCompactionOutputBlob(
    const Status& input_status, SubcompactionState* sub_compact,
    const std::vector<uint64_t>& inheritance_tree) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_SYNC_FILE);
  assert(sub_compact != nullptr);
  assert(sub_compact->blob_outfile);
  assert(sub_compact->blob_builder != nullptr);
  assert(sub_compact->current_blob_output() != nullptr);
  TEST_SYNC_POINT_CALLBACK("CompactionJob::FinishCompactionOutputBlob::Start",
                           &sub_compact->current_blob_output()->meta);

  uint64_t output_number =
      sub_compact->current_blob_output()->meta.fd.GetNumber();
  assert(output_number != 0);

  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();

  // Check for iterator errors
  Status s = input_status;
  auto meta = &sub_compact->current_blob_output()->meta;
  assert(meta != nullptr);
  if (s.ok()) {
    meta->marked_for_compaction = sub_compact->blob_builder->NeedCompact();
    meta->prop.num_entries = sub_compact->blob_builder->NumEntries();
    meta->prop.inheritance = InheritanceTreeToSet(inheritance_tree);
    assert(std::is_sorted(meta->prop.inheritance.begin(),
                          meta->prop.inheritance.end()));
    s = sub_compact->blob_builder->Finish(&meta->prop, nullptr,
                                          &inheritance_tree);
  } else {
    sub_compact->blob_builder->Abandon();
  }
  const uint64_t current_bytes = sub_compact->blob_builder->FileSize();
  if (s.ok()) {
    meta->fd.file_size = current_bytes;
  }
  sub_compact->current_blob_output()->finished = true;
  sub_compact->total_bytes += current_bytes;

  // Finish and check for file errors
  if (s.ok()) {
    StopWatch sw(env_, stats_, COMPACTION_OUTFILE_SYNC_MICROS);
    s = sub_compact->blob_outfile->Sync(db_options_.use_fsync);
  }
  if (s.ok()) {
    s = sub_compact->blob_outfile->Close();
  }
  sub_compact->blob_outfile.reset();

  TableProperties tp;
  if (s.ok()) {
    tp = sub_compact->blob_builder->GetTableProperties();
    meta->prop.num_deletions = tp.num_deletions;
    meta->prop.raw_key_size = tp.raw_key_size;
    meta->prop.raw_value_size = tp.raw_value_size;
    meta->prop.flags |= TablePropertyCache::kNoRangeDeletions;
  }

  if (s.ok()) {
    // Output to event logger and fire events.
    sub_compact->current_blob_output()->table_properties =
        std::make_shared<TableProperties>(tp);
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Generated blob #%" PRIu64 ": %" PRIu64
                   " keys, %" PRIu64 " bytes",
                   cfd->GetName().c_str(), job_id_, output_number,
                   meta->prop.num_entries, current_bytes);
  }
  std::string fname;
  FileDescriptor output_fd;
  if (meta != nullptr) {
    fname =
        TableFileName(sub_compact->compaction->immutable_cf_options()->cf_paths,
                      meta->fd.GetNumber(), meta->fd.GetPathId());
    output_fd = meta->fd;
  } else {
    fname = "(nil)";
  }
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      event_logger_, cfd->ioptions()->listeners, dbname_, cfd->GetName(), fname,
      job_id_, output_fd, tp, TableFileCreationReason::kCompaction, s);

#ifndef ROCKSDB_LITE
  // Report new file to SstFileManagerImpl
  auto sfm =
      static_cast<SstFileManagerImpl*>(db_options_.sst_file_manager.get());
  if (sfm && meta != nullptr && meta->fd.GetPathId() == 0) {
    auto fn =
        TableFileName(sub_compact->compaction->immutable_cf_options()->cf_paths,
                      meta->fd.GetNumber(), meta->fd.GetPathId());
    sfm->OnAddFile(fn);
    if (sfm->IsMaxAllowedSpaceReached()) {
      // TODO(ajkr): should we return OK() if max space was reached by the final
      // compaction output file (similarly to how flush works when full)?
      s = Status::SpaceLimit("Max allowed space was reached");
      TEST_SYNC_POINT(
          "CompactionJob::FinishCompactionOutputBlob:"
          "MaxAllowedSpaceReached");
      InstrumentedMutexLock l(db_mutex_);
      db_error_handler_->SetBGError(s, BackgroundErrorReason::kCompaction);
    }
  }
#endif

  sub_compact->blob_builder.reset();
  return s;
}

void CompactionJob::CallProcessCompaction(void* arg) {
  ProcessArg* args = (ProcessArg*)arg;
  args->job->ProcessCompaction(
      &args->job->compact_->sub_compact_states[args->task_id]);
  auto finished = std::move(args->finished);
  finished.set_value(true);
}

Status CompactionJob::InstallCompactionResults(
    const MutableCFOptions& mutable_cf_options) {
  db_mutex_->AssertHeld();

  Compaction* compaction = compact_->compaction;
  // paranoia: verify that the files that we started with
  // still exist in the current version and in the same original level.
  // This ensures that a concurrent compaction did not erroneously
  // pick the same files to compact_.
  if (!versions_->VerifyCompactionFileConsistency(compaction)) {
    Compaction::InputLevelSummaryBuffer inputs_summary;

    ROCKS_LOG_ERROR(db_options_.info_log, "[%s] [JOB %d] Compaction %s aborted",
                    compaction->column_family_data()->GetName().c_str(),
                    job_id_, compaction->InputLevelSummary(&inputs_summary));
    return Status::Corruption("Compaction input files inconsistent");
  }

  {
    Compaction::InputLevelSummaryBuffer inputs_summary;
    ROCKS_LOG_INFO(
        db_options_.info_log, "[%s] [JOB %d] Compacted %s => %" PRIu64 " bytes",
        compaction->column_family_data()->GetName().c_str(), job_id_,
        compaction->InputLevelSummary(&inputs_summary), compact_->total_bytes);
  }

  auto cfd = compaction->column_family_data();
  if (compaction->compaction_type() == kMapCompaction &&
      !compaction->input_range().empty()) {
    MapBuilder map_builder(job_id_, db_options_, env_options_, versions_,
                           stats_, dbname_);
    std::vector<MapBuilderOutput> output;
    std::vector<Range> push_range;
    for (auto& ir : compaction->input_range()) {
      push_range.emplace_back(ir.start, ir.limit, ir.include_start,
                              ir.include_limit);
    }
    db_mutex_->Unlock();
    auto s = map_builder.Build(
        *compaction->inputs(), push_range, compaction->output_level(),
        compaction->output_path_id(), cfd, compaction->input_version(),
        compact_->compaction->edit(), &output);
    if (s.ok()) {
      for (auto& o : output) {
        // test map sst
        DependenceMap empty_dependence_map;
        InternalIterator* iter = cfd->table_cache()->NewIterator(
            ReadOptions(), env_options_, cfd->internal_comparator(),
            o.file_meta, empty_dependence_map, nullptr /* range_del_agg */,
            mutable_cf_options.prefix_extractor.get(), nullptr,
            cfd->internal_stats()->GetFileReadHist(compaction->output_level()),
            false, nullptr /* arena */, false /* skip_filters */,
            compaction->output_level());
        s = iter->status();

        if (s.ok() && paranoid_file_checks_) {
          for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
          }
          s = iter->status();
        }
        delete iter;
        if (!s.ok()) {
          break;
        }
      }
    }
    db_mutex_->Lock();
    if (!s.ok()) {
      return s;
    }
    for (auto& o : output) {
      compact_->sub_compact_states[0].outputs.emplace_back();
      auto current = compact_->sub_compact_states[0].current_output();
      current->meta = std::move(o.file_meta);
      current->finished = true;
      current->table_properties.reset(o.prop.release());
    }
  } else if (compaction->compaction_type() != kGarbageCollection &&
             (compaction->compaction_type() == kMapCompaction ||
              !compaction->input_range().empty() ||
              cfd->ioptions()->enable_lazy_compaction ||
              cfd->ioptions()->compaction_dispatcher != nullptr)) {
    MapBuilder map_builder(job_id_, db_options_, env_options_, versions_,
                           stats_, dbname_);
    std::unique_ptr<TableProperties> prop;
    FileMetaData file_meta;
    std::vector<Range> deleted_range;
    std::vector<FileMetaData*> added_files;
    if (compaction->compaction_type() != kMapCompaction) {
      for (auto& sub_compact : compact_->sub_compact_states) {
        if (sub_compact.actual_start.size() == 0) {
          continue;
        }
        bool include_start = true;
        bool include_end = false;
        if (sub_compact.actual_end.size() == 0) {
          if (sub_compact.end != nullptr) {
            sub_compact.actual_end.SetMinPossibleForUserKey(*sub_compact.end);
          } else {
            sub_compact.actual_end.SetMaxPossibleForUserKey(
                compaction->GetLargestUserKey());
            include_end = true;
          }
        }
        deleted_range.emplace_back(sub_compact.actual_start.user_key(),
                                   sub_compact.actual_end.user_key(),
                                   include_start, include_end);
        for (auto& output : sub_compact.outputs) {
          added_files.emplace_back(&output.meta);
          compaction->AddOutputTableFileNumber(output.meta.fd.GetNumber());
        }
        for (auto& output : sub_compact.blob_outputs) {
          compaction->AddOutputTableFileNumber(output.meta.fd.GetNumber());
        }
      }
    }
    db_mutex_->Unlock();
    bool optimize_range_deletion = mutable_cf_options.optimize_range_deletion &&
                                   !compaction->bottommost_level();
    auto s = map_builder.Build(
        *compaction->inputs(), deleted_range, added_files,
        compaction->output_level(), compaction->output_path_id(), cfd,
        optimize_range_deletion, compaction->input_version(),
        compact_->compaction->edit(), &file_meta, &prop);
    if (s.ok() && file_meta.fd.file_size > 0) {
      // test map sst
      DependenceMap empty_dependence_map;
      InternalIterator* iter = cfd->table_cache()->NewIterator(
          ReadOptions(), env_options_, cfd->internal_comparator(), file_meta,
          empty_dependence_map, nullptr /* range_del_agg */,
          mutable_cf_options.prefix_extractor.get(), nullptr,
          cfd->internal_stats()->GetFileReadHist(compaction->output_level()),
          false, nullptr /* arena */, false /* skip_filters */,
          compaction->output_level());
      s = iter->status();

      if (s.ok() && paranoid_file_checks_) {
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        }
        s = iter->status();
      }
      delete iter;
    }
    db_mutex_->Lock();
    if (!s.ok()) {
      return s;
    }
    if (file_meta.fd.file_size > 0) {
      compact_->sub_compact_states[0].outputs.emplace_back();
      auto current = compact_->sub_compact_states[0].current_output();
      current->meta = std::move(file_meta);
      current->finished = true;
      current->table_properties.reset(prop.release());
    }
  } else {
    // Add compaction inputs
    if (compaction->compaction_type() != kGarbageCollection) {
      compaction->AddInputDeletions(compaction->edit());
    }

    for (const auto& sub_compact : compact_->sub_compact_states) {
      for (const auto& out : sub_compact.outputs) {
        compaction->edit()->AddFile(compaction->output_level(), out.meta);
        compaction->AddOutputTableFileNumber(out.meta.fd.GetNumber());
      }
    }
  }

  for (const auto& sub_compact : compact_->sub_compact_states) {
    for (const auto& out : sub_compact.blob_outputs) {
      compaction->edit()->AddFile(-1, out.meta);
      compaction->AddOutputTableFileNumber(out.meta.fd.GetNumber());
    }
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "[%s] [JOB %d] sub_compact_states.size() = %zd",
                 compaction->column_family_data()->GetName().c_str(), job_id_,
                 compact_->sub_compact_states.size());

  TablePropertiesCollection tp;
  for (const auto& state : compact_->sub_compact_states) {
    compaction->transient_stat().push_back(TableTransientStat());
    auto& tts = compaction->transient_stat().back();
    tts.stat_all = state.stat_all;
    ROCKS_LOG_INFO(db_options_.info_log, "[%s] [JOB %d] stat_all[len=%zd] = %s",
                   compaction->column_family_data()->GetName().c_str(), job_id_,
                   state.stat_all.size(), state.stat_all.c_str());
    for (const auto& output : state.outputs) {
      /*
      auto iter = output.table_properties->user_collected_properties.find(
          "User.Collected.Transient.Stat");
      if (output.table_properties->user_collected_properties.end() != iter) {
        tts.per_table[fn] = iter->second;
        output.stat_one = iter->second;
      }
      */
      auto fn =
          TableFileName(state.compaction->immutable_cf_options()->cf_paths,
                        output.meta.fd.GetNumber(), output.meta.fd.GetPathId());
      tp[fn] = output.table_properties;
    }
    for (const auto& output : state.blob_outputs) {
      auto fn =
          TableFileName(state.compaction->immutable_cf_options()->cf_paths,
                        output.meta.fd.GetNumber(), output.meta.fd.GetPathId());
      tp[fn] = output.table_properties;
    }
  }
  compaction->SetOutputTableProperties(std::move(tp));

  return versions_->LogAndApply(compaction->column_family_data(),
                                mutable_cf_options, compaction->edit(),
                                db_mutex_, db_directory_);
}

void CompactionJob::RecordCompactionIOStats() {
  RecordTick(stats_, COMPACT_READ_BYTES, IOSTATS(bytes_read));
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_READ, IOSTATS(bytes_read));
  IOSTATS_RESET(bytes_read);
  RecordTick(stats_, COMPACT_WRITE_BYTES, IOSTATS(bytes_written));
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_WRITTEN, IOSTATS(bytes_written));
  IOSTATS_RESET(bytes_written);
}

Status CompactionJob::OpenCompactionOutputFile(
    SubcompactionState* sub_compact) {
  assert(sub_compact != nullptr);
  assert(sub_compact->builder == nullptr);
  // no need to lock because VersionSet::next_file_number_ is atomic
  uint64_t file_number = versions_->NewFileNumber();
  std::string fname =
      TableFileName(sub_compact->compaction->immutable_cf_options()->cf_paths,
                    file_number, sub_compact->compaction->output_path_id());
  // Fire events.
  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
#ifndef ROCKSDB_LITE
  EventHelpers::NotifyTableFileCreationStarted(
      cfd->ioptions()->listeners, dbname_, cfd->GetName(), fname, job_id_,
      TableFileCreationReason::kCompaction);
#endif  // !ROCKSDB_LITE
  // Make the output file
  std::unique_ptr<WritableFile> writable_file;
#ifndef NDEBUG
  bool syncpoint_arg = env_options_.use_direct_writes;
  TEST_SYNC_POINT_CALLBACK("CompactionJob::OpenCompactionOutputFile",
                           &syncpoint_arg);
#endif
  Status s = NewWritableFile(env_, fname, &writable_file, env_options_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        db_options_.info_log,
        "[%s] [JOB %d] OpenCompactionOutputFiles for table #%" PRIu64
        " fails at NewWritableFile with status %s",
        sub_compact->compaction->column_family_data()->GetName().c_str(),
        job_id_, file_number, s.ToString().c_str());
    LogFlush(db_options_.info_log);
    EventHelpers::LogAndNotifyTableFileCreationFinished(
        event_logger_, cfd->ioptions()->listeners, dbname_, cfd->GetName(),
        fname, job_id_, FileDescriptor(), TableProperties(),
        TableFileCreationReason::kCompaction, s);
    return s;
  }

  SubcompactionState::Output out;
  out.meta.fd =
      FileDescriptor(file_number, sub_compact->compaction->output_path_id(), 0);
  out.finished = false;

  sub_compact->outputs.push_back(out);
  writable_file->SetIOPriority(Env::IO_LOW);
  writable_file->SetWriteLifeTimeHint(write_hint_);
  writable_file->SetPreallocationBlockSize(static_cast<size_t>(
      sub_compact->compaction->OutputFilePreallocationSize()));
  const auto& listeners =
      sub_compact->compaction->immutable_cf_options()->listeners;
  sub_compact->outfile.reset(
      new WritableFileWriter(std::move(writable_file), fname, env_options_,
                             db_options_.statistics.get(), listeners));

  // If the Column family flag is to only optimize filters for hits,
  // we can skip creating filters if this is the bottommost_level where
  // data is going to be found
  bool skip_filters = sub_compact->compaction->mutable_cf_options()
                          ->optimize_filters_for_hits &&
                      bottommost_level_;

  uint64_t output_file_creation_time =
      sub_compact->compaction->MaxInputFileCreationTime();
  if (output_file_creation_time == 0) {
    int64_t _current_time = 0;
    auto status = db_options_.env->GetCurrentTime(&_current_time);
    // Safe to proceed even if GetCurrentTime fails. So, log and proceed.
    if (!status.ok()) {
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "Failed to get current time to populate creation_time property. "
          "Status: %s",
          status.ToString().c_str());
    }
    output_file_creation_time = static_cast<uint64_t>(_current_time);
  }

  auto c = sub_compact->compaction;
  auto& moptions = *c->mutable_cf_options();
  sub_compact->builder.reset(NewTableBuilder(
      *cfd->ioptions(), moptions, cfd->internal_comparator(),
      cfd->int_tbl_prop_collector_factories(moptions), cfd->GetID(),
      cfd->GetName(), sub_compact->outfile.get(),
      sub_compact->compaction->output_compression(),
      sub_compact->compaction->output_compression_opts(),
      sub_compact->compaction->output_level(), c->compaction_load(),
      &sub_compact->compression_dict, skip_filters, output_file_creation_time,
      0 /* oldest_key_time */,
      sub_compact->compaction->compaction_type() == kMapCompaction
          ? kMapSst
          : kEssenceSst));
  LogFlush(db_options_.info_log);
  return s;
}

Status CompactionJob::OpenCompactionOutputBlob(
    SubcompactionState* sub_compact) {
  assert(sub_compact != nullptr);
  assert(sub_compact->blob_builder == nullptr);
  // no need to lock because VersionSet::next_file_number_ is atomic
  uint64_t file_number = versions_->NewFileNumber();
  std::string fname =
      TableFileName(sub_compact->compaction->immutable_cf_options()->cf_paths,
                    file_number, sub_compact->compaction->output_path_id());
  // Fire events.
  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
#ifndef ROCKSDB_LITE
  EventHelpers::NotifyTableFileCreationStarted(
      std::vector<std::shared_ptr<EventListener>>(), dbname_, cfd->GetName(),
      fname, job_id_, TableFileCreationReason::kCompaction);
#endif  // !ROCKSDB_LITE
  // Make the output file
  std::unique_ptr<WritableFile> writable_file;
#ifndef NDEBUG
  bool syncpoint_arg = env_options_.use_direct_writes;
  TEST_SYNC_POINT_CALLBACK("CompactionJob::OpenCompactionOutputFile",
                           &syncpoint_arg);
#endif
  Status s = NewWritableFile(env_, fname, &writable_file, env_options_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        db_options_.info_log,
        "[%s] [JOB %d] OpenCompactionOutputFiles for table #%" PRIu64
        " fails at NewWritableFile with status %s",
        sub_compact->compaction->column_family_data()->GetName().c_str(),
        job_id_, file_number, s.ToString().c_str());
    LogFlush(db_options_.info_log);
    EventHelpers::LogAndNotifyTableFileCreationFinished(
        event_logger_, cfd->ioptions()->listeners, dbname_, cfd->GetName(),
        fname, job_id_, FileDescriptor(), TableProperties(),
        TableFileCreationReason::kCompaction, s);
    return s;
  }

  SubcompactionState::Output out;
  out.meta.fd =
      FileDescriptor(file_number, sub_compact->compaction->output_path_id(), 0);
  out.finished = false;

  sub_compact->blob_outputs.push_back(out);
  writable_file->SetIOPriority(Env::IO_LOW);
  writable_file->SetWriteLifeTimeHint(write_hint_);
  writable_file->SetPreallocationBlockSize(static_cast<size_t>(
      sub_compact->compaction->OutputFilePreallocationSize()));
  const auto& listeners =
      sub_compact->compaction->immutable_cf_options()->listeners;
  sub_compact->blob_outfile.reset(
      new WritableFileWriter(std::move(writable_file), fname, env_options_,
                             db_options_.statistics.get(), listeners));

  uint64_t output_file_creation_time =
      sub_compact->compaction->MaxInputFileCreationTime();
  if (output_file_creation_time == 0) {
    int64_t _current_time = 0;
    auto status = db_options_.env->GetCurrentTime(&_current_time);
    // Safe to proceed even if GetCurrentTime fails. So, log and proceed.
    if (!status.ok()) {
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "Failed to get current time to populate creation_time property. "
          "Status: %s",
          status.ToString().c_str());
    }
    output_file_creation_time = static_cast<uint64_t>(_current_time);
  }

  auto c = sub_compact->compaction;
  auto& moptions = *c->mutable_cf_options();
  // skip_filters always false, Blob all hits
  sub_compact->blob_builder.reset(NewTableBuilder(
      *cfd->ioptions(), moptions, cfd->internal_comparator(),
      cfd->int_tbl_prop_collector_factories_for_blob(moptions), cfd->GetID(),
      cfd->GetName(), sub_compact->blob_outfile.get(),
      sub_compact->compaction->output_compression(),
      sub_compact->compaction->output_compression_opts(), -1 /* level */,
      c->compaction_load(), nullptr, true /* skip_filters */,
      output_file_creation_time, 0 /* oldest_key_time */, kEssenceSst));
  LogFlush(db_options_.info_log);
  return s;
}

void CompactionJob::CleanupCompaction() {
  for (SubcompactionState& sub_compact : compact_->sub_compact_states) {
    const auto& sub_status = sub_compact.status;

    if (sub_compact.builder != nullptr) {
      // May happen if we get a shutdown call in the middle of compaction
      sub_compact.builder->Abandon();
      sub_compact.builder.reset();
    } else {
      assert(!sub_status.ok() || sub_compact.outfile == nullptr);
    }
    for (const auto& out : sub_compact.outputs) {
      // If this file was inserted into the table cache then remove
      // them here because this compaction was not committed.
      if (!sub_status.ok()) {
        TableCache::Evict(table_cache_.get(), out.meta.fd.GetNumber());
      }
    }
    for (const auto& out : sub_compact.blob_outputs) {
      if (!sub_status.ok()) {
        TableCache::Evict(table_cache_.get(), out.meta.fd.GetNumber());
      }
    }
  }
  delete compact_;
  compact_ = nullptr;
}

#ifndef ROCKSDB_LITE
namespace {
void CopyPrefix(const Slice& src, size_t prefix_length, std::string* dst) {
  assert(prefix_length > 0);
  size_t length = src.size() > prefix_length ? prefix_length : src.size();
  dst->assign(src.data(), length);
}
}  // namespace

#endif  // !ROCKSDB_LITE

void CompactionJob::UpdateCompactionStats() {
  Compaction* compaction = compact_->compaction;
  compaction_stats_.num_input_files_in_non_output_levels = 0;
  compaction_stats_.num_input_files_in_output_level = 0;
  for (int input_level = 0;
       input_level < static_cast<int>(compaction->num_input_levels());
       ++input_level) {
    if (compaction->level(input_level) != compaction->output_level()) {
      UpdateCompactionInputStatsHelper(
          &compaction_stats_.num_input_files_in_non_output_levels,
          &compaction_stats_.bytes_read_non_output_levels, input_level);
    } else {
      UpdateCompactionInputStatsHelper(
          &compaction_stats_.num_input_files_in_output_level,
          &compaction_stats_.bytes_read_output_level, input_level);
    }
  }

  for (const auto& sub_compact : compact_->sub_compact_states) {
    size_t num_output_files =
        sub_compact.outputs.size() + sub_compact.blob_outputs.size();
    if (sub_compact.builder != nullptr) {
      // An error occurred so ignore the last output.
      assert(num_output_files > 0);
      --num_output_files;
    }
    compaction_stats_.num_output_files += static_cast<int>(num_output_files);

    for (const auto& out : sub_compact.outputs) {
      compaction_stats_.bytes_written += out.meta.fd.file_size;
    }
    for (const auto& out : sub_compact.blob_outputs) {
      compaction_stats_.bytes_written += out.meta.fd.file_size;
    }
    if (sub_compact.num_input_records > sub_compact.num_output_records) {
      compaction_stats_.num_dropped_records +=
          sub_compact.num_input_records - sub_compact.num_output_records;
    }
  }
}

void CompactionJob::UpdateCompactionInputStatsHelper(int* num_files,
                                                     uint64_t* bytes_read,
                                                     int input_level) {
  const Compaction* compaction = compact_->compaction;
  auto num_input_files = compaction->num_input_files(input_level);
  *num_files += static_cast<int>(num_input_files);

  for (size_t i = 0; i < num_input_files; ++i) {
    const auto* file_meta = compaction->input(input_level, i);
    *bytes_read += file_meta->fd.GetFileSize();
    compaction_stats_.num_input_records +=
        static_cast<uint64_t>(file_meta->prop.num_entries);
  }
}

void CompactionJob::UpdateCompactionJobStats(
    const InternalStats::CompactionStats& stats) const {
#ifndef ROCKSDB_LITE
  if (compaction_job_stats_) {
    compaction_job_stats_->elapsed_micros = stats.micros;

    // input information
    compaction_job_stats_->total_input_bytes =
        stats.bytes_read_non_output_levels + stats.bytes_read_output_level;
    compaction_job_stats_->num_input_records = compact_->num_input_records;
    compaction_job_stats_->num_input_files =
        stats.num_input_files_in_non_output_levels +
        stats.num_input_files_in_output_level;
    compaction_job_stats_->num_input_files_at_output_level =
        stats.num_input_files_in_output_level;

    // output information
    compaction_job_stats_->total_output_bytes = stats.bytes_written;
    compaction_job_stats_->num_output_records = compact_->num_output_records;
    compaction_job_stats_->num_output_files = stats.num_output_files;

    if (compact_->NumOutputFiles() > 0U) {
      CopyPrefix(compact_->SmallestUserKey(),
                 CompactionJobStats::kMaxPrefixLength,
                 &compaction_job_stats_->smallest_output_key_prefix);
      CopyPrefix(compact_->LargestUserKey(),
                 CompactionJobStats::kMaxPrefixLength,
                 &compaction_job_stats_->largest_output_key_prefix);
    }
  }
#else
  (void)stats;
#endif  // !ROCKSDB_LITE
}

void CompactionJob::LogCompaction() {
  Compaction* compaction = compact_->compaction;
  ColumnFamilyData* cfd = compaction->column_family_data();

  // Let's check if anything will get logged. Don't prepare all the info if
  // we're not logging
  if (db_options_.info_log_level <= InfoLogLevel::INFO_LEVEL) {
    Compaction::InputLevelSummaryBuffer inputs_summary;
    ROCKS_LOG_INFO(
        db_options_.info_log,
        "[%s] [JOB %d] Compacting %s, score %.2f, sub_compactions %zd",
        cfd->GetName().c_str(), job_id_,
        compaction->InputLevelSummary(&inputs_summary), compaction->score(),
        compact_->sub_compact_states.size());
    char scratch[2345];
    compaction->Summary(scratch, sizeof(scratch));
    ROCKS_LOG_INFO(db_options_.info_log, "[%s] %s start summary: %s\n",
                   cfd->GetName().c_str(),
                   CompactionTypeName(compaction->compaction_type()), scratch);
    // build event logger report
    auto stream = event_logger_->Log();
    stream << "job" << job_id_ << "event"
           << "compaction_started"
           << "cf_name" << cfd->GetName() << "compaction_reason"
           << GetCompactionReasonString(compaction->compaction_reason());
    for (size_t i = 0; i < compaction->num_input_levels(); ++i) {
      stream << ("files_L" + ToString(compaction->level(i)));
      stream.StartArray();
      for (auto f : *compaction->inputs(i)) {
        stream << f->fd.GetNumber();
      }
      stream.EndArray();
    }
    stream << "score" << compaction->score() << "input_data_size"
           << compaction->CalculateTotalInputSize();
  }
}

static std::map<const void*, void (*)(const void* p_obj, std::string* result)>
    g_fa_map;
std::mutex g_fa_map_mutex;

void PlantFutureAction(const void* obj,
                       void (*action)(const void* p_obj, std::string* result)) {
  assert(nullptr != obj);
  assert(nullptr != action);
  g_fa_map_mutex.lock();
  auto ib = g_fa_map.insert({obj, action});
  g_fa_map_mutex.unlock();
  assert(ib.second);
  if (!ib.second) {
    abort();
  }
}

bool EraseFutureAction(const void* obj) {
  assert(nullptr != obj);
  g_fa_map_mutex.lock();
  size_t cnt = g_fa_map.erase(obj);
  g_fa_map_mutex.unlock();
  return cnt > 0;
}

bool ExistFutureAction(const void* obj) {
  assert(nullptr != obj);
  g_fa_map_mutex.lock();
  auto end = g_fa_map.end();
  auto iter = g_fa_map.find(obj);
  bool exists = end != iter;
  g_fa_map_mutex.unlock();
  return exists;
}

bool ReapMatureAction(const void* obj, std::string* result) {
  assert(nullptr != obj);
  assert(nullptr != result);
  g_fa_map_mutex.lock();
  auto iter = g_fa_map.find(obj);
  if (g_fa_map.end() != iter) {
    auto action = std::move(iter->second);
    g_fa_map.erase(iter);
    g_fa_map_mutex.unlock();
    action(obj, result);
    return true;
  } else {
    g_fa_map_mutex.unlock();
    return false;
  }
}

}  // namespace TERARKDB_NAMESPACE
