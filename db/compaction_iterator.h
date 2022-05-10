//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <algorithm>
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "db/compaction.h"
#include "db/compaction_iteration_stats.h"
#include "db/merge_helper.h"
#include "db/range_del_aggregator.h"
#include "db/snapshot_checker.h"
#include "options/cf_options.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/terark_namespace.h"
#include "table/iterator_wrapper.h"
#include "util/chash_set.h"

namespace TERARKDB_NAMESPACE {

class CompactionIteratorBase {
 public:
  virtual ~CompactionIteratorBase() = 0;

  virtual void ResetRecordCounts() = 0;

  virtual void SeekToFirst() = 0;
  virtual void Next() = 0;

  // Getters
  virtual const Slice& key() const = 0;
  virtual const LazyBuffer& value() const = 0;
  virtual const Status& status() const = 0;
  virtual const ParsedInternalKey& ikey() const = 0;
  virtual bool Valid() const = 0;
  virtual const Slice& user_key() const = 0;
  virtual const CompactionIterationStats& iter_stats() const = 0;
  // for KV Separation or LinkSST, current value may depend on other file numbers
  virtual const std::vector<uint64_t> depended_file_numbers() const = 0;
};

class CompactionIterator: public CompactionIteratorBase {
 public:
  friend class CompactionIteratorToInternalIterator;

  // A wrapper around Compaction. Has a much smaller interface, only what
  // CompactionIterator uses. Tests can override it.
  class CompactionProxy {
   public:
    explicit CompactionProxy(const Compaction* compaction)
        : compaction_(compaction) {}

    virtual ~CompactionProxy() = default;
    virtual SeparationType separation_type() const {
      return compaction_->separation_type();
    }
    virtual int level(size_t /*compaction_input_level*/ = 0) const {
      return compaction_->level();
    }
    virtual bool KeyNotExistsBeyondOutputLevel(
        const Slice& user_key, std::vector<size_t>* level_ptrs) const {
      return compaction_->KeyNotExistsBeyondOutputLevel(user_key, level_ptrs);
    }
    virtual bool bottommost_level() const {
      return compaction_->bottommost_level();
    }
    virtual int number_levels() const { return compaction_->number_levels(); }
    virtual Slice GetLargestUserKey() const {
      return compaction_->GetLargestUserKey();
    }
    virtual bool allow_ingest_behind() const {
      return compaction_->immutable_cf_options()->allow_ingest_behind;
    }
    virtual bool preserve_deletes() const {
      return compaction_->immutable_cf_options()->preserve_deletes;
    }

   protected:
    CompactionProxy() : compaction_(nullptr) {}

   private:
    const Compaction* compaction_;
  };

  CompactionIterator(InternalIterator* input, SeparateHelper* separate_helper,
                     const Slice* end, const Comparator* cmp,
                     MergeHelper* merge_helper, SequenceNumber last_sequence,
                     std::vector<SequenceNumber>* snapshots,
                     SequenceNumber earliest_write_conflict_snapshot,
                     const SnapshotChecker* snapshot_checker, Env* env,
                     bool report_detailed_time, bool expect_valid_internal_key,
                     CompactionRangeDelAggregator* range_del_agg,
                     const Compaction* compaction = nullptr,
                     BlobConfig blob_config = BlobConfig{size_t(-1), 0.0},
                     const CompactionFilter* compaction_filter = nullptr,
                     const std::atomic<bool>* shutting_down = nullptr,
                     const SequenceNumber preserve_deletes_seqnum = 0,
                     const chash_set<uint64_t>* b = nullptr);

  // Constructor with custom CompactionProxy, used for tests.
  CompactionIterator(InternalIterator* input, SeparateHelper* separate_helper,
                     const Slice* end, const Comparator* cmp,
                     MergeHelper* merge_helper, SequenceNumber last_sequence,
                     std::vector<SequenceNumber>* snapshots,
                     SequenceNumber earliest_write_conflict_snapshot,
                     const SnapshotChecker* snapshot_checker, Env* env,
                     bool report_detailed_time, bool expect_valid_internal_key,
                     CompactionRangeDelAggregator* range_del_agg,
                     std::unique_ptr<CompactionProxy> compaction,
                     BlobConfig blob_config,
                     const CompactionFilter* compaction_filter = nullptr,
                     const std::atomic<bool>* shutting_down = nullptr,
                     const SequenceNumber preserve_deletes_seqnum = 0,
                     const chash_set<uint64_t>* b = nullptr);

  ~CompactionIterator() override;

  void ResetRecordCounts() override;

  // Seek to the beginning of the compaction iterator output.
  //
  // REQUIRED: Call only once.
  void SeekToFirst() override;

  // Produces the next record in the compaction.
  //
  // REQUIRED: SeekToFirst() has been called.
  void Next() override;

  // Getters
  const Slice& key() const override { return key_; }
  const LazyBuffer& value() const override { return value_; }
  const Status& status() const override { return status_; }
  const ParsedInternalKey& ikey() const override { return ikey_; }
  bool Valid() const override { return valid_; }
  const Slice& user_key() const override { return current_user_key_; }
  const CompactionIterationStats& iter_stats() const override { return iter_stats_; }
  void SetFilterSampleInterval(size_t filter_sample_interval);

  virtual const std::vector<uint64_t> depended_file_numbers() const {
    return {value_.file_number()};
  }

 private:
  // Processes the input stream to find the next output
  void NextFromInput();

  // Do last preparations before presenting the output to the callee. At this
  // point this only zeroes out the sequence number if possible for better
  // compression.
  void PrepareOutput();

  // Invoke compaction filter if needed.
  void InvokeFilterIfNeeded(bool* need_skip, Slice* skip_until);

  // Given a sequence number, return the sequence number of the
  // earliest snapshot that this sequence number is visible in.
  // The snapshots themselves are arranged in ascending order of
  // sequence numbers.
  // Employ a sequential search because the total number of
  // snapshots are typically small.
  inline SequenceNumber findEarliestVisibleSnapshot(
      SequenceNumber in, SequenceNumber* prev_snapshot);

  // Checks whether the currently seen ikey_ is needed for
  // incremental (differential) snapshot and hence can't be dropped
  // or seqnum be zero-ed out even if all other conditions for it are met.
  inline bool ikeyNotNeededForIncrementalSnapshot();

  CombinedInternalIterator input_;
  const Slice* end_;
  const Comparator* cmp_;
  MergeHelper* merge_helper_;
  const std::vector<SequenceNumber>* snapshots_;
  const SequenceNumber earliest_write_conflict_snapshot_;
  const SnapshotChecker* const snapshot_checker_;
  Env* env_;
  // bool report_detailed_time_;
  bool expect_valid_internal_key_;
  CompactionRangeDelAggregator* range_del_agg_;
  std::unique_ptr<CompactionProxy> compaction_;
  const BlobConfig blob_config_;
  const uint64_t blob_large_key_ratio_lsh16_;
  const CompactionFilter* compaction_filter_;
  const std::atomic<bool>* shutting_down_;
  const SequenceNumber preserve_deletes_seqnum_;
  bool bottommost_level_;
  bool valid_ = false;
  bool visible_at_tip_;
  SequenceNumber earliest_snapshot_;
  SequenceNumber latest_snapshot_;
  bool ignore_snapshots_;

  // State
  //
  // Points to a copy of the current compaction iterator output (current_key_)
  // if valid_.
  Slice key_;
  // Points to the value in the underlying iterator that corresponds to the
  // current output.
  LazyBuffer value_;
  std::string value_meta_;
  // The status is OK unless compaction iterator encounters a merge operand
  // while not having a merge operator defined.
  Status status_;
  // Stores the user key, sequence number and type of the current compaction
  // iterator output (or current key in the underlying iterator during
  // NextFromInput()).
  ParsedInternalKey ikey_;
  // Stores whether ikey_.user_key is valid. If set to false, the user key is
  // not compared against the current key in the underlying iterator.
  bool has_current_user_key_ = false;
  bool at_next_ = false;  // If false, the iterator
  // Holds a copy of the current compaction iterator output (or current key in
  // the underlying iterator during NextFromInput()).
  IterKey current_key_;
  Slice current_user_key_;
  SequenceNumber current_user_key_sequence_;
  SequenceNumber current_user_key_snapshot_;

  // True if the iterator has already returned a record for the current key.
  bool has_outputted_key_ = false;

  // truncated the value of the next key and output it without applying any
  // compaction rules.  This is used for outputting a put after a single delete.
  bool clear_and_output_next_key_ = false;

  MergeOutputIterator merge_out_iter_;
  LazyBuffer compaction_filter_value_;
  InternalKey compaction_filter_skip_until_;
  // "level_ptrs" holds indices that remember which file of an associated
  // level we were last checking during the last call to compaction->
  // KeyNotExistsBeyondOutputLevel(). This allows future calls to the function
  // to pick off where it left off since each subcompaction's key range is
  // increasing so a later call to the function must be looking for a key that
  // is in or beyond the last file checked during the previous call
  std::vector<size_t> level_ptrs_;
  CompactionIterationStats iter_stats_;

  // Used to avoid purging uncommitted values. The application can specify
  // uncommitted values by providing a SnapshotChecker object.
  bool current_key_committed_;

  bool do_separate_value_;  // separate big value
  bool do_rebuild_blob_;    // rebuild all blobs in need_rebuild_blobs if user
                            // force rebuild need_rebuild_blobs.empty() == true
  bool do_combine_value_;   // fetch and combine bigvalue from blobs

  size_t filter_sample_interval_ = 64;
  size_t filter_hit_count_ = 0;
  const chash_set<uint64_t>* rebuild_blob_set_;

 public:
  bool IsShuttingDown() {
    // This is a best-effort facility, so memory_order_relaxed is sufficient.
    return shutting_down_ && shutting_down_->load(std::memory_order_relaxed);
  }
};

// A LinkCompactionIterator takes a ordinary CompactionIterator as its input and
// iterate all KVs block by block (refer to LinkBlockRecord).
class LinkCompactionIterator: public CompactionIteratorBase {
 public:
  // @param c_iter Original compaction iterator that combines all underlying SSTs
  // @param group_sz The total number of the KV items that a LinkBlock contains.
  // @param hash_bits Total bits a key in LinkBlock will use.
  explicit LinkCompactionIterator(std::unique_ptr<CompactionIterator> c_iter,
                                  IteratorCache* iterator_cache,
                                  int group_sz,
                                  int hash_bits)
      : c_iter_(std::move(c_iter)),
        iterator_cache_(iterator_cache),
        group_sz_(group_sz),
        hash_bits_(hash_bits) {}

  ~LinkCompactionIterator() override {
    c_iter_.reset();
  }

  void ResetRecordCounts() override {
    c_iter_->ResetRecordCounts();
  }

  // We need to construct the first LinkBlockRecord here.
  void SeekToFirst() override {
    c_iter_->SeekToFirst();
    status_ = c_iter_->status();
    if(!status_.ok()) {
      return;
    }
    Next();
  }

  // We need to step forward N(LinkBlockRecord's size) times to make sure we can
  // get the next LinkBlockRecord(LBR)
  // @see LinkSstIterator
  void Next() override {
    // Each time we proceed to next LBR, we refill underlying file numbers.
    file_numbers_.clear();

    // Obtain the original KV pairs
    Slice max_key;
    Slice smallest_key; // TODO need to check if we need smallest key
    std::vector<uint32_t> hash_values;
    for(int i = 0; i < group_sz_ && c_iter_->Valid(); ++i) {
      const LazyBuffer& value = c_iter_->value();
      hash_values.emplace_back(LinkBlockRecord::hash(c_iter_->user_key(), hash_bits_));
      file_numbers_.emplace_back(value.file_number());
      if(i == group_sz_ - 1) {
        max_key = c_iter_->key();
      }
      Next();
      if(!c_iter_->status().ok()) {
        status_ = c_iter_->status();
        return;
      }
    }
    curr_lbr_ = std::make_unique<LinkBlockRecord>(iterator_cache_, max_key,
                                                  group_sz_, hash_bits_);
    curr_lbr_->Encode(file_numbers_, hash_values, smallest_key);

    // Parse internal key
    const Slice& key = curr_lbr_->MaxKey();
    if(!ParseInternalKey(key, &ikey_)) {
      status_ = Status::Incomplete("Cannot decode internal key");
    }
  }

  // Obtain the LBR's key, aka the largest key of the block range.
  const Slice& key() const override {
    return curr_lbr_->MaxKey();
  }

  const LazyBuffer& value() const override {
    return curr_lbr_->EncodedValue();
  }

  const Status& status() const override {
    return status_;
  }

  const ParsedInternalKey& ikey() const override {
    return ikey_;
  }

  bool Valid() const override {
    return status_.ok();
  }

  const Slice& user_key() const override {
    return c_iter_->user_key();
  }

  const CompactionIterationStats& iter_stats() const override {
    return c_iter_->iter_stats();
  }

  const std::vector<uint64_t> depended_file_numbers() const {
    return file_numbers_;
  }

 private:
  Status status_;
  std::unique_ptr<LinkBlockRecord> curr_lbr_;
  ParsedInternalKey ikey_;
  std::vector<uint64_t> file_numbers_;

  std::unique_ptr<CompactionIterator> c_iter_;
  IteratorCache* iterator_cache_;
  int group_sz_ = 0;
  int hash_bits_ = 0;
};

InternalIterator* NewCompactionIterator(
    CompactionIterator* (*new_compaction_iter_callback)(void*), void* arg,
    const Slice* start_user_key = nullptr);

}  // namespace TERARKDB_NAMESPACE
