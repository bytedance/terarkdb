//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/merge_helper.h"

#include <string>

#include "db/dbformat.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "port/likely.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "table/format.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"

namespace rocksdb {

MergeHelper::MergeHelper(Env* env, const Comparator* user_comparator,
                         const MergeOperator* user_merge_operator,
                         const CompactionFilter* compaction_filter,
                         Logger* logger, bool assert_valid_internal_key,
                         SequenceNumber latest_snapshot,
                         const SnapshotChecker* snapshot_checker, int level,
                         Statistics* stats,
                         const std::atomic<bool>* shutting_down)
    : env_(env),
      user_comparator_(user_comparator),
      user_merge_operator_(user_merge_operator),
      compaction_filter_(compaction_filter),
      shutting_down_(shutting_down),
      logger_(logger),
      assert_valid_internal_key_(assert_valid_internal_key),
      allow_single_operand_(false),
      latest_snapshot_(latest_snapshot),
      snapshot_checker_(snapshot_checker),
      level_(level),
      keys_(),
      filter_timer_(env_),
      total_filter_time_(0U),
      stats_(stats) {
  assert(user_comparator_ != nullptr);
  if (user_merge_operator_) {
    allow_single_operand_ = user_merge_operator_->AllowSingleOperand();
  }
}

Status MergeHelper::TimedFullMerge(const MergeOperator* merge_operator,
                                   const Slice& key, LazyBuffer* value,
                                   std::vector<LazyBuffer>& operands,
                                   LazyBuffer* result, Logger* logger,
                                   Statistics* statistics, Env* env,
                                   bool update_num_ops_stats) {
  assert(merge_operator != nullptr);

  if (operands.empty()) {
    assert(value != nullptr && result != nullptr);
    *result = std::move(*value);
    return Status::OK();
  }

  if (update_num_ops_stats) {
    MeasureTime(statistics, READ_NUM_MERGE_OPERANDS,
                static_cast<uint64_t>(operands.size()));
  }

  bool success;
  const LazyBuffer* tmp_result_operand = nullptr;
  const MergeOperator::MergeOperationInput merge_in(key, value, operands,
                                                    logger);
  MergeOperator::MergeOperationOutput merge_out(*result, tmp_result_operand);
  {
    // Setup to time the merge
    StopWatchNano timer(env, statistics != nullptr);
    PERF_TIMER_GUARD(merge_operator_time_nanos);

    // Do the merge
    success = merge_operator->FullMergeV2(merge_in, &merge_out);

    if (tmp_result_operand != nullptr) {
      // FullMergeV2 result is an existing operand
      if (tmp_result_operand == value) {
        *result = std::move(*value);
      } else {
        ptrdiff_t tmp_result_operand_index =
            tmp_result_operand - operands.data();
        assert(tmp_result_operand_index >= 0 &&
               size_t(tmp_result_operand_index) < operands.size());
        *result = std::move(operands[tmp_result_operand_index]);
      }
    }

    RecordTick(statistics, MERGE_OPERATION_TOTAL_TIME,
               statistics ? timer.ElapsedNanos() : 0);
  }

  if (!success) {
    RecordTick(statistics, NUMBER_MERGE_FAILURES);
    return Status::Corruption("Error: Could not perform merge.");
  }

  return Status::OK();
}

// PRE:  iter points to the first merge type entry
// POST: iter points to the first entry beyond the merge process (or the end)
//       keys_, operands_ are updated to reflect the merge result.
//       keys_ stores the list of keys encountered while merging.
//       operands_ stores the list of merge operands encountered while merging.
//       keys_[i] corresponds to operands_[i] for each i.
//
// TODO: Avoid the snapshot stripe map lookup in CompactionRangeDelAggregator
// and just pass the StripeRep corresponding to the stripe being merged.
Status MergeHelper::MergeUntil(const Slice& user_key,
                               CombinedInternalIterator* iter,
                               CompactionRangeDelAggregator* range_del_agg,
                               const SequenceNumber stop_before,
                               const bool at_bottom) {
  // Get a copy of the internal key, before it's invalidated by iter->Next()
  // Also maintain the list of merge operands seen.
  assert(HasOperator());
  keys_.clear();
  merge_context_.Clear();
  has_compaction_filter_skip_until_ = false;
  assert(user_merge_operator_);
  bool first_key = true;

  // We need to parse the internal key again as the parsed key is
  // backed by the internal key!
  // Assume no internal key corruption as it has been successfully parsed
  // by the caller.
  // original_key_is_iter variable is just caching the information:
  // original_key_is_iter == (iter->key().ToString() == original_key)
  bool original_key_is_iter = true;
  std::string original_key = iter->key().ToString();
  // Important:
  // orig_ikey is backed by original_key if keys_.empty()
  // orig_ikey is backed by keys_.back() if !keys_.empty()
  ParsedInternalKey orig_ikey;
  bool succ = ParseInternalKey(original_key, &orig_ikey);
  assert(succ);
  if (!succ) {
    return Status::Corruption("Cannot parse key in MergeUntil");
  }

  Status s;
  bool hit_the_next_user_key = false;
  for (; iter->Valid(); iter->Next(), original_key_is_iter = false) {
    if (IsShuttingDown()) {
      return Status::ShutdownInProgress();
    }

    ParsedInternalKey ikey;
    assert(keys_.size() == merge_context_.GetNumOperands());

    if (first_key) {
      ikey = orig_ikey;
      first_key = false;
    } else if (!ParseInternalKey(iter->key(), &ikey)) {
      // stop at corrupted key
      if (assert_valid_internal_key_) {
        assert(!"Corrupted internal key not expected.");
        return Status::Corruption("Corrupted internal key not expected.");
      }
      break;
    } else if (!user_comparator_->Equal(ikey.user_key, orig_ikey.user_key)) {
      // hit a different user key, stop right here
      hit_the_next_user_key = true;
      break;
    } else if (stop_before > 0 && ikey.sequence <= stop_before &&
               LIKELY(snapshot_checker_ == nullptr ||
                      snapshot_checker_->IsInSnapshot(ikey.sequence,
                                                      stop_before))) {
      // hit an entry that's visible by the previous snapshot, can't touch that
      break;
    }
    LazyBuffer val = iter->value(user_key);

    // At this point we are guaranteed that we need to process this key.

    assert(IsValueType(ikey.type));
    if (ikey.type != kTypeMerge && ikey.type != kTypeMergeIndex) {
      // hit a put/delete/single delete
      //   => merge the put value or a nullptr with operands_
      //   => store result in operands_.back() (and update keys_.back())
      //   => change the entry type to kTypeValue for keys_.back()
      // We are done! Success!

      // If there are no operands, just return the Status::OK(). That will cause
      // the compaction iterator to write out the key we're currently at, which
      // is the put/delete we just encountered.
      if (keys_.empty()) {
        return Status::OK();
      }

      // TODO(noetzli) If the merge operator returns false, we are currently
      // (almost) silently dropping the put/delete. That's probably not what we
      // want. Also if we're in compaction and it's a put, it would be nice to
      // run compaction filter on it.
      LazyBuffer* val_ptr;
      if ((kTypeValue == ikey.type || kTypeValueIndex == ikey.type) &&
          (range_del_agg == nullptr ||
           !range_del_agg->ShouldDelete(
               ikey, RangeDelPositioningMode::kForwardTraversal))) {
        val_ptr = &val;
      } else {
        val_ptr = nullptr;
      }
      LazyBuffer merge_result;
      s = TimedFullMerge(user_merge_operator_, ikey.user_key, val_ptr,
                         merge_context_.GetOperands(), &merge_result, logger_,
                         stats_, env_);

      // We store the result in keys_.back() and operands_.back()
      // if nothing went wrong (i.e.: no operand corruption on disk)
      if (s.ok()) {
        // The original key encountered
        original_key = std::move(keys_.back());
        orig_ikey.type = kTypeValue;
        UpdateInternalKey(&original_key, orig_ikey.sequence, orig_ikey.type);
        keys_.clear();
        merge_context_.Clear();
        keys_.emplace_front(std::move(original_key));
        merge_context_.PushOperand(std::move(merge_result));
      }
      val.reset();

      // move iter to the next entry
      iter->Next();
      return s;
    } else {
      // hit a merge
      //   => check for range tombstones covering the operand
      //   => if there is a compaction filter, apply it.
      //   => merge the operand into the front of the operands_ list
      //      if not filtered
      //   => then continue because we haven't yet seen a Put/Delete.
      //
      // Keep queuing keys and operands until we either meet a put / delete
      // request or later did a partial merge.

      CompactionFilter::Decision filter = CompactionFilter::Decision::kKeep;
      if (range_del_agg != nullptr &&
          range_del_agg->ShouldDelete(
              iter->key(), RangeDelPositioningMode::kForwardTraversal)) {
        val.clear();
        filter = CompactionFilter::Decision::kRemove;
      }

      // add an operand to the list if:
      if (ikey.sequence <= latest_snapshot_) {
        // 1) it's included in one of the snapshots. in that case we *must*
        // write it out, no matter what compaction filter says
      } else if (filter == CompactionFilter::Decision::kKeep) {
        // 2) it's not filtered by a compaction filter
        filter = FilterMerge(orig_ikey.user_key, val);
      }
      if (filter == CompactionFilter::Decision::kKeep ||
          filter == CompactionFilter::Decision::kChangeValue) {
        if (original_key_is_iter) {
          // this is just an optimization that saves us one memcpy
          keys_.push_front(std::move(original_key));
        } else {
          keys_.push_front(iter->key().ToString());
        }
        if (keys_.size() == 1) {
          // we need to re-anchor the orig_ikey because it was anchored by
          // original_key before
          ParseInternalKey(keys_.back(), &orig_ikey);
        }
        if (filter == CompactionFilter::Decision::kKeep) {
          merge_context_.PushOperand(std::move(val));
        } else {  // kChangeValue
          // Compaction filter asked us to change the operand from val to
          // compaction_filter_value_.
          assert(compaction_filter_value_.file_number() == uint64_t(-1));
          merge_context_.PushOperand(std::move(compaction_filter_value_));
        }
      } else if (filter == CompactionFilter::Decision::kRemoveAndSkipUntil) {
        // Compaction filter asked us to remove this key altogether
        // (not just this operand), along with some keys following it.
        keys_.clear();
        merge_context_.Clear();
        has_compaction_filter_skip_until_ = true;
        return Status::OK();
      }
    }
  }

  if (merge_context_.GetNumOperands() == 0) {
    // we filtered out all the merge operands
    return Status::OK();
  }

  // We are sure we have seen this key's entire history if we are at the
  // last level and exhausted all internal keys of this user key.
  // NOTE: !iter->Valid() does not necessarily mean we hit the
  // beginning of a user key, as versions of a user key might be
  // split into multiple files (even files on the same level)
  // and some files might not be included in the compaction/merge.
  //
  // There are also cases where we have seen the root of history of this
  // key without being sure of it. Then, we simply miss the opportunity
  // to combine the keys. Since VersionSet::SetupOtherInputs() always makes
  // sure that all merge-operands on the same level get compacted together,
  // this will simply lead to these merge operands moving to the next level.
  //
  // So, we only perform the following logic (to merge all operands together
  // without a Put/Delete) if we are certain that we have seen the end of key.
  bool surely_seen_the_beginning =
      (hit_the_next_user_key || !iter->Valid()) && at_bottom;
  if (surely_seen_the_beginning) {
    // do a final merge with nullptr as the existing value and say
    // bye to the merge type (it's now converted to a Put)
    assert(kTypeMerge == orig_ikey.type || kTypeMergeIndex == orig_ikey.type);
    assert(merge_context_.GetNumOperands() >= 1);
    assert(merge_context_.GetNumOperands() == keys_.size());
    LazyBuffer merge_result;
    s = TimedFullMerge(user_merge_operator_, orig_ikey.user_key, nullptr,
                       merge_context_.GetOperands(), &merge_result, logger_,
                       stats_, env_);
    if (s.ok()) {
      // The original key encountered
      // We are certain that keys_ is not empty here (see assertions couple of
      // lines before).
      original_key = std::move(keys_.back());
      orig_ikey.type = kTypeValue;
      UpdateInternalKey(&original_key, orig_ikey.sequence, orig_ikey.type);
      keys_.clear();
      merge_context_.Clear();
      keys_.emplace_front(std::move(original_key));
      merge_context_.PushOperand(std::move(merge_result));
    }
  } else {
    // We haven't seen the beginning of the key nor a Put/Delete.
    // Attempt to use the user's associative merge function to
    // merge the stacked merge operands into a single operand.
    s = Status::MergeInProgress();
    if (merge_context_.GetNumOperands() >= 2 ||
        (allow_single_operand_ && merge_context_.GetNumOperands() == 1)) {
      bool merge_success = false;
      LazyBuffer merge_result;
      {
        StopWatchNano timer(env_, stats_ != nullptr);
        PERF_TIMER_GUARD(merge_operator_time_nanos);
        merge_success = user_merge_operator_->PartialMergeMulti(
            orig_ikey.user_key, merge_context_.GetOperands(), &merge_result,
            logger_);
        RecordTick(stats_, MERGE_OPERATION_TOTAL_TIME,
                   stats_ ? timer.ElapsedNanosSafe() : 0);
      }
      if (merge_success) {
        // Merging of operands (associative merge) was successful.
        // Replace operands with the merge result
        original_key = std::move(keys_.back());
        orig_ikey.type = kTypeMerge;
        UpdateInternalKey(&original_key, orig_ikey.sequence, orig_ikey.type);
        keys_.clear();
        merge_context_.Clear();
        keys_.emplace_front(std::move(original_key));
        merge_context_.PushOperand(std::move(merge_result));
      }
    }
  }

  return s;
}

CompactionFilter::Decision MergeHelper::FilterMerge(
    const Slice& user_key, const LazyBuffer& value_slice) {
  if (compaction_filter_ == nullptr) {
    return CompactionFilter::Decision::kKeep;
  }
  if (stats_ != nullptr && ShouldReportDetailedTime(env_, stats_)) {
    filter_timer_.Start();
  }
  compaction_filter_value_.clear();
  compaction_filter_skip_until_.Clear();
  auto ret = compaction_filter_->FilterV2(
      level_, user_key, CompactionFilter::ValueType::kMergeOperand, Slice(),
      value_slice, &compaction_filter_value_,
      compaction_filter_skip_until_.rep());
  if (ret == CompactionFilter::Decision::kRemoveAndSkipUntil) {
    if (user_comparator_->Compare(*compaction_filter_skip_until_.rep(),
                                  user_key) <= 0) {
      // Invalid skip_until returned from compaction filter.
      // Keep the key as per FilterV2 documentation.
      ret = CompactionFilter::Decision::kKeep;
    } else {
      compaction_filter_skip_until_.ConvertFromUserKey(kMaxSequenceNumber,
                                                       kValueTypeForSeek);
    }
  }
  total_filter_time_ += filter_timer_.ElapsedNanosSafe();
  return ret;
}

MergeOutputIterator MergeHelper::NewIterator() && {
  MergeOutputIterator it;
  it.it_keys_ = keys_.rbegin();
  it.it_end_ = keys_.rend();
  it.it_values_ =
      std::make_move_iterator(merge_context_.GetOperands().rbegin());
  return it;
}

}  // namespace rocksdb
