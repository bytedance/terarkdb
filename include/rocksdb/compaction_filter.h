// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2013 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cassert>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/lazy_buffer.h"
#include "rocksdb/status.h"
#include "rocksdb/terark_namespace.h"
#include "utilities/util/factory.h"

namespace TERARKDB_NAMESPACE {

class Slice;
class SliceTransform;

// CompactionFilter allows an application to modify/delete a key-value at
// the time of compaction.

// Context information of a compaction run
struct CompactionFilterContext {
  // Does this compaction run include all data files
  bool is_full_compaction;
  // Is this compaction requested by the client (true),
  // or is it occurring as an automatic compaction process
  bool is_manual_compaction;
  // Is this compaction creating a file in the bottom most level?
  bool is_bottommost_level;
  // Which column family this compaction is for.
  uint32_t column_family_id;

  Slice smallest_user_key;
  Slice largest_user_key;
};

class CompactionFilter
    : public
      /// CompactionFilter can also be created by new factory mechanism.
      /// CompactionFilterFactory the old factory mechanism are also kept.
      terark::Factoryable<CompactionFilter*, Slice, CompactionFilterContext> {
 public:
  enum ValueType {
    kValue,
    kMergeOperand,
  };

  enum class Decision {
    kKeep,
    kRemove,
    kChangeValue,
    kRemoveAndSkipUntil,
  };

  using Context = CompactionFilterContext;

  virtual ~CompactionFilter() {}

  // The compaction process invokes this
  // method for kv that is being compacted. A return value
  // of false indicates that the kv should be preserved in the
  // output of this compaction run and a return value of true
  // indicates that this key-value should be removed from the
  // output of the compaction.  The application can inspect
  // the existing value of the key and make decision based on it.
  //
  // Key-Values that are results of merge operation during compaction are not
  // passed into this function. Currently, when you have a mix of Put()s and
  // Merge()s on a same key, we only guarantee to process the merge operands
  // through the compaction filters. Put()s might be processed, or might not.
  //
  // When the value is to be preserved, the application has the option
  // to modify the existing_value and pass it back through new_value.
  // value_changed needs to be set to true in this case.
  //
  // If you use snapshot feature of RocksDB (i.e. call GetSnapshot() API on a
  // DB* object), CompactionFilter might not be very useful for you. Due to
  // guarantees we need to maintain, compaction process will not call Filter()
  // on any keys that were written before the latest snapshot. In other words,
  // compaction will only call Filter() on keys written after your most recent
  // call to GetSnapshot(). In most cases, Filter() will not be called very
  // often. This is something we're fixing. See the discussion at:
  // https://www.facebook.com/groups/mysqlonrocksdb/permalink/999723240091865/
  //
  // If multithreaded compaction is being used *and* a single CompactionFilter
  // instance was supplied via Options::compaction_filter, this method may be
  // called from different threads concurrently.  The application must ensure
  // that the call is thread-safe.
  //
  // If the CompactionFilter was created by a factory, then it will only ever
  // be used by a single thread that is doing the compaction run, and this
  // call does not need to be thread-safe.  However, multiple filters may be
  // in existence and operating concurrently.
  virtual bool Filter(int /*level*/, const Slice& /*key*/,
                      const Slice& /*existing_value*/,
                      std::string* /*new_value*/,
                      bool* /*value_changed*/) const {
    return false;
  }

  // The compaction process invokes this method on every merge operand. If this
  // method returns true, the merge operand will be ignored and not written out
  // in the compaction output
  //
  // Note: If you are using a TransactionDB, it is not recommended to implement
  // FilterMergeOperand().  If a Merge operation is filtered out, TransactionDB
  // may not realize there is a write conflict and may allow a Transaction to
  // Commit that should have failed.  Instead, it is better to implement any
  // Merge filtering inside the MergeOperator.
  virtual bool FilterMergeOperand(int /*level*/, const Slice& /*key*/,
                                  const Slice& /*operand*/) const {
    return false;
  }

  // An extended API. Called for both values and merge operands.
  // Allows changing value and skipping ranges of keys.
  // The default implementation uses Filter() and FilterMergeOperand().
  // If you're overriding this method, no need to override the other two.
  // `value_type` indicates whether this key-value corresponds to a normal
  // value (e.g. written with Put())  or a merge operand (written with Merge()).
  //
  // Possible return values:
  //  * kKeep - keep the key-value pair.
  //  * kRemove - remove the key-value pair or merge operand.
  //  * kChangeValue - keep the key and change the value/operand to *new_value.
  //  * kRemoveAndSkipUntil - remove this key-value pair, and also remove
  //      all key-value pairs with key in [key, *skip_until). This range
  //      of keys will be skipped without reading, potentially saving some
  //      IO operations compared to removing the keys one by one.
  //
  //      *skip_until <= key is treated the same as Decision::kKeep
  //      (since the range [key, *skip_until) is empty).
  //
  //      Caveats:
  //       - The keys are skipped even if there are snapshots containing them,
  //         as if IgnoreSnapshots() was true; i.e. values removed
  //         by kRemoveAndSkipUntil can disappear from a snapshot - beware
  //         if you're using TransactionDB or DB::GetSnapshot().
  //       - If value for a key was overwritten or merged into (multiple Put()s
  //         or Merge()s), and compaction filter skips this key with
  //         kRemoveAndSkipUntil, it's possible that it will remove only
  //         the new value, exposing the old value that was supposed to be
  //         overwritten.
  //       - Doesn't work with PlainTableFactory in prefix mode.
  //       - If you use kRemoveAndSkipUntil, consider also reducing
  //         compaction_readahead_size option.
  //
  // Note: If you are using a TransactionDB, it is not recommended to filter
  // out or modify merge operands (ValueType::kMergeOperand).
  // If a merge operation is filtered out, TransactionDB may not realize there
  // is a write conflict and may allow a Transaction to Commit that should have
  // failed. Instead, it is better to implement any Merge filtering inside the
  // MergeOperator.
  virtual Decision FilterV2(int level, const Slice& key, ValueType value_type,
                            const Slice& /*existing_value_meta*/,
                            const LazyBuffer& existing_value,
                            LazyBuffer* new_value,
                            std::string* /*skip_until*/) const {
    auto s = existing_value.fetch();
    if (!s.ok()) {
      new_value->reset(std::move(s));
      return Decision::kChangeValue;
    }
    switch (value_type) {
      case ValueType::kValue: {
        bool value_changed = false;
        bool rv = Filter(level, key, existing_value.slice(),
                         new_value->trans_to_string(), &value_changed);
        if (rv) {
          return Decision::kRemove;
        }
        return value_changed ? Decision::kChangeValue : Decision::kKeep;
      }
      case ValueType::kMergeOperand: {
        bool rv = FilterMergeOperand(level, key, existing_value.slice());
        return rv ? Decision::kRemove : Decision::kKeep;
      }
    }
    assert(false);
    return Decision::kKeep;
  }

  // By default, compaction will only call Filter() on keys written after the
  // most recent call to GetSnapshot(). However, if the compaction filter
  // overrides IgnoreSnapshots to make it return true, the compaction filter
  // will be called even if the keys were written before the last snapshot.
  // This behavior is to be used only when we want to delete a set of keys
  // irrespective of snapshots. In particular, care should be taken
  // to understand that the values of these keys will change even if we are
  // using a snapshot.
  virtual bool IgnoreSnapshots() const { return false; }

  // Determines whether value changed by compaction filter were stable.
  // Default as false, which means stability of outcome is not promised.
  virtual bool IsStableChangeValue() const { return false; }

  // Returns a name that identifies this compaction filter.
  // The name will be printed to LOG file on start up for diagnosis.
  virtual const char* Name() const = 0;

  virtual Status Serialize(std::string* /*bytes*/) const {
    return Status::NotSupported();
  }

  virtual CompactionFilter* Clone() const { return nullptr; }
};

// Each compaction will create a new CompactionFilter allowing the
// application to know about different compactions
class CompactionFilterFactory
    : public terark::Factoryable<CompactionFilterFactory*, Slice> {
 public:
  virtual ~CompactionFilterFactory() {}

  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) = 0;

  // Returns a name that identifies this compaction filter factory.
  virtual const char* Name() const = 0;

  virtual Status Serialize(std::string* /*bytes*/) const {
    return Status::NotSupported();
  }
};

}  // namespace TERARKDB_NAMESPACE
