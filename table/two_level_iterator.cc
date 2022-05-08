//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "db/version_edit.h"
#include "rocksdb/options.h"
#include "rocksdb/terark_namespace.h"
#include "table/block.h"
#include "table/format.h"
#include "util/arena.h"
#include "util/heap.h"
#include "utilities/util/function.hpp"

namespace TERARKDB_NAMESPACE {

namespace {

class TwoLevelIndexIterator : public InternalIteratorBase<BlockHandle> {
 public:
  explicit TwoLevelIndexIterator(
      TwoLevelIteratorState* state,
      InternalIteratorBase<BlockHandle>* first_level_iter);

  virtual ~TwoLevelIndexIterator() {
    first_level_iter_.DeleteIter(false /* is_arena_mode */);
    second_level_iter_.DeleteIter(false /* is_arena_mode */);
    delete state_;
  }

  virtual void Seek(const Slice& target) override;
  virtual void SeekForPrev(const Slice& target) override;
  virtual void SeekToFirst() override;
  virtual void SeekToLast() override;
  virtual void Next() override;
  virtual void Prev() override;

  virtual bool Valid() const override { return second_level_iter_.Valid(); }
  virtual Slice key() const override {
    terarkdb_assert(Valid());
    return second_level_iter_.key();
  }
  virtual BlockHandle value() const override {
    terarkdb_assert(Valid());
    return second_level_iter_.value();
  }
  virtual Status status() const override {
    if (!first_level_iter_.status().ok()) {
      terarkdb_assert(second_level_iter_.iter() == nullptr);
      return first_level_iter_.status();
    } else if (second_level_iter_.iter() != nullptr &&
               !second_level_iter_.status().ok()) {
      return second_level_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetSecondLevelIterator(InternalIteratorBase<BlockHandle>* iter);
  void InitDataBlock();

  TwoLevelIteratorState* state_;
  IteratorWrapperBase<BlockHandle> first_level_iter_;
  IteratorWrapperBase<BlockHandle> second_level_iter_;  // May be nullptr
  Status status_;
  // If second_level_iter is non-nullptr, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the second_level_iter.
  BlockHandle data_block_handle_;
};

TwoLevelIndexIterator::TwoLevelIndexIterator(
    TwoLevelIteratorState* state,
    InternalIteratorBase<BlockHandle>* first_level_iter)
    : state_(state), first_level_iter_(first_level_iter) {}

void TwoLevelIndexIterator::Seek(const Slice& target) {
  first_level_iter_.Seek(target);

  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.Seek(target);
  }
  SkipEmptyDataBlocksForward();
}

void TwoLevelIndexIterator::SeekForPrev(const Slice& target) {
  first_level_iter_.Seek(target);
  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.SeekForPrev(target);
  }
  if (!Valid()) {
    if (!first_level_iter_.Valid() && first_level_iter_.status().ok()) {
      first_level_iter_.SeekToLast();
      InitDataBlock();
      if (second_level_iter_.iter() != nullptr) {
        second_level_iter_.SeekForPrev(target);
      }
    }
    SkipEmptyDataBlocksBackward();
  }
}

void TwoLevelIndexIterator::SeekToFirst() {
  first_level_iter_.SeekToFirst();
  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.SeekToFirst();
  }
  SkipEmptyDataBlocksForward();
}

void TwoLevelIndexIterator::SeekToLast() {
  first_level_iter_.SeekToLast();
  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.SeekToLast();
  }
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIndexIterator::Next() {
  terarkdb_assert(Valid());
  second_level_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIndexIterator::Prev() {
  terarkdb_assert(Valid());
  second_level_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIndexIterator::SkipEmptyDataBlocksForward() {
  while (second_level_iter_.iter() == nullptr ||
         (!second_level_iter_.Valid() && second_level_iter_.status().ok())) {
    // Move to next block
    if (!first_level_iter_.Valid()) {
      SetSecondLevelIterator(nullptr);
      return;
    }
    first_level_iter_.Next();
    InitDataBlock();
    if (second_level_iter_.iter() != nullptr) {
      second_level_iter_.SeekToFirst();
    }
  }
}

void TwoLevelIndexIterator::SkipEmptyDataBlocksBackward() {
  while (second_level_iter_.iter() == nullptr ||
         (!second_level_iter_.Valid() && second_level_iter_.status().ok())) {
    // Move to next block
    if (!first_level_iter_.Valid()) {
      SetSecondLevelIterator(nullptr);
      return;
    }
    first_level_iter_.Prev();
    InitDataBlock();
    if (second_level_iter_.iter() != nullptr) {
      second_level_iter_.SeekToLast();
    }
  }
}

void TwoLevelIndexIterator::SetSecondLevelIterator(
    InternalIteratorBase<BlockHandle>* iter) {
  InternalIteratorBase<BlockHandle>* old_iter = second_level_iter_.Set(iter);
  delete old_iter;
}

void TwoLevelIndexIterator::InitDataBlock() {
  if (!first_level_iter_.Valid()) {
    SetSecondLevelIterator(nullptr);
  } else {
    BlockHandle handle = first_level_iter_.value();
    if (second_level_iter_.iter() != nullptr &&
        !second_level_iter_.status().IsIncomplete() &&
        handle.offset() == data_block_handle_.offset()) {
      // second_level_iter is already constructed with this iterator, so
      // no need to change anything
    } else {
      InternalIteratorBase<BlockHandle>* iter =
          state_->NewSecondaryIterator(handle);
      data_block_handle_ = handle;
      SetSecondLevelIterator(iter);
    }
  }
}

class MapSstIterator final : public InternalIterator {
 private:
  const FileMetaData* file_meta_;
  InternalIterator* first_level_iter_;
  LazyBuffer first_level_value_;
  bool is_backword_;
  Status status_;
  IteratorCache iterator_cache_;
  Slice smallest_key_;
  Slice largest_key_;
  int include_smallest_;
  int include_largest_;
  std::vector<uint64_t> link_;
  struct HeapElement {
    InternalIterator* iter;
    Slice key;
  };
  template <bool is_less>
  class HeapComparator {
   public:
    HeapComparator(const InternalKeyComparator& comparator) : c_(comparator) {}

    bool operator()(const HeapElement& a, const HeapElement& b) const {
      return is_less ? c_.Compare(a.key, b.key) < 0
                     : c_.Compare(a.key, b.key) > 0;
    }

    const InternalKeyComparator& internal_comparator() const { return c_; }

   private:
    const InternalKeyComparator& c_;
  };
  typedef std::vector<HeapElement> HeapVectorType;
  union {
    // They have same layout, but we only use one of them at same time
    BinaryHeap<HeapElement, HeapComparator<0>, HeapVectorType> min_heap_;
    BinaryHeap<HeapElement, HeapComparator<1>, HeapVectorType> max_heap_;
  };

  bool InitFirstLevelIter() {
    min_heap_.clear();
    if (!first_level_iter_->Valid()) {
      return false;
    }
    // Manual inline MapSstElement::Decode
    const char* err_msg = "Invalid MapSstElement";
    first_level_value_ = first_level_iter_->value();
    status_ = first_level_value_.fetch();
    if (!status_.ok()) {
      return false;
    }
    Slice map_input = first_level_value_.slice();
    link_.clear();
    largest_key_ = first_level_iter_->key();
    uint64_t flags;
    uint64_t link_count;
    if (!GetVarint64(&map_input, &flags) ||
        !GetVarint64(&map_input, &link_count) ||
        !GetLengthPrefixedSlice(&map_input, &smallest_key_)) {
      status_ = Status::Corruption(err_msg);
      return false;
    }
    include_smallest_ = (flags & MapSstElement::kIncludeSmallest) != 0;
    include_largest_ = (flags & MapSstElement::kIncludeLargest) != 0;
    link_.resize(link_count);
    for (uint64_t i = 0; i < link_count; ++i) {
      if (!GetVarint64(&map_input, &link_[i])) {
        status_ = Status::Corruption(err_msg);
        return false;
      }
      terarkdb_assert(file_meta_ == nullptr ||
             std::binary_search(file_meta_->prop.dependence.begin(),
                                file_meta_->prop.dependence.end(),
                                Dependence{link_[i], 0},
                                TERARK_CMP(file_number, <)));
    }
    return true;
  }

  void InitSecondLevelMinHeap(const Slice& target, bool include) {
    InitSecondLevelMinHeapImpl(target, include);
    while (status_.ok() && min_heap_.empty()) {
      first_level_value_.reset();
      first_level_iter_->Next();
      if (InitFirstLevelIter()) {
        InitSecondLevelMinHeapImpl(smallest_key_, include_smallest_);
      } else {
        break;
      }
    }
  }

  void InitSecondLevelMinHeapImpl(const Slice& target, bool include) {
    terarkdb_assert(min_heap_.empty());
    auto& icomp = min_heap_.comparator().internal_comparator();
    for (auto file_number : link_) {
      auto it = iterator_cache_.GetIterator(file_number);
      if (!it->status().ok()) {
        status_ = it->status();
        min_heap_.clear();
        return;
      }
      it->Seek(target);
      if (!it->Valid()) {
        continue;
      }
      if (!include && icomp.Compare(it->key(), target) == 0) {
        it->Next();
        if (!it->Valid()) {
          continue;
        }
      }
      auto k = it->key();
      if (icomp.Compare(k, largest_key_) < include_largest_) {
        terarkdb_assert(IsInRange(k));
        min_heap_.push(HeapElement{it, k});
      }
    }
  }

  void InitSecondLevelMaxHeap(const Slice& target, bool include) {
    InitSecondLevelMaxHeapImpl(target, include);
    while (status_.ok() && max_heap_.empty()) {
      first_level_value_.reset();
      first_level_iter_->Prev();
      if (InitFirstLevelIter()) {
        InitSecondLevelMaxHeapImpl(largest_key_, include_largest_);
      } else {
        break;
      }
    }
  }

  void InitSecondLevelMaxHeapImpl(const Slice& target, bool include) {
    terarkdb_assert(max_heap_.empty());
    auto& icomp = min_heap_.comparator().internal_comparator();
    for (auto file_number : link_) {
      auto it = iterator_cache_.GetIterator(file_number);
      if (!it->status().ok()) {
        status_ = it->status();
        max_heap_.clear();
        return;
      }
      it->SeekForPrev(target);
      if (!it->Valid()) {
        continue;
      }
      if (!include && icomp.Compare(it->key(), target) == 0) {
        it->Prev();
        if (!it->Valid()) {
          continue;
        }
      }
      auto k = it->key();
      if (icomp.Compare(smallest_key_, k) < include_smallest_) {
        terarkdb_assert(IsInRange(k));
        max_heap_.push(HeapElement{it, k});
      }
    }
  }

  bool IsInRange(const Slice& k) {
    auto& icomp = min_heap_.comparator().internal_comparator();
    return icomp.Compare(smallest_key_, k) < include_smallest_ &&
           icomp.Compare(k, largest_key_) < include_largest_;
  }

 public:
  MapSstIterator(const FileMetaData* file_meta, InternalIterator* iter,
                 const DependenceMap& dependence_map,
                 const InternalKeyComparator& icomp, void* create_arg,
                 const IteratorCache::CreateIterCallback& create)
      : file_meta_(file_meta),
        first_level_iter_(iter),
        is_backword_(false),
        iterator_cache_(dependence_map, create_arg, create),
        include_smallest_(false),
        include_largest_(false),
        min_heap_(icomp) {
    if (file_meta != nullptr && !file_meta_->prop.is_map_sst()) {
      abort();
    }
  }

  ~MapSstIterator() {
    first_level_value_.reset();
    min_heap_.~BinaryHeap();
  }

  virtual bool Valid() const override { return !min_heap_.empty(); }
  virtual void SeekToFirst() override {
    is_backword_ = false;
    first_level_value_.reset();
    first_level_iter_->SeekToFirst();
    if (InitFirstLevelIter()) {
      InitSecondLevelMinHeap(smallest_key_, include_smallest_);
      terarkdb_assert(min_heap_.empty() || IsInRange(min_heap_.top().key));
    }
  }
  virtual void SeekToLast() override {
    is_backword_ = true;
    first_level_value_.reset();
    first_level_iter_->SeekToLast();
    if (InitFirstLevelIter()) {
      InitSecondLevelMaxHeap(largest_key_, include_largest_);
      terarkdb_assert(max_heap_.empty() || IsInRange(max_heap_.top().key));
    }
  }
  virtual void Seek(const Slice& target) override {
    is_backword_ = false;
    first_level_value_.reset();
    first_level_iter_->Seek(target);
    if (!InitFirstLevelIter()) {
      terarkdb_assert(min_heap_.empty());
      return;
    }
    auto& icomp = min_heap_.comparator().internal_comparator();
    Slice seek_target = target;
    bool include = true;
    // include_smallest ? cmp_result > 0 : cmp_result >= 0
    if (icomp.Compare(smallest_key_, target) >= include_smallest_) {
      seek_target = smallest_key_;
      include = include_smallest_;
    } else if (icomp.Compare(target, largest_key_) == 0 && !include_largest_) {
      first_level_value_.reset();
      first_level_iter_->Next();
      if (!InitFirstLevelIter()) {
        terarkdb_assert(min_heap_.empty());
        return;
      }
      seek_target = smallest_key_;
      include = include_smallest_;
    }
    InitSecondLevelMinHeap(seek_target, include);
    terarkdb_assert(min_heap_.empty() || IsInRange(min_heap_.top().key));
  }
  virtual void SeekForPrev(const Slice& target) override {
    is_backword_ = true;
    first_level_value_.reset();
    first_level_iter_->Seek(target);
    if (!first_level_iter_->Valid()) {
      first_level_iter_->SeekToLast();
    }
    if (!InitFirstLevelIter()) {
      terarkdb_assert(max_heap_.empty());
      return;
    }
    auto& icomp = min_heap_.comparator().internal_comparator();
    Slice seek_target = target;
    bool include = true;
    // include_smallest ? cmp_result > 0 : cmp_result >= 0
    if (icomp.Compare(smallest_key_, target) >= include_smallest_) {
      first_level_value_.reset();
      first_level_iter_->Prev();
      if (!InitFirstLevelIter()) {
        terarkdb_assert(max_heap_.empty());
        return;
      }
      seek_target = largest_key_;
      include = include_largest_;
    } else if (icomp.Compare(target, largest_key_) >= include_largest_) {
      seek_target = largest_key_;
      include = include_largest_;
    }
    InitSecondLevelMaxHeap(seek_target, include);
    terarkdb_assert(max_heap_.empty() || IsInRange(max_heap_.top().key));
  }
  virtual void Next() override {
    if (is_backword_) {
      InternalKey where;
      where.DecodeFrom(max_heap_.top().key);
      max_heap_.clear();
      InitSecondLevelMinHeap(where.Encode(), false);
      terarkdb_assert(min_heap_.empty() || IsInRange(max_heap_.top().key));
      is_backword_ = false;
    } else {
      auto current = min_heap_.top();
      current.iter->Next();
      if (current.iter->Valid()) {
        current.key = current.iter->key();
        min_heap_.replace_top(current);
      } else {
        min_heap_.pop();
      }
      auto& icomp = min_heap_.comparator().internal_comparator();
      if (min_heap_.empty() ||
          icomp.Compare(min_heap_.top().key, largest_key_) >=
              include_largest_) {
        // out of largest bound
        first_level_value_.reset();
        first_level_iter_->Next();
        if (InitFirstLevelIter()) {
          InitSecondLevelMinHeap(smallest_key_, include_smallest_);
          terarkdb_assert(min_heap_.empty() || IsInRange(min_heap_.top().key));
        }
      } else {
        terarkdb_assert(IsInRange(min_heap_.top().key));
      }
    }
  }
  virtual void Prev() override {
    if (!is_backword_) {
      InternalKey where;
      where.DecodeFrom(min_heap_.top().key);
      min_heap_.clear();
      InitSecondLevelMaxHeap(where.Encode(), false);
      terarkdb_assert(min_heap_.empty() || IsInRange(min_heap_.top().key));
      is_backword_ = true;
    } else {
      auto current = max_heap_.top();
      current.iter->Prev();
      if (current.iter->Valid()) {
        current.key = current.iter->key();
        max_heap_.replace_top(current);
      } else {
        max_heap_.pop();
      }
      auto& icomp = min_heap_.comparator().internal_comparator();
      if (max_heap_.empty() ||
          icomp.Compare(smallest_key_, max_heap_.top().key) >=
              include_smallest_) {
        // out of smallest bound
        first_level_value_.reset();
        first_level_iter_->Prev();
        if (InitFirstLevelIter()) {
          InitSecondLevelMaxHeap(largest_key_, include_largest_);
          terarkdb_assert(max_heap_.empty() || IsInRange(max_heap_.top().key));
        }
      } else {
        terarkdb_assert(IsInRange(max_heap_.top().key));
      }
    }
  }
  virtual Slice key() const override {
    terarkdb_assert(Valid());
    return min_heap_.top().key;
  }
  virtual LazyBuffer value() const override {
    terarkdb_assert(Valid());
    return min_heap_.top().iter->value();
  }
  virtual Status status() const override { return status_; }
};

}  // namespace

InternalIteratorBase<BlockHandle>* NewTwoLevelIterator(
    TwoLevelIteratorState* state,
    InternalIteratorBase<BlockHandle>* first_level_iter) {
  return new TwoLevelIndexIterator(state, first_level_iter);
}

InternalIterator* NewMapSstIterator(
    const FileMetaData* file_meta, InternalIterator* mediate_sst_iter,
    const DependenceMap& dependence_map, const InternalKeyComparator& icomp,
    void* callback_arg, const IteratorCache::CreateIterCallback& create_iter,
    Arena* arena) {
  terarkdb_assert(file_meta == nullptr || file_meta->prop.is_map_sst());
  if (arena == nullptr) {
    return new MapSstIterator(file_meta, mediate_sst_iter, dependence_map,
                              icomp, callback_arg, create_iter);
  } else {
    void* buffer = arena->AllocateAligned(sizeof(MapSstIterator));
    return new (buffer)
        MapSstIterator(file_meta, mediate_sst_iter, dependence_map, icomp,
                       callback_arg, create_iter);
  }
}

}  // namespace TERARKDB_NAMESPACE
