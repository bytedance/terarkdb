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
    assert(Valid());
    return second_level_iter_.key();
  }
  virtual BlockHandle value() const override {
    assert(Valid());
    return second_level_iter_.value();
  }
  virtual Status status() const override {
    if (!first_level_iter_.status().ok()) {
      assert(second_level_iter_.iter() == nullptr);
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
  assert(Valid());
  second_level_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIndexIterator::Prev() {
  assert(Valid());
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
    InternalIterator* iter{};
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
      assert(file_meta_ == nullptr ||
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
    assert(min_heap_.empty());
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
        assert(IsInRange(k));
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
    assert(max_heap_.empty());
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
        assert(IsInRange(k));
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
      assert(min_heap_.empty() || IsInRange(min_heap_.top().key));
    }
  }
  virtual void SeekToLast() override {
    is_backword_ = true;
    first_level_value_.reset();
    first_level_iter_->SeekToLast();
    if (InitFirstLevelIter()) {
      InitSecondLevelMaxHeap(largest_key_, include_largest_);
      assert(max_heap_.empty() || IsInRange(max_heap_.top().key));
    }
  }
  virtual void Seek(const Slice& target) override {
    is_backword_ = false;
    first_level_value_.reset();
    first_level_iter_->Seek(target);
    if (!InitFirstLevelIter()) {
      assert(min_heap_.empty());
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
        assert(min_heap_.empty());
        return;
      }
      seek_target = smallest_key_;
      include = include_smallest_;
    }
    InitSecondLevelMinHeap(seek_target, include);
    assert(min_heap_.empty() || IsInRange(min_heap_.top().key));
  }
  virtual void SeekForPrev(const Slice& target) override {
    is_backword_ = true;
    first_level_value_.reset();
    first_level_iter_->Seek(target);
    if (!first_level_iter_->Valid()) {
      first_level_iter_->SeekToLast();
    }
    if (!InitFirstLevelIter()) {
      assert(max_heap_.empty());
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
        assert(max_heap_.empty());
        return;
      }
      seek_target = largest_key_;
      include = include_largest_;
    } else if (icomp.Compare(target, largest_key_) >= include_largest_) {
      seek_target = largest_key_;
      include = include_largest_;
    }
    InitSecondLevelMaxHeap(seek_target, include);
    assert(max_heap_.empty() || IsInRange(max_heap_.top().key));
  }
  virtual void Next() override {
    if (is_backword_) {
      InternalKey where;
      where.DecodeFrom(max_heap_.top().key);
      max_heap_.clear();
      InitSecondLevelMinHeap(where.Encode(), false);
      assert(min_heap_.empty() || IsInRange(max_heap_.top().key));
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
        // out of the largest bound
        first_level_value_.reset();
        first_level_iter_->Next();
        if (InitFirstLevelIter()) {
          InitSecondLevelMinHeap(smallest_key_, include_smallest_);
          assert(min_heap_.empty() || IsInRange(min_heap_.top().key));
        }
      } else {
        assert(IsInRange(min_heap_.top().key));
      }
    }
  }
  virtual void Prev() override {
    if (!is_backword_) {
      InternalKey where;
      where.DecodeFrom(min_heap_.top().key);
      min_heap_.clear();
      InitSecondLevelMaxHeap(where.Encode(), false);
      assert(min_heap_.empty() || IsInRange(min_heap_.top().key));
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
        // out of the smallest bound
        first_level_value_.reset();
        first_level_iter_->Prev();
        if (InitFirstLevelIter()) {
          InitSecondLevelMaxHeap(largest_key_, include_largest_);
          assert(max_heap_.empty() || IsInRange(max_heap_.top().key));
        }
      } else {
        assert(IsInRange(max_heap_.top().key));
      }
    }
  }
  virtual Slice key() const override {
    assert(Valid());
    return min_heap_.top().key;
  }
  virtual LazyBuffer value() const override {
    assert(Valid());
    return min_heap_.top().iter->value();
  }
  virtual Status status() const override { return status_; }
};

// Each LinkSST contains a list of LinkBlock, each LinkBlock contains a certain
// number of file_numbers which indicate where the KV pairs are placed.
class LinkBlockRecord {
 public:
  LinkBlockRecord(IteratorCache* iterator_cache,
                  const Slice& key, int group_sz, int hash_bits)
      : iterator_cache_(iterator_cache),
        max_key_(key), group_sz_(group_sz), hash_bits_(hash_bits) {
    key_buffer_.resize(group_sz);
  }

  // TODO(guokuankuan@bytedance.com)
  // Encode current LinkBlock into slice, so we can put it into a LinkSST.
  // Format:
  // [file_numbers]           block_sz * uint64_t (varint)
  // [hash_values]            byte aligned (block_sz * hash_bits)
  // [smallest key]           length prefixed slice
  void Encode() {}

  // Decode from a LinkSST's value.
  bool DecodeFrom(Slice& input) {
    // Decode all file numbers for each KV pair
    for(int i = 0; i < group_sz_; ++i) {
      uint64_t file_number = 0;
      if(!GetVarint64(&input, &file_number)) {
        return false;
      }
      file_numbers_.emplace_back(file_number);
    }
    assert(file_numbers_.size() == group_sz_);
    // Decode hashed values
    int total_bits = group_sz_ * hash_bits_;
    int total_bytes = total_bits % 8 == 0 ? total_bits / 8 :total_bits / 8 + 1;
    assert(input.size() > total_bytes);
    assert(hash_bits_ <= 32);
    for(int i = 0; i < group_sz_; ++i) {
      // TODO(guokuankuan@bytedance.com) Add some UT for this function.
      // convert bit represent into uint32 hash values.
      int start_pos = i * hash_bits_;
      int start_bytes = start_pos / 8;
      int end_bytes = (start_pos + hash_bits_) / 8;
      uint32_t hash = 0;
      memcpy((char*)&hash, input.data() + start_bytes, end_bytes - start_bytes + 1);
      hash << (start_pos % 8);
      hash >> ((7 - (start_pos + hash_bits_) % 8) + (start_pos % 8));
      hash_values_.emplace_back(hash);
    }
    // Decode optional smallest key
    if(!GetLengthPrefixedSlice(&input, &smallest_key_)) {
      return false;
    }
    return true;
  }

  // There should be only one active LinkBlock during the Link SST iteration.
  // Here we load all underlying iterators within current LinkBlock and reset
  // iter_idx for further sue.
  bool ActiveLinkBlock() {
    for(int i = 0; i < group_sz_; ++i) {
      auto it = iterator_cache_->GetIterator(file_numbers_[i]);
      if(!it->status().ok()) {
        return false;
      }
    }
    iter_idx = 0;
    return true;
  }

  // Seek all iterators to their first item.
  Status SeekToFirst() {
    iter_idx = 0;
    for(int i = 0; i < group_sz_; ++i) {
      auto it = iterator_cache_->GetIterator(file_numbers_[iter_idx]);
      it->SeekToFirst();
      if(!it->status().ok()) {
        return it->status();
      }
    }
    return Status::OK();
  }

  // Seek all iterators to their last item.
  Status SeekToLast() {
    assert(group_sz_ == file_numbers_.size());
    iter_idx = group_sz_ - 1;
    for(int i = 0; i < group_sz_; ++i) {
      auto it = iterator_cache_->GetIterator(file_numbers_[iter_idx]);
      it->SeekToLast();
      if(!it->status().ok()) {
        return it->status();
      }
    }
    return Status::OK();
  }

  Status Seek(const Slice& target, const InternalKeyComparator& icomp) {
    uint32_t target_hash = hash(target, hash_bits_);
    // Lower bound search for target key
    uint32_t left = 0;
    uint32_t right = group_sz_;
    while(left < right) {
      uint32_t mid = left + (right - left) / 2;
      auto key = buffered_key(mid, file_numbers_[mid]);
      // TODO (guokuankuan@bytedance.com)
      // Shall we check key's hash value here?
      if(icomp.Compare(key, target) >= 0) {
        right = mid;
      } else {
        left = mid + 1;
      }
    }

    if(left < group_sz_) {
      iter_idx = left;
      // Prepare target SST's iterator for further use
      // TODO Shall we init all other iterators to the right place so we can
      // reuse them in later Next()/Prev()?
      auto it = iterator_cache_->GetIterator(file_numbers_[iter_idx]);
      it->Seek(target);
      return Status::OK();
    } else {
      iter_idx = -1;
      return Status::Corruption();
    }
  }

  // Move current iterator to next position, will skip all invalid records
  // (hash = 0)
  // If all subsequent items are invalid, return an error status.
  Status Next() {
    // Find the next valid position of current LinkBlock
    int next_iter_idx = iter_idx + 1;
    while(next_iter_idx < hash_values_.size()) {
      // Move forward target iterator
      auto it = iterator_cache_->GetIterator(file_numbers_[next_iter_idx]);
      it->Next();
      assert(it->status().ok());
      if(hash_values_[next_iter_idx] != INVALID_ITEM_HASH) {
        break;
      }
      next_iter_idx++;
    }

    // Exceed max boundary, we should try next LinkBlock, the iter_idx is now
    // meaningless since there should be only one LinkBlock active at the same
    // time during iteration.
    if(next_iter_idx == hash_values_.size()) {
      return Status::NotFound("Exceed LinkBlock's max boundary");
    }

    // Current LinkBlock is still in use, update iter_idx.
    iter_idx = next_iter_idx;
    return Status::OK();
  }

  // See the comment `Next()`, the `Prev()` implementation is almost the same
  // except iterator direction.
  Status Prev() {
    // Find the previous valid position of current LinkBlock
    int prev_iter_idx = iter_idx - 1;
    while(prev_iter_idx >= 0) {
      // Move backward
      auto it = iterator_cache_->GetIterator(file_numbers_[prev_iter_idx]);
      it->Prev();
      assert(it->status().ok());
      if(hash_values_[prev_iter_idx] != INVALID_ITEM_HASH) {
        break;
      }
      prev_iter_idx--;
    }

    // Exceed the smallest boundary
    if(prev_iter_idx == -1) {
      return Status::NotFound("Exceed LinkBlock's smallest boundary");
    }

    // Current LinkBlock is still in use, update iter_idx.
    iter_idx = prev_iter_idx;

    return Status::OK();
  }

  // Extract key from the underlying SST iterator
  Slice CurrentKey() const {
    auto it = iterator_cache_->GetIterator(file_numbers_[iter_idx]);
    return it->key();
  }

  // Extract value from the underlying SST iterator
  LazyBuffer CurrentValue() const {
    auto it = iterator_cache_->GetIterator(file_numbers_[iter_idx]);
    return it->value();
  }

  // Return the max key of current LinkBlock
  Slice MaxKey() const { return max_key_; }

  // If we have a non-empty `smallest_key_`, we should re-init all underlying
  // iterators (default: empty)
  Slice SmallestKey() const {return smallest_key_; }

  bool HasSmallestKey() const { return smallest_key_.valid(); }

  std::vector<uint64_t>& GetFileNumbers() { return file_numbers_;}

 private:
  // If a key's hash is marked as `INVALID_ITEM_HASH`, it means we should remove
  // this item in next compaction, thus we shouldn't read from it.
  const int INVALID_ITEM_HASH = 0;

  // TODO(guokuankuan@bytedance.com)
  // Hash a user key into an integer, limit the maximum bits.
  uint32_t hash(const Slice& user_key, int max_bits) {
    return 0;
  }

  Slice buffered_key(uint32_t idx, uint64_t file_number) {
    if(!key_buffer_[idx].valid()) {
      // Find out the occurrence between `idx` and the last position of current
      // file_number, e.g. [0 ,1, 10, 11, 9, 10, 2], if `idx` is 2, then the
      // occurrence should be {2, 5}.
      std::vector<uint32_t> occurrence = {idx};
      for(uint32_t i = idx + 1; i < group_sz_; ++i) {
        if(file_numbers_[i] == file_number) {
          occurrence.emplace_back(i);
        }
      }

      // Seek to the last position of current file_number and `Prev` back to the
      // position `idx`, fill all touched keys into the key buffer.
      auto it = iterator_cache_->GetIterator(file_number);
      it->SeekForPrev(max_key_);
      assert(it->status().ok());
      for(uint32_t i = occurrence.size() - 1; i >=0; --i) {
        uint32_t pos = occurrence[i];
        if(!key_buffer_[pos].valid()) {
          key_buffer_[pos] = it->key();
        }
        it->Prev();
      }
    }

    assert(!key_buffer_[idx].empty());
    return key_buffer_[idx];
  }

 private:
  IteratorCache* iterator_cache_;

  // The end/max key of current LinkBlock
  Slice max_key_;
  // How many KV pairs we should group into one LinkBlock.
  int group_sz_ = 8;
  // Bits count required for each underlying SST file
  int hash_bits_ = 11;
  // Cache touched keys while iterating
  std::vector<Slice> key_buffer_;
  // Indicate which SST current KV pairs belongs to.
  // file_numbers_.size() == block_sz_
  std::vector<uint64_t> file_numbers_;
  // Each KV pair has a hash value (hash(user_key))
  std::vector<uint32_t> hash_values_;
  // Current iteration index of this LinkBlock.
  int iter_idx = 0;
  // Optional, if smallest_key_exist, it means one of the underlying iterator
  // is expired, we should seek all iterators to target key again for further
  // iteration.
  Slice smallest_key_;
};

// TODO(guokuankuan@bytedance.com)
// A LinkSstIterator is almost the same to MapSstIterator.
class LinkSstIterator : public InternalIterator {
 private:
  const FileMetaData* file_meta_;
  InternalIterator* link_sst_iter_ {};
  InternalKeyComparator icomp_;
  // The smallest key of current Link SST
//  Slice smallest_key;
  // The largest key of current Link SST
//  Slice largest_key;

  IteratorCache iterator_cache_;
  std::vector<LinkBlockRecord> lbr_list_;

  Status status_;
  uint32_t cur_lbr_idx_{};

//  BinaryHeap<HeapElement, HeapComparator<0>, HeapVectorType> min_heap_;

 public:
  LinkSstIterator(const FileMetaData* file_meta, InternalIterator* iter,
                 const DependenceMap& dependence_map,
                 const InternalKeyComparator& icomp, void* create_arg,
                 const IteratorCache::CreateIterCallback& create)
      : file_meta_(file_meta),
        link_sst_iter_(iter),
        icomp_(icomp),
        iterator_cache_(dependence_map, create_arg, create) {
    if (file_meta != nullptr && !file_meta_->prop.is_map_sst()) {
      abort();
    }
  }

  ~LinkSstIterator() override = default;

 private:
  // Init all LBR by decoding all LinkSST's value.
  bool InitLinkBlockRecords() {
    LazyBuffer current_value_;
    link_sst_iter_->SeekToFirst();
    while(link_sst_iter_->Valid()) {
      current_value_ = link_sst_iter_->value();
      status_ = current_value_.fetch();
      if(!status_.ok()) {
        status_ = Status::Corruption("Failed to fetch lazy buffer");
        return false;
      }
      Slice input = current_value_.slice();
      lbr_list_.emplace_back(&iterator_cache_,
                             link_sst_iter_->key(),
                             file_meta_->prop.lbr_group_size,
                             file_meta_->prop.lbr_hash_bits);
      if(!lbr_list_.back().DecodeFrom(input)) {
        status_ = Status::Corruption("Cannot decode Link SST");
        return false;
      }
      link_sst_iter_->Next();
    }
    current_value_.reset();
    return true;
  }

  // We assume there should be a lot of underlying SST for each LinkSST, so we
  // could simply initialize all SST iterators before any iteration.
  bool InitSecondLevelIterators() {
    for(auto& lb: lbr_list_) {
      if(!lb.ActiveLinkBlock()) {
        return false;
      }
    }
    return true;
  }

 public:
  bool Valid() const override { return !lbr_list_.empty(); }
  void SeekToFirst() override {
    if(!InitLinkBlockRecords() || !InitSecondLevelIterators()) {
      status_ = Status::Aborted();
      return;
    }
    assert(!lbr_list_.empty());
    cur_lbr_idx_ = 0;
    status_ = lbr_list_[cur_lbr_idx_].SeekToFirst();
  }

  void SeekToLast() override {
    if(!InitLinkBlockRecords() || !InitSecondLevelIterators()) {
      status_ = Status::Corruption();
      return;
    }
    assert(!lbr_list_.empty());
    cur_lbr_idx_ = lbr_list_.size() - 1;
    status_ = lbr_list_[cur_lbr_idx_].SeekToLast();
  }

  // TODO(guokuankuan@bytendance.com)
  // Is input target a InternalKey ? then what is the default sequence#?
  void Seek(const Slice& target) override {
    if(!InitLinkBlockRecords() || !InitSecondLevelIterators()) {
      status_ = Status::Corruption();
      return;
    }
    // Find target LinkBlock's position
    auto it = std::lower_bound(lbr_list_.begin(), lbr_list_.end(), target,
                     [&](const LinkBlockRecord& lbr, const Slice& target){
                                 return icomp_.Compare(lbr.MaxKey(), target) < 0;
                               });
    if(it == lbr_list_.end()) {
      status_ = Status::NotFound();
      return;
    }
    cur_lbr_idx_ = it - lbr_list_.begin();
    // Do the Seek
    status_ = lbr_list_[cur_lbr_idx_].Seek(target, icomp_);
  }

  // Position at the first key at or before the target.
  void SeekForPrev(const Slice& target) override {
    if(!InitLinkBlockRecords() || !InitSecondLevelIterators()) {
      status_ = Status::Corruption();
      return;
    }

    // We adopt Seek & Prev semantics here
    Seek(target);

    // If the position's key is equal to target, we are good to go.
    if(status_.ok() && key() == target) {
      return;
    }

    // If the key is greater than target, then we need to `Prev` to the right
    // place.
    while(status_.ok() && key().compare(target) > 0) {
      Prev();
    }
  }

  void Next() override {
    while(cur_lbr_idx_ < lbr_list_.size()) {
      auto s = lbr_list_[cur_lbr_idx_].Next();
      if(s.ok()) {
        break;
      }

      // If we cannot `Next()` current LBR properly, try  next.
      cur_lbr_idx_++;
      if(cur_lbr_idx_ == lbr_list_.size()) {
        break;
      }

      assert(cur_lbr_idx_ < lbr_list_.size());
      // If next LBR has a valid smallest key, we should re-seek all iterators
      // (which means the iterators' continuous may break)
      auto lbr = lbr_list_[cur_lbr_idx_];
      if(lbr.HasSmallestKey()) {
        lbr.Seek(lbr.SmallestKey(), icomp_);
      }
    }

    // No valid position found
    if(cur_lbr_idx_ == lbr_list_.size()) {
      status_ = Status::NotFound("End of iterator exceeded");
      return;
    }
  }

  void Prev() override {
    while(cur_lbr_idx_ >= 0) {
      auto s = lbr_list_[cur_lbr_idx_].Prev();
      if(s.ok()) {
        break;
      }
      // All items were consumed, exit.
      if(cur_lbr_idx_ == 0) {
        status_ = Status::NotFound("Not more previous items!");
        return;
      }

      // If we cannot `Prev()` current LBR, try previous one, note that if current
      // LBR has a valid smallest key, we should re-seek previous LBR.
      cur_lbr_idx_--;
      auto curr_lbr = lbr_list_[cur_lbr_idx_ + 1];
      auto prev_lbr = lbr_list_[cur_lbr_idx_];
      if(curr_lbr.HasSmallestKey()) {
        prev_lbr.Seek(prev_lbr.MaxKey(), icomp_);
      }
    }
  }

  Slice key() const override {
    assert(Valid());
    return lbr_list_[cur_lbr_idx_].CurrentKey();
  }
  LazyBuffer value() const override {
    assert(Valid());
    return lbr_list_[cur_lbr_idx_].CurrentValue();
  }
  Status status() const override { return status_; }
};
}  // namespace

InternalIteratorBase<BlockHandle>* NewTwoLevelIterator(
    TwoLevelIteratorState* state,
    InternalIteratorBase<BlockHandle>* first_level_iter) {
  return new TwoLevelIndexIterator(state, first_level_iter);
}

InternalIterator* NewLinkSstIterator(
    const FileMetaData* file_meta, InternalIterator* mediate_sst_iter,
    const DependenceMap& dependence_map, const InternalKeyComparator& icomp,
    void* callback_arg, const IteratorCache::CreateIterCallback& create_iter,
    Arena* arena) {
  assert(file_meta == nullptr || file_meta->prop.is_link_sst());
  if (arena == nullptr) {
    return new LinkSstIterator(file_meta, mediate_sst_iter, dependence_map,
                              icomp, callback_arg, create_iter);
  } else {
    void* buffer = arena->AllocateAligned(sizeof(LinkSstIterator));
    return new (buffer)
        LinkSstIterator(file_meta, mediate_sst_iter, dependence_map, icomp,
                       callback_arg, create_iter);
  }
}

InternalIterator* NewMapSstIterator(
    const FileMetaData* file_meta, InternalIterator* mediate_sst_iter,
    const DependenceMap& dependence_map, const InternalKeyComparator& icomp,
    void* callback_arg, const IteratorCache::CreateIterCallback& create_iter,
    Arena* arena) {
  assert(file_meta == nullptr || file_meta->prop.is_map_sst());
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
