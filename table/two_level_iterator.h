//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db/version_edit.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/terark_namespace.h"
#include "table/iterator_wrapper.h"
#include "util/iterator_cache.h"

namespace TERARKDB_NAMESPACE {

struct ReadOptions;
class InternalKeyComparator;

// TwoLevelIteratorState expects iterators are not created using the arena
struct TwoLevelIteratorState {
  TwoLevelIteratorState() {}

  virtual ~TwoLevelIteratorState() {}
  virtual InternalIteratorBase<BlockHandle>* NewSecondaryIterator(
      const BlockHandle& handle) = 0;
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

        // Get the underlying SST's file number of target key
        Status GetFileNumber(const Slice& target, uint64_t* file_number) {
            uint32_t target_hash = hash(target, hash_bits_);
            for(int i = 0; i < group_sz_; ++i) {
                // hash collision may happen, so we should double check the key's content.
                if(target_hash == hash_values_[i]) {
                    auto it = iterator_cache_->GetIterator(file_numbers_[i]);
                    it->Seek(target);
                    if(!it->status().ok() || it->key().compare(target) != 0) {
                        continue;
                    }
                    *file_number = file_numbers_[i];
                    return Status::OK();
                }
            }
            return Status::NotFound("Target key is not exist");
        }

        // Seek inside a LinkBlock and will fetch & buffer each key we touched
        Status Seek(const Slice& target, const InternalKeyComparator& icomp) {
            uint32_t target_hash = hash(target, hash_bits_);
            // Lower bound search for target key
            uint32_t left = 0;
            uint32_t right = group_sz_;
            while(left < right) {
                uint32_t mid = left + (right - left) / 2;
                auto key = buffered_key(mid);
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
        // iterators (default: invalid)
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

        // Get target key from cache. If it's not present, fetch from iterator & cache it.
        Slice buffered_key(uint32_t idx) {
            uint64_t file_number = file_numbers_[idx];
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

// LinkSstIterator is a composition of a few ordinary iterators
class LinkSstIterator : public InternalIterator {
    private:
        const FileMetaData* file_meta_;
        InternalIterator* link_sst_iter_ {};
        InternalKeyComparator icomp_;

        IteratorCache iterator_cache_;
        std::vector<LinkBlockRecord> lbr_list_;

        Status status_;
        uint32_t cur_lbr_idx_{};

        // Some operations may invalid current iterator.
        bool valid_ = true;

        bool lbr_initialized_ = false;
        bool lbr_actived_ = false;

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
            if(lbr_initialized_) {
                return true;
            }
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
            lbr_initialized_ = true;
            return true;
        }

        // We assume there should be a lot of underlying SST for each LinkSST, so we
        // could simply initialize all SST iterators before any iteration.
        bool InitSecondLevelIterators() {
            if(lbr_actived_) {
                return true;
            }
            for(auto& lb: lbr_list_) {
                if(!lb.ActiveLinkBlock()) {
                    return false;
                }
            }
            lbr_actived_ = true;
            return true;
        }

    public:
        bool Valid() const override { return !lbr_list_.empty() && valid_; }
        void SeekToFirst() override {
            if(!InitLinkBlockRecords() || !InitSecondLevelIterators()) {
                status_ = Status::Aborted();
                return;
            }
            assert(!lbr_list_.empty());
            cur_lbr_idx_ = 0;
            status_ = lbr_list_[cur_lbr_idx_].SeekToFirst();
            valid_ = true;
        }

        void SeekToLast() override {
            if(!InitLinkBlockRecords() || !InitSecondLevelIterators()) {
                status_ = Status::Corruption();
                return;
            }
            assert(!lbr_list_.empty());
            cur_lbr_idx_ = lbr_list_.size() - 1;
            status_ = lbr_list_[cur_lbr_idx_].SeekToLast();
            valid_ = true;
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
            valid_ = true;
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
            valid_ = true;
        }

        void Next() override {
            assert(Valid());
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
            assert(Valid());
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

        // Find target key's underlying SST file number (exactly match)
        // TODO (guokuankuan@bytedance.com) Maybe we could re-use Seek() here?
        Status GetTargetFileNumber(const Slice& target, uint64_t* file_number) {
            if(!InitLinkBlockRecords()) {
                return Status::Corruption();
            }
            // Find target LinkBlock's position
            auto it = std::lower_bound(lbr_list_.begin(), lbr_list_.end(), target,
                                       [&](const LinkBlockRecord& lbr, const Slice& target){
                                           return icomp_.Compare(lbr.MaxKey(), target) < 0;
                                       });
            if(it == lbr_list_.end()) {
                return Status::NotFound();
            }

            return it->GetFileNumber(target, file_number);
        }

        Status status() const override { return status_; }
};

// Return a new two level iterator.  A two-level iterator contains an
// index iterator whose values point to a sequence of blocks where
// each block is itself a sequence of key,value pairs.  The returned
// two-level iterator yields the concatenation of all key/value pairs
// in the sequence of blocks.  Takes ownership of "index_iter" and
// will delete it when no longer needed.
//
// Uses a supplied function to convert an index_iter value into
// an iterator over the contents of the corresponding block.
// Note: this function expects first_level_iter was not created using the arena
extern InternalIteratorBase<BlockHandle>* NewTwoLevelIterator(
    TwoLevelIteratorState* state,
    InternalIteratorBase<BlockHandle>* first_level_iter);

// keep all params lifecycle please
// @return a two level iterator. for unroll map sst
extern InternalIterator* NewMapSstIterator(
    const FileMetaData* file_meta, InternalIterator* mediate_sst_iter,
    const DependenceMap& dependence_map, const InternalKeyComparator& icomp,
    void* callback_arg, const IteratorCache::CreateIterCallback& create_iter,
    Arena* arena = nullptr);

// A link sst iterator should expand all its dependencies for the callers.
// @return TwoLevelIterator
extern InternalIterator* NewLinkSstIterator(
    const FileMetaData* file_meta, InternalIterator* mediate_sst_iter,
    const DependenceMap& dependence_map, const InternalKeyComparator& icomp,
    void* callback_arg, const IteratorCache::CreateIterCallback& create_iter,
    Arena* arena = nullptr);

}  // namespace TERARKDB_NAMESPACE
