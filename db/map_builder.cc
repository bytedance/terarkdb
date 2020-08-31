//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/map_builder.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include <algorithm>
#include <list>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "db/builder.h"
#include "db/dbformat.h"
#include "db/event_helpers.h"
#include "db/range_del_aggregator.h"
#include "monitoring/thread_status_util.h"
#include "table/merging_iterator.h"
#include "table/two_level_iterator.h"
#include "util/c_style_callback.h"
#include "util/iterator_cache.h"
#include "util/sst_file_manager_impl.h"
#include "version_set.h"

namespace rocksdb {

struct FileMetaDataBoundBuilder {
  const InternalKeyComparator* icomp;
  InternalKey smallest, largest;
  SequenceNumber smallest_seqno;
  SequenceNumber largest_seqno;

  FileMetaDataBoundBuilder(const InternalKeyComparator* _icomp)
      : icomp(_icomp), smallest_seqno(kMaxSequenceNumber), largest_seqno(0) {}

  void Update(const FileMetaData* f) {
    if (smallest.size() == 0 || icomp->Compare(f->smallest, smallest) < 0) {
      smallest = f->smallest;
    }
    if (largest.size() == 0 || icomp->Compare(f->largest, largest) > 0) {
      largest = f->largest;
    }
    smallest_seqno = std::min(smallest_seqno, f->fd.smallest_seqno);
    largest_seqno = std::max(largest_seqno, f->fd.largest_seqno);
  }
};

bool IsPerfectRange(const Range& range, const FileMetaData* f,
                    const InternalKeyComparator& icomp) {
  if (f->prop.is_map_sst()) {
    return false;
  }
  int c = icomp.Compare(range.start, f->smallest.Encode());
  if (range.include_start ? c > 0 : c >= 0) {
    return false;
  }
  c = icomp.Compare(range.limit, f->largest.Encode());
  if (range.include_limit ? c < 0 : c <= 0) {
    return false;
  }
  return true;
}

namespace {

struct IteratorCacheContext {
  static InternalIterator* CreateIter(void* arg, const FileMetaData* f,
                                      const DependenceMap& dependence_map,
                                      Arena* arena, TableReader** reader_ptr) {
    IteratorCacheContext* ctx = static_cast<IteratorCacheContext*>(arg);
    DependenceMap empty_dependence_map;
    ReadOptions read_options;
    read_options.verify_checksums = true;
    read_options.fill_cache = false;
    read_options.total_order_seek = true;

    return ctx->cfd->table_cache()->NewIterator(
        read_options, *ctx->env_options, *f,
        f->prop.is_map_sst() ? empty_dependence_map : dependence_map,
        nullptr /* range_del_agg */,
        ctx->mutable_cf_options->prefix_extractor.get(), reader_ptr,
        nullptr /* no per level latency histogram */,
        false /* for_compaction */, arena, false /* skip_filters */,
        -1 /* level */);
  }
  static InternalIterator* CreateVersionIter(void* arg, Arena* arena) {
    IteratorCacheContext* ctx = static_cast<IteratorCacheContext*>(arg);
    ReadOptions read_options;
    read_options.verify_checksums = true;
    read_options.fill_cache = false;
    read_options.total_order_seek = true;
    MergeIteratorBuilder builder(&ctx->cfd->internal_comparator(), arena);
    ctx->version->AddIterators(read_options, *ctx->env_options, &builder,
                               nullptr /* range_del_agg */);
    return builder.Finish();
  }

  ColumnFamilyData* cfd;
  const MutableCFOptions* mutable_cf_options;
  Version* version;
  const EnvOptions* env_options;
};

struct RangeWithDepend {
  Slice point[2];
  bool include[2];
  bool has_delete_range;
  bool stable;
  std::vector<MapSstElement::LinkTarget> dependence;

  RangeWithDepend() = default;

  RangeWithDepend(const FileMetaData* f, Arena* arena) {
    assert(GetInternalKeySeqno(f->smallest.Encode()) != kMaxSequenceNumber);
    point[0] = f->smallest.Encode();
    if (GetInternalKeySeqno(f->largest.Encode()) == kMaxSequenceNumber) {
      point[1] = ArenaPinInternalKey(f->largest.user_key(), kMaxSequenceNumber,
                                     static_cast<ValueType>(0), arena);
    } else {
      point[1] = f->largest.Encode();
    }
    include[0] = true;
    include[1] = true;
    has_delete_range = false;
    stable = false;
    dependence.emplace_back(MapSstElement::LinkTarget{f->fd.GetNumber(), 0});
  }

  RangeWithDepend(const MapSstElement& map_element, Arena* arena) {
    point[0] = ArenaPinSlice(map_element.smallest_key, arena);
    point[1] = ArenaPinSlice(map_element.largest_key, arena);
    include[0] = map_element.include_smallest;
    include[1] = map_element.include_largest;
    has_delete_range = map_element.has_delete_range;
    stable = true;
    dependence = map_element.link;
  }
  RangeWithDepend(const Range& range, Arena* arena) {
    assert(range.include_start);
    point[0] = ArenaPinInternalKey(range.start, kMaxSequenceNumber,
                                   static_cast<ValueType>(0), arena);
    if (range.include_limit) {
      point[1] =
          ArenaPinInternalKey(range.limit, 0, static_cast<ValueType>(0), arena);
    } else {
      point[1] = ArenaPinInternalKey(range.limit, kMaxSequenceNumber,
                                     static_cast<ValueType>(0), arena);
    }
    include[0] = false;
    include[1] = true;
    has_delete_range = false;
    stable = false;
  }
};

int CompInclude(int c, size_t ab, size_t ai, size_t bb, size_t bi) {
#define CASE(a, b, c, d) \
  (((a) ? 1 : 0) | ((b) ? 2 : 0) | ((c) ? 4 : 0) | ((d) ? 8 : 0))
  if (c != 0) {
    return c;
  }
  switch (CASE(ab, ai, bb, bi)) {
    // a: [   [   (   )   )   [
    // b: (   )   ]   ]   (   ]
    case CASE(0, 1, 0, 0):
    case CASE(0, 1, 1, 0):
    case CASE(0, 0, 1, 1):
    case CASE(1, 0, 1, 1):
    case CASE(1, 0, 0, 0):
    case CASE(0, 1, 1, 1):
      return -1;
    // a: (   )   ]   ]   (   ]
    // b: [   [   (   )   )   [
    case CASE(0, 0, 0, 1):
    case CASE(1, 0, 0, 1):
    case CASE(1, 1, 0, 0):
    case CASE(1, 1, 1, 0):
    case CASE(0, 0, 1, 0):
    case CASE(1, 1, 0, 1):
      return 1;
    // a: [   ]   (   )
    // b: [   ]   (   )
    default:
      return 0;
  }
#undef CASE
}

struct MapBuilderRangesItem {
  MapBuilderRangesItem(FileMetaDataBoundBuilder&& _bound_builder, int _level,
                       bool _is_map, std::vector<RangeWithDepend>&& _ranges)
      : bound_builder(std::move(_bound_builder)),
        level(_level),
        is_map(_is_map),
        input_range_count(_ranges.size()),
        ranges(std::move(_ranges)),
        self_tombstone_index(size_t(-1)),
        inputs(nullptr) {}
  struct TombstonsItem {
    TombstonsItem(std::shared_ptr<FragmentedRangeTombstoneList> _tombstones)
        : tombstones(_tombstones) {}
    void SetRanges(const std::vector<RangeWithDepend>& _ranges) {
      ranges.clear();
      ranges.reserve(_ranges.size());
      for (auto& r : _ranges) {
        ranges.emplace_back(TombstonsRange{r.point[0], r.point[1]});
        assert(!r.include[0]);
        assert(r.include[1]);
      }
    }
    struct TombstonsRange {
      Slice start_ikey, end_ikey;
    };
    std::vector<TombstonsRange> ranges;
    std::shared_ptr<FragmentedRangeTombstoneList> tombstones;
  };

  FileMetaDataBoundBuilder bound_builder;
  int level;
  bool is_map;
  size_t input_range_count;
  std::vector<RangeWithDepend> ranges;
  std::vector<TombstonsItem> tombstones;
  size_t self_tombstone_index;
  const std::vector<FileMetaData*>* inputs;

  bool is_stable() {
    return is_map && ranges.size() == input_range_count &&
           !std::any_of(ranges.begin(), ranges.end(),
                        [](const RangeWithDepend& e) { return !e.stable; });
  }
};

class MapSstElementIterator : public MapSstRangeIterator {
 public:
  MapSstElementIterator(const std::vector<RangeWithDepend>& ranges,
                        IteratorCache& iterator_cache,
                        const InternalKeyComparator& icomp)
      : ranges_(ranges), iterator_cache_(iterator_cache), icomp_(icomp) {}
  bool Valid() const override { return !buffer_.empty(); }
  void SeekToFirst() override {
    where_ = ranges_.begin();
    PrepareNext();
  }
  void SeekToLast() override { assert(false); }
  void Seek(const Slice&) override { assert(false); }
  void SeekForPrev(const Slice&) override { assert(false); }
  void Next() override { PrepareNext(); }
  void Prev() override { assert(false); }
  Slice key() const override { return map_elements_.Key(); }
  LazyBuffer value() const override { return LazyBuffer(buffer_); }
  Status status() const override { return status_; }

  const std::unordered_map<uint64_t, uint64_t>& GetDependence() const override {
    return dependence_build_;
  }

  std::pair<size_t, double> GetSstReadAmp() const override {
    return {sst_read_amp_, sst_read_amp_ratio_};
  }

 private:
  void CheckIter(InternalIterator* iter) {
    assert(!iter->Valid());
    if (status_.ok()) {
      status_ = iter->status();
    }
  }
  void PrepareNext() {
    while (true) {
      if (where_ == ranges_.end()) {
        buffer_.clear();
        if (sst_read_amp_size_ == 0) {
          sst_read_amp_ratio_ = sst_read_amp_;
        } else {
          sst_read_amp_ratio_ /= sst_read_amp_size_;
        }
        assert(sst_read_amp_ratio_ >= 1);
        assert(sst_read_amp_ratio_ <= sst_read_amp_);
        for (auto& pair : dependence_build_) {
          auto f = iterator_cache_.GetFileMetaData(pair.first);
          assert(f != nullptr);
          assert(f->fd.file_size > 0);
          pair.second = f->prop.num_entries * pair.second / f->fd.file_size;
          pair.second = std::min<uint64_t>(pair.second, f->prop.num_entries);
          pair.second = std::max<uint64_t>(pair.second, 1);
        }
        return;
      }
      auto& start = map_elements_.smallest_key = where_->point[0];
      auto& end = map_elements_.largest_key = where_->point[1];
      assert(icomp_.Compare(start, end) <= 0);
      map_elements_.include_smallest = where_->include[0];
      map_elements_.include_largest = where_->include[1];
      map_elements_.has_delete_range = where_->has_delete_range;
      bool stable = where_->stable;
      auto& links = map_elements_.link = where_->dependence;
      assert(!map_elements_.include_smallest);
      assert(map_elements_.include_largest);

      ++where_;
      size_t range_size = 0;
      auto put_dependence = [&](uint64_t file_number, uint64_t size) {
        auto ib = dependence_build_.emplace(file_number, size);
        if (!ib.second) {
          ib.first->second += size;
        }
      };
      if (stable) {
        for (auto& link : links) {
          put_dependence(link.file_number, link.size);
          range_size += link.size;
        }
      } else {
        for (auto& link : links) {
          link.size = 0;
          const FileMetaData* meta =
              iterator_cache_.GetFileMetaData(link.file_number);
          if (meta == nullptr) {
            status_ = Status::Corruption(
                "MapSstElementIterator missing FileMetaData");
            buffer_.clear();
            return;
          }
          TableReader* reader;
          if (icomp_.Compare(meta->smallest.Encode(), end) > 0 ||
              icomp_.Compare(meta->largest.Encode(), start) <= 0) {
            // non overlap with file, drop this link
            link.file_number = uint64_t(-1);
            continue;
          } else if (icomp_.Compare(meta->smallest.Encode(), start) > 0 &&
                     icomp_.Compare(meta->largest.Encode(), end) <= 0) {
            // cover whole file
            link.size = meta->fd.GetFileSize();
            range_size += link.size;
          } else {
            auto iter = iterator_cache_.GetIterator(meta, &reader);
            if (!iter->status().ok()) {
              buffer_.clear();
              status_ = iter->status();
              return;
            }
            do {
              iter->Seek(start);
              if (!iter->Valid()) {
                CheckIter(iter);
                break;
              }
              temp_start_.DecodeFrom(iter->key());
              iter->SeekForPrev(end);
              if (!iter->Valid()) {
                CheckIter(iter);
                break;
              }
              temp_end_.DecodeFrom(iter->key());
              if (icomp_.Compare(temp_start_, temp_end_) <= 0) {
                uint64_t start_offset =
                    reader->ApproximateOffsetOf(temp_start_.Encode());
                uint64_t end_offset =
                    reader->ApproximateOffsetOf(temp_end_.Encode());
                link.size = end_offset - start_offset;
                range_size += link.size;
              }
            } while (false);
            if (!status_.ok()) {
              buffer_.clear();
              return;
            }
          }
          put_dependence(link.file_number, link.size);
        }
        links.erase(std::remove_if(links.begin(), links.end(),
                                   [](const MapSstElement::LinkTarget& link) {
                                     return link.file_number == uint64_t(-1);
                                   }),
                    links.end());
        if (links.empty()) {
          continue;
        }
      }
      sst_read_amp_ = std::max(sst_read_amp_, map_elements_.link.size());
      sst_read_amp_ratio_ += map_elements_.link.size() * range_size;
      sst_read_amp_size_ += range_size;
      map_elements_.Value(&buffer_);  // Encode value
      break;
    }
  }

 private:
  Status status_;
  MapSstElement map_elements_;
  InternalKey temp_start_, temp_end_;
  std::string buffer_;
  std::vector<RangeWithDepend>::const_iterator where_;
  const std::vector<RangeWithDepend>& ranges_;
  std::unordered_map<uint64_t, uint64_t> dependence_build_;
  size_t sst_read_amp_ = 0;
  double sst_read_amp_ratio_ = 0;
  size_t sst_read_amp_size_ = 0;
  IteratorCache& iterator_cache_;
  const InternalKeyComparator& icomp_;
};

class MapSstTombstoneIterator : public InternalIterator {
 public:
  MapSstTombstoneIterator(const MapBuilderRangesItem::TombstonsItem& rombstons,
                          const InternalKeyComparator& icomp)
      : ranges_(rombstons.ranges),
        tombstone_list_(rombstons.tombstones.get()),
        list_seq_(size_t(-1)),
        icomp_(icomp) {}

  bool Valid() const override { return encoded_key_.size() >= 8; }
  void SeekToFirst() override {
    where_ = ranges_.begin();
    list_seq_ = size_t(-1);
    PrepareNext();
  }
  void SeekToLast() override { assert(false); }
  void Seek(const Slice&) override { assert(false); }
  void SeekForPrev(const Slice&) override { assert(false); }
  void Next() override { PrepareNext(); }
  void Prev() override { assert(false); }
  Slice key() const override { return encoded_key_.Encode(); }
  LazyBuffer value() const override { return LazyBuffer(tombstone_end_key_); }
  Status status() const override { return status_; }

 private:
  bool PrepareTombstone() {
    auto uc = icomp_.user_comparator();
    if (tombstone_where_ == tombstone_list_->end() ||
        uc->Compare(tombstone_where_->start_key, end_key_) >= 0) {
      return false;
    }
    tombstone_start_key_ = tombstone_where_->start_key;
    if (uc->Compare(tombstone_start_key_, start_key_) < 0) {
      tombstone_start_key_ = start_key_;
    }
    auto next = std::next(tombstone_where_);
    while (next != tombstone_list_->end() &&
           uc->Compare(next->start_key, tombstone_where_->end_key) == 0 &&
           next->seq_end_idx - next->seq_start_idx ==
               tombstone_where_->seq_end_idx -
                   tombstone_where_->seq_start_idx &&
           std::mismatch(
               tombstone_list_->seq_iter(next->seq_start_idx),
               tombstone_list_->seq_iter(next->seq_end_idx),
               tombstone_list_->seq_iter(tombstone_where_->seq_start_idx))
                   .first == tombstone_list_->seq_iter(next->seq_end_idx)) {
      tombstone_where_ = next++;
    }
    tombstone_end_key_ = tombstone_where_->end_key;
    if (uc->Compare(tombstone_end_key_, end_key_) > 0) {
      tombstone_end_key_ = end_key_;
    }
    list_seq_ = tombstone_where_->seq_start_idx;
    encoded_key_.Set(tombstone_start_key_,
                     *tombstone_list_->seq_iter(list_seq_), kTypeRangeDeletion);
    return true;
  }
  void PrepareNext() {
    auto uc = icomp_.user_comparator();
    while (true) {
      if (where_ == ranges_.end()) {
        encoded_key_.Clear();
        return;
      }
      if (list_seq_ == size_t(-1)) {
        start_key_ = ExtractUserKey(where_->start_ikey);
        end_key_ = ExtractUserKey(where_->end_ikey);
        auto next = std::next(where_);
        while (next != ranges_.end() &&
               uc->Compare(end_key_, ExtractUserKey(next->start_ikey)) == 0) {
          assert(GetInternalKeySeqno(where_->end_ikey) == kMaxSequenceNumber);
          assert(GetInternalKeySeqno(next->start_ikey) == kMaxSequenceNumber);
          end_key_ = ExtractUserKey(next->end_ikey);
          where_ = next++;
        }
        tombstone_where_ = std::upper_bound(
            tombstone_list_->begin(), tombstone_list_->end(), start_key_,
            [uc](const Slice& a,
                 const FragmentedRangeTombstoneList::RangeTombstoneStack& b) {
              return uc->Compare(a, b.end_key) < 0;
            });
        if (PrepareTombstone()) {
          return;
        }
      } else if (++list_seq_ < tombstone_where_->seq_end_idx) {
        encoded_key_.Set(tombstone_start_key_,
                         *tombstone_list_->seq_iter(list_seq_),
                         kTypeRangeDeletion);
        return;
      } else {
        ++tombstone_where_;
        if (PrepareTombstone()) {
          return;
        }
      }
      if (!status_.ok()) {
        encoded_key_.Clear();
        return;
      }
      ++where_;
      list_seq_ = size_t(-1);
    }
  }

 private:
  Status status_;
  InternalKey encoded_key_;
  InternalKey seek_key_;
  std::vector<MapBuilderRangesItem::TombstonsItem::TombstonsRange>::
      const_iterator where_;
  Slice start_key_;
  Slice end_key_;
  const std::vector<MapBuilderRangesItem::TombstonsItem::TombstonsRange>&
      ranges_;
  std::vector<FragmentedRangeTombstoneList::RangeTombstoneStack>::const_iterator
      tombstone_where_;
  Slice tombstone_start_key_;
  Slice tombstone_end_key_;
  FragmentedRangeTombstoneList* tombstone_list_;
  size_t list_seq_;
  const InternalKeyComparator& icomp_;
};
// 更新FileMetaDataBoundBuilder上下边界， 普通文件直接加入ranges， map sst
// 对每一条kv创建一个map element 加入ranges
Status LoadRangeWithDepend(std::vector<RangeWithDepend>& ranges, Arena* arena,
                           FileMetaDataBoundBuilder* bound_builder,
                           IteratorCache& iterator_cache,
                           const FileMetaData* const* file_meta, size_t n) {
  MapSstElement map_element;
  for (size_t i = 0; i < n; ++i) {
    auto f = file_meta[i];
    if (f->prop.is_map_sst()) {
      auto iter = iterator_cache.GetIterator(f, nullptr);
      assert(iter != nullptr);
      if (!iter->status().ok()) {
        return iter->status();
      }
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        auto value = iter->value();
        auto s = value.fetch();
        if (!s.ok()) {
          return s;
        }
        if (!map_element.Decode(iter->key(), value.slice())) {
          return Status::Corruption(
              "LoadRangeWithDepend: Map sst invalid key or value");
        }
        ranges.emplace_back(map_element, arena);
      }
    } else {
      ranges.emplace_back(f, arena);
    }
    if (bound_builder != nullptr) {
      bound_builder->Update(f);
    }
  }
  return Status::OK();
}

Status AdjustRange(const InternalKeyComparator* ic, InternalIterator* iter,
                   Arena* arena, const InternalKey& largest_key,
                   std::vector<RangeWithDepend>& ranges) {
  if (ranges.empty()) {
    return Status::OK();
  }
  std::vector<RangeWithDepend> new_ranges;
  auto merge_dependence = [](std::vector<MapSstElement::LinkTarget>& e,
                             const std::vector<MapSstElement::LinkTarget>& d) {
    size_t insert_pos = e.size();
    for (auto rit = d.rbegin(); rit != d.rend(); ++rit) {
      size_t new_pos;
      for (new_pos = 0; new_pos < insert_pos; ++new_pos) {
        if (e[new_pos].file_number == rit->file_number) {
          break;
        }
      }
      if (new_pos == insert_pos) {
        e.emplace(e.begin() + new_pos, *rit);
      } else {
        insert_pos = new_pos;
      }
    }
  };
  new_ranges.clear();
  Slice largest = ArenaPinInternalKey(
      largest_key.user_key(),
      GetInternalKeySeqno(largest_key.Encode()) == kMaxSequenceNumber
          ? kMaxSequenceNumber
          : 0,
      static_cast<ValueType>(0), arena);
  auto uc = ic->user_comparator();
  InternalKey ik;
  enum { kSetMin, kSetMax };
  auto set_ik = [&ik](const Slice& uk, int min_or_max) {
    ik.Clear();
    if (min_or_max == kSetMin) {
      ik.SetMinPossibleForUserKey(uk);
    } else {
      ik.SetMaxPossibleForUserKey(uk);
    }
    return ik.Encode();
  };
  // fix range to (...] and seq_num to kMaxSequenceNumber
  for (auto it = ranges.begin(); it != ranges.end(); ++it) {
    auto range = &*it;
    // left
    if (range->include[0] ||
        GetInternalKeySeqno(range->point[0]) != kMaxSequenceNumber) {
      range->point[0] = ArenaPinInternalKey(ExtractUserKey(range->point[0]),
                                            kMaxSequenceNumber,
                                            static_cast<ValueType>(0), arena);
      range->include[0] = false;
    }
    // right
    if (ic->Compare(range->point[1], largest) >= 0) {
      range->point[1] = largest;
      range->include[1] = true;
    } else if (GetInternalKeySeqno(range->point[1]) != kMaxSequenceNumber) {
      iter->Seek(set_ik(ExtractUserKey(range->point[1]), kSetMax));
      if (iter->Valid() && ic->Compare(iter->key(), ik.Encode()) == 0) {
        iter->Next();
      }
      if (iter->Valid() && ic->Compare(iter->key(), largest) <= 0) {
        range->point[1] =
            ArenaPinInternalKey(ExtractUserKey(iter->key()), kMaxSequenceNumber,
                                static_cast<ValueType>(0), arena);
      } else if (!iter->status().ok()) {
        return iter->status();
      } else {
        range->point[1] = largest;
      }
      range->include[1] = true;
    } else {
      assert(range->include[1]);
    }
    if (new_ranges.empty()) {
      new_ranges.emplace_back(std::move(*range));
      continue;
    }
    auto last = &new_ranges.back();
    int c = uc->Compare(ExtractUserKey(range->point[0]),
                        ExtractUserKey(last->point[1]));
    if (GetInternalKeySeqno(last->point[1]) == kMaxSequenceNumber ? c >= 0
                                                                  : c > 0) {
      new_ranges.emplace_back(std::move(*range));
      continue;
    }
    RangeWithDepend split;
    split.point[0] = range->point[0];
    split.point[1] = last->point[1];
    assert(!range->include[0]);
    assert(last->include[1]);
    split.include[0] = false;
    split.include[1] = true;
    split.has_delete_range = last->has_delete_range || range->has_delete_range;
    split.stable = false;
    split.dependence = last->dependence;
    merge_dependence(split.dependence, range->dependence);

    auto is_same_dependence = [&](const RangeWithDepend& l,
                                  const RangeWithDepend& r) {
      if (l.dependence.size() == r.dependence.size()) {
        assert(std::mismatch(l.dependence.begin(), l.dependence.end(),
                             r.dependence.begin(),
                             [](const MapSstElement::LinkTarget& l,
                                const MapSstElement::LinkTarget& r) {
                               return l.file_number == r.file_number;
                             })
                   .first == l.dependence.end());
        return true;
      }
      return false;
    };
    if (is_same_dependence(*last, split) ||
        ic->Compare(last->point[0], split.point[0]) >= 0) {
      split.point[0] = last->point[0];
      assert(!last->include[0]);
      new_ranges.pop_back();
    } else {
      last->point[1] = split.point[0];
      last->stable = false;
      assert(last->include[1]);
    }
    if (is_same_dependence(split, *range) ||
        ic->Compare(split.point[1], range->point[1]) >= 0) {
      split.point[1] = range->point[1];
      assert(range->include[1]);
      new_ranges.emplace_back(std::move(split));
    } else {
      assert(GetInternalKeySeqno(split.point[1]) == kMaxSequenceNumber);
      range->point[0] = split.point[1];
      range->stable = false;
      assert(!split.include[0]);
      new_ranges.emplace_back(std::move(split));
      new_ranges.emplace_back(std::move(*range));
    }
  }
  new_ranges.swap(ranges);
  return Status::OK();
}

enum class PartitionType {
  kMerge,
  kDelete,
  kExtract,
};

// Partition two sorted non-overlap range vector
// a: [ -------- )      [ -------- ]
// b:       ( -------------- ]
// r: [ -- ]( -- )[ -- )[ -- ]( -- ]
std::vector<RangeWithDepend> PartitionRangeWithDepend(
    const std::vector<RangeWithDepend>& ranges_a,
    const std::vector<RangeWithDepend>& ranges_b,
    const InternalKeyComparator& icomp, PartitionType type) {
  std::vector<RangeWithDepend> output;
  assert(!ranges_a.empty() && !ranges_b.empty());
  const RangeWithDepend* source;
  auto put_left = [&](const Slice& key, bool include,
                      const RangeWithDepend* r) {
    assert(output.empty() || icomp.Compare(output.back().point[1], key) < 0 ||
           !output.back().include[1] || !include);
    output.emplace_back();
    auto& back = output.back();
    back.point[0] = key;
    back.include[0] = include;
    source = r;
  };
  auto put_right = [&](const Slice& key, bool include,
                       const RangeWithDepend* r) {
    auto& back = output.back();
    if (back.dependence.empty() || (icomp.Compare(key, back.point[0]) == 0 &&
                                    (!back.include[0] || !include))) {
      output.pop_back();
      return;
    }
    back.point[1] = key;
    back.include[1] = include;
    assert(icomp.Compare(back.point[0], back.point[1]) <= 0);
    if (source == nullptr || r == nullptr || source != r) {
      back.stable = false;
    }
  };
  auto put_depend = [&](const RangeWithDepend* a, const RangeWithDepend* b) {
    auto& dependence = output.back().dependence;
    auto& has_delete_range = output.back().has_delete_range;
    auto& stable = output.back().stable;
    assert(a != nullptr || b != nullptr);
    switch (type) {
      case PartitionType::kMerge:
        if (a != nullptr) {
          dependence = a->dependence;
          if (b != nullptr) {
            stable = false;
            dependence.insert(dependence.end(), b->dependence.begin(),
                              b->dependence.end());
            has_delete_range = a->has_delete_range || b->has_delete_range;
          } else {
            has_delete_range = a->has_delete_range;
            stable = a->stable;
          }
        } else {
          has_delete_range = b->has_delete_range;
          stable = b->stable;
          dependence = b->dependence;
        }
        break;
      case PartitionType::kDelete:
        if (b == nullptr) {
          has_delete_range = a->has_delete_range;
          stable = a->stable;
          dependence = a->dependence;
        } else {
          assert(b->dependence.empty());
        }
        break;
      case PartitionType::kExtract:
        if (a != nullptr && b != nullptr) {
          has_delete_range = a->has_delete_range;
          stable = a->stable;
          dependence = a->dependence;
          assert(b->dependence.empty());
        }
        break;
    }
  };
  size_t ai = 0, bi = 0;  // range index
  size_t ac, bc;          // changed
  size_t ab = 0, bb = 0;  // left bound or right bound
#define CASE(a, b, c, d) \
  (((a) ? 1 : 0) | ((b) ? 2 : 0) | ((c) ? 4 : 0) | ((d) ? 8 : 0))
  do {
    int c;
    if (ai < ranges_a.size() && bi < ranges_b.size()) {
      c = icomp.Compare(ranges_a[ai].point[ab], ranges_b[bi].point[bb]);
      c = CompInclude(c, ab, ranges_a[ai].include[ab], bb,
                      ranges_b[bi].include[bb]);
    } else {
      c = ai < ranges_a.size() ? -1 : 1;
    }
    ac = c <= 0;
    bc = c >= 0;
    switch (CASE(ab, bb, ac, bc)) {
      // out ranges_a , out ranges_b , enter ranges_a
      case CASE(0, 0, 1, 0):
        put_left(ranges_a[ai].point[ab], ranges_a[ai].include[ab],
                 &ranges_a[ai]);
        put_depend(&ranges_a[ai], nullptr);
        break;
      // in ranges_a , out ranges_b , leave ranges_a
      case CASE(1, 0, 1, 0):
        put_right(ranges_a[ai].point[ab], ranges_a[ai].include[ab],
                  &ranges_a[ai]);
        break;
      // out ranges_a , out ranges_b , enter ranges_b
      case CASE(0, 0, 0, 1):
        put_left(ranges_b[bi].point[bb], ranges_b[bi].include[bb],
                 &ranges_b[bi]);
        put_depend(nullptr, &ranges_b[bi]);
        break;
      // out ranges_a , in ranges_b , leave ranges_b
      case CASE(0, 1, 0, 1):
        put_right(ranges_b[bi].point[bb], ranges_b[bi].include[bb],
                  &ranges_b[bi]);
        break;
      // in ranges_a , out ranges_b , begin ranges_b
      case CASE(1, 0, 0, 1):
        put_right(ranges_b[bi].point[bb], !ranges_b[bi].include[bb], nullptr);
        put_left(ranges_b[bi].point[bb], ranges_b[bi].include[bb],
                 &ranges_b[bi]);
        put_depend(&ranges_a[ai], &ranges_b[bi]);
        break;
      // in ranges_a , in ranges_b , leave ranges_b
      case CASE(1, 1, 0, 1):
        put_right(ranges_b[bi].point[bb], ranges_b[bi].include[bb],
                  &ranges_b[bi]);
        put_left(ranges_b[bi].point[bb], !ranges_b[bi].include[bb], nullptr);
        put_depend(&ranges_a[ai], nullptr);
        break;
      // out ranges_a , in ranges_b , begin ranges_a
      case CASE(0, 1, 1, 0):
        put_right(ranges_a[ai].point[ab], !ranges_a[ai].include[ab], nullptr);
        put_left(ranges_a[ai].point[ab], ranges_a[ai].include[ab],
                 &ranges_a[ai]);
        put_depend(&ranges_a[ai], &ranges_b[bi]);
        break;
      // in ranges_a , in ranges_b , leave ranges_a
      case CASE(1, 1, 1, 0):
        put_right(ranges_a[ai].point[ab], ranges_a[ai].include[ab],
                  &ranges_a[ai]);
        put_left(ranges_a[ai].point[ab], !ranges_a[ai].include[ab], nullptr);
        put_depend(nullptr, &ranges_b[bi]);
        break;
      // out ranges_a , out ranges_b , enter ranges_a , enter ranges_b
      case CASE(0, 0, 1, 1):
        put_left(ranges_a[ai].point[ab], ranges_a[ai].include[ab], nullptr);
        put_depend(&ranges_a[ai], &ranges_b[bi]);
        break;
      // in ranges_a , in ranges_b , leave ranges_a , leave ranges_b
      case CASE(1, 1, 1, 1):
        put_right(ranges_a[ai].point[ab], ranges_a[ai].include[ab], nullptr);
        break;
      default:
        assert(false);
    }
    ai += (ab + ac) / 2;
    bi += (bb + bc) / 2;
    ab = (ab + ac) % 2;
    bb = (bb + bc) % 2;
  } while (ai != ranges_a.size() || bi != ranges_b.size());
#undef CASE
  return output;
}

Status LoadDeleteRangeIterImpl(
    const FileMetaData* f, const InternalKeyComparator& ic,
    IteratorCache& iterator_cache,
    std::vector<std::unique_ptr<TruncatedRangeDelIterator>>*
        range_del_iter_vec) {
  TableReader* reader;
  auto iter = iterator_cache.GetIterator(f, &reader);
  if (!iter->status().ok()) {
    return iter->status();
  }
  assert(reader != nullptr);
  std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
      reader->NewRangeTombstoneIterator(ReadOptions()));
  if (range_del_iter) {
    range_del_iter_vec->emplace_back(new TruncatedRangeDelIterator(
        std::move(range_del_iter), &ic, nullptr, nullptr));
  }
  return Status::OK();
};

// 把 f 的RangeTombstoneIterator创建出来并加入到range_del_iter_vec中，对map sst
// 则对其dependence进行递归加入
Status LoadDeleteRangeIter(
    const FileMetaData* file_meta, const InternalKeyComparator& ic,
    IteratorCache& iterator_cache,
    std::vector<std::unique_ptr<TruncatedRangeDelIterator>>*
        range_del_iter_vec) {
  if (file_meta->prop.is_map_sst() &&
      !file_meta->prop.map_handle_range_deletions()) {
    for (auto& dependence : file_meta->prop.dependence) {
      auto f = iterator_cache.GetFileMetaData(dependence.file_number);
      if (f == nullptr) {
        return Status::Aborted("Missing Dependence files");
      }
      if (f->prop.has_range_deletions()) {
        auto s = LoadDeleteRangeIter(f, ic, iterator_cache, range_del_iter_vec);
        if (!s.ok()) {
          return s;
        }
      }
    }
  } else if (file_meta->prop.has_range_deletions()) {
    auto s = LoadDeleteRangeIterImpl(file_meta, ic, iterator_cache,
                                     range_del_iter_vec);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
};

}  // namespace

MapBuilder::MapBuilder(int job_id, const ImmutableDBOptions& db_options,
                       const EnvOptions& env_options, VersionSet* versions,
                       Statistics* stats, const std::string& dbname)
    : job_id_(job_id),
      dbname_(dbname),
      db_options_(db_options),
      env_options_(env_options),
      env_(db_options.env),
      versions_(versions),
      stats_(stats) {}

Status MapBuilder::Build(const std::vector<CompactionInputFiles>& inputs,
                         const std::vector<Range>& deleted_range,
                         const std::vector<FileMetaData*>& added_files,
                         int output_level, uint32_t output_path_id,
                         ColumnFamilyData* cfd, Version* version,
                         VersionEdit* edit, FileMetaData* file_meta_ptr,
                         std::unique_ptr<TableProperties>* prop_ptr,
                         std::set<FileMetaData*>* deleted_files) {
  assert(output_level != 0 || inputs.front().level == 0);
  assert(!inputs.front().files.empty());
  auto vstorage = version->storage_info();
  auto& icomp = cfd->internal_comparator();
  IteratorCacheContext iterator_cache_ctx = {
      cfd, &version->GetMutableCFOptions(), version, &env_options_};
  IteratorCache iterator_cache(vstorage->dependence_map(), &iterator_cache_ctx,
                               IteratorCacheContext::CreateIter);
  Arena* arena = iterator_cache.GetArena();
  LazyInternalIteratorWrapper version_iter(
      IteratorCacheContext::CreateVersionIter, &iterator_cache_ctx, nullptr,
      nullptr, arena);

  std::list<std::vector<RangeWithDepend>> level_ranges;
  std::vector<std::unique_ptr<TruncatedRangeDelIterator>> range_del_iter_vec;
  MapSstElement map_element;
  FileMetaDataBoundBuilder bound_builder(&cfd->internal_comparator());
  std::vector<MapBuilderRangesItem::TombstonsItem> tombstones;
  Status s;
  size_t input_range_count = 0;

  // load input files into level_ranges
  for (auto& level_files : inputs) {
    if (level_files.files.empty()) {
      continue;
    }
    if (level_files.level == 0) {
      for (auto f : level_files.files) {
        s = LoadDeleteRangeIter(f, icomp, iterator_cache, &range_del_iter_vec);
        if (!s.ok()) {
          return s;
        }
        std::vector<RangeWithDepend> ranges;
        s = LoadRangeWithDepend(ranges, arena, &bound_builder, iterator_cache,
                                &f, 1);
        if (!s.ok()) {
          return s;
        }
        assert(std::is_sorted(ranges.begin(), ranges.end(),
                              TERARK_FIELD(point[1]) < icomp));
        input_range_count += ranges.size();
        level_ranges.emplace_back(std::move(ranges));
      }
    } else {
      for (auto f : level_files.files) {
        s = LoadDeleteRangeIter(f, icomp, iterator_cache, &range_del_iter_vec);
        if (!s.ok()) {
          return s;
        }
      }
      std::vector<RangeWithDepend> ranges;
      assert(std::is_sorted(level_files.files.begin(), level_files.files.end(),
                            TERARK_FIELD_P(largest) < icomp));
      s = LoadRangeWithDepend(ranges, arena, &bound_builder, iterator_cache,
                              level_files.files.data(),
                              level_files.files.size());
      if (!s.ok()) {
        return s;
      }
      assert(std::is_sorted(ranges.begin(), ranges.end(),
                            TERARK_FIELD(point[1]) < icomp));
      input_range_count += ranges.size();
      level_ranges.emplace_back(std::move(ranges));
    }
  }

  // merge ranges
  // TODO(zouzhizhang): multi way union
  while (level_ranges.size() > 1) {
    auto union_a = level_ranges.begin();
    auto union_b = std::next(union_a);
    size_t min_sum = union_a->size() + union_b->size();
    for (auto next = std::next(union_b); next != level_ranges.end();
         ++union_b, ++next) {
      size_t sum = union_b->size() + next->size();
      if (sum < min_sum) {
        min_sum = sum;
        union_a = union_b;
      }
    }
    union_b = std::next(union_a);
    level_ranges.insert(
        union_a,
        PartitionRangeWithDepend(*union_a, *union_b, cfd->internal_comparator(),
                                 PartitionType::kMerge));
    level_ranges.erase(union_a);
    level_ranges.erase(union_b);
  }

  if (!level_ranges.empty() && !deleted_range.empty()) {
    std::vector<RangeWithDepend> ranges;
    ranges.reserve(deleted_range.size());
    for (auto& r : deleted_range) {
      ranges.emplace_back(r, arena);
    }
    assert(std::is_sorted(ranges.begin(), ranges.end(),
                          TERARK_FIELD(point[1]) < icomp));
    level_ranges.front() = PartitionRangeWithDepend(
        level_ranges.front(), ranges, cfd->internal_comparator(),
        PartitionType::kDelete);
    if (level_ranges.front().empty()) {
      level_ranges.pop_front();
    }
  }
  if (!level_ranges.empty() && !range_del_iter_vec.empty()) {
    tombstones.emplace_back(std::shared_ptr<FragmentedRangeTombstoneList>(
        new FragmentedRangeTombstoneList(
            std::unique_ptr<InternalIteratorBase<Slice>>(
                NewTruncatedRangeDelMergingIter(&icomp, range_del_iter_vec)),
            icomp)));
    s = AdjustRange(&icomp, &version_iter, arena, bound_builder.largest,
                    level_ranges.front());
    if (!s.ok()) {
      return s;
    }
    tombstones.back().SetRanges(level_ranges.front());
  }
  if (!added_files.empty()) {
    std::vector<std::unique_ptr<TruncatedRangeDelIterator>>
        added_range_del_iter_vec;
    std::vector<RangeWithDepend> ranges;
    assert(std::is_sorted(added_files.begin(), added_files.end(),
                          TERARK_FIELD_P(largest) < icomp));
    for (auto f : added_files) {
      iterator_cache.PutFileMetaData(f);
    }
    s = LoadRangeWithDepend(ranges, arena, &bound_builder, iterator_cache,
                            added_files.data(), added_files.size());
    if (!s.ok()) {
      return s;
    }
    for (auto f : added_files) {
      s = LoadDeleteRangeIter(f, icomp, iterator_cache,
                              &added_range_del_iter_vec);
      if (!s.ok()) {
        return s;
      }
    }
    if (!added_range_del_iter_vec.empty()) {
      tombstones.emplace_back(std::shared_ptr<FragmentedRangeTombstoneList>(
          new FragmentedRangeTombstoneList(
              std::unique_ptr<InternalIteratorBase<Slice>>(
                  NewTruncatedRangeDelMergingIter(&icomp,
                                                  added_range_del_iter_vec)),
              icomp)));
      s = AdjustRange(&icomp, &version_iter, arena, added_files.back()->largest,
                      ranges);
      if (!s.ok()) {
        return s;
      }
      tombstones.back().SetRanges(ranges);
    }
    if (level_ranges.empty()) {
      level_ranges.emplace_back(std::move(ranges));
    } else {
      level_ranges.front() = PartitionRangeWithDepend(
          level_ranges.front(), ranges, cfd->internal_comparator(),
          PartitionType::kMerge);
    }
    for (auto& range_del_it : added_range_del_iter_vec) {
      range_del_iter_vec.emplace_back(std::move(range_del_it));
    }
  }
  bool build_range_deletion_ranges =
      cfd->ioptions()->compaction_style ==
          CompactionStyle::kCompactionStyleLevel &&
      cfd->ioptions()->enable_lazy_compaction &&
      output_level < vstorage->num_non_empty_levels() - 1;

  ScopedArenaIterator tombstone_iter;
  if (!tombstones.empty()) {
    MergeIteratorBuilder builder(&icomp, iterator_cache.GetArena());
    for (auto& item : tombstones) {
      builder.AddIterator(
          new (iterator_cache.GetArena()->AllocateAligned(
              sizeof(MapSstTombstoneIterator)))
              MapSstTombstoneIterator(item, cfd->internal_comparator()));
    }
    tombstone_iter.set(builder.Finish());
  }
  if (build_range_deletion_ranges && !tombstones.empty() &&
      !level_ranges.empty()) {
    std::vector<RangeWithDepend> ranges;
    auto uc = icomp.user_comparator();
    Slice last_end_key;
    for (tombstone_iter->SeekToFirst(); tombstone_iter->Valid();
         tombstone_iter->Next()) {
      auto start_key = ExtractUserKey(tombstone_iter->key());
      auto v = tombstone_iter->value();
      s = v.fetch();
      if (!s.ok()) {
        return s;
      }
      auto end_key = v.slice();

      if (!ranges.empty() && uc->Compare(start_key, last_end_key) <= 0) {
        if (uc->Compare(end_key, last_end_key) > 0) {
          ranges.back().point[1] = ArenaPinInternalKey(
              end_key, kMaxSequenceNumber, static_cast<ValueType>(0), arena);
        }
      } else {
        ranges.emplace_back();
        auto& r = ranges.back();
        r.point[0] = ArenaPinInternalKey(start_key, kMaxSequenceNumber,
                                         static_cast<ValueType>(0), arena);
        r.point[1] = ArenaPinInternalKey(end_key, kMaxSequenceNumber,
                                         static_cast<ValueType>(0), arena);
        r.include[0] = false;
        r.include[1] = true;
        r.has_delete_range = true;
        r.stable = false;
      }
      last_end_key = ExtractUserKey(ranges.back().point[1]);
    }
    // assert(!level_ranges.empty());
    level_ranges.front() = PartitionRangeWithDepend(
        level_ranges.front(), ranges, icomp, PartitionType::kMerge);
  }
  std::vector<RangeWithDepend> ranges;
  if (!level_ranges.empty()) {
    s = AdjustRange(&icomp, &version_iter, arena, bound_builder.largest,
                    level_ranges.front());
    if (!s.ok()) {
      return s;
    }
    ranges = std::move(level_ranges.front());
    level_ranges.clear();
  }
  auto edit_add_file = [edit](int level, const FileMetaData* f) {
    // don't call edit->AddFile(level, *f)
    // assert(!file_meta->table_reader_handle);
    edit->AddFile(level, f->fd.GetNumber(), f->fd.GetPathId(), f->fd.file_size,
                  f->smallest, f->largest, f->fd.smallest_seqno,
                  f->fd.largest_seqno, f->marked_for_compaction, f->prop);
  };
  auto edit_del_file = [edit, deleted_files](int level, FileMetaData* f) {
    edit->DeleteFile(level, f->fd.GetNumber());
    if (deleted_files != nullptr) {
      deleted_files->emplace(f);
    }
  };

  if (ranges.empty()) {
    for (auto& input_level : inputs) {
      for (auto f : input_level.files) {
        edit_del_file(input_level.level, f);
      }
    }
    return s;
  }

  // make sure level 0 files seqno no overlap
  if (output_level != 0 || ranges.size() == 1) {
    std::unordered_map<uint64_t, const FileMetaData*> sst_live;
    bool build_map_sst = false;
    // check is need build map
    for (auto& range : ranges) {
      if (range.dependence.size() > 1) {
        build_map_sst = true;
        break;
      }
      auto f =
          iterator_cache.GetFileMetaData(range.dependence.front().file_number);
      assert(f != nullptr);
      if (f->prop.is_map_sst()) {
        build_map_sst = true;
        break;
      }
      Range r(range.point[0], range.point[1], range.include[0],
              range.include[1]);
      if (!IsPerfectRange(r, f, icomp)) {
        build_map_sst = true;
        break;
      }
      sst_live.emplace(range.dependence.front().file_number, f);
    }
    if (!build_map_sst) {
      // unnecessary build map sst
      for (auto& input_level : inputs) {
        for (auto f : input_level.files) {
          uint64_t file_number = f->fd.GetNumber();
          if (sst_live.erase(file_number) > 0) {
            if (output_level != input_level.level) {
              edit_del_file(input_level.level, f);
              edit_add_file(output_level, f);
            }
          } else {
            edit_del_file(input_level.level, f);
          }
        }
      }
      for (auto& pair : sst_live) {
        auto f = pair.second;
        edit_add_file(output_level, f);
      }
      return s;
    }
  }
  if (inputs.size() == 1 && inputs.front().files.size() == 1 &&
      inputs.front().files.front()->prop.is_map_sst() &&
      ranges.size() == input_range_count &&
      !std::any_of(ranges.begin(), ranges.end(), !TERARK_GET(.stable))) {
    // all ranges stable, new map will equals to input map, done
    return s;
  }

  MapSstElementIterator output_iter(ranges, iterator_cache,
                                    cfd->internal_comparator());
  assert(std::is_sorted(ranges.begin(), ranges.end(),
                        TERARK_FIELD(point[1]) < icomp));
  FileMetaData file_meta;
  std::unique_ptr<TableProperties> prop;

  s = WriteOutputFile(bound_builder, &output_iter, tombstone_iter.get(),
                      output_path_id, cfd, version->GetMutableCFOptions(),
                      &file_meta, &prop);

  if (s.ok()) {
    for (auto& input_level : inputs) {
      for (auto f : input_level.files) {
        edit_del_file(input_level.level, f);
      }
    }
    for (auto f : added_files) {
      edit->AddFile(-1, *f);
      assert(f->table_reader_handle == nullptr);
    }
    edit->AddFile(output_level, file_meta);
    assert(file_meta.table_reader_handle == nullptr);
  }
  if (file_meta_ptr != nullptr) {
    *file_meta_ptr = std::move(file_meta);
  }
  if (prop_ptr != nullptr) {
    prop_ptr->swap(prop);
  }
  return s;
}  // namespace rocksdb

Status MapBuilder::Build(const std::vector<CompactionInputFiles>& inputs,
                         const std::vector<Range>& push_range, int output_level,
                         uint32_t output_path_id, ColumnFamilyData* cfd,
                         Version* version, VersionEdit* edit,
                         std::vector<MapBuilderOutput>* output) {
  assert(output_level > 0);
  auto vstorage = version->storage_info();
  auto& icomp = cfd->internal_comparator();
  IteratorCacheContext iterator_cache_ctx = {
      cfd, &version->GetMutableCFOptions(), version, &env_options_};
  IteratorCache iterator_cache(vstorage->dependence_map(), &iterator_cache_ctx,
                               IteratorCacheContext::CreateIter);
  Arena* arena = iterator_cache.GetArena();
  LazyInternalIteratorWrapper version_iter(
      IteratorCacheContext::CreateVersionIter, &iterator_cache_ctx, nullptr,
      nullptr, arena);

  std::vector<MapBuilderRangesItem> range_items;
  MapSstElement map_element;
  Status s;
  size_t output_index = size_t(-1);

  std::vector<RangeWithDepend> push_ranges;
  push_ranges.reserve(push_range.size());
  for (auto& r : push_range) {
    push_ranges.emplace_back(r, arena);
  }
  assert(std::is_sorted(push_ranges.begin(), push_ranges.end(),
                        TERARK_FIELD(point[1]) < icomp));
  // load input files into level_ranges
  for (auto& level_files : inputs) {
    if (level_files.files.empty()) {
      continue;
    }
    std::vector<std::unique_ptr<TruncatedRangeDelIterator>> range_del_iter_vec;
    assert(level_files.level <= output_level);
    FileMetaDataBoundBuilder bound_builder(&cfd->internal_comparator());
    if (level_files.level == 0) {
      for (auto f : level_files.files) {
        s = LoadDeleteRangeIter(f, icomp, iterator_cache, &range_del_iter_vec);
        if (!s.ok()) {
          return s;
        }
        std::vector<RangeWithDepend> ranges;
        s = LoadRangeWithDepend(ranges, arena, &bound_builder, iterator_cache,
                                &f, 1);
        if (!s.ok()) {
          return s;
        }
        assert(std::is_sorted(ranges.begin(), ranges.end(),
                              TERARK_FIELD(point[1]) < icomp));
        if (range_items.empty()) {
          range_items.emplace_back(FileMetaDataBoundBuilder(nullptr), 0,
                                   f->prop.is_map_sst(), std::move(ranges));
        } else {
          assert(range_items.front().level == 0);
          auto& level_ranges = range_items.front();
          level_ranges.is_map = false;
          level_ranges.ranges = PartitionRangeWithDepend(
              level_ranges.ranges, ranges, cfd->internal_comparator(),
              PartitionType::kMerge);
          level_ranges.input_range_count = size_t(-1);
        }
      }
      range_items.front().bound_builder = std::move(bound_builder);
    } else {
      for (auto f : level_files.files) {
        s = LoadDeleteRangeIter(f, icomp, iterator_cache, &range_del_iter_vec);
        if (!s.ok()) {
          return s;
        }
      }
      std::vector<RangeWithDepend> ranges;
      if (level_files.level == output_level) {
        output_index = range_items.size();
      }
      s = LoadRangeWithDepend(ranges, arena, &bound_builder, iterator_cache,
                              level_files.files.data(), level_files.size());
      if (!s.ok()) {
        return s;
      }
      bool is_map = level_files.size() == 1 &&
                    level_files.files.front()->prop.is_map_sst();
      range_items.emplace_back(std::move(bound_builder), level_files.level,
                               is_map, std::move(ranges));
    }
    auto& back = range_items.back();
    back.inputs = &level_files.files;
    if (!range_del_iter_vec.empty()) {
      back.self_tombstone_index = back.tombstones.size();
      back.tombstones.emplace_back(
          std::shared_ptr<FragmentedRangeTombstoneList>(
              new FragmentedRangeTombstoneList(
                  std::unique_ptr<InternalIteratorBase<Slice>>(
                      NewTruncatedRangeDelMergingIter(&icomp,
                                                      range_del_iter_vec)),
                  icomp)));
    }
  }
  if (output_index == size_t(-1)) {
    output_index = range_items.size();
    range_items.emplace_back(
        FileMetaDataBoundBuilder(&cfd->internal_comparator()), output_level,
        false, std::vector<RangeWithDepend>());
  }

  // process levels
  for (auto rit = range_items.rbegin(); rit != range_items.rend(); ++rit) {
    auto& level_ranges = *rit;
    if (level_ranges.level == output_level) {
      continue;
    }
    std::vector<RangeWithDepend> extract = PartitionRangeWithDepend(
        level_ranges.ranges, push_ranges, cfd->internal_comparator(),
        PartitionType::kExtract);
    if (!extract.empty()) {
      level_ranges.ranges = PartitionRangeWithDepend(
          level_ranges.ranges, push_ranges, cfd->internal_comparator(),
          PartitionType::kDelete);
      auto& bound_builder = range_items[output_index].bound_builder;
      for (auto& r : extract) {
        for (auto& dependence : r.dependence) {
          const FileMetaData* f =
              iterator_cache.GetFileMetaData(dependence.file_number);
          if (f == nullptr) {
            // TODO log error
            return Status::Corruption("MapBuilder::Build missing FileMetaData");
          }
          bound_builder.Update(f);
        }
      }
      auto& output_range_items = range_items[output_index];
      if (!level_ranges.tombstones.empty()) {
        s = AdjustRange(&icomp, &version_iter, arena,
                        level_ranges.bound_builder.largest, extract);
        if (!s.ok()) {
          return s;
        }
        assert(level_ranges.tombstones.size() == 1);
        output_range_items.tombstones.emplace_back(
            level_ranges.tombstones.front().tombstones);
        output_range_items.tombstones.back().SetRanges(extract);
      }
      output_range_items.ranges =
          output_range_items.ranges.empty()
              ? std::move(extract)
              : PartitionRangeWithDepend(extract, output_range_items.ranges,
                                         cfd->internal_comparator(),
                                         PartitionType::kMerge);
    }
  }

  auto edit_add_file = [edit](int level, const FileMetaData* f) {
    // don't call edit->AddFile(level, *f)
    // assert(!file_meta->table_reader_handle);
    edit->AddFile(level, f->fd.GetNumber(), f->fd.GetPathId(), f->fd.file_size,
                  f->smallest, f->largest, f->fd.smallest_seqno,
                  f->fd.largest_seqno, f->marked_for_compaction, f->prop);
  };
  auto edit_del_file = [edit](int level, FileMetaData* f) {
    edit->DeleteFile(level, f->fd.GetNumber());
  };

  for (auto& level_ranges : range_items) {
    s = AdjustRange(&icomp, &version_iter, arena,
                    level_ranges.bound_builder.largest, level_ranges.ranges);
    if (!s.ok()) {
      return s;
    }
    if (level_ranges.ranges.empty()) {
      if (level_ranges.inputs != nullptr) {
        for (auto f : *level_ranges.inputs) {
          edit->DeleteFile(level_ranges.level, f->fd.GetNumber());
        }
      }
      continue;
    }
    if (level_ranges.self_tombstone_index != size_t(-1)) {
      level_ranges.tombstones[level_ranges.self_tombstone_index].SetRanges(
          level_ranges.ranges);
    }
    // make sure level 0 files seqno no overlap
    if (level_ranges.level != 0 || level_ranges.ranges.size() == 1) {
      std::unordered_map<uint64_t, const FileMetaData*> sst_live;
      bool build_map_sst = false;
      // check is need build map
      for (auto& range : level_ranges.ranges) {
        if (range.dependence.size() > 1) {
          build_map_sst = true;
          break;
        }
        auto f = iterator_cache.GetFileMetaData(
            range.dependence.front().file_number);
        assert(f != nullptr);
        if (f->prop.is_map_sst()) {
          build_map_sst = true;
          break;
        }
        Range r(range.point[0], range.point[1], range.include[0],
                range.include[1]);
        if (!IsPerfectRange(r, f, icomp)) {
          build_map_sst = true;
          break;
        }
        sst_live.emplace(range.dependence.front().file_number, f);
      }
      if (!build_map_sst) {
        // unnecessary build map sst
        if (level_ranges.inputs != nullptr) {
          for (auto f : *level_ranges.inputs) {
            uint64_t file_number = f->fd.GetNumber();
            if (sst_live.erase(file_number) == 0) {
              edit_del_file(level_ranges.level, f);
            }
          }
        }
        for (auto& pair : sst_live) {
          auto f = pair.second;
          edit_add_file(level_ranges.level, f);
        }
        continue;
      }
    }
    if (level_ranges.is_stable()) {
      // all ranges stable, new map will equals to input map, done
      continue;
    }

    MapSstElementIterator output_iter(level_ranges.ranges, iterator_cache,
                                      cfd->internal_comparator());
    ScopedArenaIterator tombstone_iter;
    if (!level_ranges.tombstones.empty()) {
      MergeIteratorBuilder builder(&icomp, iterator_cache.GetArena());
      for (auto& item : level_ranges.tombstones) {
        builder.AddIterator(
            new (iterator_cache.GetArena()->AllocateAligned(
                sizeof(MapSstTombstoneIterator)))
                MapSstTombstoneIterator(item, cfd->internal_comparator()));
      }
      tombstone_iter.set(builder.Finish());
    }

    assert(std::is_sorted(level_ranges.ranges.begin(),
                          level_ranges.ranges.end(),
                          TERARK_FIELD(point[1]) < icomp));
    MapBuilderOutput output_item;
    s = WriteOutputFile(level_ranges.bound_builder, &output_iter,
                        tombstone_iter.get(), output_path_id, cfd,
                        version->GetMutableCFOptions(), &output_item.file_meta,
                        &output_item.prop);

    if (!s.ok()) {
      return s;
    }
    if (level_ranges.inputs != nullptr) {
      for (auto f : *level_ranges.inputs) {
        edit_del_file(level_ranges.level, f);
      }
    }
    edit->AddFile(level_ranges.level, output_item.file_meta);
    assert(output_item.file_meta.table_reader_handle == nullptr);

    if (output != nullptr) {
      output_item.level = level_ranges.level;
      output->emplace_back(std::move(output_item));
    }
  }
  return s;
}

Status MapBuilder::WriteOutputFile(
    const FileMetaDataBoundBuilder& bound_builder,
    MapSstRangeIterator* range_iter, InternalIterator* tombstone_iter,
    uint32_t output_path_id, ColumnFamilyData* cfd,
    const MutableCFOptions& mutable_cf_options, FileMetaData* file_meta,
    std::unique_ptr<TableProperties>* prop) {
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>> collectors;

  // no need to lock because VersionSet::next_file_number_ is atomic
  uint64_t file_number = versions_->NewFileNumber();
  std::string fname =
      TableFileName(cfd->ioptions()->cf_paths, file_number, output_path_id);
  // Fire events.
#ifndef ROCKSDB_LITE
  EventHelpers::NotifyTableFileCreationStarted(
      cfd->ioptions()->listeners, dbname_, cfd->GetName(), fname, 0,
      TableFileCreationReason::kCompaction);
#endif  // !ROCKSDB_LITE

  // Make the output file
  std::unique_ptr<WritableFile> writable_file;
  auto s = NewWritableFile(env_, fname, &writable_file, env_options_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "[%s] [JOB %d] BuildMapSst for table #%" PRIu64
                    " fails at NewWritableFile with status %s",
                    cfd->GetName().c_str(), job_id_, file_number,
                    s.ToString().c_str());
    LogFlush(db_options_.info_log);
    EventHelpers::LogAndNotifyTableFileCreationFinished(
        nullptr, cfd->ioptions()->listeners, dbname_, cfd->GetName(), fname, -1,
        FileDescriptor(), TableProperties(),
        TableFileCreationReason::kCompaction, s);
    return s;
  }

  file_meta->fd = FileDescriptor(file_number, output_path_id, 0);

  writable_file->SetIOPriority(Env::IO_LOW);
  writable_file->SetWriteLifeTimeHint(Env::WLTH_SHORT);
  // map sst always small
  writable_file->SetPreallocationBlockSize(4ULL << 20);
  std::unique_ptr<WritableFileWriter> outfile(new WritableFileWriter(
      std::move(writable_file), fname, env_options_, stats_));

  uint64_t output_file_creation_time;
  {
    int64_t _current_time = 0;
    auto status = env_->GetCurrentTime(&_current_time);
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

  // map sst don't need compression or filters
  std::unique_ptr<TableBuilder> builder(NewTableBuilder(
      *cfd->ioptions(), mutable_cf_options, cfd->internal_comparator(),
      &collectors, cfd->GetID(), cfd->GetName(), outfile.get(), kNoCompression,
      CompressionOptions(), -1 /* level */, 0 /* compaction_load */,
      nullptr /* compression_dict */, true /* skip_filters */,
      output_file_creation_time, 0 /* oldest_key_time */, kMapSst));
  LogFlush(db_options_.info_log);

  // Update boundaries
  file_meta->smallest = bound_builder.smallest;
  file_meta->largest = bound_builder.largest;
  file_meta->fd.smallest_seqno = bound_builder.smallest_seqno;
  file_meta->fd.largest_seqno = bound_builder.largest_seqno;

  for (range_iter->SeekToFirst(); s.ok() && range_iter->Valid();
       range_iter->Next()) {
    s = builder->Add(range_iter->key(), range_iter->value());
  }
  if (s.ok() && !range_iter->status().ok()) {
    s = range_iter->status();
  }
  bool has_range_deletions = false;
  if (s.ok() && tombstone_iter != nullptr) {
    for (tombstone_iter->SeekToFirst(); s.ok() && tombstone_iter->Valid();
         tombstone_iter->Next()) {
      s = builder->AddTombstone(tombstone_iter->key(), tombstone_iter->value());
      has_range_deletions = true;
    }
    if (s.ok() && !tombstone_iter->status().ok()) {
      s = tombstone_iter->status();
    }
  }

  // Prepare prop
  file_meta->prop.num_entries = builder->NumEntries();
  file_meta->prop.purpose = kMapSst;
  file_meta->prop.flags |= TablePropertyCache::kMapHandleRangeDeletions;
  file_meta->prop.flags |=
      has_range_deletions ? 0 : TablePropertyCache::kNoRangeDeletions;
  std::tie(file_meta->prop.max_read_amp, file_meta->prop.read_amp) =
      range_iter->GetSstReadAmp();
  auto& dependence_build = range_iter->GetDependence();
  auto& dependence = file_meta->prop.dependence;
  dependence.reserve(dependence_build.size());
  for (auto& pair : dependence_build) {
    dependence.emplace_back(Dependence{pair.first, pair.second});
  }
  std::sort(dependence.begin(), dependence.end(), TERARK_CMP(file_number, <));

  // Map sst don't write tombstones
  if (s.ok()) {
    s = builder->Finish(&file_meta->prop, nullptr);
  } else {
    builder->Abandon();
  }
  file_meta->marked_for_compaction = builder->NeedCompact();
  const uint64_t current_entries = builder->NumEntries();
  const uint64_t current_bytes = builder->FileSize();
  if (s.ok()) {
    file_meta->fd.file_size = current_bytes;
  }
  // Finish and check for file errors
  if (s.ok()) {
    StopWatch sw(env_, stats_, COMPACTION_OUTFILE_SYNC_MICROS);
    s = outfile->Sync(db_options_.use_fsync);
  }
  if (s.ok()) {
    s = outfile->Close();
  }
  outfile.reset();

  if (s.ok()) {
    prop->reset(new TableProperties(builder->GetTableProperties()));
    file_meta->prop.raw_key_size = (*prop)->raw_key_size;
    file_meta->prop.raw_value_size = (*prop)->raw_value_size;
    // Output to event logger and fire events.
    const char* compaction_msg =
        file_meta->marked_for_compaction ? " (need compaction)" : "";
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Generated map table #%" PRIu64 ": %" PRIu64
                   " keys, %" PRIu64 " bytes%s",
                   cfd->GetName().c_str(), job_id_, file_number,
                   current_entries, current_bytes, compaction_msg);
  }
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      nullptr, cfd->ioptions()->listeners, dbname_, cfd->GetName(), fname, -1,
      file_meta->fd, *prop ? **prop : TableProperties(),
      TableFileCreationReason::kCompaction, s);

#ifndef ROCKSDB_LITE
  // Report new file to SstFileManagerImpl
  auto sfm =
      static_cast<SstFileManagerImpl*>(db_options_.sst_file_manager.get());
  if (sfm && file_meta->fd.GetPathId() == 0) {
    sfm->OnAddFile(fname);
    if (sfm->IsMaxAllowedSpaceReached()) {
      // TODO(ajkr): should we return OK() if max space was reached by the final
      // compaction output file (similarly to how flush works when full)?
      s = Status::SpaceLimit("Max allowed space was reached");
    }
  }
#endif

  builder.reset();
  return s;
}

struct MapElementIterator : public InternalIterator {
  MapElementIterator(const FileMetaData* const* meta_array, size_t meta_size,
                     const InternalKeyComparator* icmp, void* callback_arg,
                     const IteratorCache::CreateIterCallback& create_iter)
      : meta_array_(meta_array),
        meta_size_(meta_size),
        icmp_(icmp),
        callback_arg_(callback_arg),
        create_iter_(create_iter),
        where_(meta_size) {
    assert(meta_size > 0);
  }
  ~MapElementIterator() { ResetIter(); }
  virtual bool Valid() const override { return where_ < meta_size_; }
  virtual void Seek(const Slice& target) override {
    where_ =
        std::lower_bound(meta_array_, meta_array_ + meta_size_, target,
                         [this](const FileMetaData* f, const Slice& t) {
                           return icmp_->Compare(f->largest.Encode(), t) < 0;
                         }) -
        meta_array_;
    if (where_ == meta_size_) {
      ResetIter();
      return;
    }
    if (meta_array_[where_]->prop.is_map_sst()) {
      if (!InitIter()) {
        return;
      }
      iter_->Seek(target);
      if (!iter_->Valid()) {
        ResetIter();
        if (++where_ == meta_size_) {
          return;
        }
        if (meta_array_[where_]->prop.is_map_sst()) {
          if (!InitIter()) {
            return;
          }
          iter_->SeekToFirst();
        }
      }
    } else {
      ResetIter();
    }
    Update();
  }
  virtual void SeekForPrev(const Slice& target) override {
    where_ =
        std::upper_bound(meta_array_, meta_array_ + meta_size_, target,
                         [this](const Slice& t, const FileMetaData* f) {
                           return icmp_->Compare(t, f->largest.Encode()) < 0;
                         }) -
        meta_array_;
    if (where_-- == 0) {
      where_ = meta_size_;
      ResetIter();
      return;
    }
    if (meta_array_[where_]->prop.is_map_sst()) {
      if (!InitIter()) {
        return;
      }
      iter_->SeekForPrev(target);
      if (!iter_->Valid()) {
        ResetIter();
        if (where_-- == 0) {
          where_ = meta_size_;
          return;
        }
        if (meta_array_[where_]->prop.is_map_sst()) {
          if (!InitIter()) {
            return;
          }
          iter_->SeekToLast();
        }
      }
    } else {
      ResetIter();
    }
    Update();
  }
  virtual void SeekToFirst() override {
    where_ = 0;
    if (meta_array_[where_]->prop.is_map_sst()) {
      if (!InitIter()) {
        return;
      }
      iter_->SeekToFirst();
    } else {
      ResetIter();
    }
    Update();
  }
  virtual void SeekToLast() override {
    where_ = meta_size_ - 1;
    if (meta_array_[where_]->prop.is_map_sst()) {
      if (!InitIter()) {
        return;
      }
      iter_->SeekToLast();
    } else {
      ResetIter();
    }
    Update();
  }
  virtual void Next() override {
    if (iter_) {
      assert(iter_->Valid());
      value_.reset();
      iter_->Next();
      if (iter_->Valid()) {
        Update();
        return;
      }
    }
    if (++where_ == meta_size_) {
      ResetIter();
      return;
    }
    if (meta_array_[where_]->prop.is_map_sst()) {
      if (!InitIter()) {
        return;
      }
      iter_->SeekToFirst();
    } else {
      ResetIter();
    }
    Update();
  }
  virtual void Prev() override {
    if (iter_) {
      assert(iter_->Valid());
      value_.reset();
      iter_->Prev();
      if (iter_->Valid()) {
        Update();
        return;
      }
    }
    if (where_-- == 0) {
      where_ = meta_size_;
      ResetIter();
      return;
    }
    if (meta_array_[where_]->prop.is_map_sst()) {
      if (!InitIter()) {
        return;
      }
      iter_->SeekToLast();
    } else {
      ResetIter();
    }
    Update();
  }
  Slice key() const override {
    assert(where_ < meta_size_);
    return key_;
  }
  LazyBuffer value() const override {
    assert(where_ < meta_size_);
    return LazyBufferReference(value_);
  }
  virtual Status status() const override {
    return iter_ ? iter_->status() : Status::OK();
  }

  bool InitIter() {
    DependenceMap empty_dependence_map;
    ResetIter(create_iter_(callback_arg_, meta_array_[where_],
                           empty_dependence_map, nullptr, nullptr));
    if (iter_->status().ok()) {
      return true;
    }
    where_ = meta_size_;
    return false;
  }
  void ResetIter(InternalIterator* iter = nullptr) {
    value_.reset();
    iter_.reset(iter);
  }
  void Update() {
    if (iter_) {
      key_ = iter_->key();
      value_ = iter_->value();
    } else {
      const FileMetaData* f = meta_array_[where_];
      element_.smallest_key = f->smallest.Encode();
      element_.largest_key = f->largest.Encode();
      element_.include_smallest = true;
      element_.include_largest = true;
      element_.has_delete_range = false;  // for pick_range_deletion
      element_.link.clear();
      element_.link.emplace_back(
          MapSstElement::LinkTarget{f->fd.GetNumber(), f->fd.GetFileSize()});
      key_ = element_.Key();
      value_.reset(element_.Value(&buffer_));
    }
  }

  const FileMetaData* const* meta_array_;
  size_t meta_size_;
  const InternalKeyComparator* icmp_;
  void* callback_arg_;
  IteratorCache::CreateIterCallback create_iter_;
  size_t where_;
  MapSstElement element_;
  std::string buffer_;
  std::unique_ptr<InternalIterator> iter_;
  Slice key_;
  LazyBuffer value_;
};

InternalIterator* NewMapElementIterator(
    const FileMetaData* const* meta_array, size_t meta_size,
    const InternalKeyComparator* icmp, void* callback_arg,
    const IteratorCache::CreateIterCallback& create_iter, Arena* arena) {
  if (meta_size == 0) {
    return NewEmptyInternalIterator(arena);
  } else if (meta_size == 1 && meta_array[0]->prop.is_map_sst()) {
    DependenceMap empty_dependence_map;
    return create_iter(callback_arg, meta_array[0], empty_dependence_map, arena,
                       nullptr);
  } else if (arena == nullptr) {
    return new MapElementIterator(meta_array, meta_size, icmp, callback_arg,
                                  create_iter);
  } else {
    return new (arena->AllocateAligned(sizeof(MapElementIterator)))
        MapElementIterator(meta_array, meta_size, icmp, callback_arg,
                           create_iter);
  }
}

#if 0
struct PartitionRangeTest {
  PartitionRangeTest() {
    Arena arena;
    std::vector<RangeWithDepend> a, b;
    auto push = [&arena](std::vector<RangeWithDepend>& v, const char* l,
                         const char* r, bool stable,
                         std::initializer_list<uint64_t> fn_list = {}) {
      v.emplace_back(Range(l, r, true, false), &arena);
      auto& back = v.back();
      for (auto fn : fn_list) {
        back.dependence.emplace_back(MapSstElement::LinkTarget{fn, 0});
      }
      back.stable = stable;
    };
    push(a, "00", "10", true, {1});
    push(b, "00", "05", false);
    InternalKeyComparator icmp(BytewiseComparator());
    auto c = PartitionRangeWithDepend(a, b, icmp, PartitionType::kDelete);
    printf("");
  }
};

static PartitionRangeTest init_test;
#endif

}  // namespace rocksdb
