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
#include "monitoring/thread_status_util.h"
#include "table/merging_iterator.h"
#include "table/two_level_iterator.h"
#include "util/c_style_callback.h"
#include "util/iterator_cache.h"
#include "util/sst_file_manager_impl.h"

namespace rocksdb {

struct FileMetaDataBoundBuilder {
  const InternalKeyComparator* icomp;
  InternalKey smallest, largest;
  SequenceNumber smallest_seqno;
  SequenceNumber largest_seqno;
  uint64_t creation_time;

  FileMetaDataBoundBuilder(const InternalKeyComparator* _icomp)
      : icomp(_icomp),
        smallest_seqno(kMaxSequenceNumber),
        largest_seqno(0),
        creation_time(0) {}

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
  return range.include_start && range.include_limit &&
         f->prop.purpose == kEssenceSst &&
         icomp.Compare(range.start, f->smallest.Encode()) == 0 &&
         icomp.Compare(range.limit, f->largest.Encode()) == 0;
}

namespace {

struct IteratorCacheContext {
  static InternalIterator* invoke(void* arg, const FileMetaData* f,
                                  const DependenceMap& dependence_map,
                                  Arena* arena, TableReader** reader_ptr) {
    IteratorCacheContext* ctx = static_cast<IteratorCacheContext*>(arg);
    DependenceMap empty_dependence_map;
    ReadOptions read_options;
    read_options.verify_checksums = true;
    read_options.fill_cache = false;
    read_options.total_order_seek = true;

    return ctx->cfd->table_cache()->NewIterator(
        read_options, *ctx->env_options, ctx->cfd->internal_comparator(), *f,
        f->prop.purpose == kMapSst ? empty_dependence_map : dependence_map,
        nullptr, ctx->mutable_cf_options->prefix_extractor.get(), reader_ptr,
        nullptr /* no per level latency histogram */,
        false /* for_compaction */, arena, false /* skip_filters */, -1);
  }

  ColumnFamilyData* cfd;
  const MutableCFOptions* mutable_cf_options;
  const EnvOptions* env_options;
};

struct RangeWithDepend {
  InternalKey point[2];
  bool include[2];
  bool no_records;
  bool has_delete_range;
  bool stable;
  std::vector<MapSstElement::LinkTarget> dependence;

  RangeWithDepend() = default;

  RangeWithDepend(const FileMetaData* f) {
    assert(GetInternalKeySeqno(f->smallest.Encode()) != kMaxSequenceNumber);
    point[0] = f->smallest;
    if (GetInternalKeySeqno(f->largest.Encode()) == kMaxSequenceNumber) {
      point[1].Set(f->largest.user_key(), kMaxSequenceNumber, kTypeDeletion);
    } else {
      point[1] = f->largest;
    }
    include[0] = true;
    include[1] = true;
    no_records = false;
    has_delete_range = false;
    stable = false;
    dependence.emplace_back(MapSstElement::LinkTarget{f->fd.GetNumber(), 0});
  }

  RangeWithDepend(const MapSstElement& map_element) {
    point[0].DecodeFrom(map_element.smallest_key);
    point[1].DecodeFrom(map_element.largest_key);
    include[0] = map_element.include_smallest;
    include[1] = map_element.include_largest;
    no_records = map_element.no_records;
    has_delete_range = map_element.has_delete_range;
    stable = true;
    dependence = map_element.link;
  }
  RangeWithDepend(const Range& range) {
    point[0].DecodeFrom(range.start);
    point[1].DecodeFrom(range.limit);
    include[0] = range.include_start;
    include[1] = range.include_limit;
    no_records = false;
    has_delete_range = false;
    stable = false;
  }
};

bool IsEmptyMapSstElement(const RangeWithDepend& range,
                          const InternalKeyComparator& icomp) {
  if (range.dependence.size() != 1) {
    return false;
  }
  if (icomp.user_comparator()->Compare(range.point[0].user_key(),
                                       range.point[1].user_key()) != 0) {
    return false;
  }
  ParsedInternalKey pikey;
  if (!ParseInternalKey(range.point[1].Encode(), &pikey)) {
    // TODO log error
    return false;
  }
  return pikey.sequence == kMaxSequenceNumber;
}

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
}  // namespace

class MapSstElementIterator {
 public:
  MapSstElementIterator(const std::vector<RangeWithDepend>& ranges,
                        IteratorCache& iterator_cache,
                        const InternalKeyComparator& icomp)
      : ranges_(ranges), iterator_cache_(iterator_cache), icomp_(icomp) {}
  bool Valid() const { return !buffer_.empty(); }
  void SeekToFirst() {
    where_ = ranges_.begin();
    PrepareNext();
  }
  void Next() { PrepareNext(); }
  Slice key() const { return map_elements_.Key(); }
  Slice value() const { return buffer_; }
  Status status() const { return status_; }

  const std::unordered_map<uint64_t, uint64_t>& GetDependence() const {
    return dependence_build_;
  }

  std::pair<size_t, double> GetSstReadAmp() const {
    return {sst_read_amp_, sst_read_amp_ratio_};
  }

  bool HasDeleteRange() const { return has_delete_range_; }

 private:
  void CheckIter(InternalIterator* iter) {
    assert(!iter->Valid());
    if (status_.ok()) {
      status_ = iter->status();
    }
  }
  void PrepareNext() {
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
    auto& start = map_elements_.smallest_key = where_->point[0].Encode();
    auto& end = map_elements_.largest_key = where_->point[1].Encode();
    assert(icomp_.Compare(start, end) <= 0);
    map_elements_.include_smallest = where_->include[0];
    map_elements_.include_largest = where_->include[1];
    bool& no_records = map_elements_.no_records = where_->no_records;
    bool& has_delete_range = map_elements_.has_delete_range =
        where_->has_delete_range;
    bool stable = where_->stable;
    map_elements_.link = where_->dependence;
    assert(map_elements_.include_smallest);
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
      for (auto& link : map_elements_.link) {
        put_dependence(link.file_number, link.size);
        range_size += link.size;
      }
    } else {
      no_records = true;
      for (auto& link : map_elements_.link) {
        link.size = 0;
        TableReader* reader;
        auto iter = iterator_cache_.GetIterator(link.file_number, &reader);
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
            no_records = false;
          }
        } while (false);
        if (!status_.ok()) {
          buffer_.clear();
          return;
        }
        put_dependence(link.file_number, link.size);
      }
    }
    sst_read_amp_ = std::max(sst_read_amp_, map_elements_.link.size());
    sst_read_amp_ratio_ += map_elements_.link.size() * range_size;
    sst_read_amp_size_ += range_size;
    has_delete_range_ |= has_delete_range;
    map_elements_.Value(&buffer_);  // Encode value
  }

 private:
  Status status_;
  MapSstElement map_elements_;
  InternalKey temp_start_, temp_end_;
  std::string buffer_;
  std::vector<RangeWithDepend>::const_iterator where_;
  const std::vector<RangeWithDepend>& ranges_;
  std::unordered_map<uint64_t, uint64_t> dependence_build_;
  bool has_delete_range_ = false;
  size_t sst_read_amp_ = 0;
  double sst_read_amp_ratio_ = 0;
  size_t sst_read_amp_size_ = 0;
  IteratorCache& iterator_cache_;
  const InternalKeyComparator& icomp_;
};

namespace {

Status AppendUserKeyRange(std::vector<RangeStorage>& ranges,
                          IteratorCache& iterator_cache,
                          const FileMetaData* const* file_meta, size_t n) {
  MapSstElement map_element;
  for (size_t i = 0; i < n; ++i) {
    auto f = file_meta[i];
    if (f->prop.purpose == kMapSst) {
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
          return Status::Corruption("LoadRange: Map sst invalid key or value");
        }
        ranges.emplace_back(ExtractUserKey(map_element.smallest_key),
                            ExtractUserKey(map_element.largest_key), true,
                            true);
      }
    } else {
      ranges.emplace_back(f->smallest.user_key(), f->largest.user_key(), true,
                          true);
    }
  }
  return Status::OK();
}

Status LoadRangeWithDepend(std::vector<RangeWithDepend>& ranges,
                           FileMetaDataBoundBuilder* bound_builder,
                           IteratorCache& iterator_cache,
                           const FileMetaData* const* file_meta, size_t n) {
  MapSstElement map_element;
  for (size_t i = 0; i < n; ++i) {
    auto f = file_meta[i];
    TableReader* reader;
    if (f->prop.purpose == kMapSst) {
      auto iter = iterator_cache.GetIterator(f, &reader);
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
        ranges.emplace_back(map_element);
      }
    } else {
      auto iter = iterator_cache.GetIterator(f, &reader);
      assert(iter != nullptr);
      if (!iter->status().ok()) {
        return iter->status();
      }
      ranges.emplace_back(f);
    }
    if (bound_builder != nullptr) {
      bound_builder->Update(f);
      bound_builder->creation_time =
          std::max(bound_builder->creation_time,
                   reader->GetTableProperties()->creation_time);
    }
  }
  return Status::OK();
}

Status AdjustRange(ColumnFamilyData* cfd, Version* version,
                   const EnvOptions& env_options,
                   const std::vector<RangeWithDepend>& ranges,
                   std::vector<RangeWithDepend>& new_ranges) {
  if (ranges.empty()) {
    return Status::OK();
  }
  Arena arena;
  ScopedArenaIterator iterator;
  auto get_iter = [&] {
    if (iterator.get() == nullptr) {
      ReadOptions read_options;
      read_options.verify_checksums = true;
      read_options.fill_cache = false;
      read_options.total_order_seek = true;
      MergeIteratorBuilder builder(&cfd->internal_comparator(), &arena);
      version->AddIterators(read_options, env_options, &builder, nullptr);
      iterator.set(builder.Finish());
    }
    return iterator.get();
  };
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
  auto ic = &cfd->internal_comparator();
  auto uc = ic->user_comparator();
  InternalKey ik;
  enum { kSetMin, kSetMax };
  auto set_ik = [&ik](const InternalKey& k, int min_or_max) {
    ik.Clear();
    if (min_or_max == kSetMin) {
      ik.SetMinPossibleForUserKey(k.user_key());
    } else {
      ik.SetMaxPossibleForUserKey(k.user_key());
    }
    return ik.Encode();
  };
  for (auto range : ranges) {
    if (!range.include[0]) {
      auto iter = get_iter();
      iter->Seek(range.point[0].Encode());
      if (iter->Valid() &&
          ic->Compare(iter->key(), range.point[0].Encode()) == 0) {
        iter->Next();
      }
      if (!iter->Valid()) {
        if (!iter->status().ok()) {
          return iter->status();
        }
        continue;
      }
      assert(ic->Compare(iter->key(), range.point[0].Encode()) > 0);
      if (ic->Compare(iter->key(), range.point[1].Encode()) > 0) {
        continue;
      }
      range.point[0].DecodeFrom(iter->key());
      range.include[0] = true;
    }
    if (!range.include[1]) {
      auto iter = get_iter();
      iter->SeekForPrev(range.point[1].Encode());
      if (iter->Valid() &&
          ic->Compare(iter->key(), range.point[1].Encode()) == 0) {
        iter->Prev();
      }
      if (!iter->Valid()) {
        if (!iter->status().ok()) {
          return iter->status();
        }
        continue;
      }
      assert(ic->Compare(iter->key(), range.point[1].Encode()) < 0);
      if (ic->Compare(iter->key(), range.point[0].Encode()) < 0) {
        continue;
      }
      range.point[1].DecodeFrom(iter->key());
      range.include[1] = true;
    }
    if (new_ranges.empty()) {
      new_ranges.emplace_back(std::move(range));
      continue;
    }
    auto last = &new_ranges.back();
    auto& split_key = range.point[0];
    if (uc->Compare(last->point[1].user_key(), split_key.user_key()) != 0) {
      new_ranges.emplace_back(std::move(range));
      continue;
    }
    auto iter = get_iter();
    RangeWithDepend split;
    split.include[0] = true;
    split.include[1] = true;
    split.no_records = last->no_records && range.no_records;
    split.has_delete_range = last->has_delete_range || range.has_delete_range;
    split.stable = false;
    split.dependence = last->dependence;
    merge_dependence(split.dependence, range.dependence);
    if (uc->Compare(last->point[0].user_key(), split_key.user_key()) != 0) {
      iter->SeekForPrev(set_ik(split_key, kSetMin));
      assert(!iter->Valid() ||
             uc->Compare(ExtractUserKey(iter->key()), ik.user_key()) < 0);
      if (iter->Valid() &&
          ic->Compare(iter->key(), last->point[0].Encode()) >= 0) {
        last->point[1].DecodeFrom(iter->key());
      } else if (!iter->status().ok()) {
        return iter->status();
      } else {
        new_ranges.pop_back();
      }
    } else {
      new_ranges.pop_back();
    }
    do {
      iter->Seek(set_ik(split_key, kSetMin));
      if (!iter->Valid()) {
        if (!iter->status().ok()) {
          return iter->status();
        }
        break;
      }
      assert(ic->Compare(iter->key(), ik.Encode()) >= 0);
      if (uc->Compare(ExtractUserKey(iter->key()), split_key.user_key()) != 0) {
        break;
      }
      split.point[0].DecodeFrom(iter->key());
      iter->SeekForPrev(set_ik(split_key, kSetMax));
      if (!iter->Valid()) {
        if (!iter->status().ok()) {
          return iter->status();
        }
        break;
      }
      assert(ic->Compare(iter->key(), ik.Encode()) <= 0);
      if (uc->Compare(ExtractUserKey(iter->key()), split_key.user_key()) != 0) {
        break;
      }
      split.point[1].DecodeFrom(iter->key());
      assert(ic->Compare(split.point[0], split.point[1]) <= 0);
      new_ranges.emplace_back(std::move(split));
    } while (false);

    if (uc->Compare(split_key.user_key(), range.point[1].user_key()) != 0) {
      iter->Seek(set_ik(split_key, kSetMax));
      if (iter->Valid() && ic->Compare(iter->key(), ik.Encode()) == 0) {
        iter->Next();
      }
      assert(!iter->Valid() || ic->Compare(iter->key(), ik.Encode()) > 0);
      if (iter->Valid() &&
          ic->Compare(iter->key(), range.point[1].Encode()) <= 0) {
        range.point[0].DecodeFrom(iter->key());
        new_ranges.emplace_back(std::move(range));
      } else if (!iter->status().ok()) {
        return iter->status();
      }
    }
  }
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
  auto put_left = [&](const InternalKey& key, bool include,
                      const RangeWithDepend* r) {
    assert(output.empty() || icomp.Compare(output.back().point[1], key) < 0 ||
           !output.back().include[1] || !include);
    output.emplace_back();
    auto& back = output.back();
    back.point[0] = key;
    back.include[0] = include;
    source = r;
  };
  auto put_right = [&](const InternalKey& key, bool include,
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
    if (IsEmptyMapSstElement(back, icomp)) {
      output.pop_back();
    }
    if (source == nullptr || r == nullptr || source != r) {
      back.stable = false;
    }
  };
  auto put_depend = [&](const RangeWithDepend* a, const RangeWithDepend* b) {
    auto& dependence = output.back().dependence;
    auto& no_records = output.back().no_records;
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
            no_records = a->no_records;
            has_delete_range = a->has_delete_range;
            stable = a->stable;
          }
        } else {
          no_records = b->no_records;
          has_delete_range = b->has_delete_range;
          stable = b->stable;
          dependence = b->dependence;
        }
        break;
      case PartitionType::kDelete:
        if (b == nullptr) {
          no_records = a->no_records;
          has_delete_range = a->has_delete_range;
          stable = a->stable;
          dependence = a->dependence;
        } else {
          assert(b->dependence.empty());
        }
        break;
      case PartitionType::kExtract:
        if (a != nullptr && b != nullptr) {
          no_records = a->no_records;
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
                         const std::vector<const FileMetaData*>& added_files,
                         bool unroll_delete_range, int output_level,
                         uint32_t output_path_id, ColumnFamilyData* cfd,
                         Version* version, VersionEdit* edit,
                         FileMetaData* file_meta_ptr,
                         std::unique_ptr<TableProperties>* prop_ptr,
                         std::set<FileMetaData*>* deleted_files) {
  assert(output_level != 0 || inputs.front().level == 0);
  assert(!inputs.front().files.empty());
  auto vstorage = version->storage_info();
  auto& icomp = cfd->internal_comparator();
  IteratorCacheContext iterator_cache_ctx = {
      cfd, &version->GetMutableCFOptions(), &env_options_};
  IteratorCache iterator_cache(vstorage->dependence_map(), &iterator_cache_ctx,
                               IteratorCacheContext::invoke);

  std::list<std::vector<RangeWithDepend>> level_ranges;
  std::vector<RangeWithDepend> range_deletion_ranges;
  MapSstElement map_element;
  FileMetaDataBoundBuilder bound_builder(&cfd->internal_comparator());
  Status s;
  size_t input_range_count = 0;

  // load range_deletions
  auto load_range_deletion = [&](const FileMetaData* f) {
    if (f->prop.purpose == kMapSst ||
        (f->prop.flags & TablePropertyCache::kHasRangeDeletions) == 0) {
      return Status::OK();
    }
    TableReader* reader;
    auto iter = iterator_cache.GetIterator(f, &reader);
    if (!iter->status().ok()) {
      return iter->status();
    }
    assert(reader != nullptr);
    std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
        reader->NewRangeTombstoneIterator(ReadOptions()));
    assert(range_del_iter);
    if (!range_del_iter) {
      return Status::OK();
    }
    auto& ranges = range_deletion_ranges;
    for (range_del_iter->SeekToTopFirst(); range_del_iter->Valid();
         range_del_iter->TopNext()) {
      if (!ranges.empty() && icomp.user_comparator()->Compare(
                                 range_del_iter->start_key(),
                                 ranges.back().point[1].user_key()) == 0) {
        ranges.back().point[1].Clear();
        ranges.back().point[1].SetMinPossibleForUserKey(
            range_del_iter->end_key());
      } else {
        ranges.emplace_back();
        auto& r = ranges.back();
        r.point[0].SetMinPossibleForUserKey(range_del_iter->start_key());
        r.point[1].SetMinPossibleForUserKey(range_del_iter->end_key());
        r.include[0] = true;
        r.include[1] = false;
        r.has_delete_range = true;
        r.no_records = true;
        r.stable = false;
      }
    }
    return Status::OK();
  };

  // load input files into level_ranges
  for (auto& level_files : inputs) {
    if (level_files.files.empty()) {
      continue;
    }
    if (unroll_delete_range) {
      for (auto f : level_files.files) {
        s = load_range_deletion(f);
        if (!s.ok()) {
          return s;
        }
      }
    }
    if (level_files.level == 0) {
      for (auto f : level_files.files) {
        std::vector<RangeWithDepend> ranges;
        s = LoadRangeWithDepend(ranges, &bound_builder, iterator_cache, &f, 1);
        if (!s.ok()) {
          return s;
        }
        assert(std::is_sorted(
            ranges.begin(), ranges.end(),
            [&icomp](const RangeWithDepend& a, const RangeWithDepend& b) {
              return icomp.Compare(a.point[1], b.point[1]) < 0;
            }));
        input_range_count += ranges.size();
        level_ranges.emplace_back(std::move(ranges));
      }
    } else {
      std::vector<RangeWithDepend> ranges;
      assert(std::is_sorted(
          level_files.files.begin(), level_files.files.end(),
          [&icomp](const FileMetaData* f1, const FileMetaData* f2) {
            return icomp.Compare(f1->largest, f2->largest) < 0;
          }));
      s = LoadRangeWithDepend(ranges, &bound_builder, iterator_cache,
                              level_files.files.data(),
                              level_files.files.size());
      if (!s.ok()) {
        return s;
      }
      assert(std::is_sorted(
          ranges.begin(), ranges.end(),
          [&icomp](const RangeWithDepend& a, const RangeWithDepend& b) {
            return icomp.Compare(a.point[1], b.point[1]) < 0;
          }));
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
      ranges.emplace_back(r);
    }
    assert(std::is_sorted(
        ranges.begin(), ranges.end(),
        [&icomp](const RangeWithDepend& a, const RangeWithDepend& b) {
          return icomp.Compare(a.point[1], b.point[1]) < 0;
        }));
    level_ranges.front() = PartitionRangeWithDepend(
        level_ranges.front(), ranges, cfd->internal_comparator(),
        PartitionType::kDelete);
    if (level_ranges.front().empty()) {
      level_ranges.pop_front();
    }
  }
  if (!added_files.empty()) {
    std::vector<RangeWithDepend> ranges;
    assert(std::is_sorted(
        added_files.begin(), added_files.end(),
        [&icomp](const FileMetaData* f1, const FileMetaData* f2) {
          return icomp.Compare(f1->largest, f2->largest) < 0;
        }));
    s = LoadRangeWithDepend(ranges, &bound_builder, iterator_cache,
                            added_files.data(), added_files.size());
    if (!s.ok()) {
      return s;
    }
    if (level_ranges.empty()) {
      level_ranges.emplace_back(std::move(ranges));
    } else {
      level_ranges.front() = PartitionRangeWithDepend(
          level_ranges.front(), ranges, cfd->internal_comparator(),
          PartitionType::kMerge);
    }
    if (unroll_delete_range) {
      for (auto f : added_files) {
        s = load_range_deletion(f);
        if (!s.ok()) {
          return s;
        }
      }
    }
  }

  if (!level_ranges.empty() && !range_deletion_ranges.empty()) {
    auto& ranges = range_deletion_ranges;
    std::sort(ranges.begin(), ranges.end(),
              [&icomp](const RangeWithDepend& f1, const RangeWithDepend& f2) {
                return icomp.Compare(f1.point[0], f2.point[0]) < 0;
              });
    size_t c = 0, n = ranges.size();
    auto it = ranges.begin();
    for (size_t i = 1; i < n; ++i) {
      if (icomp.Compare(it[c].point[1], it[i].point[0]) >= 0) {
        if (icomp.Compare(it[c].point[1], it[i].point[1]) < 0) {
          it[c].point[1] = std::move(it[i].point[1]);
        }
      } else if (++c != i) {
        it[c] = std::move(it[i]);
      }
    }
    ranges.resize(c + 1);
    level_ranges.front() = PartitionRangeWithDepend(
        level_ranges.front(), ranges, icomp, PartitionType::kMerge);
  }
  std::vector<RangeWithDepend> ranges;
  if (!level_ranges.empty()) {
    s = AdjustRange(cfd, version, env_options_, level_ranges.front(), ranges);
    level_ranges.clear();
    if (!s.ok()) {
      return s;
    }
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
  if (range_deletion_ranges.empty() &&
      (output_level != 0 || ranges.size() == 1)) {
    std::unordered_map<uint64_t, const FileMetaData*> sst_live;
    bool build_map_sst = false;
    // check is need build map
    for (auto it = ranges.begin(); it != ranges.end(); ++it) {
      if (it->dependence.size() > 1) {
        build_map_sst = true;
        break;
      }
      auto f =
          iterator_cache.GetFileMetaData(it->dependence.front().file_number);
      assert(f != nullptr);
      Range r(it->point[0].Encode(), it->point[1].Encode(), it->include[0],
              it->include[1]);
      if (!IsPerfectRange(r, f, icomp)) {
        build_map_sst = true;
        break;
      }
      sst_live.emplace(it->dependence.front().file_number, f);
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
      inputs.front().files.front()->prop.purpose == kMapSst &&
      ranges.size() == input_range_count &&
      !std::any_of(ranges.begin(), ranges.end(),
                   [](const RangeWithDepend& e) { return !e.stable; })) {
    // all ranges stable, new map will equals to input map, done
    return s;
  }

  using IterType = MapSstElementIterator;
  void* buffer = iterator_cache.GetArena()->AllocateAligned(sizeof(IterType));
  std::unique_ptr<IterType, void (*)(IterType*)> output_iter(
      new (buffer) IterType(ranges, iterator_cache, cfd->internal_comparator()),
      [](IterType* iter) { iter->~IterType(); });

  assert(std::is_sorted(
      ranges.begin(), ranges.end(),
      [&icomp](const RangeWithDepend& f1, const RangeWithDepend& f2) {
        return icomp.Compare(f1.point[1], f2.point[1]) < 0;
      }));

  FileMetaData file_meta;
  std::unique_ptr<TableProperties> prop;

  s = WriteOutputFile(bound_builder, output_iter.get(), output_path_id, cfd,
                      version->GetMutableCFOptions(), &file_meta, &prop);

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
}

Status MapBuilder::Build(const std::vector<CompactionInputFiles>& inputs,
                         const std::vector<Range>& push_range, int output_level,
                         uint32_t output_path_id, ColumnFamilyData* cfd,
                         Version* version, VersionEdit* edit,
                         std::vector<MapBuilderOutput>* output) {
  assert(output_level > 0);
  auto vstorage = version->storage_info();
  auto& icomp = cfd->internal_comparator();
  IteratorCacheContext iterator_cache_ctx = {
      cfd, &version->GetMutableCFOptions(), &env_options_};
  IteratorCache iterator_cache(vstorage->dependence_map(), &iterator_cache_ctx,
                               IteratorCacheContext::invoke);

  struct RangesItem {
    RangesItem(FileMetaDataBoundBuilder&& _bound_builder, int _level,
               bool _is_map, std::vector<RangeWithDepend>&& _ranges)
        : bound_builder(std::move(_bound_builder)),
          level(_level),
          is_map(_is_map),
          input_range_count(_ranges.size()),
          ranges(std::move(_ranges)),
          inputs(nullptr) {}

    FileMetaDataBoundBuilder bound_builder;
    int level;
    bool is_map;
    size_t input_range_count;
    std::vector<RangeWithDepend> ranges;
    const std::vector<FileMetaData*>* inputs;

    bool is_stable() {
      return is_map && ranges.size() == input_range_count &&
             !std::any_of(ranges.begin(), ranges.end(),
                          [](const RangeWithDepend& e) { return !e.stable; });
    }
  };
  std::vector<RangesItem> range_items;
  MapSstElement map_element;
  Status s;
  size_t output_index = size_t(-1);

  std::vector<RangeWithDepend> push_ranges;
  push_ranges.reserve(push_range.size());
  for (auto& r : push_range) {
    push_ranges.emplace_back(r);
  }
  assert(std::is_sorted(
      push_ranges.begin(), push_ranges.end(),
      [&icomp](const RangeWithDepend& a, const RangeWithDepend& b) {
        return icomp.Compare(a.point[1], b.point[1]) < 0;
      }));

  // load input files into level_ranges
  for (auto& level_files : inputs) {
    if (level_files.files.empty()) {
      continue;
    }
    assert(level_files.level <= output_level);
    FileMetaDataBoundBuilder bound_builder(&cfd->internal_comparator());
    if (level_files.level == 0) {
      for (auto f : level_files.files) {
        std::vector<RangeWithDepend> ranges;
        s = LoadRangeWithDepend(ranges, &bound_builder, iterator_cache, &f, 1);
        if (!s.ok()) {
          return s;
        }
        assert(std::is_sorted(
            ranges.begin(), ranges.end(),
            [&icomp](const RangeWithDepend& a, const RangeWithDepend& b) {
              return icomp.Compare(a.point[1], b.point[1]) < 0;
            }));
        if (range_items.empty()) {
          bool is_map = level_files.size() == 1 &&
                        level_files.files.front()->prop.purpose == kMapSst;
          range_items.emplace_back(FileMetaDataBoundBuilder(nullptr), 0, is_map,
                                   std::move(ranges));
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
      std::vector<RangeWithDepend> ranges;
      if (level_files.level == output_level) {
        output_index = range_items.size();
      }
      s = LoadRangeWithDepend(ranges, &bound_builder, iterator_cache,
                              level_files.files.data(), level_files.size());
      if (!s.ok()) {
        return s;
      }
      bool is_map = level_files.size() == 1 &&
                    level_files.files.front()->prop.purpose == kMapSst;
      range_items.emplace_back(std::move(bound_builder), level_files.level,
                               is_map, std::move(ranges));
    }
    range_items.back().inputs = &level_files.files;
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
    auto extract = PartitionRangeWithDepend(level_ranges.ranges, push_ranges,
                                            cfd->internal_comparator(),
                                            PartitionType::kExtract);
    if (!extract.empty()) {
      level_ranges.ranges = PartitionRangeWithDepend(
          level_ranges.ranges, push_ranges, cfd->internal_comparator(),
          PartitionType::kDelete);
      auto& bound_builder = range_items[output_index].bound_builder;
      for (auto& r : extract) {
        for (auto& dependence : r.dependence) {
          TableReader* reader;
          const FileMetaData* f;
          s = iterator_cache.GetReader(dependence.file_number, &reader, &f);
          if (!s.ok()) {
            return s;
          }
          bound_builder.Update(f);
          bound_builder.creation_time =
              std::max(bound_builder.creation_time,
                       reader->GetTableProperties()->creation_time);
        }
      }
      auto& output_ranges = range_items[output_index].ranges;
      output_ranges = output_ranges.empty()
                          ? std::move(extract)
                          : PartitionRangeWithDepend(extract, output_ranges,
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
    std::vector<RangeWithDepend> adjusted_range;
    s = AdjustRange(cfd, version, env_options_, level_ranges.ranges,
                    adjusted_range);
    if (!s.ok()) {
      return s;
    }
    level_ranges.ranges = std::move(adjusted_range);
    if (level_ranges.ranges.empty()) {
      if (level_ranges.inputs != nullptr) {
        for (auto f : *level_ranges.inputs) {
          edit->DeleteFile(level_ranges.level, f->fd.GetNumber());
        }
      }
      continue;
    }
    // make sure level 0 files seqno no overlap
    if (level_ranges.level != 0 || level_ranges.ranges.size() == 1) {
      std::unordered_map<uint64_t, const FileMetaData*> sst_live;
      bool build_map_sst = false;
      // check is need build map
      for (auto it = level_ranges.ranges.begin();
           it != level_ranges.ranges.end(); ++it) {
        if (it->dependence.size() > 1) {
          build_map_sst = true;
          break;
        }
        auto f =
            iterator_cache.GetFileMetaData(it->dependence.front().file_number);
        assert(f != nullptr);
        Range r(it->point[0].Encode(), it->point[1].Encode(), it->include[0],
                it->include[1]);
        if (!IsPerfectRange(r, f, icomp)) {
          build_map_sst = true;
          break;
        }
        sst_live.emplace(it->dependence.front().file_number, f);
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

    using IterType = MapSstElementIterator;
    void* buffer = iterator_cache.GetArena()->AllocateAligned(sizeof(IterType));
    std::unique_ptr<IterType, void (*)(IterType*)> output_iter(
        new (buffer) IterType(level_ranges.ranges, iterator_cache,
                              cfd->internal_comparator()),
        [](IterType* iter) { iter->~IterType(); });

    assert(std::is_sorted(
        level_ranges.ranges.begin(), level_ranges.ranges.end(),
        [&icomp](const RangeWithDepend& f1, const RangeWithDepend& f2) {
          return icomp.Compare(f1.point[1], f2.point[1]) < 0;
        }));

    MapBuilderOutput output_item;
    s = WriteOutputFile(level_ranges.bound_builder, output_iter.get(),
                        output_path_id, cfd, version->GetMutableCFOptions(),
                        &output_item.file_meta, &output_item.prop);

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

Status MapBuilder::GetInputCoverage(
    const std::vector<CompactionInputFiles>& inputs, const Slice* lower_bound,
    const Slice* upper_bound, VersionStorageInfo* vstorage,
    ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    std::vector<RangeStorage>* coverage) {
  IteratorCacheContext iterator_cache_ctx = {cfd, &mutable_cf_options,
                                             &env_options_};
  IteratorCache iterator_cache(vstorage->dependence_map(), &iterator_cache_ctx,
                               IteratorCacheContext::invoke);

  for (auto& level_files : inputs) {
    auto s = AppendUserKeyRange(*coverage, iterator_cache,
                                level_files.files.data(), level_files.size());
    if (!s.ok()) {
      return s;
    }
  }
  auto uc = cfd->user_comparator();
  std::sort(coverage->begin(), coverage->end(),
            [uc](const RangeStorage& a, const RangeStorage& b) {
              return uc->Compare(a.start, b.start) < 0;
            });
  if (coverage->size() > 1) {
    size_t c = 0, n = coverage->size();
    auto it = coverage->begin();
    for (size_t i = 1; i < n; ++i) {
      if (uc->Compare(it[c].limit, it[i].start) >= 0) {
        if (uc->Compare(it[c].limit, it[i].limit) < 0) {
          it[c].limit = std::move(it[i].limit);
        }
      } else if (++c != i) {
        it[c] = std::move(it[i]);
      }
    }
    coverage->resize(c + 1);
  }
  if (lower_bound != nullptr) {
    auto find =
        std::lower_bound(coverage->begin(), coverage->end(), *lower_bound,
                         [uc](const RangeStorage& a, const Slice& b) {
                           return uc->Compare(a.limit, b) < 0;
                         });
    if (find != coverage->end()) {
      if (uc->Compare(find->start, *lower_bound) < 0) {
        find->start.assign(lower_bound->data(), lower_bound->size());
      }
      coverage->erase(coverage->begin(), find);
    }
  }
  if (upper_bound != nullptr) {
    auto find =
        std::upper_bound(coverage->begin(), coverage->end(), *upper_bound,
                         [uc](const Slice& a, const RangeStorage& b) {
                           return uc->Compare(a, b.start) < 0;
                         });
    if (find != coverage->begin()) {
      --find;
      if (uc->Compare(find->limit, *upper_bound) >= 0) {
        find->limit.assign(upper_bound->data(), upper_bound->size());
        find->include_limit = false;
      }
      coverage->erase(find + 1, coverage->end());
    }
  }
  return Status::OK();
}

Status MapBuilder::WriteOutputFile(
    const FileMetaDataBoundBuilder& bound_builder,
    MapSstElementIterator* range_iter, uint32_t output_path_id,
    ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    FileMetaData* file_meta, std::unique_ptr<TableProperties>* prop) {
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

  uint64_t output_file_creation_time = bound_builder.creation_time;
  if (output_file_creation_time == 0) {
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
      true /* ignore_key_type */, output_file_creation_time,
      0 /* oldest_key_time */, kMapSst));
  LogFlush(db_options_.info_log);

  // Update boundaries
  file_meta->smallest = bound_builder.smallest;
  file_meta->largest = bound_builder.largest;
  file_meta->fd.smallest_seqno = bound_builder.smallest_seqno;
  file_meta->fd.largest_seqno = bound_builder.largest_seqno;

  for (range_iter->SeekToFirst(); range_iter->Valid(); range_iter->Next()) {
    builder->Add(range_iter->key(), LazyBuffer(range_iter->value()));
  }
  if (!range_iter->status().ok()) {
    s = range_iter->status();
  }

  // Prepare prop
  file_meta->prop.num_entries = builder->NumEntries();
  file_meta->prop.purpose = kMapSst;
  std::tie(file_meta->prop.max_read_amp, file_meta->prop.read_amp) =
      range_iter->GetSstReadAmp();
  auto& dependence_build = range_iter->GetDependence();
  auto& dependence = file_meta->prop.dependence;
  dependence.reserve(dependence_build.size());
  for (auto& pair : dependence_build) {
    dependence.emplace_back(Dependence{pair.first, pair.second});
  }
  std::sort(dependence.begin(), dependence.end(),
            [](const Dependence& l, const Dependence& r) {
              return l.file_number < r.file_number;
            });

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
    // Output to event logger and fire events.
    const char* compaction_msg =
        file_meta->marked_for_compaction ? " (need compaction)" : "";
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Generated map table #%" PRIu64 ": %" PRIu64
                   " keys, %" PRIu64 " bytes%s",
                   cfd->GetName().c_str(), job_id_, file_number,
                   current_entries, current_bytes, compaction_msg);
    file_meta->prop.flags |= range_iter->HasDeleteRange()
                                 ? TablePropertyCache::kHasRangeDeletions
                                 : 0;
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
    if (meta_array_[where_]->prop.purpose == kMapSst) {
      if (!InitIter()) {
        return;
      }
      iter_->Seek(target);
      if (!iter_->Valid()) {
        ResetIter();
        if (++where_ == meta_size_) {
          return;
        }
        if (meta_array_[where_]->prop.purpose == kMapSst) {
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
    if (meta_array_[where_]->prop.purpose == kMapSst) {
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
        if (meta_array_[where_]->prop.purpose == kMapSst) {
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
    if (meta_array_[where_]->prop.purpose == kMapSst) {
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
    if (meta_array_[where_]->prop.purpose == kMapSst) {
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
    if (meta_array_[where_]->prop.purpose == kMapSst) {
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
    if (meta_array_[where_]->prop.purpose == kMapSst) {
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
      element_.no_records = false;
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
  } else if (meta_size == 1 && meta_array[0]->prop.purpose == kMapSst) {
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

}  // namespace rocksdb
