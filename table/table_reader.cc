//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table_reader.h"

#include <inttypes.h>

#include "db/dbformat.h"
#include "memtable/skiplist.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/statistics.h"
#include "rocksdb/terark_namespace.h"
#include "table/get_context.h"
#include "table/scoped_arena_iterator.h"
#include "table/table_builder.h"
#include "util/arena.h"
#include "util/static_map_index.h"

namespace TERARKDB_NAMESPACE {

class StaticMapIndex;

void TableReader::RangeScan(const Slice* begin,
                            const SliceTransform* prefix_extractor, void* arg,
                            bool (*callback_func)(void* arg, const Slice& key,
                                                  LazyBuffer&& value)) {
  Arena arena;
  ScopedArenaIterator iter(
      NewIterator(ReadOptions(), prefix_extractor, &arena));
  {
    PERF_TIMER_GUARD(get_from_map_time);
    begin == nullptr ? iter->SeekToFirst() : iter->Seek(*begin);
  }
  for (; iter->Valid() && callback_func(arg, iter->key(), iter->value());
       iter->Next()) {
  }
}

void TableReader::UpdateMaxCoveringTombstoneSeq(
    const ReadOptions& readOptions, const Slice& user_key,
    SequenceNumber* max_covering_tombstone_seq) {
  if (max_covering_tombstone_seq != nullptr &&
      !readOptions.ignore_range_deletions) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
        NewRangeTombstoneIterator(readOptions));
    if (range_del_iter != nullptr) {
      *max_covering_tombstone_seq =
          std::max(*max_covering_tombstone_seq,
                   range_del_iter->MaxCoveringTombstoneSeqnum(user_key));
    }
  }
}
class TableMapIndexReader : public TableReader {
  class Iter : public InternalIterator {
   public:
    Iter(StaticMapIndex* _index) : index(_index) {}
    bool Valid() const override { return idx >= 0 && idx < index->key_nums; }
    void SeekToFirst() override { idx = 0; }
    void SeekToLast() override { idx = index->key_nums - 1; }
    void SeekForPrev(const Slice& key) override {
      idx = index->getIdx(key) - 1;
    }
    void Seek(const Slice& key) override { idx = index->getIdx(key); }
    void Next() override { idx++; }
    void Prev() override { idx--; }
    Slice key() const override { return index->getKey(idx); }
    LazyBuffer value() const override { return LazyBuffer(index->getKey(idx)); }
    Status status() const override { return Status::OK(); }

   private:
    StaticMapIndex* index;
    int idx;
  };

  // Logic same as for(it->Seek(begin); it->Valid() && callback(*it); ++it) {}
  // Specialization for performance
 public:
  ~TableMapIndexReader() {
    if (index != nullptr) {
      delete index;
    }
  }
  TableMapIndexReader(const ImmutableCFOptions& icfo,
                      std::unique_ptr<TableReader>& table_reader)
      : immutable_cfoptions_(icfo), file_number_(table_reader->FileNumber()) {}
  void RangeScan(const Slice* begin, const SliceTransform* prefix_extractor,
                 void* arg,
                 bool (*callback_func)(void* arg, const Slice& key,
                                       LazyBuffer&& value)) {
    int k = 0;
    {
      PERF_TIMER_GUARD(get_from_map_time);
      if (begin != nullptr) k = index->getIdx(*begin);
    }
    for (; k < index->key_nums &&
           callback_func(arg, index->getKey(k), LazyBuffer(index->getValue(k)));
         k++) {
    }
  }
  uint64_t ApproximateOffsetOf(const Slice& key) override { return 0; }

  void SetupForCompaction() override {}

  std::shared_ptr<const TableProperties> GetTableProperties() const override {
    return table_properties_;
  }
  InternalIterator* NewIterator(const ReadOptions&,
                                const SliceTransform* prefix_extractor,
                                Arena* arena = nullptr,
                                bool skip_filters = false,
                                bool for_compaction = false) override {
    return new Iter(index);
  }
  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions& read_options) override {
    if (fragmented_range_dels_ == nullptr) {
      return nullptr;
    }
    SequenceNumber snapshot = kMaxSequenceNumber;
    if (read_options.snapshot != nullptr) {
      snapshot = read_options.snapshot->GetSequenceNumber();
    }
    auto icomp = &immutable_cfoptions_.internal_comparator;
    return new FragmentedRangeTombstoneIterator(fragmented_range_dels_, *icomp,
                                                snapshot);
  }

  size_t ApproximateMemoryUsage() const override {
    if (index == nullptr)
      return 0;
    else
      return index->size();
  }
  uint64_t FileNumber() const override { return file_number_; }

  Status Get(const ReadOptions& readOptions, const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool skip_filters = false) override {
    assert(false);
    return Status();
  }

  Status Open(std::unique_ptr<TableReader>& table_reader);

 private:
  const ImmutableCFOptions& immutable_cfoptions_;
  StaticMapIndex* index = nullptr;

  const uint64_t file_number_;

  std::shared_ptr<const FragmentedRangeTombstoneList> fragmented_range_dels_;
  std::shared_ptr<const TableProperties> table_properties_;
};

Status TableMapIndexReader::Open(std::unique_ptr<TableReader>& table_reader) {
  Status status;
  auto iter = table_reader->NewIterator(ReadOptions(), nullptr);
  index = new StaticMapIndex(&immutable_cfoptions_.internal_comparator);
  StopWatchNano timer(immutable_cfoptions_.env, /*auto_start=*/true);
  status = BuildStaticMapIndex(iter, index);
  if (index != nullptr) {
    ROCKS_LOG_INFO(
        immutable_cfoptions_.info_log,
        "[BuildMapIndex] finished build map index, elapsed_nanos=%" PRIu64
        ", key_size= %d, value_lens= %d, key_nums= %d",
        timer.ElapsedNanos(), index->key_len, index->value_len,
        index->key_nums);
    //    index->DebugString();
  }
  return status;
}

// for small sst and frequence-used sst, force it in memory

constexpr uint16_t kMaxHeight = 12;  // same as SkipList constructor
class TableMemReader : public TableReader {
  struct MemEntry {
    MemEntry(int) {
      // create empty entry
    }
    MemEntry(Slice _ikey) : ikey(_ikey), idx(0) {}
    MemEntry(Slice _ikey, Slice _val, uint64_t _idx)
        : ikey(_ikey), value(_val), idx(_idx) {}
    Slice ikey;
    Slice value;
    uint64_t idx;
    uint64_t Size() { return ikey.size() + value.size(); }
  };
  struct MemEntryComparator {
    MemEntryComparator(const InternalKeyComparator& _c) : c(_c) {}
    int operator()(const MemEntry& l, const MemEntry& r) const {
      return c.Compare(l.ikey, r.ikey);
    }
    const InternalKeyComparator& c;
  };

  typedef SkipList<MemEntry, MemEntryComparator> Holder;
  class Iter : public InternalIterator {
   public:
    Iter(TableMemReader& reader)
        : iter_(&reader.GetList()), file_number_(reader.FileNumber()) {}
    bool Valid() const override { return iter_.Valid(); }
    void SeekToFirst() override { iter_.SeekToFirst(); }
    void SeekToLast() override { iter_.SeekToLast(); }
    void SeekForPrev(const Slice& key) override {
      auto entry = MemEntry(key);
      iter_.SeekForPrev(entry);
    }
    void Seek(const Slice& key) override {
      auto entry = MemEntry(key);
      iter_.Seek(entry);
    }
    void Next() override { iter_.Next(); }
    void Prev() override { iter_.Prev(); }
    Slice key() const override { return iter_.key().ikey; }
    LazyBuffer value() const override {
      return LazyBuffer(iter_.key().value, false, file_number_);
    }
    Status status() const override { return Status::OK(); }

   private:
    Holder::Iterator iter_;
    uint64_t file_number_;
  };

 public:
  TableMemReader(const ImmutableCFOptions& icfo,
                 const TableReaderOptions& table_reader_options,
                 std::unique_ptr<TableReader>& table_reader)
      : listkey_arena_(table_reader->GetTableProperties()->raw_key_size),
        val_arena_(table_reader->GetTableProperties()->raw_value_size),
        list_(icfo.internal_comparator, &listkey_arena_,
              kMaxHeight /*max_height*/),
        immutable_cfoptions_(icfo),
        table_reader_options_(table_reader_options),
        file_number_(table_reader->FileNumber()),
        file_data_size_(table_reader->GetTableProperties()->data_size) {}
  void RangeScan(const Slice* begin, const SliceTransform* prefix_extractor,
                 void* arg,
                 bool (*callback_func)(void* arg, const Slice& key,
                                       LazyBuffer&& value)) override {
    Iter iter(*this);
    for (begin == nullptr ? iter.SeekToFirst() : iter.Seek(*begin);
         iter.Valid() && callback_func(arg, iter.key(), iter.value());
         iter.Next()) {
    }
  }
  InternalIterator* NewIterator(const ReadOptions&,
                                const SliceTransform* prefix_extractor,
                                Arena* arena = nullptr,
                                bool skip_filters = false,
                                bool for_compaction = false) override {
    if (arena == nullptr) {
      return new Iter(*this);
    } else {
      auto* mem = arena->AllocateAligned(sizeof(TableMemReader::Iter));
      return new (mem) Iter(*this);
    }
  }
  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions& read_options) override {
    if (fragmented_range_dels_ == nullptr) {
      return nullptr;
    }
    SequenceNumber snapshot = kMaxSequenceNumber;
    if (read_options.snapshot != nullptr) {
      snapshot = read_options.snapshot->GetSequenceNumber();
    }
    auto icomp = &table_reader_options_.internal_comparator;
    return new FragmentedRangeTombstoneIterator(fragmented_range_dels_, *icomp,
                                                snapshot);
  }
  uint64_t ApproximateOffsetOf(const Slice& key) override;

  void SetupForCompaction() override {}

  std::shared_ptr<const TableProperties> GetTableProperties() const override {
    assert(table_properties_);
    return table_properties_;
  }

  size_t ApproximateMemoryUsage() const override {
    return listkey_arena_.ApproximateMemoryUsage() +
           val_arena_.ApproximateMemoryUsage();
  }

  uint64_t FileNumber() const override { return file_number_; }

  Status Get(const ReadOptions& readOptions, const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool skip_filters = false) override;
  Status Open(std::unique_ptr<TableReader>& table_reader);
  const Holder& GetList() { return list_; }

 private:
  Arena listkey_arena_;
  Arena val_arena_;
  Holder list_;
  const ImmutableCFOptions& immutable_cfoptions_;
  const TableReaderOptions& table_reader_options_;
  const uint64_t file_number_;
  const uint64_t file_data_size_;
  uint64_t entry_size_;

  std::shared_ptr<const FragmentedRangeTombstoneList> fragmented_range_dels_;
  std::shared_ptr<const TableProperties> table_properties_;
};

uint64_t TableMemReader::ApproximateOffsetOf(const Slice& key) {
  // FIXME
  Holder::Iterator iter(&list_);
  auto entry = MemEntry(key);
  iter.Seek(entry);

  if (!iter.Valid()) {
    return 0;
  }

  return uint64_t((iter.key().idx / (double)table_properties_->num_entries) *
                  file_data_size_);
}

Status TableMemReader::Get(const ReadOptions& readOptions, const Slice& key,
                           GetContext* get_context,
                           const SliceTransform* /*prefix_extractor*/,
                           bool /*skip_filters*/) {
  Status status;

  Holder::Iterator iter(&list_);
  auto entry = MemEntry(key);
  iter.Seek(entry);
  bool match = false;
  while (iter.Valid()) {
    ParsedInternalKey parsed_cur_key;
    if (!ParseInternalKey(iter.key().ikey, &parsed_cur_key)) {
      status = Status::Corruption(Slice());
      break;
    }
    if (!get_context->SaveValue(
            parsed_cur_key, LazyBuffer(iter.key().value, false, file_number_),
            &match)) {
      break;
    }
    iter.Next();
  }
  return status;
}

Status TableMemReader::Open(std::unique_ptr<TableReader>& table_reader) {
  Status status;
  table_properties_ = table_reader->GetTableProperties();
  fragmented_range_dels_ = table_reader->GetFragmentedRangeTombstoneList();

  uint64_t entry_idx = 0;
  auto iter = table_reader->NewIterator(ReadOptions(), nullptr);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    auto lazy_val = iter->value();
    auto s = lazy_val.fetch();
    if (!s.ok()) {
      return s;
    }
    MemEntry entry(ArenaPinSlice(iter->key(), &listkey_arena_),
                   ArenaPinSlice(lazy_val.slice(), &val_arena_), entry_idx);
    list_.Insert(entry);
    entry_size_ += entry.Size();
    ++entry_idx;
  }
  return status;
}

Status NewTableMemReader(const ImmutableCFOptions& icfo,
                         const TableReaderOptions& table_reader_options,
                         std::unique_ptr<TableReader>& file_table_reader,
                         std::unique_ptr<TableReader>* mem_table_reader) {
  TableMemReader* mem_reader =
      new TableMemReader(icfo, table_reader_options, file_table_reader);

  Status status = mem_reader->Open(file_table_reader);
  if (status.ok()) mem_table_reader->reset(mem_reader);
  return status;
}
Status NewMapIndexReader(const ImmutableCFOptions& icfo,
                         std::unique_ptr<TableReader>& file_table_reader,
                         std::unique_ptr<TableReader>* mem_table_reader) {
  TableMapIndexReader* map_index_reader =
      new TableMapIndexReader(icfo, file_table_reader);

  Status status = map_index_reader->Open(file_table_reader);
  if (status.ok()) mem_table_reader->reset(map_index_reader);
  return status;
}
}  // namespace TERARKDB_NAMESPACE
