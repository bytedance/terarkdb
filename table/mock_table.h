//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <db/version_edit.h>

#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/table.h"
#include "rocksdb/terark_namespace.h"
#include "table/internal_iterator.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "util/kv_map.h"
#include "util/mutexlock.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace TERARKDB_NAMESPACE {
namespace mock {

stl_wrappers::KVMap MakeMockFile(
    std::initializer_list<std::pair<const std::string, std::string>> l = {});

struct MockTableFileSystem {
  port::Mutex mutex;
  struct FileData {
    stl_wrappers::KVMap table;
    stl_wrappers::KVMap tombstone;
    std::shared_ptr<const TableProperties> prop;
  };
  std::map<uint32_t, FileData> files;
};

class MockTableReader : public TableReader {
 public:
  MockTableReader(const MockTableFileSystem::FileData& file_data)
      : file_data_(file_data) {}

  InternalIterator* NewIterator(const ReadOptions&,
                                const SliceTransform* prefix_extractor,
                                Arena* arena = nullptr,
                                bool skip_filters = false,
                                bool for_compaction = false) override;

  Status Get(const ReadOptions& readOptions, const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool skip_filters = false) override;

  uint64_t ApproximateOffsetOf(const Slice& /*key*/) override { return 0; }

  virtual size_t ApproximateMemoryUsage() const override { return 0; }

  uint64_t FileNumber() const override { return uint64_t(-1); }

  void SetupForCompaction() override {}

  std::shared_ptr<const TableProperties> GetTableProperties() const override;

  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions& /*read_options*/) override;

  ~MockTableReader() {}

 private:
  const MockTableFileSystem::FileData& file_data_;
};

template <class ValueType>
class MockTableIterator : public InternalIteratorBase<ValueType> {
 public:
  explicit MockTableIterator(const stl_wrappers::KVMap& table) : table_(table) {
    itr_ = table_.end();
  }

  bool Valid() const override { return itr_ != table_.end(); }

  void SeekToFirst() override { itr_ = table_.begin(); }

  void SeekToLast() override {
    itr_ = table_.end();
    Prev();
  }

  void Seek(const Slice& target) override {
    std::string str_target(target.data(), target.size());
    itr_ = table_.lower_bound(str_target);
  }

  void SeekForPrev(const Slice& target) override {
    std::string str_target(target.data(), target.size());
    itr_ = table_.upper_bound(str_target);
    Prev();
  }

  void Next() override { ++itr_; }

  void Prev() override {
    if (itr_ == table_.begin()) {
      itr_ = table_.end();
    } else {
      --itr_;
    }
  }

  Slice key() const override { return Slice(itr_->first); }

  ValueType value() const override { return ValueType(itr_->second); }

  Status status() const override { return Status::OK(); }

 private:
  const stl_wrappers::KVMap& table_;
  stl_wrappers::KVMap::const_iterator itr_;
};

class MockTableBuilder : public TableBuilder {
 public:
  MockTableBuilder(uint32_t id, MockTableFileSystem* file_system)
      : id_(id), file_system_(file_system) {
    file_data_.table = MakeMockFile({});
    file_data_.tombstone = MakeMockFile({});
  }

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~MockTableBuilder() {}

  Status AddToTable(const Slice& key, const LazyBuffer& value,
                    stl_wrappers::KVMap& table) {
    auto s = value.fetch();
    if (!s.ok()) {
      return s;
    }
    table.insert({key.ToString(), value.ToString()});
    return Status::OK();
  }

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  Status Add(const Slice& key, const LazyBuffer& value) override {
    return AddToTable(key, value, file_data_.table);
  }

  Status AddTombstone(const Slice& key, const LazyBuffer& value) override {
    return AddToTable(key, value, file_data_.tombstone);
  }

  Status Finish(const TablePropertyCache* prop, const std::vector<uint64_t>*,
                const std::vector<uint64_t>* inheritance_tree) override {
    prop_.num_entries = file_data_.table.size();
    prop_.raw_key_size = file_data_.table.size();
    prop_.raw_value_size = file_data_.table.size();
    if (prop != nullptr) {
      prop_.purpose = prop->purpose;
      prop_.max_read_amp = prop->max_read_amp;
      prop_.read_amp = prop->read_amp;
      prop_.dependence = prop->dependence;
    }
    if (inheritance_tree != nullptr) {
      prop_.inheritance_tree = *inheritance_tree;
    }
    file_data_.prop = std::make_shared<const TableProperties>(prop_);
    MutexLock lock_guard(&file_system_->mutex);
    file_system_->files.emplace(id_, file_data_);
    return Status::OK();
  }

  void Abandon() override {}

  uint64_t NumEntries() const override {
    return file_data_.table.size() + file_data_.tombstone.size();
  }

  uint64_t FileSize() const override {
    return file_data_.table.size() + file_data_.tombstone.size();
  }

  TableProperties GetTableProperties() const override { return prop_; }

 private:
  uint32_t id_;
  MockTableFileSystem* file_system_;
  MockTableFileSystem::FileData file_data_;
  TableProperties prop_;
};

class MockTableFactory : public TableFactory {
 public:
  MockTableFactory();
  const char* Name() const override { return "MockTable"; }
  Status NewTableReader(
      const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool prefetch_index_and_filter_in_cache = true) const override;
  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_familly_id, WritableFileWriter* file) const override;

  // This function will directly create mock table instead of going through
  // MockTableBuilder. file_contents has to have a format of <internal_key,
  // value>. Those key-value pairs will then be inserted into the mock table.
  Status CreateMockTable(Env* env, const std::string& fname,
                         stl_wrappers::KVMap file_contents);

  virtual Status SanitizeOptions(
      const DBOptions& /*db_opts*/,
      const ColumnFamilyOptions& /*cf_opts*/) const override {
    return Status::OK();
  }

  virtual std::string GetPrintableTableOptions() const override {
    return std::string();
  }

  // This function will assert that only a single file exists and that the
  // contents are equal to file_contents
  void AssertSingleFile(const stl_wrappers::KVMap& file_contents,
                        const stl_wrappers::KVMap& range_deletions);
  void AssertLatestFile(const stl_wrappers::KVMap& file_contents);

 private:
  uint32_t GetAndWriteNextID(WritableFileWriter* file) const;
  uint32_t GetIDFromFile(RandomAccessFileReader* file) const;

  mutable MockTableFileSystem file_system_;
  mutable std::atomic<uint32_t> next_id_;
};

}  // namespace mock
}  // namespace TERARKDB_NAMESPACE
