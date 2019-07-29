// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2013 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <future>
#include <string>

#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/advanced_options.h"

namespace rocksdb {

class ColumnFamilyHandle;
struct FileMetaData;
class VersionStorageInfo;
struct ImmutableCFOptions;
struct MutableCFOptions;

struct CompactionWorkerResult {
  Status status;
  std::string actual_start, actual_end;
  struct FileInfo {
    std::string smallest, largest, file_name;
    SequenceNumber smallest_seqno, largest_seqno;
    size_t file_size;
    bool being_compacted;
  };
  std::vector<FileInfo> files;
};

struct CompactionContext {
  // options
  std::string user_comparator;
  std::string merge_operator;
  std::string merge_operator_data;
  std::string compaction_filter;
  std::string compaction_filter_factory;
  rocksdb::CompactionFilter::Context compaction_filter_context;
  std::string compaction_filter_data;
  std::string table_factory;
  std::string table_factory_options;
  uint32_t bloom_locality;
  std::vector<std::string> cf_paths;
  std::string prefix_extractor;
  // compaction
  bool has_start, has_end;
  std::string start, end;
  rocksdb::SequenceNumber last_sequence;
  rocksdb::SequenceNumber earliest_write_conflict_snapshot;
  rocksdb::SequenceNumber preserve_deletes_seqnum;
  std::vector<rocksdb::FileMetaData> file_metadata;
  std::vector<std::pair<int, uint64_t>> inputs;
  std::string cf_name;
  uint64_t target_file_size;
  rocksdb::CompressionType compression;
  rocksdb::CompressionOptions compression_opts;
  std::vector<rocksdb::SequenceNumber> existing_snapshots;
  bool bottommost_level;
  std::vector<std::string> int_tbl_prop_collector_factories;
};

class CompactionWorker {
 public:
  virtual ~CompactionWorker() = default;

  virtual std::packaged_task<CompactionWorkerResult()> StartCompaction(
      const CompactionContext& context) = 0;

  virtual const char* Name() const = 0;
};

class RemoteCompactionWorker : CompactionWorker {
 public:
  virtual std::packaged_task<CompactionWorkerResult()> StartCompaction(
      const CompactionContext& context) override;

  virtual const char* Name() const override {
    return "RemoteCompactionWorker";
  }

  virtual std::future<std::string> DoCompaction(
      const std::string& data) = 0;

  class Client {
   public:
    Client(EnvOptions env_options, Env* env);
    virtual ~Client();

    using CreateTableFactoryCallback =
        std::shared_ptr<TableFactory> (*)(const std::string& options);
    using CreateMergeOperatorCallback = std::shared_ptr<MergeOperator> (*)();

    void RegistComparator(const Comparator*);
    void RegistPrefixExtractor(std::shared_ptr<const SliceTransform>);
    void RegistTableFactory(const char* Name, CreateTableFactoryCallback);
    void RegistMergeOperator(CreateMergeOperatorCallback);
    void RegistCompactionFilter(std::shared_ptr<CompactionFilterFactory>);
    void RegistCompactionFilter(
        std::shared_ptr<TablePropertiesCollectorFactory>);

    virtual std::string GenerateOutputFileName(size_t file_index) = 0;

    std::string DoCompaction(const std::string& data);

   protected:
    Client(const Client&) = delete;
    Client& operator = (const Client&) = delete;

    struct Rep;
    Rep* rep_;
  };
};


}  // namespace rocksdb
