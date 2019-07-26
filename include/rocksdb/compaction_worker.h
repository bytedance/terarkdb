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

#include "rocksdb/env.h"
#include "rocksdb/status.h"
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
  struct FileInfo {
    std::string input_start, input_end, file_name;
  };
  std::vector<FileInfo> files;
};

class CompactionWorker {
 public:
  virtual ~CompactionWorker() = default;

  virtual std::packaged_task<CompactionWorkerResult()> StartCompaction(
      VersionStorageInfo* input_version,
      const ImmutableCFOptions& immutable_cf_options,
      const MutableCFOptions& mutable_cf_options,
      std::vector<std::pair<int, std::vector<const FileMetaData*>>> inputs,
      uint64_t target_file_size, CompressionType compression,
      CompressionOptions compression_opts,
      const Slice* start, const Slice* end) = 0;

  virtual const char* Name() const = 0;
};

class RemoteCompactionWorker : CompactionWorker {
 public:
  virtual std::packaged_task<CompactionWorkerResult()> StartCompaction(
      VersionStorageInfo* input_version,
      const ImmutableCFOptions& immutable_cf_options,
      const MutableCFOptions& mutable_cf_options,
      std::vector<std::pair<int, std::vector<const FileMetaData*>>> inputs,
      uint64_t target_file_size, CompressionType compression,
      CompressionOptions compression_opts, const Slice* start,
      const Slice* end) override;

  virtual const char* Name() const override {
    return "RemoteCompactionWorker";
  }

  virtual std::future<std::string> DoCompaction(
      const std::string& data) = 0;

  class Client {
   public:
    Client(EnvOptions env_options, Env* env);
    virtual ~Client();

    void RegistComparator(const Comparator*);
    void RegistTableFactory(std::shared_ptr<TableFactory>);
    void RegistMergeOperator(std::shared_ptr<MergeOperator>);
    void RegistCompactionFilter(std::shared_ptr<CompactionFilterFactory>);

    virtual std::string DoCompaction(const std::string& data);

   protected:
    Client(const Client&) = delete;
    Client& operator = (const Client&) = delete;

    struct Rep;
    Rep* rep_;
  };
};


}  // namespace rocksdb
