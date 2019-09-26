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
struct CompactionWorkerContext;
struct CompactionWorkerResult;
struct FileMetaData;
class VersionStorageInfo;
struct ImmutableCFOptions;
struct MutableCFOptions;

class CompactionDispatcher {
 public:
  virtual ~CompactionDispatcher() = default;

  virtual std::function<CompactionWorkerResult()> StartCompaction(
      const CompactionWorkerContext& context) = 0;

  virtual const char* Name() const = 0;
};

class RemoteCompactionDispatcher : public CompactionDispatcher {
 public:
  virtual std::function<CompactionWorkerResult()> StartCompaction(
      const CompactionWorkerContext& context) override;

  virtual const char* Name() const override;

  virtual std::future<std::string> DoCompaction(const std::string& data) = 0;

  class Worker {
   public:
    Worker(EnvOptions env_options, Env* env);
    virtual ~Worker();

    using CreateTableFactoryCallback =
        std::function<Status(std::shared_ptr<TableFactory>*,
                             const std::string& options)>;
    using CreateMergeOperatorCallback =
    std::function<Status(std::shared_ptr<MergeOperator>*)>;

    void RegistComparator(const Comparator*);
    void RegistPrefixExtractor(std::shared_ptr<const SliceTransform>);
    void RegistTableFactory(const char* Name, CreateTableFactoryCallback);
    void RegistMergeOperator(CreateMergeOperatorCallback);
    void RegistCompactionFilter(const CompactionFilter*);
    void RegistCompactionFilterFactory(
        std::shared_ptr<CompactionFilterFactory>);
    void RegistTablePropertiesCollectorFactory(
        std::shared_ptr<TablePropertiesCollectorFactory>);

    virtual std::string GenerateOutputFileName(size_t file_index) = 0;

    std::string DoCompaction(const std::string& data);

   protected:
    Worker(const Worker&) = delete;
    Worker& operator = (const Worker&) = delete;

    struct Rep;
    Rep* rep_;
  };
};

extern std::shared_ptr<CompactionDispatcher>
    NewCommandLineCompactionDispatcher(std::string cmd);


}  // namespace rocksdb
