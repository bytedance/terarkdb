//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#pragma once
#include <unordered_map>
#include "rocksdb/env.h"
#include "rocksdb/slice_transform.h"

namespace rocksdb {

class TableCache;
class VersionStorageInfo;
class VersionEdit;
class VersionSet;
struct FileMetaData;
class InternalStats;
struct VersionBuilderDebugger;

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionBuilder {
  friend VersionBuilderDebugger;

 public:
  VersionBuilder(const EnvOptions& env_options, TableCache* table_cache,
                 VersionStorageInfo* base_vstorage, Logger* info_log = nullptr);
  ~VersionBuilder();
  void CheckConsistency(VersionStorageInfo* vstorage);
  void CheckConsistencyForDeletes(VersionEdit* edit, uint64_t number,
                                  int level);
  bool CheckConsistencyForNumLevels();
  void Apply(VersionEdit* edit);
  void SaveTo(VersionStorageInfo* vstorage);
  void LoadTableHandlers(InternalStats* internal_stats,
                         bool prefetch_index_and_filter_in_cache,
                         const SliceTransform* prefix_extractor,
                         bool load_essence_sst, int max_threads = 1);
  void UpgradeFileMetaData(const SliceTransform* prefix_extractor,
                           int max_threads = 1);

  void SetContext(VersionSet*);

 private:
  class Rep;
  Rep* rep_;
};

extern bool NewestFirstBySeqNo(FileMetaData* a, FileMetaData* b);
}  // namespace rocksdb
