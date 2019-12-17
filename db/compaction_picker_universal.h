//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#ifndef ROCKSDB_LITE

#include "db/compaction_picker.h"

namespace rocksdb {
class UniversalCompactionPicker : public CompactionPicker {
 public:
  UniversalCompactionPicker(TableCache* table_cache,
                            const EnvOptions& env_options,
                            const ImmutableCFOptions& ioptions,
                            const InternalKeyComparator* icmp)
      : CompactionPicker(table_cache, env_options, ioptions, icmp) {}
  Compaction* PickCompaction(const std::string& cf_name,
                             const MutableCFOptions& mutable_cf_options,
                             VersionStorageInfo* vstorage,
                             const std::vector<SequenceNumber>& snapshots,
                             LogBuffer* log_buffer) override;

  Compaction* CompactRange(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, int input_level, int output_level,
      uint32_t output_path_id, uint32_t max_subcompactions,
      const InternalKey* begin, const InternalKey* end,
      InternalKey** compaction_end, bool* manual_conflict,
      const std::unordered_set<uint64_t>* files_being_compact,
      bool enable_lazy_compaction) override;

  int MaxOutputLevel() const override { return NumberLevels() - 1; }

  bool NeedsCompaction(const VersionStorageInfo* vstorage) const override;

 private:
  // Pick Universal compaction to limit read amplification
  Compaction* PickCompactionToReduceSortedRunsOld(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, double score, unsigned int ratio,
      unsigned int num_files, const std::vector<SortedRun>& sorted_runs,
      LogBuffer* log_buffer);

  // Pick Universal compaction to limit space amplification.
  Compaction* PickCompactionToReduceSizeAmp(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, double score,
      const std::vector<SortedRun>& sorted_runs, LogBuffer* log_buffer);

  Compaction* PickDeleteTriggeredCompaction(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, double score,
      const std::vector<SortedRun>& sorted_runs, LogBuffer* log_buffer);

  Compaction* PickTrivialMoveCompaction(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, const std::vector<SortedRun>& sorted_runs,
      LogBuffer* log_buffer);

  // Pick Universal compaction to limit read amplification
  Compaction* PickCompactionToReduceSortedRuns(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, double score,
      std::vector<SortedRun>* sorted_runs, size_t reduce_sorted_run_target,
      LogBuffer* log_buffer);

  // Used in universal compaction when the enabled_trivial_move
  // option is set. Checks whether there are any overlapping files
  // in the input. Returns true if the input files are non
  // overlapping.
  bool IsInputFilesNonOverlapping(Compaction* c);

  static std::vector<SortedRun> CalculateSortedRuns(
      const VersionStorageInfo& vstorage, const ImmutableCFOptions& ioptions,
      const MutableCFOptions& mutable_cf_options);

  // Pick a path ID to place a newly generated file, with its estimated file
  // size.
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            uint64_t file_size);
};
}  // namespace rocksdb
#endif  // !ROCKSDB_LITE
