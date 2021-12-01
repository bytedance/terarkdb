// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/table_properties_collector.h"
#include "options/db_options.h"
#include "rocksdb/options.h"
#include "rocksdb/terark_namespace.h"
#include "util/compression.h"

namespace TERARKDB_NAMESPACE {

// ImmutableCFOptions is a data struct used by RocksDB internal. It contains a
// subset of Options that should not be changed during the entire lifetime
// of DB. Raw pointers defined in this struct do not have ownership to the data
// they point to. Options contains std::shared_ptr to these data.
struct ImmutableCFOptions {
  ImmutableCFOptions();
  explicit ImmutableCFOptions(const Options& options);

  ImmutableCFOptions(const ImmutableDBOptions& db_options,
                     const ColumnFamilyOptions& cf_options);

  CompactionStyle compaction_style;

  CompactionPri compaction_pri;

  const Comparator* user_comparator;
  InternalKeyComparator internal_comparator;

  MergeOperator* merge_operator;

  const ValueExtractorFactory* value_meta_extractor_factory;

  const TtlExtractorFactory* ttl_extractor_factory;

  const CompactionFilter* compaction_filter;

  CompactionFilterFactory* compaction_filter_factory;

  CompactionDispatcher* compaction_dispatcher;

  int min_write_buffer_number_to_merge;

  int max_write_buffer_number_to_maintain;

  bool enable_lazy_compaction;

  bool pin_table_properties_in_reader;

  bool inplace_update_support;

  UpdateStatus (*inplace_callback)(char* existing_value,
                                   uint32_t* existing_value_size,
                                   Slice delta_value,
                                   std::string* merged_value);

  Logger* info_log;

  Statistics* statistics;

  RateLimiter* rate_limiter;

  InfoLogLevel info_log_level;

  Env* env;

  // Allow the OS to mmap file for reading sst tables. Default: false
  bool allow_mmap_reads;

  // Allow the OS to mmap file for writing. Default: false
  bool allow_mmap_writes;

  std::vector<DbPath> db_paths;

  MemTableRepFactory* memtable_factory;

  AtomicFlushGroup* atomic_flush_group;

  TableFactory* table_factory;

  Options::TablePropertiesCollectorFactories
      table_properties_collector_factories;

  bool advise_random_on_open;

  bool allow_mmap_populate;

  // This options is required by PlainTableReader. May need to move it
  // to PlainTableOptions just like bloom_bits_per_key
  uint32_t bloom_locality;

  bool purge_redundant_kvs_while_flush;

  bool use_fsync;

  std::vector<CompressionType> compression_per_level;

  CompressionType bottommost_compression;

  CompressionOptions bottommost_compression_opts;

  CompressionOptions compression_opts;

  bool level_compaction_dynamic_level_bytes;

  Options::AccessHint access_hint_on_compaction_start;

  bool new_table_reader_for_compaction_inputs;

  int num_levels;

  bool force_consistency_checks;

  bool allow_ingest_behind;

  bool preserve_deletes;

  // A vector of EventListeners which callback functions will be called
  // when specific RocksDB event happens.
  std::vector<std::shared_ptr<EventListener>> listeners;

  std::shared_ptr<Cache> row_cache;

  const SliceTransform* memtable_insert_with_hint_prefix_extractor;

  std::vector<DbPath> cf_paths;

  std::shared_ptr<std::vector<std::unique_ptr<IntTblPropCollectorFactory>>>
      int_tbl_prop_collector_factories_for_blob;
};

struct BlobConfig {
  size_t blob_size;
  double large_key_ratio;
};

struct MutableCFOptions {
  explicit MutableCFOptions(const ColumnFamilyOptions& options, Env* env);

  MutableCFOptions()
      : write_buffer_size(0),
        max_write_buffer_number(0),
        arena_block_size(0),
        memtable_prefix_bloom_size_ratio(0),
        memtable_huge_page_size(0),
        max_successive_merges(0),
        inplace_update_num_locks(0),
        prefix_extractor(nullptr),
        disable_auto_compactions(false),
        max_subcompactions(0),
        blob_size(0),
        blob_large_key_ratio(0),
        blob_gc_ratio(0),
        target_blob_file_size(0),
        blob_file_defragment_size(0),
        max_dependence_blob_overlap(0),
        maintainer_job_ratio(0),
        soft_pending_compaction_bytes_limit(0),
        hard_pending_compaction_bytes_limit(0),
        level0_file_num_compaction_trigger(0),
        level0_slowdown_writes_trigger(0),
        level0_stop_writes_trigger(0),
        max_compaction_bytes(0),
        target_file_size_base(0),
        target_file_size_multiplier(0),
        max_bytes_for_level_base(0),
        max_bytes_for_level_multiplier(0),
        max_sequential_skip_in_iterations(0),
        paranoid_file_checks(false),
        report_bg_io_stats(false),
        optimize_filters_for_hits(false),
        optimize_range_deletion(false),
        compression(Snappy_Supported() ? kSnappyCompression : kNoCompression),
        ttl_gc_ratio(1.000),
        ttl_max_scan_gap(0) {}

  explicit MutableCFOptions(const Options& options);

  BlobConfig get_blob_config() const {
    return BlobConfig{blob_size, blob_large_key_ratio};
  }

  // Must be called after any change to MutableCFOptions
  void RefreshDerivedOptions(int num_levels);

  void RefreshDerivedOptions(const ImmutableCFOptions& ioptions);

  int MaxBytesMultiplerAdditional(int level) const {
    if (level >=
        static_cast<int>(max_bytes_for_level_multiplier_additional.size())) {
      return 1;
    }
    return max_bytes_for_level_multiplier_additional[level];
  }

  void Dump(Logger* log) const;

  // Memtable related options
  size_t write_buffer_size;
  int max_write_buffer_number;
  size_t arena_block_size;
  std::shared_ptr<MemTableRepFactory> memtable_factory;
  double memtable_prefix_bloom_size_ratio;
  size_t memtable_huge_page_size;
  size_t max_successive_merges;
  size_t inplace_update_num_locks;
  std::shared_ptr<const SliceTransform> prefix_extractor;

  // Compaction related options
  bool disable_auto_compactions;
  uint32_t max_subcompactions;
  size_t blob_size;
  double blob_large_key_ratio;
  double blob_gc_ratio;
  uint64_t target_blob_file_size;
  uint64_t blob_file_defragment_size;
  size_t max_dependence_blob_overlap;
  double maintainer_job_ratio;
  uint64_t soft_pending_compaction_bytes_limit;
  uint64_t hard_pending_compaction_bytes_limit;
  int level0_file_num_compaction_trigger;
  int level0_slowdown_writes_trigger;
  int level0_stop_writes_trigger;
  uint64_t max_compaction_bytes;
  uint64_t target_file_size_base;
  int target_file_size_multiplier;
  uint64_t max_bytes_for_level_base;
  double max_bytes_for_level_multiplier;
  std::vector<int> max_bytes_for_level_multiplier_additional;
  CompactionOptionsUniversal compaction_options_universal;

  // Misc options
  uint64_t max_sequential_skip_in_iterations;
  bool paranoid_file_checks;
  bool report_bg_io_stats;

  bool optimize_filters_for_hits;
  bool optimize_range_deletion;
  CompressionType compression;

  // Derived options
  // Per-level target file size.
  std::vector<uint64_t> max_file_size;

  double ttl_gc_ratio;
  size_t ttl_max_scan_gap;

  std::shared_ptr<std::vector<std::unique_ptr<IntTblPropCollectorFactory>>>
      int_tbl_prop_collector_factories;
};

uint64_t MultiplyCheckOverflow(uint64_t op1, double op2);

// Get the max file size in a given level.
uint64_t MaxFileSizeForLevel(const MutableCFOptions& cf_options, int level,
                             CompactionStyle compaction_style,
                             int base_level = 1,
                             bool level_compaction_dynamic_level_bytes = false);

uint64_t MaxBlobSize(const MutableCFOptions& cf_options, int num_levels,
                     CompactionStyle compaction_style);

}  // namespace TERARKDB_NAMESPACE
