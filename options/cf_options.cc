//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "options/cf_options.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include <cassert>
#include <limits>
#include <string>

#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

ImmutableCFOptions::ImmutableCFOptions(const Options& options)
    : ImmutableCFOptions(ImmutableDBOptions(options), options) {}

ImmutableCFOptions::ImmutableCFOptions(const ImmutableDBOptions& db_options,
                                       const ColumnFamilyOptions& cf_options)
    : compaction_style(cf_options.compaction_style),
      compaction_pri(cf_options.compaction_pri),
      user_comparator(cf_options.comparator),
      internal_comparator(InternalKeyComparator(cf_options.comparator)),
      merge_operator(cf_options.merge_operator.get()),
      value_meta_extractor_factory(
          cf_options.value_meta_extractor_factory.get()),
      ttl_extractor_factory(cf_options.ttl_extractor_factory.get()),
      compaction_filter(cf_options.compaction_filter),
      compaction_filter_factory(cf_options.compaction_filter_factory.get()),
      compaction_dispatcher(cf_options.compaction_dispatcher.get()),
      min_write_buffer_number_to_merge(
          cf_options.min_write_buffer_number_to_merge),
      max_write_buffer_number_to_maintain(
          cf_options.max_write_buffer_number_to_maintain),
      enable_lazy_compaction(cf_options.enable_lazy_compaction),
      pin_table_properties_in_reader(cf_options.pin_table_properties_in_reader),
      inplace_update_support(cf_options.inplace_update_support),
      inplace_callback(cf_options.inplace_callback),
      info_log(db_options.info_log.get()),
      statistics(db_options.statistics.get()),
      rate_limiter(db_options.rate_limiter.get()),
      info_log_level(db_options.info_log_level),
      env(db_options.env),
      allow_mmap_reads(db_options.allow_mmap_reads),
      allow_mmap_writes(db_options.allow_mmap_writes),
      db_paths(db_options.db_paths),
      memtable_factory(cf_options.memtable_factory.get()),
      atomic_flush_group(cf_options.atomic_flush_group.get()),
      table_factory(cf_options.table_factory.get()),
      table_properties_collector_factories(
          cf_options.table_properties_collector_factories),
      advise_random_on_open(db_options.advise_random_on_open),
      allow_mmap_populate(db_options.allow_mmap_populate),
      bloom_locality(cf_options.bloom_locality),
      purge_redundant_kvs_while_flush(
          cf_options.purge_redundant_kvs_while_flush),
      use_fsync(db_options.use_fsync),
      compression_per_level(cf_options.compression_per_level),
      bottommost_compression(cf_options.bottommost_compression),
      bottommost_compression_opts(cf_options.bottommost_compression_opts),
      compression_opts(cf_options.compression_opts),
      level_compaction_dynamic_level_bytes(
          cf_options.level_compaction_dynamic_level_bytes),
      access_hint_on_compaction_start(
          db_options.access_hint_on_compaction_start),
      new_table_reader_for_compaction_inputs(
          db_options.new_table_reader_for_compaction_inputs),
      num_levels(cf_options.num_levels),
      force_consistency_checks(cf_options.force_consistency_checks),
      allow_ingest_behind(db_options.allow_ingest_behind),
      preserve_deletes(db_options.preserve_deletes),
      listeners(db_options.listeners),
      row_cache(db_options.row_cache),
      memtable_insert_with_hint_prefix_extractor(
          cf_options.memtable_insert_with_hint_prefix_extractor.get()),
      cf_paths(cf_options.cf_paths) {
  if (ttl_extractor_factory != nullptr) {
    int_tbl_prop_collector_factories_for_blob = std::make_shared<
        std::vector<std::unique_ptr<IntTblPropCollectorFactory>>>();
    for (auto& f : table_properties_collector_factories) {
      int_tbl_prop_collector_factories_for_blob->emplace_back(
          new UserKeyTablePropertiesCollectorFactory(f));
    }
  }
}

// Multiple two operands. If they overflow, return op1.
uint64_t MultiplyCheckOverflow(uint64_t op1, double op2) {
  if (op1 == 0 || op2 <= 0) {
    return 0;
  }
  if (port::kMaxUint64 / op1 < op2) {
    return op1;
  }
  return static_cast<uint64_t>(op1 * op2);
}

// when level_compaction_dynamic_level_bytes is true and leveled compaction
// is used, the base level is not always L1, so precomupted max_file_size can
// no longer be used. Recompute file_size_for_level from base level.
uint64_t MaxFileSizeForLevel(const MutableCFOptions& cf_options, int level,
                             CompactionStyle compaction_style, int base_level,
                             bool level_compaction_dynamic_level_bytes) {
  if (!level_compaction_dynamic_level_bytes || level < base_level ||
      compaction_style != kCompactionStyleLevel) {
    if (cf_options.max_file_size.size() == 1 && level == 1) {
      level = 0;
    }
    assert(level >= 0);
    assert(level < (int)cf_options.max_file_size.size());
    return cf_options.max_file_size[level];
  } else {
    assert(level >= 0 && base_level >= 0);
    assert(level - base_level < (int)cf_options.max_file_size.size());
    return cf_options.max_file_size[std::max(1, level - base_level)];
  }
}

uint64_t MaxBlobSize(const MutableCFOptions& cf_options, int num_levels,
                     CompactionStyle compaction_style) {
  size_t target_blob_file_size = cf_options.target_blob_file_size;
  if (target_blob_file_size == 0) {
    target_blob_file_size =
        MaxFileSizeForLevel(cf_options, num_levels - 1, compaction_style);
  }
  return target_blob_file_size;
}

void MutableCFOptions::RefreshDerivedOptions(int num_levels) {
  max_file_size.resize(num_levels);
  max_file_size[0] = 0;  // unlimited
  if (num_levels > 1) {
    max_file_size[1] = target_file_size_base;
  }
  for (int i = 2; i < num_levels; ++i) {
    max_file_size[i] = MultiplyCheckOverflow(max_file_size[i - 1],
                                             target_file_size_multiplier);
  }
}

void MutableCFOptions::RefreshDerivedOptions(
    const ImmutableCFOptions& ioptions) {
  RefreshDerivedOptions(ioptions.num_levels);

  int_tbl_prop_collector_factories = std::make_shared<
      std::vector<std::unique_ptr<IntTblPropCollectorFactory>>>();
  for (auto& f : ioptions.table_properties_collector_factories) {
    int_tbl_prop_collector_factories->emplace_back(
        new UserKeyTablePropertiesCollectorFactory(f));
  }
  if (ioptions.ttl_extractor_factory != nullptr) {
    int_tbl_prop_collector_factories->emplace_back(
        NewTtlIntTblPropCollectorFactory(ioptions.ttl_extractor_factory,
                                         ttl_gc_ratio, ttl_max_scan_gap));
  }
}

void MutableCFOptions::Dump(Logger* log) const {
  // Memtable related options
  ROCKS_LOG_INFO(log,
                 "                        write_buffer_size: %" ROCKSDB_PRIszt,
                 write_buffer_size);
  ROCKS_LOG_INFO(log, "                  max_write_buffer_number: %d",
                 max_write_buffer_number);
  ROCKS_LOG_INFO(log,
                 "                         arena_block_size: %" ROCKSDB_PRIszt,
                 arena_block_size);
  ROCKS_LOG_INFO(log, "              memtable_prefix_bloom_ratio: %f",
                 memtable_prefix_bloom_size_ratio);
  ROCKS_LOG_INFO(log,
                 "                  memtable_huge_page_size: %" ROCKSDB_PRIszt,
                 memtable_huge_page_size);
  ROCKS_LOG_INFO(log,
                 "                    max_successive_merges: %" ROCKSDB_PRIszt,
                 max_successive_merges);
  ROCKS_LOG_INFO(log,
                 "                 inplace_update_num_locks: %" ROCKSDB_PRIszt,
                 inplace_update_num_locks);
  ROCKS_LOG_INFO(
      log, "                         prefix_extractor: %s",
      prefix_extractor == nullptr ? "nullptr" : prefix_extractor->Name());
  ROCKS_LOG_INFO(log, "                 disable_auto_compactions: %d",
                 disable_auto_compactions);
  ROCKS_LOG_INFO(log, "                       max_subcompactions: %u",
                 max_subcompactions);
  ROCKS_LOG_INFO(log, "                                blob_size: %zd",
                 blob_size);
  ROCKS_LOG_INFO(log, "                     blob_large_key_ratio: %f",
                 blob_large_key_ratio);
  ROCKS_LOG_INFO(log, "                            blob_gc_ratio: %f",
                 blob_gc_ratio);
  ROCKS_LOG_INFO(log, "                    target_blob_file_size: %" PRIu64,
                 target_blob_file_size);
  ROCKS_LOG_INFO(log, "                blob_file_defragment_size: %" PRIu64,
                 blob_file_defragment_size);
  ROCKS_LOG_INFO(log, "              max_dependence_blob_overlap: %zu",
                 max_dependence_blob_overlap);
  ROCKS_LOG_INFO(log, "                     maintainer_job_ratio: %f",
                 maintainer_job_ratio);
  ROCKS_LOG_INFO(log, "      soft_pending_compaction_bytes_limit: %" PRIu64,
                 soft_pending_compaction_bytes_limit);
  ROCKS_LOG_INFO(log, "      hard_pending_compaction_bytes_limit: %" PRIu64,
                 hard_pending_compaction_bytes_limit);
  ROCKS_LOG_INFO(log, "       level0_file_num_compaction_trigger: %d",
                 level0_file_num_compaction_trigger);
  ROCKS_LOG_INFO(log, "           level0_slowdown_writes_trigger: %d",
                 level0_slowdown_writes_trigger);
  ROCKS_LOG_INFO(log, "               level0_stop_writes_trigger: %d",
                 level0_stop_writes_trigger);
  ROCKS_LOG_INFO(log, "                     max_compaction_bytes: %" PRIu64,
                 max_compaction_bytes);
  ROCKS_LOG_INFO(log, "                    target_file_size_base: %" PRIu64,
                 target_file_size_base);
  ROCKS_LOG_INFO(log, "              target_file_size_multiplier: %d",
                 target_file_size_multiplier);
  ROCKS_LOG_INFO(log, "                 max_bytes_for_level_base: %" PRIu64,
                 max_bytes_for_level_base);
  ROCKS_LOG_INFO(log, "           max_bytes_for_level_multiplier: %f",
                 max_bytes_for_level_multiplier);
  ROCKS_LOG_INFO(log, "                             ttl_gc_ratio: %f",
                 ttl_gc_ratio);
  ROCKS_LOG_INFO(log, "                         ttl_max_scan_gap: %zd",
                 ttl_max_scan_gap);
  std::string result;
  char buf[10];
  for (const auto m : max_bytes_for_level_multiplier_additional) {
    snprintf(buf, sizeof(buf), "%d, ", m);
    result += buf;
  }
  if (result.size() >= 2) {
    result.resize(result.size() - 2);
  } else {
    result = "";
  }

  ROCKS_LOG_INFO(log, "max_bytes_for_level_multiplier_additional: %s",
                 result.c_str());
  ROCKS_LOG_INFO(log, "        max_sequential_skip_in_iterations: %" PRIu64,
                 max_sequential_skip_in_iterations);
  ROCKS_LOG_INFO(log, "                     paranoid_file_checks: %d",
                 paranoid_file_checks);
  ROCKS_LOG_INFO(log, "                       report_bg_io_stats: %d",
                 report_bg_io_stats);
  ROCKS_LOG_INFO(log, "                optimize_filters_for_hits: %d",
                 optimize_filters_for_hits);
  ROCKS_LOG_INFO(log, "                  optimize_range_deletion: %d",
                 optimize_range_deletion);
  ROCKS_LOG_INFO(log, "                              compression: %d",
                 static_cast<int>(compression));

  // Universal Compaction Options
  ROCKS_LOG_INFO(log, "compaction_options_universal.size_ratio : %d",
                 compaction_options_universal.size_ratio);
  ROCKS_LOG_INFO(log, "compaction_options_universal.min_merge_width : %d",
                 compaction_options_universal.min_merge_width);
  ROCKS_LOG_INFO(log, "compaction_options_universal.max_merge_width : %d",
                 compaction_options_universal.max_merge_width);
  ROCKS_LOG_INFO(
      log, "compaction_options_universal.max_size_amplification_percent : %d",
      compaction_options_universal.max_size_amplification_percent);
  ROCKS_LOG_INFO(log,
                 "compaction_options_universal.compression_size_percent : %d",
                 compaction_options_universal.compression_size_percent);
  ROCKS_LOG_INFO(log, "compaction_options_universal.stop_style : %d",
                 compaction_options_universal.stop_style);
  ROCKS_LOG_INFO(
      log, "compaction_options_universal.allow_trivial_move : %d",
      static_cast<int>(compaction_options_universal.allow_trivial_move));
}

MutableCFOptions::MutableCFOptions(const ColumnFamilyOptions& options, Env* env)
    : write_buffer_size(options.write_buffer_size),
      max_write_buffer_number(options.max_write_buffer_number),
      arena_block_size(options.arena_block_size),
      memtable_factory(options.memtable_factory),
      memtable_prefix_bloom_size_ratio(
          options.memtable_prefix_bloom_size_ratio),
      memtable_huge_page_size(options.memtable_huge_page_size),
      max_successive_merges(options.max_successive_merges),
      inplace_update_num_locks(options.inplace_update_num_locks),
      prefix_extractor(options.prefix_extractor),
      disable_auto_compactions(options.disable_auto_compactions),
      max_subcompactions(options.max_subcompactions),
      blob_size(options.blob_size),
      blob_large_key_ratio(options.blob_large_key_ratio),
      blob_gc_ratio(options.blob_gc_ratio),
      target_blob_file_size(options.target_blob_file_size),
      blob_file_defragment_size(options.blob_file_defragment_size),
      max_dependence_blob_overlap(options.max_dependence_blob_overlap),
      maintainer_job_ratio(options.maintainer_job_ratio),
      soft_pending_compaction_bytes_limit(
          options.soft_pending_compaction_bytes_limit),
      hard_pending_compaction_bytes_limit(
          options.hard_pending_compaction_bytes_limit),
      level0_file_num_compaction_trigger(
          options.level0_file_num_compaction_trigger),
      level0_slowdown_writes_trigger(options.level0_slowdown_writes_trigger),
      level0_stop_writes_trigger(options.level0_stop_writes_trigger),
      max_compaction_bytes(options.max_compaction_bytes),
      target_file_size_base(options.target_file_size_base),
      target_file_size_multiplier(options.target_file_size_multiplier),
      max_bytes_for_level_base(options.max_bytes_for_level_base),
      max_bytes_for_level_multiplier(options.max_bytes_for_level_multiplier),
      max_bytes_for_level_multiplier_additional(
          options.max_bytes_for_level_multiplier_additional),
      compaction_options_universal(options.compaction_options_universal),
      max_sequential_skip_in_iterations(
          options.max_sequential_skip_in_iterations),
      paranoid_file_checks(options.paranoid_file_checks),
      report_bg_io_stats(options.report_bg_io_stats),
      optimize_filters_for_hits(options.optimize_filters_for_hits),
      optimize_range_deletion(options.optimize_range_deletion),
      compression(options.compression),
      ttl_gc_ratio(options.ttl_gc_ratio),
      ttl_max_scan_gap(options.ttl_max_scan_gap) {
  RefreshDerivedOptions(options.num_levels);

  int_tbl_prop_collector_factories = std::make_shared<
      std::vector<std::unique_ptr<IntTblPropCollectorFactory>>>();
  for (auto& f : options.table_properties_collector_factories) {
    int_tbl_prop_collector_factories->emplace_back(
        new UserKeyTablePropertiesCollectorFactory(f));
  }
  if (options.ttl_extractor_factory != nullptr) {
    int_tbl_prop_collector_factories->emplace_back(
        NewTtlIntTblPropCollectorFactory(options.ttl_extractor_factory.get(),
                                         ttl_gc_ratio, ttl_max_scan_gap));
  }
}

MutableCFOptions::MutableCFOptions(const Options& options)
    : MutableCFOptions(ColumnFamilyOptions(options), options.env) {}

}  // namespace TERARKDB_NAMESPACE
