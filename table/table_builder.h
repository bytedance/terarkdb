//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <stdint.h>

#include <string>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "db/table_properties_collector.h"
#include "options/cf_options.h"
#include "rocksdb/options.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/terark_namespace.h"
#include "table/internal_iterator.h"
#include "util/file_reader_writer.h"

namespace TERARKDB_NAMESPACE {

class Slice;
class Status;
struct TablePropertyCache;

struct TableReaderOptions {
  // @param skip_filters Disables loading/accessing the filter block
  TableReaderOptions(const ImmutableCFOptions& _ioptions,
                     const SliceTransform* _prefix_extractor,
                     const EnvOptions& _env_options,
                     const InternalKeyComparator& _internal_comparator,
                     bool _skip_filters = false, bool _immortal = false,
                     int _level = -1, uint64_t _file_number = uint64_t(-1))
      : TableReaderOptions(_ioptions, _prefix_extractor, _env_options,
                           _internal_comparator, _skip_filters, _immortal,
                           _level, _file_number, 0 /* _largest_seqno */) {}

  // @param skip_filters Disables loading/accessing the filter block
  TableReaderOptions(const ImmutableCFOptions& _ioptions,
                     const SliceTransform* _prefix_extractor,
                     const EnvOptions& _env_options,
                     const InternalKeyComparator& _internal_comparator,
                     bool _skip_filters, bool _immortal, int _level,
                     uint64_t _file_number, SequenceNumber _largest_seqno)
      : ioptions(_ioptions),
        prefix_extractor(_prefix_extractor),
        env_options(_env_options),
        internal_comparator(_internal_comparator),
        skip_filters(_skip_filters),
        immortal(_immortal),
        level(_level),
        file_number(_file_number),
        largest_seqno(_largest_seqno) {}

  const ImmutableCFOptions& ioptions;
  const SliceTransform* prefix_extractor;
  const EnvOptions& env_options;
  const InternalKeyComparator& internal_comparator;
  // This is only used for BlockBasedTable (reader)
  bool skip_filters;
  // Whether the table will be valid as long as the DB is open
  bool immortal;
  // what level this table/file is on, -1 for "not set, don't know"
  int level;
  // file number of current sst
  uint64_t file_number;
  // largest seqno in the table
  SequenceNumber largest_seqno;
};

struct TableBuilderOptions {
  TableBuilderOptions(
      const ImmutableCFOptions& _ioptions, const MutableCFOptions& _moptions,
      const InternalKeyComparator& _internal_comparator,
      const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
          _int_tbl_prop_collector_factories,
      CompressionType _compression_type,
      const CompressionOptions& _compression_opts,
      const std::string* _compression_dict, bool _skip_filters,
      const std::string& _column_family_name, int _level,
      double _compaction_load, uint64_t _creation_time = 0,
      int64_t _oldest_key_time = 0, SstPurpose _sst_purpose = kEssenceSst)
      : ioptions(_ioptions),
        moptions(_moptions),
        internal_comparator(_internal_comparator),
        int_tbl_prop_collector_factories(_int_tbl_prop_collector_factories),
        compression_type(_compression_type),
        compression_opts(_compression_opts),
        compression_dict(_compression_dict),
        skip_filters(_skip_filters),
        column_family_name(_column_family_name),
        level(_level),
        compaction_load(_compaction_load),
        creation_time(_creation_time),
        oldest_key_time(_oldest_key_time),
        sst_purpose(_sst_purpose) {}
  const ImmutableCFOptions& ioptions;
  const MutableCFOptions& moptions;
  const InternalKeyComparator& internal_comparator;
  const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
      int_tbl_prop_collector_factories;
  CompressionType compression_type;
  const CompressionOptions& compression_opts;
  // Data for presetting the compression library's dictionary, or nullptr.
  const std::string* compression_dict;
  bool skip_filters;  // only used by BlockBasedTableBuilder
  // Ignore key type, force store all keys, no tombstones
  const std::string& column_family_name;
  int level;  // what level this table/file is on, -1 for "not set, don't know"
  const double compaction_load;
  const uint64_t creation_time;
  const int64_t oldest_key_time;
  const SstPurpose sst_purpose;
  Slice smallest_user_key;
  Slice largest_user_key;

  void PushIntTblPropCollectors(
      std::vector<std::unique_ptr<IntTblPropCollector>>* collectors,
      uint32_t cf_id) const {
    if (!int_tbl_prop_collector_factories) {
      return;
    }
    collectors->reserve(collectors->size() +
                        int_tbl_prop_collector_factories->size() + 2);
    TablePropertiesCollectorFactory::Context ctx;
    ctx.column_family_id = cf_id;
    ctx.smallest_user_key = smallest_user_key;
    ctx.largest_user_key = largest_user_key;
    for (auto& collector_factories : *int_tbl_prop_collector_factories) {
      collectors->emplace_back(
          collector_factories->CreateIntTblPropCollector(ctx));
    }
  }
};

// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.
class TableBuilder {
 public:
  // REQUIRES: Either Finish() or Abandon() has been called.
  virtual ~TableBuilder() {}

  virtual void SetSecondPassIterator(InternalIterator*) {}

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual Status Add(const Slice& key, const LazyBuffer& value) = 0;

  virtual Status AddTombstone(const Slice& /*key*/,
                              const LazyBuffer& /*value*/) {
    return Status::NotSupported();
  }

  // Finish building the table.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual Status Finish(
      const TablePropertyCache* prop, const std::vector<uint64_t>* snapshots,
      const std::vector<uint64_t>* inheritance_tree = nullptr) = 0;

  // Indicate that the contents of this builder should be abandoned.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual void Abandon() = 0;

  // Number of calls to Add() so far.
  virtual uint64_t NumEntries() const = 0;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  virtual uint64_t FileSize() const = 0;

  // If the user defined table properties collector suggest the file to
  // be further compacted.
  virtual bool NeedCompact() const { return false; }

  // Returns table properties
  virtual TableProperties GetTableProperties() const = 0;
};

}  // namespace TERARKDB_NAMESPACE
