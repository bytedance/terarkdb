// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <stdint.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/status.h"
#include "rocksdb/terark_namespace.h"
#include "rocksdb/types.h"
#include "utilities/util/factory.h"

namespace TERARKDB_NAMESPACE {

// -- Table Properties
// Other than basic table properties, each table may also have the user
// collected properties.
// The value of the user-collected properties are encoded as raw bytes --
// users have to interpret these values by themselves.
// Note: To do prefix seek/scan in `UserCollectedProperties`, you can do
// something similar to:
//
// UserCollectedProperties props = ...;
// for (auto pos = props.lower_bound(prefix);
//      pos != props.end() && pos->first.compare(0, prefix.size(), prefix) == 0;
//      ++pos) {
//   ...
// }
typedef std::map<std::string, std::string> UserCollectedProperties;

// table properties' human-readable names in the property block.
struct TablePropertiesNames {
  static const std::string kDataSize;
  static const std::string kIndexSize;
  static const std::string kIndexPartitions;
  static const std::string kTopLevelIndexSize;
  static const std::string kIndexKeyIsUserKey;
  static const std::string kIndexValueIsDeltaEncoded;
  static const std::string kFilterSize;
  static const std::string kRawKeySize;
  static const std::string kRawValueSize;
  static const std::string kNumDataBlocks;
  static const std::string kNumEntries;
  static const std::string kDeletedKeys;
  static const std::string kMergeOperands;
  static const std::string kNumRangeDeletions;
  static const std::string kFormatVersion;
  static const std::string kFixedKeyLen;
  static const std::string kFilterPolicy;
  static const std::string kColumnFamilyName;
  static const std::string kColumnFamilyId;
  static const std::string kComparator;
  static const std::string kMergeOperator;
  static const std::string kValueMetaExtractorName;
  static const std::string kPrefixExtractorName;
  static const std::string kPropertyCollectors;
  static const std::string kCompression;
  static const std::string kCreationTime;
  static const std::string kOldestKeyTime;
  static const std::string kSnapshots;
  static const std::string kPurpose;
  static const std::string kReadAmp;
  static const std::string kDependence;
  static const std::string kDependenceEntryCount;
  static const std::string kInheritanceChain;
  static const std::string kEarliestTimeBeginCompact;
  static const std::string kLatestTimeEndCompact;
};

extern const std::string kPropertiesBlock;
extern const std::string kCompressionDictBlock;
extern const std::string kRangeDelBlock;

// `TablePropertiesCollector` provides the mechanism for users to collect
// their own properties that they are interested in. This class is essentially
// a collection of callback functions that will be invoked during table
// building. It is constructed with TablePropertiesCollectorFactory. The methods
// don't need to be thread-safe, as we will create exactly one
// TablePropertiesCollector object per table and then call it sequentially
class TablePropertiesCollector {
 public:
  virtual ~TablePropertiesCollector() {}

  // DEPRECATE User defined collector should implement AddUserKey(), though
  //           this old function still works for backward compatible reason.
  // Add() will be called when a new key/value pair is inserted into the table.
  // @params key    the user key that is inserted into the table.
  // @params value  the value that is inserted into the table.
  virtual Status Add(const Slice& /*key*/, const Slice& /*value*/) {
    return Status::InvalidArgument(
        "TablePropertiesCollector::Add() deprecated.");
  }

  // AddUserKey() will be called when a new key/value pair is inserted into the
  // table.
  // @params key    the user key that is inserted into the table.
  // @params value  the value that is inserted into the table.
  virtual Status AddUserKey(const Slice& key, const Slice& value,
                            EntryType /*type*/, SequenceNumber /*seq*/,
                            uint64_t /*file_size*/) {
    // For backwards-compatibility.
    return Add(key, value);
  }

  // Finish() will be called when a table has already been built and is ready
  // for writing the properties block.
  // @params properties  User will add their collected statistics to
  // `properties`.
  virtual Status Finish(UserCollectedProperties* properties) = 0;

  // Return the human-readable properties, where the key is property name and
  // the value is the human-readable form of value.
  virtual UserCollectedProperties GetReadableProperties() const = 0;

  // The name of the properties collector can be used for debugging purpose.
  virtual const char* Name() const = 0;

  // EXPERIMENTAL Return whether the output file should be further compacted
  virtual bool NeedCompact() const { return false; }
};

// Constructs TablePropertiesCollector. Internals create a new
// TablePropertiesCollector for each new table
class TablePropertiesCollectorFactory
    : public std::enable_shared_from_this<TablePropertiesCollectorFactory>,
      public terark::Factoryable<TablePropertiesCollectorFactory*> {
 public:
  struct Context {
    uint32_t column_family_id;
    Slice smallest_user_key;
    Slice largest_user_key;
    static const uint32_t kUnknownColumnFamily;
  };

  virtual ~TablePropertiesCollectorFactory() {}
  // has to be thread-safe
  virtual TablePropertiesCollector* CreateTablePropertiesCollector(Context) = 0;

  virtual bool NeedSerialize() const { return false; }

  // Maybe Serialize a part of meta data
  virtual Status Serialize(std::string*, Context) const {
    return Status::NotSupported("Serialize()", this->Name());
  }
  virtual Status Deserialize(Slice) {
    return Status::NotSupported("Deserialize()", this->Name());
  }

  // The name of the properties collector can be used for debugging purpose.
  virtual const char* Name() const = 0;
};

// TableProperties contains a bunch of read-only properties of its associated
// table.
struct TablePropertiesBase {
  // the total size of all data blocks.
  uint64_t data_size = 0;
  // the size of index block.
  uint64_t index_size = 0;
  // Total number of index partitions if kTwoLevelIndexSearch is used
  uint64_t index_partitions = 0;
  // Size of the top-level index if kTwoLevelIndexSearch is used
  uint64_t top_level_index_size = 0;
  // Whether the index key is user key. Otherwise it includes 8 byte of sequence
  // number added by internal key format.
  uint64_t index_key_is_user_key = 0;
  // Whether delta encoding is used to encode the index values.
  uint64_t index_value_is_delta_encoded = 0;
  // the size of filter block.
  uint64_t filter_size = 0;
  // total raw key size
  uint64_t raw_key_size = 0;
  // total raw value size
  uint64_t raw_value_size = 0;
  // the number of blocks in this table
  uint64_t num_data_blocks = 0;
  // the number of entries in this table
  uint64_t num_entries = 0;
  // the number of deletions in the table
  uint64_t num_deletions = 0;
  // the number of merge operands in the table
  uint64_t num_merge_operands = 0;
  // the number of range deletions in this table
  uint64_t num_range_deletions = 0;
  // format version, reserved for backward compatibility
  uint64_t format_version = 0;
  // If 0, key is variable length. Otherwise number of bytes for each key.
  uint64_t fixed_key_len = 0;
  // ID of column family for this SST file, corresponding to the CF identified
  // by column_family_name.
  uint64_t column_family_id = TERARKDB_NAMESPACE::
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily;
  // The time when the SST file was created.
  // Since SST files are immutable, this is equivalent to last modified time.
  uint64_t creation_time = 0;
  // Timestamp of the earliest key. 0 means unknown.
  uint64_t oldest_key_time = 0;

  // Name of the column family with which this SST file is associated.
  // If column family is unknown, `column_family_name` will be an empty string.
  std::string column_family_name;

  // The name of the filter policy used in this table.
  // If no filter policy is used, `filter_policy_name` will be an empty string.
  std::string filter_policy_name;

  // The name of the comparator used in this table.
  std::string comparator_name;

  // The name of the merge operator used in this table.
  // If no merge operator is used, `merge_operator_name` will be "nullptr".
  std::string merge_operator_name;

  // The name of the value meta extractor used in this table.
  // If no value meta extractor is used, `value_meta_extractor_name` will be
  // "nullptr".
  std::string value_meta_extractor_name;

  // The name of the ttl extractor used in this table.
  // If no ttl extractor is used, `ttl_extractor_name` will be
  // "nullptr".
  std::string ttl_extractor_name;

  // The name of the prefix extractor used in this table
  // If no prefix extractor is used, `prefix_extractor_name` will be "nullptr".
  std::string prefix_extractor_name;

  // The names of the property collectors factories used in this table
  // separated by commas
  // {collector_name[1]},{collector_name[2]},{collector_name[3]} ..
  std::string property_collectors_names;

  // The compression algo used to compress the SST files.
  std::string compression_name;

  // Snapshots
  std::vector<SequenceNumber> snapshots;

  // Zero for essence sst
  uint8_t purpose = 0;

  // Max Read amp from sst
  uint16_t max_read_amp = 1;

  // Expt read amp from sst
  float read_amp = 1;

  // Make these sst hidden
  std::vector<Dependence> dependence;

  // Inheritance chain
  std::vector<uint64_t> inheritance_chain;

  // convert this object to a human readable form
  //   @prop_delim: delimiter for each property.
  std::string ToString(const std::string& prop_delim = "; ",
                       const std::string& kv_delim = "=") const;
};

struct TableProperties : public TablePropertiesBase {
  // user collected properties
  UserCollectedProperties user_collected_properties;
  UserCollectedProperties readable_properties;

  // Aggregate the numerical member variables of the specified
  // TableProperties.
  void Add(const TableProperties& tp);
};

// Extra properties
// Below is a list of non-basic properties that are collected by database
// itself. Especially some properties regarding to the internal keys (which
// is unknown to `table`).
//
// DEPRECATED: these properties now belong as TableProperties members. Please
// use TableProperties::num_deletions and TableProperties::num_merge_operands,
// respectively.
extern uint64_t GetDeletedKeys(const UserCollectedProperties& props);
extern uint64_t GetMergeOperands(const UserCollectedProperties& props,
                                 bool* property_present);
extern void GetCompactionTimePoint(const UserCollectedProperties& props,
                                   uint64_t* earliest_time_begin_compact,
                                   uint64_t* latest_time_end_compact);

}  // namespace TERARKDB_NAMESPACE
