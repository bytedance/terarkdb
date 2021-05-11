// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/cassandra/cassandra_compaction_filter.h"

#include <string>

#include "rocksdb/slice.h"
#include "rocksdb/terark_namespace.h"
#include "utilities/cassandra/format.h"

namespace TERARKDB_NAMESPACE {
namespace cassandra {

const char* CassandraCompactionFilter::Name() const {
  return "CassandraCompactionFilter";
}

CompactionFilter::Decision CassandraCompactionFilter::FilterV2(
    int /*level*/, const Slice& /*key*/, ValueType value_type,
    const Slice& /*existing_value_meta*/, const LazyBuffer& existing_value,
    LazyBuffer* new_value, std::string* /*skip_until*/) const {
  bool value_changed = false;
  auto s = existing_value.fetch();
  if (!s.ok()) {
    new_value->reset(std::move(s));
    return Decision::kChangeValue;
  }
  RowValue row_value =
      RowValue::Deserialize(existing_value.data(), existing_value.size());
  RowValue compacted =
      purge_ttl_on_expiration_
          ? row_value.RemoveExpiredColumns(&value_changed)
          : row_value.ConvertExpiredColumnsToTombstones(&value_changed);

  if (value_type == ValueType::kValue) {
    compacted = compacted.RemoveTombstones(gc_grace_period_in_seconds_);
  }

  if (compacted.Empty()) {
    return Decision::kRemove;
  }

  if (value_changed) {
    compacted.Serialize(new_value->trans_to_string());
    return Decision::kChangeValue;
  }

  return Decision::kKeep;
}

}  // namespace cassandra
}  // namespace TERARKDB_NAMESPACE
