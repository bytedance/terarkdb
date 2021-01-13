// Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License.

#pragma once

#include <memory>
#include <string>

#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "utilities/util/factory.h"

namespace rocksdb {

class Slice;
class Status;

struct TtlExtractorContext {
  // Which column family this compaction is for.
  uint32_t column_family_id;
  Env* env;
};

class TtlExtractor {
 public:
  virtual ~TtlExtractor(){};

  // Extract a custom info from a specified key value pair. This method is
  // called when we need get left ttl about key.
  virtual Status Extract(EntryType entry_type, const Slice& user_key,
                         const Slice& value_or_meta, bool* has_ttl,
                         std::chrono::seconds* ttl) const = 0;
};

class TtlExtractorFactory
    : public terark::Factoryable<TtlExtractorFactory*, Slice> {
 public:
  virtual ~TtlExtractorFactory() {}

  using TtlContext = TtlExtractorContext;

  virtual std::unique_ptr<TtlExtractor> CreateTtlExtractor(
      const TtlContext& context) const = 0;

  virtual const char* Name() const = 0;

  virtual Status Serialize(std::string* /*bytes*/) const {
    return Status::NotSupported();
  }
};

}  // namespace rocksdb
