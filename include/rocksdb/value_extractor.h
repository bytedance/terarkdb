#pragma once

#include <string>
#include <terark/util/factory.hpp>

namespace rocksdb {

class Slice;
class Status;

struct ValueExtractorContext {
  // Which column family this compaction is for.
  uint32_t column_family_id;
};

class ValueExtractor {
 public:
  virtual ~ValueExtractor(){};

  // Extract a custom info from a specified key value pair. This method is
  // called when a value will trans to separate.
  virtual Status Extract(const Slice& key, const Slice& value,
                         std::string* output) const = 0;
};

class ValueExtractorFactory
    : public terark::Factoryable<ValueExtractorFactory*, Slice> {
 public:
  virtual ~ValueExtractorFactory() {}

  using Context = ValueExtractorContext;

  virtual std::unique_ptr<ValueExtractor> CreateValueExtractor(
      const Context& context) const = 0;

  // Returns a name that identifies this value extractor factory.
  virtual const char* Name() const = 0;

  virtual Status Serialize(std::string* /*bytes*/) const {
    return Status::NotSupported();
  }
};

}  // namespace rocksdb
