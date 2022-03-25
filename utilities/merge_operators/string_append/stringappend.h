/**
 * A MergeOperator for rocksdb that implements string append.
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#pragma once
#include "terarkdb/merge_operator.h"
#include "terarkdb/slice.h"
#include "terarkdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

class StringAppendOperator : public AssociativeMergeOperator {
 public:
  // Constructor: specify delimiter
  explicit StringAppendOperator(char delim_char);

  virtual bool Merge(const Slice& key, const Slice* existing_value,
                     const Slice& value, std::string* new_value,
                     Logger* logger) const override;

  virtual const char* Name() const override;

 private:
  char delim_;  // The delimiter is inserted between elements
};

}  // namespace TERARKDB_NAMESPACE
