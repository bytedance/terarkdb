/**
 * A TEST MergeOperator for rocksdb that implements string append.
 * It is built using the MergeOperator interface rather than the simpler
 * AssociativeMergeOperator interface. This is useful for testing/benchmarking.
 * While the two operators are semantically the same, all production code
 * should use the StringAppendOperator defined in stringappend.{h,cc}. The
 * operator defined in the present file is primarily for testing.
 *
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#pragma once
#include <deque>
#include <string>
#include <utility>

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

namespace rocksdb {

class StringAppendTESTOperator : public MergeOperator {
 public:
  // Constructor with delimiter
  explicit StringAppendTESTOperator(char delim_char): delim_(std::string(1, delim_char)) {};

  explicit StringAppendTESTOperator(std::string delim_str) : delim_(std::move(delim_str)) {};

  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override;

  bool PartialMergeMulti(const Slice& key,
                         const std::vector<LazyBuffer>& operand_list,
                         LazyBuffer* new_value, Logger* logger) const override;

  const char* Name() const override;

 protected:
  // A version of PartialMerge that actually performs "partial merging".
  // Use this to simulate the exact behaviour of the StringAppendOperator.
  bool _AssocPartialMergeMulti(const Slice& key,
                               const std::deque<Slice>& operand_list,
                               std::string* new_value, Logger* logger) const;

  // char delim_;       
  // Use variable length delimiter.  
  std::string delim_;

};

} // namespace rocksdb
