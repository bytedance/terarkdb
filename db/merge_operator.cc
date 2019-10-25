//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
/**
 * Back-end implementation details specific to the Merge Operator.
 */

#include "rocksdb/merge_operator.h"
#include "rocksdb/status.h"

namespace rocksdb {

bool MergeOperator::FullMergeV2(const MergeOperationInput& merge_in,
                                MergeOperationOutput* merge_out) const {
  // If FullMergeV2 is not implemented, we convert the operand_list to
  // std::deque<std::string> and pass it to FullMerge
  std::deque<std::string> operand_list_str;
  for (auto& op : merge_in.operand_list) {
    if (!op.fetch().ok()) {
      return false;
    }
    operand_list_str.emplace_back(op.data(), op.size());
  }
  const Slice* existing_value = nullptr;
  if (merge_in.existing_value != nullptr) {
    if (!merge_in.existing_value->fetch().ok()) {
      return false;
    }
    existing_value = &merge_in.existing_value->get_slice();
  }
  return FullMerge(merge_in.key, existing_value, operand_list_str,
                   merge_out->new_value.trans_to_string(), merge_in.logger);
}

// The default implementation of PartialMergeMulti, which invokes
// PartialMerge multiple times internally and merges two operands at
// a time.
bool MergeOperator::PartialMergeMulti(
    const Slice& key, const std::vector<LazyBuffer>& operand_list,
    LazyBuffer* new_value, Logger* logger) const {
  assert(operand_list.size() >= 2);
  // Simply loop through the operands
  LazyBuffer temp_slice = LazyBufferReference(operand_list[0]);

  LazyBuffer temp_value;
  for (size_t i = 1; i < operand_list.size(); ++i) {
    auto& operand = operand_list[i];
    if (!PartialMerge(key, temp_slice, operand, &temp_value, logger)) {
      return false;
    }
    std::swap(temp_value, *new_value);
    temp_slice = LazyBufferReference(*new_value);
  }

  // The result will be in *new_value. All merges succeeded.
  return true;
}

// Given a "real" merge from the library, call the user's
// associative merge function one-by-one on each of the operands.
// NOTE: It is assumed that the client's merge-operator will handle any errors.
bool AssociativeMergeOperator::FullMergeV2(
    const MergeOperationInput& merge_in,
    MergeOperationOutput* merge_out) const {
  // Simply loop through the operands
  LazyBuffer temp_value;
  const LazyBuffer* existing_value = merge_in.existing_value;
  for (const auto& operand : merge_in.operand_list) {
    if ((existing_value != nullptr &&
         !existing_value->fetch().ok()) ||
        !operand.fetch().ok()) {
      return false;
    }
    temp_value.clear();
    const Slice* existing_value_slice =
        existing_value == nullptr ? nullptr : &existing_value->get_slice();
    if (!Merge(merge_in.key, existing_value_slice, operand.get_slice(),
               temp_value.trans_to_string(), merge_in.logger)) {
      return false;
    }
    std::swap(temp_value, merge_out->new_value);
    existing_value = &merge_out->new_value;
  }

  // The result will be in *new_value. All merges succeeded.
  return true;
}

// Call the user defined simple merge on the operands;
// NOTE: It is assumed that the client's merge-operator will handle any errors.
bool AssociativeMergeOperator::PartialMerge(const Slice& key,
                                            const LazyBuffer& left_operand,
                                            const LazyBuffer& right_operand,
                                            LazyBuffer* new_value,
                                            Logger* logger) const {
  if (!left_operand.fetch().ok() || !right_operand.fetch().ok()) {
    return false;
  }
  return Merge(key, &left_operand.get_slice(), right_operand.get_slice(),
               new_value->trans_to_string(), logger);
}

} // namespace rocksdb
