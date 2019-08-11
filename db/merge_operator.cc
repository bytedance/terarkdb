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
    if (!op.inplace_decode().ok()) {
      return false;
    }
    operand_list_str.emplace_back(op.data(), op.size());
  }
  const Slice* existing_value = nullptr;
  if (merge_in.existing_value != nullptr) {
    if (!merge_in.existing_value->inplace_decode().ok()) {
      return false;
    }
    existing_value = merge_in.existing_value->slice_ptr();
  }
  return FullMerge(merge_in.key, existing_value, operand_list_str,
                   merge_out->new_value.trans_to_buffer(), merge_in.logger);
}

// The default implementation of PartialMergeMulti, which invokes
// PartialMerge multiple times internally and merges two operands at
// a time.
bool MergeOperator::PartialMergeMulti(
    const Slice& key, const std::vector<LazySlice>& operand_list,
    LazySlice* new_value, Logger* logger) const {
  assert(operand_list.size() >= 2);
  // Simply loop through the operands
  LazySlice temp_slice = LazySliceReference(operand_list[0]);

  LazySlice temp_value;
  for (size_t i = 1; i < operand_list.size(); ++i) {
    auto& operand = operand_list[i];
    if (!PartialMerge(key, temp_slice, operand, &temp_value, logger)) {
      return false;
    }
    std::swap(temp_value, *new_value);
    temp_slice = LazySliceReference(*new_value);
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
  LazySlice temp_value;
  const LazySlice* existing_value = merge_in.existing_value;
  for (const auto& operand : merge_in.operand_list) {
    if ((existing_value != nullptr &&
         !existing_value->inplace_decode().ok()) ||
        !operand.inplace_decode().ok()) {
      return false;
    }
    temp_value.clear();
    const Slice* existing_value_slice =
        existing_value == nullptr ? nullptr : existing_value->slice_ptr();
    if (!Merge(merge_in.key, existing_value_slice, operand,
               temp_value.trans_to_buffer(), merge_in.logger)) {
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
                                            const LazySlice& left_operand,
                                            const LazySlice& right_operand,
                                            LazySlice* new_value,
                                            Logger* logger) const {
  if (!left_operand.inplace_decode().ok() ||
      !right_operand.inplace_decode().ok()) {
    return false;
  }
  return Merge(key, left_operand.slice_ptr(), right_operand,
               new_value->trans_to_buffer(), logger);
}

} // namespace rocksdb
