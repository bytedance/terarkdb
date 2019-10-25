//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include <string>
#include <vector>
#include "db/dbformat.h"
#include "rocksdb/slice.h"
#include "table/internal_iterator.h"

namespace rocksdb {

// The merge context for merging a user key.
// When doing a Get(), DB will create such a class and pass it when
// issuing Get() operation to memtables and version_set. The operands
// will be fetched from the context when issuing partial of full merge.
class MergeContext {
 public:
  // Clear all the operands
  void Clear() {
    operand_list_.clear();
  }

  // Push a merge operand
  void PushOperand(LazyBuffer&& operand_slice, bool operand_pinned = true) {
    SetDirectionBackward();

    operand_list_.emplace_back(std::move(operand_slice));
    if (operand_pinned) {
      operand_list_.back().pin();
    }
  }

  // Push back a merge operand
  void PushOperandBack(LazyBuffer&& operand_slice, bool operand_pinned = true) {
    SetDirectionForward();

    operand_list_.emplace_back(std::move(operand_slice));
    if (operand_pinned) {
      operand_list_.back().pin();
    }
  }

  // return total number of operands in the list
  size_t GetNumOperands() const {
    return operand_list_.size();
  }

  // Get the operand at the index.
  const LazyBuffer& GetOperand(int index) {
    SetDirectionForward();
    return operand_list_[index];
  }

  // Same as GetOperandsDirectionForward
  std::vector<LazyBuffer>& GetOperands() {
    return GetOperandsDirectionForward();
  }

  // Return all the operands in the order as they were merged (passed to
  // FullMerge or FullMergeV2)
  std::vector<LazyBuffer>& GetOperandsDirectionForward() {
    SetDirectionForward();
    return operand_list_;
  }

  // Return all the operands in the reversed order relative to how they were
  // merged (passed to FullMerge or FullMergeV2)
  std::vector<LazyBuffer>& GetOperandsDirectionBackward() {
    SetDirectionBackward();
    return operand_list_;
  }

 private:
  void SetDirectionForward() {
    if (operands_reversed_ == true) {
      std::reverse(operand_list_.begin(), operand_list_.end());
      operands_reversed_ = false;
    }
  }

  void SetDirectionBackward() {
    if (operands_reversed_ == false) {
      std::reverse(operand_list_.begin(), operand_list_.end());
      operands_reversed_ = true;
    }
  }

  // List of operands
  std::vector<LazyBuffer> operand_list_;
  bool operands_reversed_ = true;
};

} // namespace rocksdb
