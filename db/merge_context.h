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

const std::vector<FutureSlice> empty_operand_list;

// The merge context for merging a user key.
// When doing a Get(), DB will create such a class and pass it when
// issuing Get() operation to memtables and version_set. The operands
// will be fetched from the context when issuing partial of full merge.
class MergeContext {
 public:
  // Clear all the operands
  void Clear() {
    if (operand_list_) {
      operand_list_->clear();
    }
  }

  // Push a merge operand
  template<class ...Args>
  void PushOperand(Args&&...args) {
    Initialize();
    SetDirectionBackward();

    operand_list_->emplace_back(std::forward<Args>(args)...);
  }

  // Push back a merge operand
  template<class ...Args>
  void PushOperandBack(Args&&...args) {
    Initialize();
    SetDirectionForward();

    operand_list_->emplace_back(std::forward<Args>(args)...);
  }

  // return total number of operands in the list
  size_t GetNumOperands() const {
    if (!operand_list_) {
      return 0;
    }
    return operand_list_->size();
  }

  // Get the operand at the index.
  const FutureSlice& GetOperand(int index) {
    assert(operand_list_);

    SetDirectionForward();
    return (*operand_list_)[index];
  }

  // Same as GetOperandsDirectionForward
  const std::vector<FutureSlice>& GetOperands() {
    return GetOperandsDirectionForward();
  }

  // Return all the operands in the order as they were merged (passed to
  // FullMerge or FullMergeV2)
  const std::vector<FutureSlice>& GetOperandsDirectionForward() {
    if (!operand_list_) {
      return empty_operand_list;
    }

    SetDirectionForward();
    return *operand_list_;
  }

  // Return all the operands in the reversed order relative to how they were
  // merged (passed to FullMerge or FullMergeV2)
  const std::vector<FutureSlice>& GetOperandsDirectionBackward() {
    if (!operand_list_) {
      return empty_operand_list;
    }

    SetDirectionBackward();
    return *operand_list_;
  }

 private:
  void Initialize() {
    if (!operand_list_) {
      operand_list_.reset(new std::vector<FutureSlice>());
    }
  }

  void SetDirectionForward() {
    if (operands_reversed_ == true) {
      std::reverse(operand_list_->begin(), operand_list_->end());
      operands_reversed_ = false;
    }
  }

  void SetDirectionBackward() {
    if (operands_reversed_ == false) {
      std::reverse(operand_list_->begin(), operand_list_->end());
      operands_reversed_ = true;
    }
  }

  // List of operands
  std::unique_ptr<std::vector<FutureSlice>> operand_list_;
  bool operands_reversed_ = true;
};

} // namespace rocksdb
