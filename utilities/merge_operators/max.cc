//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <memory>

#include "rocksdb/merge_operator.h"
#include "rocksdb/lazy_slice.h"
#include "utilities/merge_operators.h"

using rocksdb::FutureSlice;
using rocksdb::Slice;
using rocksdb::LazySlice;
using rocksdb::Logger;
using rocksdb::MergeOperator;

namespace {  // anonymous namespace

// Merge operator that picks the maximum operand, Comparison is based on
// Slice::compare
class MaxOperator : public MergeOperator {
 public:
  virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                           MergeOperationOutput* merge_out) const override {
    LazySlice max_slice;
    const FutureSlice*& max = merge_out->existing_operand;
    if (merge_in.existing_value) {
      max_slice = merge_in.existing_value->get();
    } else {
      max_slice.reset(Slice());
    }
    if (!max_slice.decode().ok()) {
      return false;
    }

    for (const auto& op : merge_in.operand_list) {
      LazySlice op_slice = op.get();
      if (!op_slice.decode().ok()) {
        return false;
      }
      if (max_slice->compare(*op_slice) < 0) {
        max = &op;
        max_slice = std::move(op_slice);
      }
    }

    return true;
  }

  virtual bool PartialMerge(const Slice& /*key*/,
                            const FutureSlice& left_operand,
                            const FutureSlice& right_operand,
                            std::string* new_value,
                            Logger* /*logger*/) const override {
    LazySlice left_slice = left_operand.get();
    LazySlice right_slice = right_operand.get();
    if (!left_slice.decode().ok() || !right_slice.decode().ok()) {
      return false;
    }
    if (left_slice->compare(*right_slice) >= 0) {
      new_value->assign(left_slice->data(), left_slice->size());
    } else {
      new_value->assign(right_slice->data(), right_slice->size());
    }
    return true;
  }

  virtual bool PartialMergeMulti(const Slice& /*key*/,
                                 const std::vector<FutureSlice>& operand_list,
                                 std::string* new_value,
                                 Logger* /*logger*/) const override {
    LazySlice max = LazySlice(Slice());
    if (!max.decode().ok()) {
      return false;
    }
    for (const auto& operand : operand_list) {
      LazySlice operand_slice = operand.get();
      if (!operand_slice.decode().ok()) {
        return false;
      }
      if (max->compare(*operand_slice) < 0) {
        max = std::move(operand_slice);
      }
    }

    new_value->assign(max->data(), max->size());
    return true;
  }

  virtual const char* Name() const override { return "MaxOperator"; }
};

}  // end of anonymous namespace

namespace rocksdb {

std::shared_ptr<MergeOperator> MergeOperators::CreateMaxOperator() {
  return std::make_shared<MaxOperator>();
}
}
