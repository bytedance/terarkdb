//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <memory>

#include "rocksdb/lazy_buffer.h"
#include "rocksdb/merge_operator.h"
#include "utilities/merge_operators.h"

using rocksdb::LazyBuffer;
using rocksdb::LazyBufferReference;
using rocksdb::Logger;
using rocksdb::MergeOperator;
using rocksdb::Slice;

namespace {  // anonymous namespace

// Merge operator that picks the maximum operand, Comparison is based on
// Slice::compare
class MaxOperator : public MergeOperator {
 public:
  virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                           MergeOperationOutput* merge_out) const override {
    const LazyBuffer*& max = merge_out->existing_operand;
    if (merge_in.existing_value) {
      if (!Fetch(*merge_in.existing_value, &merge_out->new_value)) {
        return true;
      }
      max = merge_in.existing_value;
    }

    for (const auto& op : merge_in.operand_list) {
      if (!Fetch(op, &merge_out->new_value)) {
        max = nullptr;
        return true;
      }
      if (max == nullptr || max->slice().compare(op.slice()) < 0) {
        max = &op;
      }
    }

    return true;
  }

  virtual bool PartialMerge(const Slice& /*key*/,
                            const LazyBuffer& left_operand,
                            const LazyBuffer& right_operand,
                            LazyBuffer* new_value,
                            Logger* /*logger*/) const override {
    if (!Fetch(left_operand, new_value) || !Fetch(right_operand, new_value)) {
      return true;
    }
    if (left_operand.slice().compare(right_operand.slice()) >= 0) {
      new_value->assign(left_operand);
    } else {
      new_value->assign(right_operand);
    }
    return true;
  }

  virtual bool PartialMergeMulti(const Slice& /*key*/,
                                 const std::vector<LazyBuffer>& operand_list,
                                 LazyBuffer* new_value,
                                 Logger* /*logger*/) const override {
    LazyBuffer max;
    for (const auto& operand : operand_list) {
      assert(max.valid());
      if (!Fetch(operand, new_value)) {
        return true;
      }
      if (max.slice().compare(operand.slice()) < 0) {
        max.reset(operand.slice());
      }
    }

    new_value->assign(max);
    return true;
  }

  virtual const char* Name() const override { return "MaxOperator"; }
};

}  // end of anonymous namespace

namespace rocksdb {

std::shared_ptr<MergeOperator> MergeOperators::CreateMaxOperator() {
  return std::make_shared<MaxOperator>();
}

static MergeOperator* NewMaxOperator(const std::string& /*options*/) {
  return new MaxOperator;
}

TERARK_FACTORY_REGISTER(MaxOperator, &NewMaxOperator);
TERARK_FACTORY_REGISTER_EX(MaxOperator, "max", &NewMaxOperator);

}  // namespace rocksdb
