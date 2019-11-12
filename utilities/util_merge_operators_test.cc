//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/testharness.h"
#include "util/testutil.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

class UtilMergeOperatorTest : public testing::Test {
 public:
  UtilMergeOperatorTest() {}

  std::string FullMergeV2(std::string existing_value,
                          std::vector<std::string> operands,
                          std::string key = "") {
    std::string result_string;
    LazyBuffer result(&result_string);
    const LazyBuffer* result_operand = nullptr;

    LazyBuffer existing_value_buffer(existing_value);
    std::vector<LazyBuffer> operands_buffer(operands.begin(), operands.end());

    const MergeOperator::MergeOperationInput merge_in(
        key, &existing_value_buffer, operands_buffer, nullptr);
    MergeOperator::MergeOperationOutput merge_out(result, result_operand);
    merge_operator_->FullMergeV2(merge_in, &merge_out);

    if (result_operand != nullptr) {
      if (result_operand == &existing_value_buffer) {
        result_string = std::move(existing_value);
      } else {
        ptrdiff_t result_operand_index =
            result_operand - operands_buffer.data();
        assert(result_operand_index >= 0 &&
               size_t(result_operand_index) < operands.size());
        result_string = std::move(operands[result_operand_index]);
      }
    } else {
      std::move(result).dump(&result_string);
    }
    return result_string;
  }

  std::string FullMergeV2(std::vector<std::string> operands,
                          std::string key = "") {
    std::string result_string;
    LazyBuffer result(&result_string);
    const LazyBuffer* result_operand = nullptr;

    std::vector<LazyBuffer> operands_buffer(operands.begin(), operands.end());

    const MergeOperator::MergeOperationInput merge_in(key, nullptr,
                                                      operands_buffer, nullptr);
    MergeOperator::MergeOperationOutput merge_out(result, result_operand);
    merge_operator_->FullMergeV2(merge_in, &merge_out);

    if (result_operand != nullptr) {
      ptrdiff_t result_operand_index = result_operand - operands_buffer.data();
      assert(result_operand_index >= 0 &&
             size_t(result_operand_index) < operands.size());
      result_string = std::move(operands[result_operand_index]);
    } else {
      std::move(result).dump(&result_string);
    }
    return result_string;
  }

  std::string PartialMerge(std::string left, std::string right,
                           std::string key = "") {
    std::string result_string;
    LazyBuffer result;

    merge_operator_->PartialMerge(key, LazyBuffer(left), LazyBuffer(right),
                                  &result, nullptr);
    std::move(result).dump(&result_string);
    return result_string;
  }

  std::string PartialMergeMulti(std::deque<std::string> operands,
                                std::string key = "") {
    std::string result_string;
    LazyBuffer result;
    std::vector<LazyBuffer> operands_buffer(operands.begin(), operands.end());

    merge_operator_->PartialMergeMulti(key, operands_buffer, &result, nullptr);
    std::move(result).dump(&result_string);
    return result_string;
  }

 protected:
  std::shared_ptr<MergeOperator> merge_operator_;
};

TEST_F(UtilMergeOperatorTest, MaxMergeOperator) {
  merge_operator_ = MergeOperators::CreateMaxOperator();

  EXPECT_EQ("B", FullMergeV2("B", {"A"}));
  EXPECT_EQ("B", FullMergeV2("A", {"B"}));
  EXPECT_EQ("", FullMergeV2({"", "", ""}));
  EXPECT_EQ("A", FullMergeV2({"A"}));
  EXPECT_EQ("ABC", FullMergeV2({"ABC"}));
  EXPECT_EQ("Z", FullMergeV2({"ABC", "Z", "C", "AXX"}));
  EXPECT_EQ("ZZZ", FullMergeV2({"ABC", "CC", "Z", "ZZZ"}));
  EXPECT_EQ("a", FullMergeV2("a", {"ABC", "CC", "Z", "ZZZ"}));

  EXPECT_EQ("z", PartialMergeMulti({"a", "z", "efqfqwgwew", "aaz", "hhhhh"}));

  EXPECT_EQ("b", PartialMerge("a", "b"));
  EXPECT_EQ("z", PartialMerge("z", "azzz"));
  EXPECT_EQ("a", PartialMerge("a", ""));
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
