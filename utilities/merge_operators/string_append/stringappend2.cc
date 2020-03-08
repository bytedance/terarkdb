/**
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#include "stringappend2.h"

#include <assert.h>

#include <memory>
#include <string>

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

// Implementation for the merge operation (concatenates two strings)
bool StringAppendTESTOperator::FullMergeV2(
    const MergeOperationInput& merge_in,
    MergeOperationOutput* merge_out) const {
  // Clear the *new_value for writing.
  merge_out->new_value.clear();

  if (merge_in.existing_value == nullptr && merge_in.operand_list.size() == 1) {
    // Only one operand
    merge_out->existing_operand = &merge_in.operand_list.back();
    return true;
  }

  // Only print the delimiter after the first entry has been printed
  bool printDelim = false;
  auto builder = merge_out->new_value.get_builder();
  size_t pos = 0;
  size_t cap = 0;
  if (!builder->valid()) {
    return true;
  }
  auto append = [builder, &pos, &cap](const void* data, size_t size) {
    if (size == 0) {
      return true;
    }
    size_t new_pos = pos + size;
    if (new_pos > cap) {
      cap = std::max(new_pos, size_t(cap * 1.5));
      if (!builder->uninitialized_resize(cap)) {
        return false;
      }
    }
    ::memcpy(builder->data() + pos, data, size);
    pos = new_pos;
    return true;
  };

  // Prepend the *existing_value if one exists.
  if (merge_in.existing_value) {
    if (!Fetch(*merge_in.existing_value, &merge_out->new_value)) {
      return true;
    }
    if (!append(merge_in.existing_value->data(),
                merge_in.existing_value->size())) {
      return true;
    }
    printDelim = !delim_.empty();
  }

  // Concatenate the sequence of strings (and add a delimiter between each)
  for (auto it = merge_in.operand_list.begin();
       it != merge_in.operand_list.end(); ++it) {
    if (printDelim) {
      if (!append(delim_.c_str(), delim_.size())) {
        return true;
      }
    }
    if (!Fetch(*it, &merge_out->new_value)) {
      return true;
    }
    if (!append(it->data(), it->size())) {
      return true;
    }
    printDelim = !delim_.empty();
  }
  builder->uninitialized_resize(pos);

  return true;
}

bool StringAppendTESTOperator::PartialMergeMulti(
    const Slice& /*key*/, const std::vector<LazyBuffer>& /*operand_list*/,
    LazyBuffer* /*new_value*/, Logger* /*logger*/) const {
  return false;
}

// A version of PartialMerge that actually performs "partial merging".
// Use this to simulate the exact behaviour of the StringAppendOperator.
bool StringAppendTESTOperator::_AssocPartialMergeMulti(
    const Slice& /*key*/, const std::deque<Slice>& operand_list,
    std::string* new_value, Logger* /*logger*/) const {
  // Clear the *new_value for writing
  assert(new_value);
  new_value->clear();
  assert(operand_list.size() >= 2);

  // Generic append
  // Determine and reserve correct size for *new_value.
  size_t size = 0;
  for (const auto& operand : operand_list) {
    size += operand.size() + delim_.size();
  }
  size -= delim_.size();  // since we have one less delimiter
  new_value->reserve(size);

  // Apply concatenation
  new_value->assign(operand_list.front().data(), operand_list.front().size());

  for (auto it = operand_list.begin() + 1; it != operand_list.end(); ++it) {
    new_value->append(delim_);
    new_value->append(it->data(), it->size());
  }

  return true;
}

const char* StringAppendTESTOperator::Name() const {
  return "StringAppendTESTOperator";
}

std::shared_ptr<MergeOperator>
MergeOperators::CreateStringAppendTESTOperator() {
  return std::make_shared<StringAppendTESTOperator>(',');
}

std::shared_ptr<MergeOperator>
MergeOperators::CreateStringAppendTESTOperator(std::string delim) {
  return std::make_shared<StringAppendTESTOperator>(delim);
}

static MergeOperator* NewStringAppendTESTOperator(const std::string& options) {
  assert(options.size() <= 1);
  char delim = options.size() ? options[0] : ',';
  return new StringAppendTESTOperator(delim);
}

TERARK_FACTORY_REGISTER(StringAppendTESTOperator, &NewStringAppendTESTOperator);
TERARK_FACTORY_REGISTER_EX(StringAppendTESTOperator, "stringappend",
                           &NewStringAppendTESTOperator);

}  // namespace rocksdb
