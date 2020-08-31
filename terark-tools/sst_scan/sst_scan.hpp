#pragma once

#include <options/cf_options.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <table/block_based_table_builder.h>
#include <table/format.h>
#include <table/get_context.h>
#include <table/meta_blocks.h>
#include <table/plain_table_builder.h>
#include <table/table_builder.h>
#include <table/terark_zip_internal.h>
#include <table/terark_zip_table.h>

#include <cassert>
#include <cstdio>
#include <cstring>
#include <iostream>

using namespace rocksdb;

namespace terark {

inline std::string ToHex(const char* data_, int size_) {
  std::string result;
  static const char hextab[] = "0123456789ABCDEF";
  if (size_) {
    result.resize(2 * size_);
    auto beg = &result[0];
    for (int i = 0; i < size_; ++i) {
      unsigned char c = data_[i];
      beg[i * 2 + 0] = hextab[c >> 4];
      beg[i * 2 + 1] = hextab[c & 0xf];
    }
  }
  return result;
}

class KeyPathAnalysis {
 public:
  InternalKeyComparator internal_comparator_;

  std::unique_ptr<RandomAccessFileReader> file_reader_;
  std::unique_ptr<TableReader> table_reader_;

  EnvOptions envOptions_;        // env level
  Options options_;              // database level
  ImmutableCFOptions ioptions_;  // column familiy level

  bool DEBUG_INFO = true;

  // tmp storage
  std::unordered_map<std::string, int> mp;

  std::unordered_map<std::string, size_t> sz;

  std::unique_ptr<TableProperties> table_properties_;

  void printTableType(const uint64_t magic_number);

 public:
  KeyPathAnalysis()
      : internal_comparator_(BytewiseComparator()), ioptions_(options_){};

  ~KeyPathAnalysis() = default;

  uint64_t GetMagicNumber(const std::string& sst_fname);

  Status GetTableReader(const std::string& sst_fname);

  void Get(const std::string& sst_fname, const Slice& key);

  void ListKeys(const std::string& sst_fname, bool print_val);

  void ListAllEmptyValues(const std::string& sst_fname);

  void Seek(const std::string& sst_fname, const Slice& key);

  void GetSize(const std::string& sst_fname);
};
}  // namespace terark
