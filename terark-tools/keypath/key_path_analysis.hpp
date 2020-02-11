#pragma once

#include <cassert>
#include <cstdio>
#include <cstring>
#include <iostream>

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

using namespace rocksdb;

namespace terark {

class KeyPathAnalysis {
 private:
  InternalKeyComparator internal_comparator_;

  std::unique_ptr<RandomAccessFileReader> file_reader_;
  std::unique_ptr<TableReader> table_reader_;

  EnvOptions envOptions_;        // env level
  Options options_;              // database level
  ImmutableCFOptions ioptions_;  // column familiy level

  std::unique_ptr<TableProperties> table_properties_;

  void printTableType(const uint64_t magic_number);

 public:
  KeyPathAnalysis()
      : internal_comparator_(BytewiseComparator()), ioptions_(options_){};

  ~KeyPathAnalysis() = default;

  uint64_t GetMagicNumber(const std::string& sst_fname);

  Status GetTableReader(const std::string& sst_fname);

  void Get(const std::string& sst_fname, const Slice& key);

  void ListKeys(const std::string& sst_fname);

  void Seek(const std::string& sst_fname, const Slice& key);
};
}  // namespace terark
