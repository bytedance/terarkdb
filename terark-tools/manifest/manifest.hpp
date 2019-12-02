#pragma once

#include <cassert>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <map>

#include <db/version_set.h>
#include <options/cf_options.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
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

class ManifestAnalysis {
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
  ManifestAnalysis()
      : internal_comparator_(BytewiseComparator()), ioptions_(options_){};

  ~ManifestAnalysis() = default;

  void Validate(const std::string& manifest_fname);

  void ListCFNames(std::unique_ptr<SequentialFileReader>& file_reader,
                   std::map<uint32_t, std::string>& result);
};
}  // namespace terark
