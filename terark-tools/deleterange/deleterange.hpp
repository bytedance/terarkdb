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

namespace rocksdb {
extern const uint64_t kPlainTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;
}  // namespace rocksdb

namespace terark {

InternalKeyComparator internal_comparator_(BytewiseComparator());

std::unique_ptr<RandomAccessFileReader> file_reader_;
std::unique_ptr<TableReader> table_reader_;

EnvOptions envOptions_;                  // env level
Options options_;                        // database level
ImmutableCFOptions ioptions_(options_);  // column familiy level

std::unique_ptr<TableProperties> table_properties_;

class DeleteRange {
 public:
  static void PrintTableType(const uint64_t magic_number) {
    if (magic_number == kTerarkZipTableMagicNumber) {
      std::cout << "Magic Number Table Type: "
                << "kTerarkZipTableMagicNumber" << std::endl;
    } else if (magic_number == kBlockBasedTableMagicNumber) {
      std::cout << "Magic Number Table Type: "
                << "kBlockBasedTableMagicNumber" << std::endl;
    } else if (magic_number == kLegacyBlockBasedTableMagicNumber) {
      std::cout << "Magic Number Table Type: "
                << "kLegacyBlockBasedTableMagicNumber" << std::endl;
    } else if (magic_number == rocksdb::kPlainTableMagicNumber) {
      std::cout << "Magic Number Table Type: "
                << "kPlainTableMagicNumber" << std::endl;
    } else if (magic_number == rocksdb::kLegacyPlainTableMagicNumber) {
      std::cout << "Magic Number Table Type: "
                << "kLegacyPlainTableMagicNumber" << std::endl;
    } else {
      std::cout << "Magic Number Table Type: "
                << "Unknown" << std::endl;
    }
  }

  static uint64_t GetMagicNumber(const std::string& sst_fname) {
    std::cout << "GetMagicNumber(" << sst_fname << ")..." << std::endl;
    uint64_t magic_number = Footer::kInvalidTableMagicNumber;
    Footer footer;

    std::unique_ptr<RandomAccessFile> file;
    uint64_t file_size = 0;
    Status s = options_.env->NewRandomAccessFile(sst_fname, &file, envOptions_);
    if (s.ok()) {
      s = options_.env->GetFileSize(sst_fname, &file_size);
      std::cout << "SST FileSize : " << file_size << std::endl;
    } else {
      std::cout << "GetMagicNumber(" << sst_fname << "), GetFileSize Failed!"
                << std::endl;
    }

    file_reader_.reset(new RandomAccessFileReader(std::move(file), sst_fname));
    s = ReadFooterFromFile(file_reader_.get(), nullptr, file_size, &footer);

    if (s.ok()) {
      magic_number = footer.table_magic_number();
      std::cout << "Magic Number: " << magic_number << std::endl;
      PrintTableType(magic_number);
    } else {
      std::cout << "GetMagicNumber(" << sst_fname
                << "), Read Magic Number Failed!" << std::endl;
    }
    return magic_number;
  }

  static Status GetTableReader(const std::string& sst_fname,
                               int& deletion_cnt) {
    auto magic_number = GetMagicNumber(sst_fname);
    std::unique_ptr<RandomAccessFile> file;

    // TererkZipTable have to use mmap to read sst files
    envOptions_.use_mmap_reads = true;
    options_.env->NewRandomAccessFile(sst_fname, &file, envOptions_);
    file_reader_.reset(new RandomAccessFileReader(std::move(file), sst_fname));
    options_.comparator = &internal_comparator_;

    // For old sst format, ReadTableProperties might fail but file can be read
    TableProperties* table_properties = nullptr;
    uint64_t file_size = 0;
    auto s = options_.env->GetFileSize(sst_fname, &file_size);
    std::cout << "Try ReadTableProperties, file_size = " << file_size
              << std::endl;
    s = rocksdb::ReadTableProperties(file_reader_.get(), file_size,
                                     magic_number, ioptions_,
                                     &table_properties);
    if (s.ok()) {
      table_properties_.reset(table_properties);
      // TODO init options based on different magic number
      TerarkZipConfigFromEnv(options_, options_);
    } else {
      std::cout << "Not able to read table properties" << std::endl;
      return s;
    }

    std::cout << "Creating Table Reader by options..." << std::endl;
    auto readerOptions = TableReaderOptions(ioptions_, nullptr, envOptions_,
                                            internal_comparator_);
    s = options_.table_factory->NewTableReader(
        readerOptions, std::move(file_reader_), file_size, &table_reader_);
    if (s.ok()) {
      std::cout << "Finish TableReader Creation for" << sst_fname << std::endl;
    } else {
      std::cout << "Failed to Build TableReader for sst file: " << sst_fname
                << std::endl;
      std::cout << "Status: " << s.getState() << std::endl;
      return Status::Aborted();
    }

    deletion_cnt += table_properties_->num_range_deletions;
    return s;
  }
};

}  // namespace terark