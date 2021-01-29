// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifndef ROCKSDB_LITE

#include <string>

#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/terark_zip_table.h"

namespace rocksdb {

struct EnvOptions;

using std::unique_ptr;
class Status;
class RandomAccessFile;
class WritableFile;
class Table;
class TableBuilder;
const char* BlockBasedTableName = "BlockBasedTable";
const char* TerarkZipTableName = "TerarkZipTable";
const char* PlainTableName = "PlainTable";
const char* CuckooTableName = "CuckooTable";

class AdaptiveTableFactory : public TableFactory {
 public:
  ~AdaptiveTableFactory() {}

  explicit AdaptiveTableFactory(
      std::vector<std::shared_ptr<TableFactory>> table_factory =
          std::vector<std::shared_ptr<TableFactory>>());

  const char* Name() const override { return name_.c_str(); }

  Status NewTableReader(
      const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table,
      bool prefetch_index_and_filter_in_cache = true) const override;

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id, WritableFileWriter* file) const override;

  // Sanitizes the specified DB Options.
  Status SanitizeOptions(
      const DBOptions& /*db_opts*/,
      const ColumnFamilyOptions& /*cf_opts*/) const override {
    return Status::OK();
  }

  std::string GetPrintableTableOptions() const override;

  Status GetOptionString(std::string* /*opt_string*/,
                         const std::string& /*delimiter*/) const override;

  void* GetOptions() override { return table_factory_to_write_->GetOptions(); }

 private:
  std::shared_ptr<TableFactory> table_factory_to_write_;
  std::shared_ptr<TableFactory> block_based_table_factory_;
  std::shared_ptr<TableFactory> plain_table_factory_;
  std::shared_ptr<TableFactory> cuckoo_table_factory_;
  std::shared_ptr<TableFactory> terark_zip_table_factory_;
  std::string name_;
};

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
