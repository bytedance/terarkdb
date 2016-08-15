/*
 * terark_zip_table.h
 *
 *  Created on: 2016Äê8ÔÂ9ÈÕ
 *      Author: leipeng
 */

#pragma once

#ifndef TERARK_ZIP_TABLE_H_
#define TERARK_ZIP_TABLE_H_

#include <memory>
#include <string>
#include <stdint.h>

#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>

namespace rocksdb {

class Status;
class RandomAccessFile;
class WritableFile;
class Table;
class TableBuilder;

class Block;
struct BlockContents;
class BlockHandle;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;
class InternalKeyComparator;
class TerarkZipTableKeyDecoder;
class GetContext;
class InternalIterator;

struct TerarkZipTableOptions {
	unsigned fixed_key_len = 0;
	int indexNestLevel = 3;
	double sampleRatio = 0.03;
	std::string localTempDir = "/tmp";
};

class TerarkZipTableFactory : public TableFactory {
 public:
  ~TerarkZipTableFactory();
  explicit
  TerarkZipTableFactory(const TerarkZipTableOptions& = TerarkZipTableOptions());

  const char* Name() const override;

  Status
  NewTableReader(const TableReaderOptions& table_reader_options,
                 unique_ptr<RandomAccessFileReader>&& file,
                 uint64_t file_size,
				 unique_ptr<TableReader>* table) const override;

  TableBuilder*
  NewTableBuilder(const TableBuilderOptions& table_builder_options,
				  uint32_t column_family_id,
				  WritableFileWriter* file) const override;

  std::string GetPrintableTableOptions() const override;

  const TerarkZipTableOptions& table_options() const;

  static const char kValueTypeSeqId0 = char(0xFF);

  // Sanitizes the specified DB Options.
  Status SanitizeOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;

  void* GetOptions() override;

 private:
  TerarkZipTableOptions table_options_;
};

}  // namespace rocksdb

#endif /* TERARK_ZIP_TABLE_H_ */
