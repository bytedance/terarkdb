// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE
#include "table/adaptive_table_factory.h"

#include "port/port.h"
#include "rocksdb/terark_namespace.h"
#include "table/format.h"
#include "table/table_builder.h"
#include "utilities/util/factory.h"

namespace TERARKDB_NAMESPACE {

AdaptiveTableFactory::AdaptiveTableFactory(
    std::shared_ptr<TableFactory> table_factory_to_write,
    std::shared_ptr<TableFactory> block_based_table_factory,
    std::shared_ptr<TableFactory> plain_table_factory,
    std::shared_ptr<TableFactory> cuckoo_table_factory)
    : table_factory_to_write_(table_factory_to_write),
      block_based_table_factory_(block_based_table_factory),
      plain_table_factory_(plain_table_factory),
      cuckoo_table_factory_(cuckoo_table_factory) {
  if (!plain_table_factory_) {
    plain_table_factory_.reset(NewPlainTableFactory());
  }
  if (!block_based_table_factory_) {
    block_based_table_factory_.reset(NewBlockBasedTableFactory());
  }
  if (!cuckoo_table_factory_) {
    cuckoo_table_factory_.reset(NewCuckooTableFactory());
  }
  if (!table_factory_to_write_) {
    table_factory_to_write_ = block_based_table_factory_;
  }
}

extern const uint64_t kPlainTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;
extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kLegacyBlockBasedTableMagicNumber;
extern const uint64_t kCuckooTableMagicNumber;

Status AdaptiveTableFactory::NewTableReader(
    const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table,
    bool /*prefetch_index_and_filter_in_cache*/) const {
  Footer footer;
  auto s = ReadFooterFromFile(file.get(), nullptr /* prefetch_buffer */,
                              file_size, &footer);
  if (!s.ok()) {
    return s;
  }
  if (footer.table_magic_number() == kPlainTableMagicNumber ||
      footer.table_magic_number() == kLegacyPlainTableMagicNumber) {
    return plain_table_factory_->NewTableReader(
        table_reader_options, std::move(file), file_size, table);
  } else if (footer.table_magic_number() == kBlockBasedTableMagicNumber ||
             footer.table_magic_number() == kLegacyBlockBasedTableMagicNumber) {
    return block_based_table_factory_->NewTableReader(
        table_reader_options, std::move(file), file_size, table);
  } else if (footer.table_magic_number() == kCuckooTableMagicNumber) {
    return cuckoo_table_factory_->NewTableReader(
        table_reader_options, std::move(file), file_size, table);
  } else {
    return Status::NotSupported("Unidentified table format");
  }
}

TableBuilder* AdaptiveTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options, uint32_t column_family_id,
    WritableFileWriter* file) const {
  return table_factory_to_write_->NewTableBuilder(table_builder_options,
                                                  column_family_id, file);
}

std::string AdaptiveTableFactory::GetPrintableTableOptions() const {
  std::string ret;
  ret.resize(20000);
  char* pos = &ret[0];
  char* end = pos + ret.size();

  if (table_factory_to_write_) {
    pos += snprintf(
        pos, end - pos, "  write factory (%s) options:\n%s\n",
        (table_factory_to_write_->Name() ? table_factory_to_write_->Name()
                                         : ""),
        table_factory_to_write_->GetPrintableTableOptions().c_str());
  }
  if (plain_table_factory_) {
    pos += snprintf(
        pos, end - pos, "  %s options:\n%s\n",
        plain_table_factory_->Name() ? plain_table_factory_->Name() : "",
        plain_table_factory_->GetPrintableTableOptions().c_str());
  }
  if (block_based_table_factory_) {
    pos += snprintf(
        pos, end - pos, "  %s options:\n%s\n",
        (block_based_table_factory_->Name() ? block_based_table_factory_->Name()
                                            : ""),
        block_based_table_factory_->GetPrintableTableOptions().c_str());
  }
  if (cuckoo_table_factory_) {
    pos += snprintf(
        pos, end - pos, "  %s options:\n%s\n",
        cuckoo_table_factory_->Name() ? cuckoo_table_factory_->Name() : "",
        cuckoo_table_factory_->GetPrintableTableOptions().c_str());
  }
  assert(pos <= end);
  ret.resize(pos - &ret[0]);
  return ret;
}

Status AdaptiveTableFactory::GetOptionString(
    std::string* opt_string, const std::string& delimiter) const {
  opt_string->clear();
  table_factory_to_write_->GetOptionString(opt_string, delimiter);
  return Status::OK();
}

extern TableFactory* NewAdaptiveTableFactory(
    std::shared_ptr<TableFactory> table_factory_to_write,
    std::shared_ptr<TableFactory> block_based_table_factory,
    std::shared_ptr<TableFactory> plain_table_factory,
    std::shared_ptr<TableFactory> cuckoo_table_factory) {
  return new AdaptiveTableFactory(table_factory_to_write,
                                  block_based_table_factory,
                                  plain_table_factory, cuckoo_table_factory);
}

}  // namespace TERARKDB_NAMESPACE

TERARK_FACTORY_INSTANTIATE_GNS(TERARKDB_NAMESPACE::TableFactory*,
                               const std::string&, TERARKDB_NAMESPACE::Status*);

#endif  // ROCKSDB_LITE
