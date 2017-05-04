/*
 * terark_zip_table_builder.h
 *
 *  Created on: 2017-05-02
 *      Author: zhaoming
 */


#pragma once

#ifndef TERARK_ZIP_TABLE_BUILDER_H_
#define TERARK_ZIP_TABLE_BUILDER_H_

// project headers
#include "terark_zip_table.h"
#include "terark_zip_internal.h"
#include "terark_zip_common.h"
#include "terark_zip_index.h"
// std headers
#include <random>
// rocksdb headers
#include <table/table_builder.h>
#include <table/block_builder.h>
#include <table/format.h>
#include <util/arena.h>
// terark headers
#include <terark/fstring.hpp>
#include <terark/valvec.hpp>
#include <terark/bitmap.hpp>
#include <terark/stdtypes.hpp>
#include <terark/histogram.hpp>
#include <terark/zbs/blob_store.hpp>
#include <terark/zbs/dict_zip_blob_store.hpp>
#include <terark/bitfield_array.hpp>
#include <terark/util/fstrvec.hpp>

namespace rocksdb {

using terark::fstring;
using terark::fstrvec;
using terark::valvec;
using terark::byte_t;
using terark::febitvec;
using terark::BlobStore;
using terark::Uint32Histogram;
using terark::DictZipBlobStore;

extern std::mutex g_sumMutex;
extern size_t g_sumKeyLen;
extern size_t g_sumValueLen;
extern size_t g_sumUserKeyLen;
extern size_t g_sumUserKeyNum;
extern size_t g_sumEntryNum;
extern long long g_lastTime;

class TerarkZipTableBuilder : public TableBuilder, boost::noncopyable {
public:
  TerarkZipTableBuilder(
    const TerarkZipTableOptions&,
    const TableBuilderOptions& tbo,
    uint32_t column_family_id,
    WritableFileWriter* file,
    size_t key_prefixLen);

  ~TerarkZipTableBuilder();

  void Add(const Slice& key, const Slice& value) override;
  Status status() const override { return status_; }
  Status Finish() override;
  void Abandon() override;
  uint64_t NumEntries() const override { return properties_.num_entries; }
  uint64_t FileSize() const override;
  TableProperties GetTableProperties() const override;
  void SetSecondPassIterator(InternalIterator* reader) override {
    if (!table_options_.disableSecondPassIter) {
      second_pass_iter_ = reader;
    }
  }

private:
  struct KeyValueStatus {
    TerarkIndex::KeyStat stat;
    std::string prefix;
    Uint32Histogram key;
    Uint32Histogram value;
    bitfield_array<2> type;
    size_t keyFileBegin = 0;
    size_t keyFileEnd = 0;
    size_t valueFileBegin = 0;
    size_t valueFileEnd = 0;
  };
  void AddPrevUserKey(bool finish = false);
  void OfflineZipValueData();
  void UpdateValueLenHistogram();
  Status EmptyTableFinish();
  Status OfflineFinish();
  Status ZipValueToFinish(fstring tmpIndexFile, std::function<void()> waitIndex);
  Status ZipValueToFinishMulti(fstring tmpIndexFile, std::function<void()> waitIndex);
  void DebugPrepare();
  void DebugCleanup();
  void BuilderWriteValues(KeyValueStatus& kvs, std::function<void(fstring val)> write);
  Status WriteStore(TerarkIndex* index, BlobStore* store
    , KeyValueStatus& kvs, std::function<void(const void*, size_t)> write
    , BlockHandle dataBlock
    , long long& t5, long long& t6, long long& t7);
  Status WriteSSTFile(long long t3, long long t4
    , fstring tmpIndexFile, terark::BlobStore* zstore
    , fstring dictMem
    , const DictZipBlobStore::ZipStat& dzstat);
  Status WriteSSTFileMulti(fstring tmpIndexFile
    , fstring tmpStoreFilePrefix
    , fstring dictMem
    , const DictZipBlobStore::ZipStat& dzstat);
  Status WriteMetaData(std::initializer_list<std::pair<const std::string*, BlockHandle> > blocks);
  DictZipBlobStore::ZipBuilder* createZipBuilder() const;

  Arena arena_;
  const TerarkZipTableOptions& table_options_;
  // fuck out TableBuilderOptions
  const ImmutableCFOptions& ioptions_;
  std::vector<std::unique_ptr<IntTblPropCollector>> collectors_;
  // end fuck out TableBuilderOptions
  InternalIterator* second_pass_iter_ = nullptr;
  valvec<KeyValueStatus> histogram_;
  valvec<byte_t> prevUserKey_;
  terark::febitvec valueBits_;
  TempFileDeleteOnClose tmpKeyFile_;
  TempFileDeleteOnClose tmpValueFile_;
  TempFileDeleteOnClose tmpSampleFile_;
  FileStream tmpDumpFile_;
  AutoDeleteFile tmpZipDictFile_;
  AutoDeleteFile tmpZipValueFile_;
  std::mt19937_64 randomGenerator_;
  uint64_t sampleUpperBound_;
  size_t sampleLenSum_ = 0;
  WritableFileWriter* file_;
  uint64_t offset_ = 0;
  uint64_t zeroSeqCount_ = 0;
  Status status_;
  TableProperties properties_;
  std::unique_ptr<DictZipBlobStore::ZipBuilder> zbuilder_;
  BlockBuilder range_del_block_;
  terark::fstrvec valueBuf_; // collect multiple values for one key
  bool closed_ = false;  // Either Finish() or Abandon() has been called.
  bool isReverseBytewiseOrder_;
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
  bool isUint64Comparator_;
#endif

  long long t0 = 0;
  size_t key_prefixLen_;
};


}  // namespace rocksdb

#endif /* TERARK_ZIP_TABLE_BUILDER_H_ */
