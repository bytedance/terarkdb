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
#include <future>
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
#include <terark/zbs/zip_reorder_map.hpp>
#include <terark/bitfield_array.hpp>
#include <terark/util/fstrvec.hpp>

namespace rocksdb {

using terark::fstring;
using terark::fstrvec;
using terark::valvec;
using terark::UintVecMin0;
using terark::byte_t;
using terark::febitvec;
using terark::BlobStore;
using terark::ZReorderMap;
using terark::Uint64Histogram;
using terark::DictZipBlobStore;

class TerarkZipTableBuilder : public TableBuilder, boost::noncopyable {
public:
  TerarkZipTableBuilder(
    const TerarkZipTableFactory* table_factory,
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
  struct BuildIndexParams {
    TempFileDeleteOnClose data;
    TerarkIndex::KeyStat stat;
    std::future<Status> wait;
    uint64_t indexFileBegin = 0;
    uint64_t indexFileEnd = 0;
  };
  struct KeyValueStatus {
    valvec<char> prefix;
    valvec<char> commonPrefix;
    Uint64Histogram key;
    Uint64Histogram value;
    bitfield_array<2> type;
    size_t split = 0;
    uint64_t indexFileBegin = 0;
    uint64_t indexFileEnd = 0;
    uint64_t valueFileBegin = 0;
    uint64_t valueFileEnd = 0;
    valvec<std::unique_ptr<BuildIndexParams>> build;
  };
  void AddPrevUserKey(bool finish = false);
  void OfflineZipValueData();
  void UpdateValueLenHistogram();
  struct WaitHandle : boost::noncopyable {
    WaitHandle();
    WaitHandle(size_t);
    WaitHandle(WaitHandle&&);
    WaitHandle& operator = (WaitHandle&&);
    size_t myWorkMem;
    void Release(size_t size = 0);
    ~WaitHandle();
  };
  WaitHandle WaitForMemory(const char* who, size_t memorySize);
  Status EmptyTableFinish();
  Status OfflineFinish();
  void BuildIndex(BuildIndexParams& param, KeyValueStatus& kvs);
  Status WaitBuildIndex();
  struct BuildReorderParams {
    AutoDeleteFile tmpReorderFile;
    bitfield_array<2> type;
  };
  void BuildReorderMap(BuildReorderParams& params,
    KeyValueStatus& kvs,
    fstring mmap_memory);
  WaitHandle LoadSample(std::unique_ptr<DictZipBlobStore::ZipBuilder>& zbuilder);
#if defined(TerocksPrivateCode)
  struct BuildStoreParams {
    NativeDataInput<InputBuffer>& input;
    KeyValueStatus& kvs;
    WaitHandle handle;
    fstring fpath;
    size_t offset;
  };
  void buildZeroLengthBlobStore(BuildStoreParams& params);
  void buildPlainBlobStore(BuildStoreParams& params);
  void buildMixedLenBlobStore(BuildStoreParams& params);
  void buildZipOffsetBlobStore(BuildStoreParams& params);
#endif // TerocksPrivateCode
  Status ZipValueToFinish();
#if defined(TerocksPrivateCode)
  Status ZipValueToFinishMulti();
#endif // TerocksPrivateCode
  void DebugPrepare();
  void DebugCleanup();
  void BuilderWriteValues(NativeDataInput<InputBuffer>& tmpValueFileinput
    , KeyValueStatus& kvs, std::function<void(fstring val)> write);
  void DoWriteAppend(const void* data, size_t size);
  Status WriteStore(fstring indexMmap, BlobStore* store
    , KeyValueStatus& kvs
    , BlockHandle& dataBlock
    , long long& t5, long long& t6, long long& t7);
  Status WriteSSTFile(long long t3, long long t4
    , fstring tmpStoreFile
    , fstring tmpDictFile
    , const DictZipBlobStore::ZipStat& dzstat);
#if defined(TerocksPrivateCode)
  Status WriteSSTFileMulti(long long t3, long long t4
    , fstring tmpStoreFile
    , fstring tmpDictFile
    , const DictZipBlobStore::ZipStat& dzstat);
#endif // TerocksPrivateCode
  Status WriteMetaData(std::initializer_list<std::pair<const std::string*, BlockHandle> > blocks);
  DictZipBlobStore::ZipBuilder* createZipBuilder() const;

  Arena arena_;
  const TerarkZipTableOptions& table_options_;
  const TerarkZipTableFactory* table_factory_;
  // fuck out TableBuilderOptions
  const ImmutableCFOptions& ioptions_;
  std::vector<std::unique_ptr<IntTblPropCollector>> collectors_;
  // end fuck out TableBuilderOptions
  InternalIterator* second_pass_iter_ = nullptr;
  size_t keydataSeed_ = 0;
  valvec<KeyValueStatus> histogram_;
  TerarkIndex::KeyStat *currentStat_ = nullptr;
  valvec<byte_t> prevUserKey_;
  terark::febitvec valueBits_;
  TempFileDeleteOnClose tmpValueFile_;
  TempFileDeleteOnClose tmpSampleFile_;
  AutoDeleteFile tmpIndexFile_;
  std::mutex indexBuildMutex_;
  FileStream tmpDumpFile_;
  AutoDeleteFile tmpZipDictFile_;
  AutoDeleteFile tmpZipValueFile_;
  std::mt19937_64 randomGenerator_;
  uint64_t sampleUpperBound_;
  size_t sampleLenSum_ = 0;
  size_t singleIndexMemLimit = 0;
  WritableFileWriter* file_;
  uint64_t offset_ = 0;
  uint64_t zeroSeqCount_ = 0;
  size_t seqExpandSize_ = 0;
  size_t multiValueExpandSize_ = 0;
  Status status_;
  TableProperties properties_;
  std::unique_ptr<DictZipBlobStore::ZipBuilder> zbuilder_;
  BlockBuilder range_del_block_;
  terark::fstrvec valueBuf_; // collect multiple values for one key
  bool waitInited_ = false;
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
