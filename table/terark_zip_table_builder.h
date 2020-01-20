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
#include "terark_zip_common.h"
#include "terark_zip_internal.h"
#include "terark_zip_table.h"
// std headers
#include <future>
#include <random>
// rocksdb headers
#include <table/block_builder.h>
#include <table/format.h>
#include <table/internal_iterator.h>
#include <table/table_builder.h>
#include <util/arena.h>
// terark headers
#include <terark/bitfield_array.hpp>
#include <terark/bitmap.hpp>
#include <terark/entropy/entropy_base.hpp>
#include <terark/fstring.hpp>
#include <terark/histogram.hpp>
#include <terark/idx/terark_zip_index.hpp>
#include <terark/stdtypes.hpp>
#include <terark/util/fstrvec.hpp>
#include <terark/util/tmpfile.hpp>
#include <terark/valvec.hpp>
#include <terark/zbs/abstract_blob_store.hpp>
#include <terark/zbs/dict_zip_blob_store.hpp>
#include <terark/zbs/zip_reorder_map.hpp>

namespace rocksdb {

template <typename T> struct AsyncTask;

using terark::AbstractBlobStore;
using terark::AutoDeleteFile;
using terark::byte_t;
using terark::DictZipBlobStore;
using terark::febitvec;
using terark::FilePair;
using terark::freq_hist_o1;
using terark::fstring;
using terark::fstrvec;
using terark::TempFileDeleteOnClose;
using terark::TerarkIndex;
using terark::Uint64Histogram;
using terark::UintVecMin0;
using terark::valvec;
using terark::ZReorderMap;

class TerarkZipTableBuilder : public TableBuilder, boost::noncopyable {
 public:
  TerarkZipTableBuilder(const TerarkZipTableFactory* table_factory,
                        const TerarkZipTableOptions&,
                        const TableBuilderOptions& tbo,
                        uint32_t column_family_id, WritableFileWriter* file,
                        size_t key_prefixLen);

  ~TerarkZipTableBuilder();

  Status Add(const Slice& key, const LazyBuffer& value) override;
  Status AddTombstone(const Slice& key, const LazyBuffer& value) override;
  Status Finish(const TablePropertyCache* prop,
                const std::vector<SequenceNumber>* snapshots) override;
  Status AbortFinish(const std::exception& ex);
  void Abandon() override;
  uint64_t NumEntries() const override { return properties_.num_entries; }
  uint64_t FileSize() const override;
  TableProperties GetTableProperties() const override;
  bool NeedCompact() const override { return false; }
  void SetSecondPassIterator(InternalIterator* reader) override {
    if (!table_options_.disableSecondPassIter) {
      second_pass_iter_ = reader;
    }
  }

 private:
  struct RangeStatus {
    fstrvec prefixVec;
    TerarkIndex::KeyStat stat;
    Uint64Histogram valueHist;
    febitvec valueBits;
    uint64_t seqType = 0;
    uint64_t zeroSeqCount = 0;
    size_t prevSamePrefix = 0;
    valvec<std::shared_ptr<FilePair>> fileVec;

    RangeStatus(fstring key, size_t globalPrefixLen, uint64_t seqType);
    RangeStatus() = default;
    RangeStatus(const RangeStatus&) = default;
    RangeStatus(RangeStatus&&) = default;
    RangeStatus& operator=(const RangeStatus&) = default;
    RangeStatus& operator=(RangeStatus&&) = default;

    void AddKey(fstring key, size_t globalPrefixLen, size_t samePrefix,
                size_t valueLen, bool zeroSeq);
    void AddValueBit();
  };

  struct KeyValueStatus {
    RangeStatus status;
    freq_hist_o1 valueFreq;
    size_t valueEntropyLen;
    bitfield_array<2> type;
    uint64_t indexFileBegin = 0;
    uint64_t indexFileEnd = 0;
    uint64_t valueFileBegin = 0;
    uint64_t valueFileEnd = 0;
    bool isValueBuild = false;
    bool isUseDictZip = false;
    bool isFullValue = false;
    std::unique_ptr<AsyncTask<Status>> indexWait;
    std::unique_ptr<AsyncTask<Status>> storeWait;
    std::atomic<size_t> keyFileRef = {2};

    KeyValueStatus(RangeStatus&& s, freq_hist_o1&& f);
  };
  std::shared_ptr<FilePair> NewFilePair();
  void AddPrevUserKey(size_t samePrefix, std::initializer_list<RangeStatus*> r,
                      std::initializer_list<RangeStatus*> e);
  void AddValueBit();
  bool MergeRangeStatus(RangeStatus* aa, RangeStatus* bb, RangeStatus* ab,
                        size_t entropyLen);
  struct WaitHandle : boost::noncopyable {
    WaitHandle();
    WaitHandle(size_t);
    WaitHandle(WaitHandle&&) noexcept;
    WaitHandle& operator=(WaitHandle&&) noexcept;
    size_t myWorkMem;
    void Release(size_t size = 0);
    ~WaitHandle();
  };
  WaitHandle WaitForMemory(const char* who, size_t memorySize);
  Status EmptyTableFinish();
  std::unique_ptr<AsyncTask<Status>> Async(std::function<Status()> func,
                                                   void* tag);
  void BuildIndex(KeyValueStatus& kvs, size_t entropyLen);
  enum BuildStoreFlag {
    BuildStoreInit = 1,
    BuildStoreSync = 2,
  };
  Status BuildStore(KeyValueStatus& kvs, DictZipBlobStore::ZipBuilder* zbuilder,
                    uint64_t flag);
  std::unique_ptr<AsyncTask<Status>> CompressDict(fstring tmpDictFile,
                                                          fstring dict,
                                                          std::string* type,
                                                          long long* td);
  Status WaitBuildIndex();
  Status WaitBuildStore();
  struct BuildReorderParams {
    AutoDeleteFile tmpReorderFile;
    bitfield_array<2> type;
  };
  void BuildReorderMap(std::unique_ptr<TerarkIndex>& index,
                       BuildReorderParams& params, KeyValueStatus& kvs,
                       fstring mmap_memory, AbstractBlobStore* store,
                       long long& t6);
  WaitHandle LoadSample(
      std::unique_ptr<DictZipBlobStore::ZipBuilder>& zbuilder);
  struct BuildStoreParams {
    KeyValueStatus& kvs;
    WaitHandle handle;
    fstring fpath;
    size_t offset;
  };
  Status buildEntropyZipBlobStore(BuildStoreParams& params);
  Status buildZeroLengthBlobStore(BuildStoreParams& params);
  Status buildMixedLenBlobStore(BuildStoreParams& params);
  Status buildZipOffsetBlobStore(BuildStoreParams& params);
  Status ZipValueToFinish();
  Status ZipValueToFinishMulti();
  Status BuilderWriteValues(KeyValueStatus& kvs,
                            std::function<void(fstring val)> write);
  void DoWriteAppend(const void* data, size_t size);
  Status WriteIndexStore(fstring indexMmap, AbstractBlobStore* store,
                         KeyValueStatus& kvs, BlockHandle& dataBlock,
                         size_t kvs_index, long long& t5, long long& t6,
                         long long& t7);
  Status WriteSSTFile(long long t3, long long t4, long long td,
                      fstring tmpDictFile, const std::string& dictInfo,
                      uint64_t dicthash,
                      const DictZipBlobStore::ZipStat& dzstat);
  Status WriteSSTFileMulti(long long t3, long long t4, long long td,
                           fstring tmpDictFile, const std::string& dictType,
                           uint64_t dicthash,
                           const DictZipBlobStore::ZipStat& dzstat);
  Status WriteMetaData(
      const std::string& dictType, size_t entropy,
      std::initializer_list<std::pair<const std::string*, BlockHandle>> blocks);
  DictZipBlobStore::ZipBuilder* createZipBuilder() const;

  Arena arena_;
  TerarkZipTableOptions table_options_;
  const TerarkZipTableFactory* table_factory_;
  const ImmutableCFOptions& ioptions_;
  TerarkZipMultiOffsetInfo offset_info_;
  std::vector<std::unique_ptr<IntTblPropCollector>> collectors_;
  InternalIterator* second_pass_iter_ = nullptr;
  size_t nameSeed_ = 0;
  size_t keyDataSize_ = 0;
  size_t valueDataSize_ = 0;
  size_t prevSamePrefix_ = 0;
  std::unique_ptr<RangeStatus> r22_, r11_, r00_, r21_, r10_, r20_;
  struct FreqPair {
    freq_hist_o1 k, v;
    FreqPair() : k(false), v(true) {}
  };
  std::unique_ptr<FreqPair> freq_[3];
  freq_hist_o1 kv_freq_;
  valvec<std::unique_ptr<KeyValueStatus>> prefixBuildInfos_;
  std::shared_ptr<FilePair> filePair_;
  InternalKey prevKey_;
  TempFileDeleteOnClose tmpSentryFile_;
  TempFileDeleteOnClose tmpSampleFile_;
  AutoDeleteFile tmpIndexFile_;
  AutoDeleteFile tmpStoreFile_;
  AutoDeleteFile tmpZipStoreFile_;
  uint64_t tmpStoreFileSize_ = 0;
  uint64_t tmpZipStoreFileSize_ = 0;
  std::mutex indexBuildMutex_;
  std::mutex storeBuildMutex_;
  FileStream tmpDumpFile_;
  AutoDeleteFile tmpZipDictFile_;
  AutoDeleteFile tmpZipValueFile_;
  std::mt19937_64 randomGenerator_;
  uint64_t sampleUpperBound_;
  size_t sampleLenSum_ = 0;
  size_t singleIndexMaxSize_ = 0;
  WritableFileWriter* file_;
  uint64_t offset_ = 0;
  uint64_t estimateOffset_ = 0;
  size_t dictSize_ = 0;
  float estimateRatio_ = 0;
  size_t seqExpandSize_ = 0;
  size_t multiValueExpandSize_ = 0;
  TableProperties properties_;
  BlockBuilder range_del_block_;
  fstrvec valueBuf_;  // collect multiple values for one key
  valvec<byte_t> valueTestBuf_;
  uint64_t next_freq_size_ = 1ULL << 20;
  bool waitInited_ = false;
  bool closed_ = false;  // Either Finish() or Abandon() has been called.
  bool isReverseBytewiseOrder_;
  int level_;

  long long t0 = 0;
  // Tags in following union are only indicating different function types for
  // Schedule() functions. Keeping the memory layout without further ado.
  union {
    size_t prefixLen_;
    struct {
      char indexTag;
      char storeTag;
      char dictTag;
    };
  };
  double compaction_load_;
};

}  // namespace rocksdb

#endif /* TERARK_ZIP_TABLE_BUILDER_H_ */
