/*
 * terark_zip_table_reader.h
 *
 *  Created on: 2017-05-02
 *      Author: zhaoming
 */

#pragma once

#ifndef TERARK_ZIP_TABLE_READER_H_
#define TERARK_ZIP_TABLE_READER_H_

// project headers
#include "terark_zip_internal.h"
#include "terark_zip_table.h"
// boost headers
#include <boost/noncopyable.hpp>
// rocksdb headers
#include <rocksdb/options.h>
#include <table/block.h>
#include <table/table_builder.h>
#include <table/table_reader.h>
#include <util/arena.h>
// terark headers
#include <terark/bitfield_array.hpp>
#include <terark/entropy/entropy_base.hpp>
#include <terark/idx/terark_zip_index.hpp>
#include <terark/util/fstrvec.hpp>
#include <terark/util/throw.hpp>
#include <terark/zbs/abstract_blob_store.hpp>

namespace rocksdb {

using terark::ContextBuffer;
using terark::TerarkContext;
using terark::TerarkIndex;

Status ReadMetaBlockAdapte(class RandomAccessFileReader* file,
                           uint64_t file_size, uint64_t table_magic_number,
                           const struct ImmutableCFOptions& ioptions,
                           const std::string& meta_block_name,
                           struct BlockContents* contents);

class TerarkZipTableReaderBase : public TableReader, boost::noncopyable {
 private:
  std::shared_ptr<const FragmentedRangeTombstoneList> fragmented_range_dels_;

 protected:
  const TableReaderOptions table_reader_options_;

  std::shared_ptr<const TableProperties> table_properties_;
  unique_ptr<RandomAccessFileReader> file_;
  Slice file_data_;

  virtual SequenceNumber GetSequenceNumber() const = 0;

  Status LoadTombstone(RandomAccessFileReader* file, uint64_t file_size);

  uint64_t FileNumber() const override {
    return table_reader_options_.file_number;
  }

  TerarkZipTableReaderBase(const TableReaderOptions& tro)
      : table_reader_options_(tro) {}

 public:
  virtual FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions& read_options) override;

  std::shared_ptr<const TableProperties> GetTableProperties() const override;

  void MmapColdize(const void* addr, size_t len);
  void MmapColdize(terark::fstring mem) { MmapColdize(mem.data(), mem.size()); }
  template <class Vec>
  void MmapColdize(const Vec& uv) {
    MmapColdize(uv.data(), uv.mem_size());
  }
};

class TerarkEmptyTableReader : public TerarkZipTableReaderBase {
  class Iter : public InternalIterator, boost::noncopyable {
   public:
    Iter() {}
    ~Iter() {}
    bool Valid() const override { return false; }
    void SeekToFirst() override {}
    void SeekToLast() override {}
    void SeekForPrev(const Slice&) override {}
    void Seek(const Slice&) override {}
    void Next() override {}
    void Prev() override {}
    Slice key() const override { THROW_STD(invalid_argument, "Invalid call"); }
    LazyBuffer value() const override {
      THROW_STD(invalid_argument, "Invalid call");
    }
    Status status() const override { return Status::OK(); }
  };
  SequenceNumber global_seqno_;

 public:
  InternalIterator* NewIterator(const ReadOptions& /*ro*/,
                                const SliceTransform* /*prefix_extractor*/,
                                Arena* a, bool /*skip_filters*/,
                                bool /*for_compaction*/) override {
    return a ? new (a->AllocateAligned(sizeof(Iter))) Iter() : new Iter();
  }
  void Prepare(const Slice&) override {}
  Status Get(const ReadOptions& /*readOptions*/, const Slice& /*key*/,
             GetContext* /*get_context*/,
             const SliceTransform* /*prefix_extractor*/,
             bool /*skip_filters*/) override {
    return Status::OK();
  }
  Status RangeScan(const Slice* /*begin*/,
                   const SliceTransform* /*prefix_extractor*/, void* /*arg*/,
                   bool (*/*callback_func*/)(void* arg, const Slice& key,
                                             LazyBuffer&& value)) override {
    return Status::OK();
  }
  size_t ApproximateMemoryUsage() const override { return 100; }
  uint64_t ApproximateOffsetOf(const Slice&) override { return 0; }
  void SetupForCompaction() override {}

  virtual ~TerarkEmptyTableReader() {}
  TerarkEmptyTableReader(const TableReaderOptions& o)
      : TerarkZipTableReaderBase(o),
        global_seqno_(kDisableGlobalSequenceNumber) {}
  Status Open(RandomAccessFileReader* file, uint64_t file_size);

 private:
  SequenceNumber GetSequenceNumber() const override { return global_seqno_; }
};

struct TerarkZipSubReader {
  LruReadonlyCache* cache_ = nullptr;
  size_t subIndex_;
  size_t rawReaderOffset_;
  size_t rawReaderSize_;
  size_t estimateUnzipCap_;
  bool storeUsePread_;
  intptr_t storeFD_;
  RandomAccessFile* storeFileObj_;
  size_t storeOffset_;
  unique_ptr<TerarkIndex> index_;
  unique_ptr<terark::AbstractBlobStore> store_;
  bitfield_array<2> type_;
  uint64_t file_number_;

  enum {
    FlagNone = 0,
    FlagSkipFilter = 1,
  };

  void InitUsePread(int minPreadLen);

  void GetRecordAppend(size_t recId, valvec<byte_t>* tbuf) const;
  void GetRecordAppend(size_t recId, terark::BlobStore::CacheOffsets*) const;

  Status Get(SequenceNumber, const ReadOptions&, const Slice& key, GetContext*,
             int flag) const;
  size_t DictRank(fstring key) const;

  ~TerarkZipSubReader();
};

/**
 * one user key map to a record id: the index NO. of a key in NestLoudsTrie,
 * the record id is used to direct index a type enum(small integer) array,
 * the record id is also used to access the value store
 */
class TerarkZipTableReader : public TerarkZipTableReaderBase {
 public:
  InternalIterator* NewIterator(const ReadOptions&,
                                const SliceTransform* prefix_extractor,
                                Arena* a, bool skip_filters,
                                bool for_compaction) override;

  template <bool reverse, bool ZipOffset>
  InternalIterator* NewIteratorImpl(const ReadOptions&, Arena* a,
                                    ContextBuffer* buffer, TerarkContext* ctx);

  void Prepare(const Slice& /*target*/) override {}

  Status Get(const ReadOptions& readOptions, const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool skip_filters) override;

  Status RangeScan(const Slice* begin, const SliceTransform* prefix_extractor,
                   void* arg,
                   bool (*callback_func)(void* arg, const Slice& key,
                                         LazyBuffer&& value)) override;

  uint64_t ApproximateOffsetOf(const Slice& key) override;
  void SetupForCompaction() override {}

  size_t ApproximateMemoryUsage() const override { return file_data_.size(); }

  virtual ~TerarkZipTableReader();
  TerarkZipTableReader(const TerarkZipTableFactory* table_factory,
                       const TableReaderOptions&, const TerarkZipTableOptions&);
  Status Open(RandomAccessFileReader* file, uint64_t file_size);

 private:
  SequenceNumber GetSequenceNumber() const override { return global_seqno_; }

  TerarkZipSubReader subReader_;
  static const size_t kNumInternalBytes = 8;
  valvec<byte_t> dict_;
  valvec<byte_t> meta_;
  const TerarkZipTableFactory* table_factory_;
  SequenceNumber global_seqno_;
  const TerarkZipTableOptions& tzto_;
  bool isReverseBytewiseOrder_;
  Status LoadIndex(Slice mem);
};

class TerarkZipTableMultiReader : public TerarkZipTableReaderBase {
 public:
  InternalIterator* NewIterator(const ReadOptions&,
                                const SliceTransform* prefix_extractor,
                                Arena* a, bool skip_filters,
                                bool for_compaction) override;

  template <bool reverse, bool ZipOffset>
  InternalIterator* NewIteratorImpl(const ReadOptions&, Arena* a,
                                    ContextBuffer* buffer, TerarkContext* ctx);

  using TerarkZipTableReaderBase::NewRangeTombstoneIterator;

  void Prepare(const Slice& /*target*/) override {}

  Status Get(const ReadOptions& readOptions, const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool skip_filters) override;

  Status RangeScan(const Slice* begin, const SliceTransform* prefix_extractor,
                   void* arg,
                   bool (*callback_func)(void* arg, const Slice& key,
                                         LazyBuffer&& value)) override;

  uint64_t ApproximateOffsetOf(const Slice& key) override;
  void SetupForCompaction() override {}

  size_t ApproximateMemoryUsage() const override { return file_data_.size(); }

  virtual ~TerarkZipTableMultiReader();
  TerarkZipTableMultiReader(const TerarkZipTableFactory* table_factory,
                            const TableReaderOptions&,
                            const TerarkZipTableOptions&);
  Status Open(RandomAccessFileReader* file, uint64_t file_size);

  class SubIndex {
   private:
    LruReadonlyCache* cache_ = nullptr;
    intptr_t cache_fi_ = -1;
    size_t partCount_;
    size_t iteratorSize_ = 0;
    terark::fstrvec bounds_;
    valvec<TerarkZipSubReader> subReader_;
    bool hasAnyZipOffset_;

    struct PartIndexOperator {
      const SubIndex* p;
      fstring operator[](size_t i) const;
    };

   public:
    ~SubIndex();

    Status Init(fstring offsetMemory, const byte_t* baseAddress,
                terark::AbstractBlobStore::Dictionary dict, int minPreadLen,
                RandomAccessFile* fileObj, LruReadonlyCache* cache,
                uint64_t file_number, bool warmUpIndexOnOpen, bool reverse);

    size_t GetSubCount() const;
    const TerarkZipSubReader* GetSubReader(size_t i) const;
    const TerarkZipSubReader* LowerBoundSubReader(fstring key) const;
    const TerarkZipSubReader* LowerBoundSubReaderReverse(fstring key) const;
    size_t IteratorSize() const { return iteratorSize_; }
    bool HasAnyZipOffset() const { return hasAnyZipOffset_; }
  };

 private:
  SequenceNumber GetSequenceNumber() const override { return global_seqno_; }

  SubIndex subIndex_;
  static const size_t kNumInternalBytes = 8;
  valvec<byte_t> dict_;
  valvec<byte_t> meta_;
  const TerarkZipTableFactory* table_factory_;
  SequenceNumber global_seqno_;
  const TerarkZipTableOptions& tzto_;
  bool isReverseBytewiseOrder_;
};

}  // namespace rocksdb

#endif /* TERARK_ZIP_TABLE_READER_H_ */
