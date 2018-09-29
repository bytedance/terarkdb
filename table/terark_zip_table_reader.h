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
#include "terark_zip_table.h"
#include "terark_zip_internal.h"
#include "terark_zip_index.h"
// boost headers
#include <boost/noncopyable.hpp>
// rocksdb headers
#include <rocksdb/options.h>
#include <util/arena.h>
#include <table/table_reader.h>
#include <table/table_builder.h>
#include <table/block.h>
// terark headers
#include <terark/util/throw.hpp>
#include <terark/bitfield_array.hpp>
#include <terark/zbs/abstract_blob_store.hpp>

namespace rocksdb {

Status ReadMetaBlockAdapte(class RandomAccessFileReader* file,
                           uint64_t file_size,
                           uint64_t table_magic_number,
                           const struct ImmutableCFOptions& ioptions,
                           const std::string& meta_block_name,
                           struct BlockContents* contents);

class TerarkZipTableTombstone {

private:
  std::shared_ptr<Block> tombstone_;

protected:
  virtual SequenceNumber GetSequenceNumber() const = 0;
  virtual const TableReaderOptions& GetTableReaderOptions() const = 0;

  Status LoadTombstone(RandomAccessFileReader* file, uint64_t file_size);

public:
  virtual InternalIterator*
    NewRangeTombstoneIterator(const ReadOptions& read_options);

  virtual ~TerarkZipTableTombstone() {}
};

class TerarkEmptyTableReader
  : public TerarkZipTableTombstone
  , public TableReader
  , boost::noncopyable {
  class Iter : public SourceInternalIterator, boost::noncopyable {
  public:
    Iter() {}
    ~Iter() {}
    void SetPinnedItersMgr(PinnedIteratorsManager*) {}
    bool Valid() const override { return false; }
    void SeekToFirst() override {}
    void SeekToLast() override {}
    void SeekForPrev(const Slice&) override {}
    void Seek(const Slice&) override {}
    void Next() override {}
    void Prev() override {}
    Slice key() const override { THROW_STD(invalid_argument, "Invalid call"); }
    Slice value() const override { THROW_STD(invalid_argument, "Invalid call"); }
    Status status() const override { return Status::OK(); }
    bool IsKeyPinned() const override { return false; }
    bool IsValuePinned() const override { return false; }
  };
  const TableReaderOptions table_reader_options_;
  const TerarkZipTableFactory* table_factory_;
  std::shared_ptr<const TableProperties> table_properties_;
  SequenceNumber global_seqno_;
  Slice  file_data_;
  unique_ptr<RandomAccessFileReader> file_;
public:
  SourceInternalIterator*
    NewIterator(const ReadOptions&, Arena* a, bool) override {
    return a ? new(a->AllocateAligned(sizeof(Iter)))Iter() : new Iter();
  }
  using TerarkZipTableTombstone::NewRangeTombstoneIterator;
  void Prepare(const Slice&) override {}
  Status Get(const ReadOptions&, const Slice&, GetContext*, bool) override {
    return Status::OK();
  }
  size_t ApproximateMemoryUsage() const override { return 100; }
  uint64_t ApproximateOffsetOf(const Slice&) override { return 0; }
  void SetupForCompaction() override {}
  std::shared_ptr<const TableProperties>
    GetTableProperties() const override { return table_properties_; }

  virtual ~TerarkEmptyTableReader() {}
  TerarkEmptyTableReader(const TerarkZipTableFactory* table_factory,
                         const TableReaderOptions& o)
    : table_reader_options_(o)
    , table_factory_(table_factory)
    , global_seqno_(kDisableGlobalSequenceNumber) {
  }
  Status Open(RandomAccessFileReader* file, uint64_t file_size);
private:
  SequenceNumber GetSequenceNumber() const override {
    return global_seqno_;
  }
  const TableReaderOptions& GetTableReaderOptions() const override {
    return table_reader_options_;
  }
};

struct TerarkZipSubReader {
  LruReadonlyCache* cache_ = nullptr;
  size_t subIndex_;
  size_t rawReaderOffset_;
  size_t rawReaderSize_;
  bool   storeUsePread_;
  intptr_t   storeFD_;
  size_t storeOffset_;
  std::string prefix_;
  unique_ptr<TerarkIndex> index_;
  unique_ptr<terark::AbstractBlobStore> store_;
  bitfield_array<2> type_;
  std::string commonPrefix_;

  enum {
    FlagNone = 0,
    FlagSkipFilter = 1,
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
    FlagUint64Comparator = 2,
#endif
  };

  void InitUsePread(int minPreadLen);

  void GetRecordAppend(size_t recId, valvec<byte_t>* tbuf) const;

  Status Get(SequenceNumber, const ReadOptions&, const Slice& key,
    GetContext*, int flag) const;
  size_t DictRank(fstring key) const;

  ~TerarkZipSubReader();
};

/**
 * one user key map to a record id: the index NO. of a key in NestLoudsTrie,
 * the record id is used to direct index a type enum(small integer) array,
 * the record id is also used to access the value store
 */
class TerarkZipTableReader
  : public TerarkZipTableTombstone
  , public TableReader
  , boost::noncopyable {
public:
  SourceInternalIterator*
    NewIterator(const ReadOptions&, Arena*, bool skip_filters) override;

  using TerarkZipTableTombstone::NewRangeTombstoneIterator;

  void Prepare(const Slice& target) override {}

  Status Get(const ReadOptions&, const Slice& key, GetContext*,
    bool skip_filters) override;

  uint64_t ApproximateOffsetOf_old(const Slice& key);
  uint64_t ApproximateOffsetOf_new(const Slice& key);
  uint64_t ApproximateOffsetOf(const Slice& key) override;
  void SetupForCompaction() override {}

  std::shared_ptr<const TableProperties>
    GetTableProperties() const override { return table_properties_; }

  size_t ApproximateMemoryUsage() const override { return file_data_.size(); }

  virtual ~TerarkZipTableReader();
  TerarkZipTableReader(const TerarkZipTableFactory* table_factory,
                       const TableReaderOptions&,
                       const TerarkZipTableOptions&);
  Status Open(RandomAccessFileReader* file, uint64_t file_size);

private:
  SequenceNumber GetSequenceNumber() const override {
    return global_seqno_;
  }
  const TableReaderOptions& GetTableReaderOptions() const override {
    return table_reader_options_;
  }

  TerarkZipSubReader subReader_;
  static const size_t kNumInternalBytes = 8;
  Slice  file_data_;
  unique_ptr<RandomAccessFileReader> file_;
  valvec<byte_t> dict_;
  const TableReaderOptions table_reader_options_;
  const TerarkZipTableFactory* table_factory_;
  std::shared_ptr<const TableProperties> table_properties_;
  SequenceNumber global_seqno_;
  const TerarkZipTableOptions& tzto_;
  bool isReverseBytewiseOrder_;
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
  bool isUint64Comparator_;
#endif
  Status LoadIndex(Slice mem);
};


class TerarkZipTableMultiReader
  : public TerarkZipTableTombstone
  , public TableReader
  , boost::noncopyable {
public:

  SourceInternalIterator*
    NewIterator(const ReadOptions&, Arena*, bool skip_filters) override;

  using TerarkZipTableTombstone::NewRangeTombstoneIterator;

  void Prepare(const Slice& target) override {}

  Status Get(const ReadOptions&, const Slice& key, GetContext*,
    bool skip_filters) override;

  uint64_t ApproximateOffsetOf_old(const Slice& key);
  uint64_t ApproximateOffsetOf_new(const Slice& key);
  uint64_t ApproximateOffsetOf(const Slice& key) override;
  void SetupForCompaction() override {}

  std::shared_ptr<const TableProperties>
    GetTableProperties() const override { return table_properties_; }

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
    size_t prefixLen_;
    size_t alignedPrefixLen_;
    valvec<byte_t> prefixSet_;
    valvec<TerarkZipSubReader> subReader_;

    struct PartIndexOperator {
      const SubIndex* p;
      fstring operator[](size_t i) const;
    };

    const TerarkZipSubReader* (SubIndex::*LowerBoundSubReaderFunc)(fstring) const;

    const TerarkZipSubReader* LowerBoundSubReaderU64Sequential(fstring key) const;
    const TerarkZipSubReader* LowerBoundSubReaderU64Binary(fstring key) const;
    const TerarkZipSubReader* LowerBoundSubReaderU64BinaryReverse(fstring key) const;
    const TerarkZipSubReader* LowerBoundSubReaderBytewise(fstring key) const;
    const TerarkZipSubReader* LowerBoundSubReaderBytewiseReverse(fstring key) const;

  public:
    ~SubIndex();
    Status Init(fstring offsetMemory,
                fstring indexMemory,
                fstring storeMemory,
                terark::AbstractBlobStore::Dictionary dict,
                fstring typeMemory,
                fstring commonPrefixMemory,
                int minPreadLen,
                intptr_t fileFD,
                LruReadonlyCache* cache,
                bool reverse);
    size_t GetPrefixLen() const;
    size_t GetSubCount() const;
    const TerarkZipSubReader* GetSubReader(size_t i) const;
    const TerarkZipSubReader* LowerBoundSubReader(fstring key) const;
  };

private:
  SequenceNumber GetSequenceNumber() const override {
    return global_seqno_;
  }
  const TableReaderOptions& GetTableReaderOptions() const override {
    return table_reader_options_;
  }

  SubIndex subIndex_;
  static const size_t kNumInternalBytes = 8;
  Slice  file_data_;
  unique_ptr<RandomAccessFileReader> file_;
  valvec<byte_t> dict_;
  const TableReaderOptions table_reader_options_;
  const TerarkZipTableFactory* table_factory_;
  std::shared_ptr<const TableProperties> table_properties_;
  SequenceNumber global_seqno_;
  const TerarkZipTableOptions& tzto_;
  bool isReverseBytewiseOrder_;
};

}  // namespace rocksdb

#endif /* TERARK_ZIP_TABLE_READER_H_ */
