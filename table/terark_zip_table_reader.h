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
#include <terark/zbs/blob_store.hpp>

namespace rocksdb {

class TerarkZipTableIterator;

class TerarkEmptyTableReader : public TableReader, boost::noncopyable {
  class Iter : public InternalIterator, boost::noncopyable {
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
  std::shared_ptr<const TableProperties> table_properties_;
  SequenceNumber global_seqno_;
  shared_ptr<Block> tombstone_;
  Slice  file_data_;
  unique_ptr<RandomAccessFileReader> file_;
public:
  InternalIterator*
    NewIterator(const ReadOptions&, Arena* a, bool) override {
    return a ? new(a->AllocateAligned(sizeof(Iter)))Iter() : new Iter();
  }
  InternalIterator*
    NewRangeTombstoneIterator(const ReadOptions& read_options) override;
  void Prepare(const Slice&) override {}
  Status Get(const ReadOptions&, const Slice&, GetContext*, bool) override {
    return Status::OK();
  }
  size_t ApproximateMemoryUsage() const override { return 100; }
  uint64_t ApproximateOffsetOf(const Slice&) override { return 0; }
  void SetupForCompaction() override {}
  std::shared_ptr<const TableProperties>
    GetTableProperties() const override { return table_properties_; }
  ~TerarkEmptyTableReader() {}
  TerarkEmptyTableReader(const TableReaderOptions& o)
    : table_reader_options_(o)
    , global_seqno_(kDisableGlobalSequenceNumber) {
  }
  Status Open(RandomAccessFileReader* file, uint64_t file_size);
};


/**
* one user key map to a record id: the index NO. of a key in NestLoudsTrie,
* the record id is used to direct index a type enum(small integer) array,
* the record id is also used to access the value store
*/
class TerarkZipTableReader : public TableReader, boost::noncopyable {
public:
  InternalIterator*
    NewIterator(const ReadOptions&, Arena*, bool skip_filters) override;

  InternalIterator*
    NewRangeTombstoneIterator(const ReadOptions& read_options) override;

  void Prepare(const Slice& target) override {}

  Status Get(const ReadOptions&, const Slice& key, GetContext*,
    bool skip_filters) override;

  uint64_t ApproximateOffsetOf(const Slice& key) override { return 0; }
  void SetupForCompaction() override {}

  std::shared_ptr<const TableProperties>
    GetTableProperties() const override { return table_properties_; }

  size_t ApproximateMemoryUsage() const override { return file_data_.size(); }

  ~TerarkZipTableReader();
  TerarkZipTableReader(const TableReaderOptions&, const TerarkZipTableOptions&);
  Status Open(RandomAccessFileReader* file, uint64_t file_size);

private:
  unique_ptr<terark::BlobStore> valstore_;
  unique_ptr<TerarkIndex> keyIndex_;
  shared_ptr<Block> tombstone_;
  Slice commonPrefix_;
  bitfield_array<2> typeArray_;
  static const size_t kNumInternalBytes = 8;
  Slice  file_data_;
  unique_ptr<RandomAccessFileReader> file_;
  const TableReaderOptions table_reader_options_;
  std::shared_ptr<const TableProperties> table_properties_;
  SequenceNumber global_seqno_;
  const TerarkZipTableOptions& tzto_;
  bool isReverseBytewiseOrder_;
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
  bool isUint64Comparator_;
#endif
  friend class TerarkZipTableIterator;
  Status LoadIndex(Slice mem);
};

}  // namespace rocksdb

#endif /* TERARK_ZIP_TABLE_READER_H_ */
