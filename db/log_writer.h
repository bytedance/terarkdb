//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <stdint.h>

#include <memory>

#include "db/log_format.h"
//#include "db/version_set.h"
#include "db/write_thread.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/table_reader.h"

namespace rocksdb {

class WritableFileWriter;
class VersionSet;

using std::unique_ptr;

namespace log {

/**
 * Writer is a general purpose log stream writer. It provides an append-only
 * abstraction for writing data. The details of the how the data is written is
 * handled by the WriteableFile sub-class implementation.
 *
 * File format:
 *
 * File is broken down into variable sized records. The format of each record
 * is described below.
 *       +-----+-------------+--+----+----------+------+-- ··· ----+
 * File  | r0  |        r1   |P | r2 |    r3    |  r4  |           |
 *       +-----+-------------+--+----+----------+------+-- ··· ----+
 *       <--- kBlockSize ------>|<-- kBlockSize ------>|
 *  rn = variable size records
 *  P = Padding
 *
 * Data is written out in kBlockSize chunks. If next record does not fit
 * into the space left, the leftover space will be padded with \0.
 *
 * Legacy record format:
 *
 * +---------+-----------+-----------+--- ··· ---+
 * |CRC (4B) | Size (2B) | Type (1B) | Payload   |
 * +---------+-----------+-----------+--- ··· ---+
 *
 * CRC = 32bit hash computed over the record type and payload using CRC
 * Size = Length of the payload data
 * Type = Type of record
 *        (kZeroType, kFullType, kFirstType, kLastType, kMiddleType )
 *        The type is used to group a bunch of records together to represent
 *        blocks that are larger than kBlockSize
 * Payload = Byte stream as long as specified by the payload size
 *
 * Recyclable record format:
 *
 * +---------+-----------+-----------+----------------+--- ··· ---+
 * |CRC (4B) | Size (2B) | Type (1B) | Log number (4B)| Payload   |
 * +---------+-----------+-----------+----------------+--- ··· ---+
 *
 * Same as above, with the addition of
 * Log number = 32bit log file number, so that we can distinguish between
 * records written by the most recent log writer vs a previous one.
 */

class WalBlobReader : public TableReader {
 public:
  explicit WalBlobReader(std::unique_ptr<RandomAccessFile>&& src,
                         uint64_t log_no, const ImmutableDBOptions& idbo);
  ~WalBlobReader() {}

  InternalIterator* NewIterator(const ReadOptions&, const SliceTransform*,
                                Arena*, bool, bool) override ;

  void RangeScan(const Slice*, const SliceTransform*, void*,
                 bool (*)(void* arg, const Slice& key,
                          LazyBuffer&& value)) override {
    assert(false);
  }
  uint64_t ApproximateOffsetOf(const Slice&) override {
    assert(false);
    return 0;
  }

  void SetupForCompaction() override { assert(false); }

  std::shared_ptr<const TableProperties> GetTableProperties() const override {
    assert(false);
    return nullptr;
  }

  size_t ApproximateMemoryUsage() const override { return 0; }

  uint64_t FileNumber() const override { return log_number_; }

  Status Get(const ReadOptions&, const Slice&, GetContext*,
             const SliceTransform*, bool) override {
    assert(false);
    return Status::OK();
  }

  Status GetBlob(const Slice& value_content, GetContext* get_context);

 protected:
  TableProperties table_properties_;

 private:
  std::shared_ptr<Cache> blob_cache_ = nullptr;

  uint64_t wal_header_size_;
  uint64_t log_number_;
  std::unique_ptr<RandomAccessFile> src_;
};

//using InternalIterator = InternalIteratorBase<LazyBuffer>;
//class WalBlobIterator : public InternalIterator {
// public:
//  WalBlobIterator() {}
//  virtual ~WalBlobIterator() {}
//
//  // An iterator is either positioned at a key/value pair, or
//  // not valid.  This method returns true iff the iterator is valid.
//  // Always returns false if !status().ok().
//  virtual bool Valid() const = 0;
//
//  // Position at the first key in the source.  The iterator is Valid()
//  // after this call iff the source is not empty.
//  virtual void SeekToFirst() = 0;
//
//  // Position at the last key in the source.  The iterator is
//  // Valid() after this call iff the source is not empty.
//  virtual void SeekToLast() = 0;
//
//  // Position at the first key in the source that at or past target
//  // The iterator is Valid() after this call iff the source contains
//  // an entry that comes at or past target.
//  // All Seek*() methods clear any error status() that the iterator had prior to
//  // the call; after the seek, status() indicates only the error (if any) that
//  // happened during the seek, not any past errors.
//  virtual void Seek(const Slice& target) = 0;
//
//  // Position at the first key in the source that at or before target
//  // The iterator is Valid() after this call iff the source contains
//  // an entry that comes at or before target.
//  virtual void SeekForPrev(const Slice& target) = 0;
//
//  // Moves to the next entry in the source.  After this call, Valid() is
//  // true iff the iterator was not positioned at the last entry in the source.
//  // REQUIRES: Valid()
//  virtual void Next() = 0;
//
//  // Moves to the previous entry in the source.  After this call, Valid() is
//  // true iff the iterator was not positioned at the first entry in source.
//  // REQUIRES: Valid()
//  virtual void Prev() = 0;
//
//  // Return the key for the current entry.  The underlying storage for
//  // the returned slice is valid only until the next modification of
//  // the iterator.
//  // REQUIRES: Valid()
//  virtual Slice key() const = 0;
//
//  // If an error has occurred, return it.  Else return an ok status.
//  // If non-blocking IO is requested and this operation cannot be
//  // satisfied without doing some IO, then this returns Status::Incomplete().
//  virtual Status status() const = 0;
//
//  // True if the iterator is invalidated because it is out of the iterator
//  // upper bound
//  virtual bool IsOutOfBound() { return false; }
//
//  virtual Status GetProperty(std::string /*prop_name*/, std::string* /*prop*/) {
//    return Status::NotSupported("");
//  }
//
// protected:
//  void SeekForPrevImpl(const Slice& target, const Comparator* cmp) {
//    Seek(target);
//    if (!Valid()) {
//      SeekToLast();
//    }
//    while (Valid() && cmp->Compare(target, key()) < 0) {
//      Prev();
//    }
//  }
//
// private:
//  // No copying allowed
//  InternalIteratorCommon(const InternalIteratorCommon&) = delete;
//  InternalIteratorCommon& operator=(const InternalIteratorCommon&) = delete;
//};



class Writer {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit Writer(std::unique_ptr<WritableFileWriter>&& dest,
                  uint64_t log_number, bool recycle_log_files, VersionSet* vs,
                  bool manual_flush = false);

  explicit Writer(std::unique_ptr<WritableFileWriter>&& dest,
                  uint64_t log_number, bool recycle_log_files,
                  bool manual_flush = false);
  ~Writer();

  Status AddRecord(const Slice& slice, size_t num_entries,
                   WriteThread::Writer*);
  Status AddRecord(const Slice& slice) {
    return AddRecord(slice, 0,
                     nullptr);  // if do not have write thread, then do not
                                // need update num_entries
  }

  WritableFileWriter* file() { return dest_.get(); }
  const WritableFileWriter* file() const { return dest_.get(); }

  uint64_t get_log_number() const { return log_number_; }
  uint64_t get_num_entries() const { return num_entries_; }

  Status WriteBuffer();

  bool TEST_BufferIsEmpty();

  std::shared_ptr<Cache> table_cache_;

 private:
  std::unique_ptr<WritableFileWriter> dest_;
  size_t block_offset_;  // Current offset in block
  size_t num_entries_;   // Current KV-entry in  file
  size_t block_counts_;  // Current block number in log file
  uint64_t log_number_;
  bool recycle_log_files_;

  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];

  Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

  // If true, it does not flush after each write. Instead it relies on the
  // upper layer to manually does the flush by calling ::WriteBuffer()
  bool manual_flush_;

  VersionSet* vs_ = nullptr;  // global version set reference

  // No copying allowed
  Writer(const Writer&);
  void operator=(const Writer&);
};

}  // namespace log
}  // namespace rocksdb
