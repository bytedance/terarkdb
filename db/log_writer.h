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
#include <queue>
#include <string>

#include "db/dbformat.h"
#include "db/log_format.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/table_reader.h"
#include "util/crc32c.h"
#include "util/filename.h"

namespace rocksdb {

class WritableFileWriter;
class VersionSet;
class ColumnFamilyData;

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

// inheriting TableReader is for put it in table cache, but only implement
// GetFromHande(for Get BlobVal) and NewIterator(for GC)
class WalBlobReader : public TableReader {
 public:
  explicit WalBlobReader(std::unique_ptr<RandomAccessFile>&& src,
                         uint64_t log_no, const ImmutableDBOptions& idbo,
                         const EnvOptions& eo);
  ~WalBlobReader() {}
  InternalIterator* NewIteratorWithCF(const ReadOptions&, uint32_t cf_id,
                                      const ImmutableCFOptions& ioptions,
                                      Arena* arena) override;
  Status GetFromHandle(const ReadOptions& readOptions, const Slice& handle,
                       GetContext* get_context) override;

  // no implement yet
  InternalIterator* NewIterator(const ReadOptions&, const SliceTransform*,
                                Arena*, bool, bool) override {
    return NewErrorInternalIterator(Status::NotSupported());
  }
  uint64_t ApproximateOffsetOf(const Slice&) override { return 0; }
  void SetupForCompaction() override {}
  std::shared_ptr<const TableProperties> GetTableProperties() const override {
    return std::make_shared<const TableProperties>();
  }
  size_t ApproximateMemoryUsage() const override { return 0; }
  uint64_t FileNumber() const override { return log_number_; }
  Status Get(const ReadOptions&, const Slice&, GetContext*,
             const SliceTransform*, bool) override {
    return Status::NotSupported();
  }

 private:
  friend class WalBlobIterator;

  // generate an id from the file
  void GenerateCacheUniqueId(const Slice& log_handle, std::string& uid) const {
    char prefix_id[kMaxCacheKeyPrefixSize];
    size_t prefix_length =
        src_->GetUniqueId(&prefix_id[0], kMaxCacheKeyPrefixSize);

    uid.append(&prefix_id[0], prefix_length);
    assert(prefix_length == uid.size() &&
           memcmp(uid.data(), prefix_id, prefix_length) == 0);
    uid.append(log_handle.data(), log_handle.size());
  }

  const WalIndexFooter* GetWalIndexFooter() {
    assert(index_file_data_.valid());
    return reinterpret_cast<const WalIndexFooter*>(index_file_data_.data() +
                                                   index_file_data_.size() -
                                                   sizeof(WalIndexFooter));
  }
  Status CheckIndexFileCrc(uint32_t);
  Status GetBlob(const Slice& value_content, LazyBuffer* blob) const;
  const CFKvHandlesEntry* GetCFKvHandlesEntry(uint32_t cf_id);
  std::shared_ptr<Cache> blob_cache_ = nullptr;

  uint64_t wal_header_size_;
  uint64_t log_number_;
  std::unique_ptr<RandomAccessFile> src_;
  std::unique_ptr<RandomAccessFile> src_idx_;
  const ImmutableDBOptions& ioptions_;
  const EnvOptions& env_options_;

  Slice index_file_data_;

  static const size_t kMaxCacheKeyPrefixSize = kMaxVarint64Length * 3 + 1;
};

class WalBlobIterator : public InternalIteratorBase<LazyBuffer> {
 public:
#ifndef NDEBUG
  WalBlobIterator(const WalBlobReader* reader, const CFKvHandlesEntry* cfe,
                  const ImmutableCFOptions& ioptions)
      : cf_entry_ptr_(cfe),
        i_(0),
        reader_(reader),
        cf_data_(reader->index_file_data_.data() + cfe->offset_,
                 kWalEntrySize * cfe->count_),
        ioptions_(ioptions)
#else
  WalBlobIterator(const WalBlobReader* reader, const CFKvHandlesEntry* cfe)
      : cf_entry_ptr_(cfe),
        i_(0),
        reader_(reader),
        cf_data_(reader->index_file_data_.data() + cfe->offset_,
                 kWalEntrySize * cfe->count_)
#endif
  {
    SeekToFirst();
  }
  bool Valid() const override {
    return i_ < cf_entry_ptr_->count_ && status_.ok();
  }
  Status status() const override { return status_; }
  Slice key() const override {
    // return internal key
    assert(iter_key_.GetKey().valid() && iter_key_.GetKey().size() > 8);
    return iter_key_.GetKey();
  }
  LazyBuffer value() const override {
    assert(value_.valid());
    return LazyBuffer(value_.slice(), false, reader_->FileNumber());
  }
  void SeekToFirst() override {
#ifndef NDEBUG
    i_ = 0;
    last_key_.clear();
    status_ = Status::OK();
    if (Valid()) {
      status_ = FetchKV();
      if (status_.ok()) {
        last_key_.assign(iter_key_.GetKey().data(), iter_key_.GetKey().size());
      }
    }
#else
    i_ = 0;
    status_ = Status::OK();
    if (Valid()) {
      status_ = FetchKV();
    }
#endif
  }

  void Next() override;

  // NOT support now
  void SeekToLast() override {
    assert(false);
    status_ = Status::NotSupported("WalBlobIterator::SeekToLast");
  }
  void Seek(const Slice& /*target*/) override {
    assert(false);
    status_ = Status::NotSupported("WalBlobIterator::Seek");
  }
  void SeekForPrev(const Slice& /*target*/) override {
    assert(false);
    status_ = Status::NotSupported("WalBlobIterator::SeekForPrev");
  }
  void Prev() override {
    assert(false);
    status_ = Status::NotSupported("WalBlobIterator::Prev");
  }

 private:
  Status FetchKV();

  const CFKvHandlesEntry* cf_entry_ptr_;
  uint64_t i_;
  const WalBlobReader* reader_;
  Slice cf_data_;
#ifndef NDEBUG
  const ImmutableCFOptions& ioptions_;
  std::string last_key_;
#endif
  Status status_;
  LazyBuffer value_;
  IterKey iter_key_;
  ParsedInternalKey parsed_ikey_;
};

class Writer {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit Writer(std::unique_ptr<WritableFileWriter>&& dest,
                  uint64_t log_number, bool recycle_log_files = false,
                  bool manual_flush = false);

  ~Writer();

  Status AddRecord(const Slice& slice, size_t num_entries,
                   uint64_t* wal_offset);

  Status AddRecord(const Slice& slice) {
    // if do not have write thread, then do not need update num_entries
    return AddRecord(slice, 0, nullptr);
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

  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];

  Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

  // If true, it does not flush after each write. Instead it relies on the
  // upper layer to manually does the flush by calling ::WriteBuffer()
  bool manual_flush_;

  // No copying allowed
  Writer(const Writer&);
  void operator=(const Writer&);
};

class WalIndexWriter {
 public:
  WalIndexWriter(std::unique_ptr<WritableFile>&& file,
                 const std::string& _file_name, const EnvOptions& options,
                 const ImmutableDBOptions& ioptions)
      : index_file_(new WritableFileWriter(std::move(file), _file_name, options,
                                           ioptions.statistics.get(),
                                           ioptions.listeners)) {}
  Status Finish() {
    auto s = WriteFooter();
    if (s.ok()) {
      s = index_file_->Flush();
    }
    index_file_.reset();
    cf_indexes_.clear();
    return s;
  }

  Status WriteCF(uint32_t cf_id,
                 const std::vector<std::pair<ParsedInternalKey, WalEntry>>&
                     sorted_entries);

 private:
  Status WriteFooter();
  std::unique_ptr<WritableFileWriter> index_file_ = nullptr;
  std::vector<CFKvHandlesEntry> cf_indexes_;
};

class WalIndexReader {
 public:
  WalIndexReader(uint32_t cf_id,
                 const std::unique_ptr<RandomAccessFile>* src_idx)
      : cf_id_(cf_id), src_idx_(src_idx) {}
  ~WalIndexReader();

 private:
  /* data */
  const uint32_t cf_id_;
  const std::unique_ptr<RandomAccessFile>* src_idx_ = nullptr;
};

}  // namespace log
}  // namespace rocksdb
