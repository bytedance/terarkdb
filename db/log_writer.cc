//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <stdint.h>

#include <iostream>
#include <memory>

#include "db/column_family.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "db/write_thread.h"
#include "options/db_options.h"
#include "rocksdb/env.h"
#include "table/get_context.h"
#include "terark/util/crc.hpp"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"

namespace rocksdb {
namespace log {

Writer::Writer(std::unique_ptr<WritableFileWriter>&& dest, uint64_t log_number,
               bool recycle_log_files, bool manual_flush)
    : dest_(std::move(dest)),
      block_offset_(0),
      num_entries_(0),
      block_counts_(0),
      log_number_(log_number),
      manual_flush_(manual_flush) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc_[i] = crc32c::Value(&t, 1);
  }
}

Writer::~Writer() { WriteBuffer(); }

Status Writer::WriteBuffer() { return dest_->Flush(); }

Status Writer::AddRecord(const Slice& slice, size_t num_entries,
                         uint64_t* wal_offset) {
  const int header_size = kHeaderSize;

  const char* ptr = slice.data();
  size_t left = slice.size();
  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    const int64_t leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    if (leftover < header_size) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize and
        // kRecyclableHeaderSize being <= 11)
        assert(header_size <= 11);
        s = dest_->Append(Slice("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                                static_cast<size_t>(leftover)));
        if (!s.ok()) {
          break;
        }
      }
      block_offset_ = 0;
      block_counts_++;
    }

    // Invariant: we never leave < header_size bytes in a block.
    assert(static_cast<int64_t>(kBlockSize - block_offset_) >= header_size);

    const size_t avail = kBlockSize - block_offset_ - header_size;
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    uint64_t ahead_data_size = dest_->GetFileSize();
    assert(ahead_data_size == block_counts_ * kBlockSize + block_offset_);
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    if (s.ok() && wal_offset != nullptr && *wal_offset == uint64_t(-1)) {
      // writebatch's content and set writebatch_content_offset only once.
      *wal_offset =
          GetFirstEntryPhysicalOffset(ahead_data_size, header_size, avail);
      assert(*wal_offset % kBlockSize >= (uint64_t)header_size);
    }
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);

  num_entries_ += num_entries;
  return s;
}

bool Writer::TEST_BufferIsEmpty() { return dest_->TEST_BufferIsEmpty(); }

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
  assert(n <= 0xffff);  // Must fit in two bytes

  size_t header_size;
  char buf[kHeaderSize];

  // Format the header
  buf[4] = static_cast<char>(n & 0xff);
  buf[5] = static_cast<char>(n >> 8);
  buf[6] = static_cast<char>(t);

  uint32_t crc = type_crc_[t];
  if (t < kRecyclableFullType) {
    // Legacy record format
    assert(block_offset_ + kHeaderSize + n <= kBlockSize);
    header_size = kHeaderSize;
  }

  // Compute the crc of the record type and the payload.
  crc = crc32c::Extend(crc, ptr, n);
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  Status s = dest_->Append(Slice(buf, header_size));
  assert(s.ok());
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, n));
    if (s.ok()) {
      if (!manual_flush_) {
        s = dest_->Flush();
      }
    }
  }
  block_offset_ += header_size + n;
  return s;
}

WalBlobReader::WalBlobReader(std::unique_ptr<RandomAccessFile>&& src,
                             uint64_t log_no, const ImmutableDBOptions& idbo,
                             const EnvOptions& eo)
    : blob_cache_(idbo.blob_cache),
      wal_header_size_(kHeaderSize),
      log_number_(log_no),
      src_(std::move(src)),
      ioptions_(idbo),
      env_options_(eo) {}

class Blob {
 public:
  explicit Blob(size_t capacity) : size_(capacity) {
    buf_ = (char*)malloc(capacity);
    slice_ = Slice(buf_, 0);
  }
  ~Blob() {
    free(buf_);
    buf_ = nullptr;
    size_ = 0;
  }

  void ShrinkVal(size_t head_size, size_t record_header_size) {
    assert(head_size != 0 && size_ == slice_.size() && buf_ == slice_.data());
    size_t kBlockAvailSize = kBlockSize - record_header_size;

    char* cur_offset = buf_ + head_size;
    size_t remain_bytes = size_ - head_size;
    while (remain_bytes > 0) {
      size_t cur_size =
          remain_bytes > kBlockAvailSize ? kBlockAvailSize : remain_bytes;
      char* actual_data = cur_offset + record_header_size;
      size_t actual_size = cur_size - record_header_size;
      memmove(cur_offset, actual_data, actual_size);

      cur_offset += actual_size;
      remain_bytes -= cur_size;
    }

    size_ = cur_offset - buf_;
    // buf_ = (char*)realloc(buf_, size_); // ? realloc may allocating new mem
    slice_ = Slice(buf_, size_);  // tailing unused space just wasted
  }

  uint64_t DataSize() { return size_; }

  Slice slice_;
  char* buf_ = nullptr;
  uint64_t size_;

 private:
  // No copying allowed
  Blob(const Blob&) = delete;
  void operator=(const Blob&) = delete;
};

// Delete the entry resided in the cache.
template <class Entry>
void DeleteCachedEntry(const Slice& /*key*/, void* Blob) {
  auto entry = reinterpret_cast<Entry*>(Blob);
  delete entry;
}

Status WalBlobReader::GetBlob(const Slice& log_handle,
                              LazyBuffer* lazy_blob) const {
  assert(lazy_blob);
  auto release_cache = [](void* c, void* h) {
    static_cast<Cache*>(c)->Release(static_cast<Cache::Handle*>(h), false);
  };
  // checkout blob_cache
  assert(log_handle.size() == sizeof(DefaultLogHandle));
  std::string blob_uid;
  GenerateCacheUniqueId(log_handle, blob_uid);
  Cache::Handle* handle = blob_cache_->Lookup(blob_uid);
  Status status = Status::OK();
  if (handle) {
    Blob* b = (Blob*)blob_cache_->Value(handle);
    lazy_blob->reset(b->slice_,
                     Cleanable(release_cache, blob_cache_.get(), handle),
                     log_number_);
    return Status::OK();
  }

  // decode and calculate sizes
  DefaultLogHandle content(log_handle);
  size_t blob_physical_length =
      GetPhysicalLength(content.length, content.offset, wal_header_size_);
  size_t head_size = content.length;
  size_t tail_size = 0;
  if (blob_physical_length > content.length) {
    head_size = kBlockSize - content.offset % kBlockSize;
    assert(head_size != 0 && head_size != kBlockSize);
    size_t kBlockAvailSize = kBlockSize - wal_header_size_;
    tail_size = (content.length - head_size) % kBlockAvailSize;
  }

  // read log file and check checksum
#ifndef NDEBUG
  uint64_t _file_size;
  std::string log_name = LogFileName(ioptions_.wal_dir, log_number_);
  auto _s = ioptions_.env->GetFileSize(log_name, &_file_size);
  assert(_file_size >= content.offset);
#endif  // !NDEBUG
  Blob* blob = new Blob(blob_physical_length);
  status = src_->Read(content.offset, blob_physical_length, &(blob->slice_),
                      blob->buf_);
  if (!status.ok()) {
    return status;
  }
  assert(blob->slice_.size() == blob_physical_length);
  if (head_size != 0) {
    uint32_t head_crc =
        terark::Crc16c_update(0, blob->slice_.data(), head_size);
    if (content.head_crc != head_crc) {
      assert(false);
      return Status::IOError("head_crc not match");
    }
  }
  if (tail_size != 0) {
    uint32_t tail_crc = terark::Crc16c_update(
        0, blob->slice_.data() + blob->slice_.size() - tail_size, tail_size);
    if (tail_crc != content.tail_crc) {
      return Status::IOError("tail_crc not match");
    }
  }
  // check middletype crc
  char* header = const_cast<char*>(blob->slice_.data()) + head_size;
  const char* tailer = blob->slice_.data() - tail_size;
  while (header <= tailer - kBlockSize) {
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);
    assert(type == kMiddleType);

    uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
    uint32_t actual_crc =
        crc32c::Value(header + 6, length + wal_header_size_ - 6);
    assert(actual_crc == expected_crc);
    header += kBlockSize;

    (void)type, (void)actual_crc, (void)expected_crc;
  }

  // insert into blob_cache, and return
  if (head_size != content.length) {
    // cross more than one block, need remove middle log record header
    blob->ShrinkVal(head_size, wal_header_size_);
  }
  status = blob_cache_->Insert(blob_uid, blob, sizeof(Blob) + blob->DataSize(),
                               &DeleteCachedEntry<Blob>, &handle);
  if (!status.ok()) {
    return status;
  }
  Blob* b = (Blob*)blob_cache_->Value(handle);
  lazy_blob->reset(b->slice_,
                   Cleanable(release_cache, blob_cache_.get(), handle),
                   log_number_);
  return status;
}

const CFKvHandlesEntry* WalBlobReader::GetCFKvHandlesEntry(uint32_t cf_id) {
  const WalIndexFooter* wal_index_footer = GetWalIndexFooter();
  for (uint32_t i = 0; i < wal_index_footer->count_; ++i) {
    auto cf_entry_offset = [&]() {
      return index_file_data_.data() + index_file_data_.size() -
             sizeof(WalIndexFooter) - (i + 1) * (sizeof(CFKvHandlesEntry));
    };
    const CFKvHandlesEntry* entry =
        reinterpret_cast<const CFKvHandlesEntry*>(cf_entry_offset());
    if (entry->id_ == cf_id) {
      return entry;
    }
  }
  return nullptr;  // not found
}

Status WalBlobReader::CheckIndexFileCrc(uint32_t cf_id) {
  assert(index_file_data_.valid());
  auto footer_ptr = GetWalIndexFooter();
  uint64_t footer_size =
      footer_ptr->count_ * sizeof(CFKvHandlesEntry) + sizeof(WalIndexFooter);
  auto footer_content_offset = [&]() {
    return footer_ptr - sizeof(CFKvHandlesEntry) * footer_ptr->count_;
  };

  uint32_t footer_crc = terark::Crc16c_update(0, footer_content_offset(),
                                              footer_size - sizeof(uint32_t));
  if (footer_crc != footer_ptr->crc32_) {
    return Status::IOError("WalIndex CheckSum Wrong(footer checksum mismatch)");
  }

  auto cf_entry_ptr = GetCFKvHandlesEntry(cf_id);
  uint32_t cf_kv_handles_crc =
      terark::Crc16c_update(0, index_file_data_.data() + cf_entry_ptr->offset_,
                            cf_entry_ptr->count_ * kWalEntrySize);
  if (cf_kv_handles_crc != cf_entry_ptr->crc32_) {
    return Status::IOError(
        "WalIndex CheckSum Wrong(handles group checksum mismatch");
  }

  return Status::OK();
}
InternalIterator* WalBlobReader::NewIteratorWithCF(
    const ReadOptions&, uint32_t cf_id, const ImmutableCFOptions& ioptions,
    Arena* arena) {
  const std::string filename = LogIndexFileName(ioptions_.wal_dir, log_number_);
  uint64_t file_size = 0;
  auto s = ioptions_.env->GetFileSize(filename, &file_size);
  if (UNLIKELY(!s.ok())) {
    s = Status::IOError(filename, "Index File Not Found");
  }

  if (s.ok() && (!index_file_data_.valid() || index_file_data_.empty())) {
    // since many cf may shared same wal, index_file_data_ maybe shared with
    // other cf iter
    // FIXME use mmap for convenience here, fix it maybe
    EnvOptions env_options_for_index = env_options_;
    env_options_for_index.use_mmap_reads = true;
    env_options_for_index.use_direct_reads = false;
    s = ioptions_.env->NewRandomAccessFile(filename, &src_idx_,
                                           env_options_for_index);
    if (s.ok()) {
      s = src_idx_->Read(0, file_size, &index_file_data_,
                         nullptr /*mmap read*/);
      CheckIndexFileCrc(cf_id);
    }
  }
  if (s.ok() && index_file_data_.size() != file_size) {
    s = Status::IOError("Invalid file size");
  }
  if (!s.ok()) {
    return NewErrorInternalIterator(s, arena);
  }

  auto entry_ptr = GetCFKvHandlesEntry(cf_id);
  assert(entry_ptr->offset_ % kWalEntrySize == 0);
  if (arena == nullptr) {
#ifndef NDEBUG
    return new WalBlobIterator(this, entry_ptr, ioptions);
#else
    return new WalBlobIterator(this, entry_ptr);
#endif  // NDEBUG
  } else {
    auto* mem = arena->AllocateAligned(sizeof(WalBlobIterator));
#ifndef NDEBUG
    return new WalBlobIterator(this, entry_ptr, ioptions);
#else
    return new WalBlobIterator(this, entry_ptr);
#endif  // NDEBUG
  }
}

Status WalBlobReader::GetFromHandle(const ReadOptions& readOptions,
                                    const Slice& handle,
                                    GetContext* get_context) {
  LazyBuffer value;
  auto s = GetBlob(handle, &value);
  if (s.ok()) {
    bool unused;
    bool read_more = get_context->SaveValue(
        ParsedInternalKey(handle, kMaxSequenceNumber, kTypeValue),
        std::move(value), &unused);
    (void)read_more;
    assert(!read_more);
    assert(unused);
  }
  return s;
}

void WalBlobIterator::Next() {
  ++i_;
  if (Valid()) {
    FetchKV();
#ifndef NDEBUG
    if (!last_key_.empty()) {
      (void)ioptions_;
      // NO repeated internal_key, since there is no same seq even in trx
      assert(ioptions_.internal_comparator.Compare(iter_key_.GetKey(),
                                                   Slice(last_key_)) > 0);
    }
    last_key_.assign(iter_key_.GetKey().data(), iter_key_.GetKey().size());
#endif
  }
}

Status WalBlobIterator::FetchKV() {
  uint64_t seq;
  ValueType type;

  // unpack seq & type
  const char* cur_kv_base = cf_data_.data() + i_ * kWalEntrySize;
  Slice packed_seq(cur_kv_base + 2 * kDefaultLogHandleSize, 8);
  UnPackSequenceAndType(*(reinterpret_cast<const uint64_t*>(packed_seq.data())),
                        &seq, &type);
  assert(type == kTypeMerge || type == kTypeValue);

  // read userkey use WalBlobReader getblob with handle
  // in SeparateCf, key has already remove prefixed length, so this handle point
  // to user key data
  Slice k_handle(cur_kv_base, kDefaultLogHandleSize);
  LazyBuffer lazy_key;
  reader_->GetBlob(k_handle, &lazy_key);
  auto s = lazy_key.fetch();
  assert(s.ok() && lazy_key.slice().size() != 0);
  iter_key_.SetInternalKey(lazy_key.slice(), seq, type);
  parsed_ikey_ = ParsedInternalKey(lazy_key.slice(), seq, type);

  // read user value
  Slice v_handle(cur_kv_base + kDefaultLogHandleSize, kDefaultLogHandleSize);
  reader_->GetBlob(v_handle, &value_);
  s = value_.fetch();
  assert(value_.valid());
  assert(s.ok());

  return s;
}

Status WalIndexWriter::WriteCF(
    uint32_t cf_id,
    const std::vector<std::pair<ParsedInternalKey, WalEntry>>& sorted_entries) {
  uint32_t crc32 = 0;

  CFKvHandlesEntry wci;
  wci.id_ = cf_id;
  wci.offset_ = index_file_->GetFileSize();
  assert(wci.offset_ % sizeof(WalEntry) == 0);
  wci.count_ = sorted_entries.size();

  Status status;
  for (auto& t : sorted_entries) {
    DefaultLogHandle content(t.second.GetSlice());
    assert(content.length != 0);
    status = index_file_->Append(t.second.GetSlice());
    crc32 = crc32c::Extend(crc32, t.second.GetSlice().data(),
                           t.second.GetSlice().size());
    if (!status.ok()) {
      return status;
    }
  }
  wci.crc32_ = crc32;

  cf_indexes_.push_back(wci);
  return status;
}

Status WalIndexWriter::WriteFooter() {
  uint32_t footer_crc = 0;
  for (auto& i : cf_indexes_) {
    auto s = index_file_->Append(Slice((char*)&i, sizeof(CFKvHandlesEntry)));
    if (!s.ok()) {
      return s;
    }
    footer_crc =
        crc32c::Extend(footer_crc, (char*)&i, sizeof(CFKvHandlesEntry));
  }

  WalIndexFooter wal_index_footer;
  wal_index_footer.count_ = static_cast<uint32_t>(cf_indexes_.size());
  wal_index_footer.crc32_ =
      crc32c::Extend(footer_crc, (char*)&wal_index_footer.count_, 4);
  auto s = index_file_->Append(
      Slice((char*)&wal_index_footer, sizeof(wal_index_footer)));
  if (!s.ok()) {
    return s;
  }
  return s;
}

}  // namespace log
}  // namespace rocksdb
