//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/dbformat.h"

#include "db/log_format.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <stdio.h>

#include "db/log_writer.h"
#include "db/write_batch_internal.h"
#include "monitoring/perf_context_imp.h"
#include "port/port.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace rocksdb {

// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
const ValueType kValueTypeForSeek = kTypeMergeIndex;
const ValueType kValueTypeForSeekForPrev = kTypeDeletion;

uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  assert(IsExtendedValueType(t));
  return (seq << 8) | t;
}

EntryType GetEntryType(ValueType value_type) {
  switch (value_type) {
    case kTypeValue:
      return kEntryPut;
    case kTypeDeletion:
      return kEntryDelete;
    case kTypeSingleDeletion:
      return kEntrySingleDelete;
    case kTypeMerge:
      return kEntryMerge;
    case kTypeRangeDeletion:
      return kEntryRangeDeletion;
    case kTypeValueIndex:
      return kEntryValueIndex;
    case kTypeMergeIndex:
      return kEntryMergeIndex;
    default:
      return kEntryOther;
  }
}

bool ParseFullKey(const Slice& internal_key, FullKey* fkey) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }
  fkey->user_key = ikey.user_key;
  fkey->sequence = ikey.sequence;
  fkey->type = GetEntryType(ikey.type);
  return true;
}

void UnPackSequenceAndType(uint64_t packed, uint64_t* seq, ValueType* t) {
  *seq = packed >> 8;
  *t = static_cast<ValueType>(packed & 0xff);

  assert(*seq <= kMaxSequenceNumber);
  assert(IsExtendedValueType(*t));
}

void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

void AppendInternalKeyFooter(std::string* result, SequenceNumber s,
                             ValueType t) {
  PutFixed64(result, PackSequenceAndType(s, t));
}

std::string ParsedInternalKey::DebugString(bool hex) const {
  char buf[50];
  snprintf(buf, sizeof(buf), "' seq:%" PRIu64 ", type:%d", sequence,
           static_cast<int>(type));
  std::string result = "'";
  result += user_key.ToString(hex);
  result += buf;
  return result;
}

std::string InternalKey::DebugString(bool hex) const {
  std::string result;
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) {
    result = parsed.DebugString(hex);
  } else {
    result = "(bad)";
    result.append(EscapeString(rep_));
  }
  return result;
}

const char* InternalKeyComparator::Name() const { return name_.c_str(); }

int InternalKeyComparator::Compare(const ParsedInternalKey& a,
                                   const ParsedInternalKey& b) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_->Compare(a.user_key, b.user_key);
  PERF_COUNTER_ADD(user_key_comparison_count, 1);
  if (r == 0) {
    if (a.sequence > b.sequence) {
      r = -1;
    } else if (a.sequence < b.sequence) {
      r = +1;
    } else if (a.type > b.type) {
      r = -1;
    } else if (a.type < b.type) {
      r = +1;
    }
  }
  return r;
}

void InternalKeyComparator::FindShortestSeparator(std::string* start,
                                                  const Slice& limit) const {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.data(), user_start.size());
  user_comparator_->FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() <= user_start.size() &&
      user_comparator_->Compare(user_start, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  user_comparator_->FindShortSuccessor(&tmp);
  if (tmp.size() <= user_key.size() &&
      user_comparator_->Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

LookupKey::LookupKey(const Slice& _user_key, SequenceNumber s) {
  size_t usize = _user_key.size();
  size_t needed = usize + 13;  // A conservative estimate
  char* dst;
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  start_ = dst;
  // NOTE: We don't support users keys of more than 2GB :)
  dst = EncodeVarint32(dst, static_cast<uint32_t>(usize + 8));
  kstart_ = dst;
  memcpy(dst, _user_key.data(), usize);
  dst += usize;
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
}

void IterKey::EnlargeBuffer(size_t key_size) {
  // If size is smaller than buffer size, continue using current buffer,
  // or the static allocated one, as default
  assert(key_size > buf_size_);
  // Need to enlarge the buffer.
  ResetBuffer();
  buf_ = new char[key_size];
  buf_size_ = key_size;
}

Status SeparateHelper::TransToSeparate(
    const Slice& internal_key, LazyBuffer& value, uint64_t file_number,
    const Slice& meta, bool is_merge, bool is_index,
    const ValueExtractor* value_meta_extractor) {
  assert(file_number != uint64_t(-1));
  if (value_meta_extractor == nullptr || is_merge) {
    value.reset(EncodeFileNumber(file_number), true, file_number);
    return Status::OK();
  }
  if (is_index) {
    Slice parts[] = {EncodeFileNumber(file_number), meta};
    value.reset(SliceParts(parts, 2), file_number);
    return Status::OK();
  } else {
    auto s = value.fetch();
    if (!s.ok()) {
      return s;
    }
    std::string value_meta;
    s = value_meta_extractor->Extract(ExtractUserKey(internal_key),
                                      value.slice(), &value_meta);
    if (s.ok()) {
      Slice parts[] = {EncodeFileNumber(file_number), value_meta};
      value.reset(SliceParts(parts, 2), file_number);
    }
    return s;
  }
}

Slice ArenaPinSlice(const Slice& slice, Arena* arena) {
  char* buf = static_cast<char*>(arena->Allocate(slice.size() + 1));
  memcpy(buf, slice.data(), slice.size());
  buf[slice.size()] = '\0';  // better for debug read
  return Slice(buf, slice.size());
}

Slice ArenaPinInternalKey(const Slice& user_key, SequenceNumber seq,
                          ValueType type, Arena* arena) {
  size_t key_size = user_key.size() + 8;
  char* buf = static_cast<char*>(arena->Allocate(key_size + 1));
  memcpy(buf, user_key.data(), user_key.size());
  EncodeFixed64(buf + user_key.size(), PackSequenceAndType(seq, type));
  buf[key_size] = '\0';  // better for debug read
  return Slice(buf, key_size);
}

ValueIndex::ValueIndex(const Slice& slice) {
  assert(slice.size() >= sizeof(uint64_t));
  uint64_t packed_log_number = DecodeFixed64(slice.data());
  log_type = (LogHandleType)(packed_log_number & kLogHandleTypeMask);
  file_number = packed_log_number & kFileNumberMask;

  switch (log_type) {
    case kEmpty:
      meta_or_value = Slice(slice.data() + 8, slice.size() - 8);
      break;
    case kDefault: {
      assert(slice.size() >= kDefaultLogIndexSize);
      log_handle = Slice(slice.data() + 8, kDefaultLogHandleSize);
      meta_or_value = Slice(slice.data() + kDefaultLogIndexSize,
                            slice.size() - kDefaultLogIndexSize);
    } break;
    case kReverse0:
    case kReverse1: {
      assert(false);
    } break;
  }
}

size_t GetPhysicalLength(uint64_t logical_length, uint64_t physical_offset,
                         uint64_t wal_header_size) {
  // physical_offset is data size ahead of current data, it can end at tail of
  // last block exactly which make it be zero. when it is 0, first block has no
  // remain space
  assert(physical_offset == 0 || physical_offset >= wal_header_size);
  if (logical_length == 0) {
    return 0;
  }

  size_t first_block_remain_size =
      (log::kBlockSize - physical_offset % log::kBlockSize) % log::kBlockSize;

  size_t block_avail_size = log::kBlockSize - wal_header_size;
  return logical_length +
         wal_header_size * ((block_avail_size - 1 + logical_length -
                             first_block_remain_size) /
                            block_avail_size);
}

uint64_t GetPhysicalOffset(uint64_t ahead_data_offset, size_t ahead_data_size,
                           size_t header_size) {
  assert(header_size == log::kHeaderSize);  // forbid recycle log

  size_t ahead_data_physical_size =
      GetPhysicalLength(ahead_data_size, ahead_data_offset, header_size);
  uint64_t result = ahead_data_offset + ahead_data_physical_size;
  if (0 == (ahead_data_offset + ahead_data_physical_size) % log::kBlockSize) {
    // ahead value data end exactly at one block's tail, current value need a
    // header
    result += header_size;
  }
  return result;
}

// "record" refer to a logical record of wal which is a whole writebatch
// "entry" refer to a logical key-value-pair in write batch
uint64_t GetFirstEntryPhysicalOffset(uint64_t batch_record_offset,
                                     uint64_t header_size, uint64_t avail_) {
  uint64_t avail = log::kBlockSize - batch_record_offset % log::kBlockSize -
                   log::kHeaderSize;
  assert(avail == avail_);
  if (WriteBatchInternal::kHeader >= avail) {
    // write batch's header cross blocks, so it makes we need add two header
    // before first entry in this write batch
    return batch_record_offset + WriteBatchInternal::kHeader + 2 * header_size;
  } else {
    return batch_record_offset + WriteBatchInternal::kHeader + header_size;
  }
}

uint64_t GetAheadDataOffset(uint64_t cur_offset,
                            uint64_t ahead_data_size_with_header,
                            uint64_t header_size, bool is_cleared) {
  uint64_t ahead_data_size =
      ahead_data_size_with_header - WriteBatchInternal::kHeader;
  if (cur_offset == 0 || is_cleared) return uint64_t(-1);
  assert(cur_offset > log::kHeaderSize);  // must have ahead data
  uint64_t tail_data_size = cur_offset % log::kBlockSize - header_size;
  uint64_t ahead_block_count =
      (ahead_data_size - tail_data_size) % log::kBlockSize;
  uint64_t ahead_content_physical_offset =
      cur_offset - ahead_data_size - ahead_block_count * header_size -
                  tail_data_size >
              0
          ? header_size
          : 0;
  return ahead_content_physical_offset;
}

}  // namespace rocksdb
