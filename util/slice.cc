//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include "rocksdb/slice_transform.h"
#include "rocksdb/slice.h"
#include "table/format.h"
#include "util/string_util.h"
#include <stdio.h>

namespace rocksdb {

namespace {

class FixedPrefixTransform : public SliceTransform {
 private:
  size_t prefix_len_;
  std::string name_;

 public:
  explicit FixedPrefixTransform(size_t prefix_len)
      : prefix_len_(prefix_len),
        // Note that if any part of the name format changes, it will require
        // changes on options_helper in order to make RocksDBOptionsParser work
        // for the new change.
        // TODO(yhchiang): move serialization / deserializaion code inside
        // the class implementation itself.
        name_("rocksdb.FixedPrefix." + ToString(prefix_len_)) {}

  virtual const char* Name() const override { return name_.c_str(); }

  virtual Slice Transform(const Slice& src) const override {
    assert(InDomain(src));
    return Slice(src.data(), prefix_len_);
  }

  virtual bool InDomain(const Slice& src) const override {
    return (src.size() >= prefix_len_);
  }

  virtual bool InRange(const Slice& dst) const override {
    return (dst.size() == prefix_len_);
  }

  virtual bool FullLengthEnabled(size_t* len) const override {
    *len = prefix_len_;
    return true;
  }

  virtual bool SameResultWhenAppended(const Slice& prefix) const override {
    return InDomain(prefix);
  }
};

class CappedPrefixTransform : public SliceTransform {
 private:
  size_t cap_len_;
  std::string name_;

 public:
  explicit CappedPrefixTransform(size_t cap_len)
      : cap_len_(cap_len),
        // Note that if any part of the name format changes, it will require
        // changes on options_helper in order to make RocksDBOptionsParser work
        // for the new change.
        // TODO(yhchiang): move serialization / deserializaion code inside
        // the class implementation itself.
        name_("rocksdb.CappedPrefix." + ToString(cap_len_)) {}

  virtual const char* Name() const override { return name_.c_str(); }

  virtual Slice Transform(const Slice& src) const override {
    assert(InDomain(src));
    return Slice(src.data(), std::min(cap_len_, src.size()));
  }

  virtual bool InDomain(const Slice& /*src*/) const override { return true; }

  virtual bool InRange(const Slice& dst) const override {
    return (dst.size() <= cap_len_);
  }

  virtual bool FullLengthEnabled(size_t* len) const override {
    *len = cap_len_;
    return true;
  }

  virtual bool SameResultWhenAppended(const Slice& prefix) const override {
    return prefix.size() >= cap_len_;
  }
};

class NoopTransform : public SliceTransform {
 public:
  explicit NoopTransform() { }

  virtual const char* Name() const override { return "rocksdb.Noop"; }

  virtual Slice Transform(const Slice& src) const override { return src; }

  virtual bool InDomain(const Slice& /*src*/) const override { return true; }

  virtual bool InRange(const Slice& /*dst*/) const override { return true; }

  virtual bool SameResultWhenAppended(const Slice& /*prefix*/) const override {
    return false;
  }
};

}

// 2 small internal utility functions, for efficient hex conversions
// and no need for snprintf, toupper etc...
// Originally from wdt/util/EncryptionUtils.cpp - for ToString(true)/DecodeHex:
char toHex(unsigned char v) {
  if (v <= 9) {
    return '0' + v;
  }
  return 'A' + v - 10;
}
// most of the code is for validation/error check
int fromHex(char c) {
  // toupper:
  if (c >= 'a' && c <= 'f') {
    c -= ('a' - 'A');  // aka 0x20
  }
  // validation
  if (c < '0' || (c > '9' && (c < 'A' || c > 'F'))) {
    return -1;  // invalid not 0-9A-F hex char
  }
  if (c <= '9') {
    return c - '0';
  }
  return c - 'A' + 10;
}

Slice::Slice(const SliceParts& parts, std::string* buf) {
  size_t length = 0;
  for (int i = 0; i < parts.num_parts; ++i) {
    length += parts.parts[i].size();
  }
  buf->reserve(length);

  for (int i = 0; i < parts.num_parts; ++i) {
    buf->append(parts.parts[i].data(), parts.parts[i].size());
  }
  data_ = buf->data();
  size_ = buf->size();
}

// Return a string that contains the copy of the referenced data.
std::string Slice::ToString(bool hex) const {
  std::string result;  // RVO/NRVO/move
  if (hex) {
    result.reserve(2 * size_);
    for (size_t i = 0; i < size_; ++i) {
      unsigned char c = data_[i];
      result.push_back(toHex(c >> 4));
      result.push_back(toHex(c & 0xf));
    }
    return result;
  } else {
    result.assign(data_, size_);
    return result;
  }
}

// Originally from rocksdb/utilities/ldb_cmd.h
bool Slice::DecodeHex(std::string* result) const {
  std::string::size_type len = size_;
  if (len % 2) {
    // Hex string must be even number of hex digits to get complete bytes back
    return false;
  }
  if (!result) {
    return false;
  }
  result->clear();
  result->reserve(len / 2);

  for (size_t i = 0; i < len;) {
    int h1 = fromHex(data_[i++]);
    if (h1 < 0) {
      return false;
    }
    int h2 = fromHex(data_[i++]);
    if (h2 < 0) {
      return false;
    }
    result->push_back(static_cast<char>((h1 << 4) | h2));
  }
  return true;
}

const SliceTransform* NewFixedPrefixTransform(size_t prefix_len) {
  return new FixedPrefixTransform(prefix_len);
}

const SliceTransform* NewCappedPrefixTransform(size_t cap_len) {
  return new CappedPrefixTransform(cap_len);
}

const SliceTransform* NewNoopTransform() {
  return new NoopTransform;
}

const LazySliceMeta* InvalidLazySliceMeta() {
  class DummyLazySliceMeta : public LazySliceMeta{
    Status decode(const Slice& /*raw*/, const void* /*_arg0*/,
                        const void* /*_arg1*/,
                        Slice* /*value*/) const override {
      return Status::NotSupported("DummyLazySliceMeta");
    }
  };
  static DummyLazySliceMeta meta_impl;
  return &meta_impl;
}

LazySlice MakeLazySliceReference(const LazySlice& slice) {
  class SliceMetaImpl : public LazySliceMeta{
    Status decode(const Slice& /*raw*/, const void* _arg0,
                  const void* /*_arg1*/, Slice* value) const override {
      const LazySlice* slice_ptr = reinterpret_cast<const LazySlice*>(_arg0);
      auto s = slice_ptr->decode();
      if (s.ok()) {
        *value = *slice_ptr->get();
      }
      return s;
    }
  };
  static SliceMetaImpl meta_impl;
  return LazySlice(slice.raw(), &meta_impl, &slice);
}

void FutureSlice::reset(const Slice& slice, bool copy, uint64_t file_number) {
  enum {
    kIsCopySlice = 1, kHasFileNumber = 2
  };
  struct SliceMetaImpl : public FutureSliceMeta {
    LazySlice to_lazy_slice(const Slice& storage) const override {
      assert(!storage.empty());
      Slice input(storage.data() + 1, storage.size() - 1);
      uint64_t file_number, slice_ptr, slice_size;
      switch(storage[0]) {
      case 0:
        if (GetVarint64(&input, &slice_ptr) &&
            GetVarint64(&input, &slice_size)) {
          return LazySlice(Slice(reinterpret_cast<const char*>(slice_ptr),
                                 slice_size));
        }
        break;
      case kHasFileNumber:
        if (GetVarint64(&input, &file_number) &&
            GetVarint64(&input, &slice_ptr) &&
            GetVarint64(&input, &slice_size)) {
          return LazySlice(Slice(reinterpret_cast<const char*>(slice_ptr),
                                 slice_size), file_number);
        }
        break;
      case kIsCopySlice | kHasFileNumber:
        if (GetVarint64(&input, &file_number)) {
          return LazySlice(input, file_number);
        }
        break;
      default:
        break;
      }
      assert(false);
      return LazySlice();
    }
  };
  static SliceMetaImpl meta_impl;
  if (copy) {
    if (file_number == size_t(-1)) {
      buffer()->assign(slice.data(), slice.size());
    } else {
      char buf[kMaxVarint64Length * 1 + 1];
      buf[0] = kIsCopySlice | kHasFileNumber;
      char* ptr = buf + 1;
      ptr = EncodeVarint64(buf, file_number);
      auto size = static_cast<size_t>(ptr - buf);
      storage_.reserve(size + slice.size());
      storage_.assign(buf, size);
      storage_.append(slice.data(), slice.size());
      meta_ = &meta_impl;
    }
  } else {
    if (file_number == size_t(-1)) {
      char buf[kMaxVarint64Length * 2 + 1];
      buf[0] = 0;
      char* ptr = buf + 1;
      ptr = EncodeVarint64(ptr, reinterpret_cast<uint64_t>(slice.data()));
      ptr = EncodeVarint64(ptr, slice.size());
      storage_.assign(buf, static_cast<size_t>(ptr - buf));
      meta_ = &meta_impl;
    } else {
      char buf[kMaxVarint64Length * 3 + 1];
      buf[0] = kHasFileNumber;
      char* ptr = buf + 1;
      ptr = EncodeVarint64(buf, file_number);
      ptr = EncodeVarint64(ptr, reinterpret_cast<uint64_t>(slice.data()));
      ptr = EncodeVarint64(ptr, slice.size());
      storage_.assign(buf, static_cast<size_t>(ptr - buf));
      meta_ = &meta_impl;
    }
  }
}

void FutureSlice::reset(const LazySlice& slice, bool copy) {
  enum {
    kError, kPtr
  };
  struct SliceMetaImpl : public FutureSliceMeta, public LazySliceMeta {
    LazySlice to_lazy_slice(const Slice& storage) const override {
      return LazySlice(storage, this);
    }
    Status decode(const Slice& raw, const void* /*arg0*/,
                  const void* /*arg1*/, Slice* value) const override {
      assert(!raw.empty());
      Slice input;
      uint64_t slice_ptr;
      switch (raw[raw.size() - 1]) {
      case kError:
        return Status::Corruption(Slice(raw.data(), raw.size() - 1));
      case kPtr:
        input = raw;
        if (GetFixed64(&input, &slice_ptr)) {
          const LazySlice& slice_ref =
              *reinterpret_cast<const LazySlice*>(slice_ptr);
          auto s = slice_ref.decode();
          if (s.ok()) {
            *value = *slice_ref;
          }
          return s;
        }
        break;
      }
      return Status::Corruption("LazySlice decode fail");
    }
  };
  static SliceMetaImpl meta_impl;
  meta_ = nullptr;
  if (copy) {
    auto s = slice.decode();
    if (s.ok()) {
      reset(*slice, false, slice.file_number());
    } else {
      auto err_msg = s.ToString();
      storage_.reserve(err_msg.size() + kMaxVarint64Length + 1);
      PutVarint64(&storage_, err_msg.size());
      storage_.append(err_msg.data(), err_msg.size());
      storage_.push_back(kError);
      meta_ = &meta_impl;
    }
  } else if (slice.meta() != nullptr &&
             slice.meta()->to_future(slice, this).ok()) {
    // encode_future done everything
  } else {
    PutFixed64(&storage_, reinterpret_cast<uint64_t>(&slice));
    storage_.push_back(kPtr);
    meta_ = &meta_impl;
  }
}

std::string* FutureSlice::buffer() {
  struct SliceMetaImpl : public FutureSliceMeta {
    LazySlice to_lazy_slice(const Slice& storage) const override {
      return LazySlice(storage);
    }
  };
  static SliceMetaImpl meta_impl;
  meta_ = &meta_impl;
  return &storage_;
}

}  // namespace rocksdb
