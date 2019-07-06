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

const LazySliceMeta* EmptyLazySliceMeta() {
  class SliceMetaImpl : public LazySliceMeta{
    Status decode(const Slice& /*raw*/, void* /*arg*/,
                  const void* /*const_arg*/, void*& /*temp*/,
                  Slice* /*value*/) const override {
      return Status::Corruption("Invalid LazySlice");
    }
  };
  static SliceMetaImpl meta_impl;
  return &meta_impl;
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
      ptr = EncodeVarint64(ptr, file_number);
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
      ptr = EncodeVarint64(ptr, file_number);
      ptr = EncodeVarint64(ptr, reinterpret_cast<uint64_t>(slice.data()));
      ptr = EncodeVarint64(ptr, slice.size());
      storage_.assign(buf, static_cast<size_t>(ptr - buf));
      meta_ = &meta_impl;
    }
  }
}

void FutureSlice::reset(const LazySlice& slice) {
  struct SliceMetaImpl : public FutureSliceMeta, public LazySliceMeta {
    LazySlice to_lazy_slice(const Slice& storage) const override {
      return LazySlice(storage, this);
    }
    Status decode(const Slice& raw, void* /*arg*/, const void* /*const_arg*/,
                  void*& /*temp*/, Slice* /*value*/) const override {
      return Status::Corruption(raw);
    }
  };
  static SliceMetaImpl meta_impl;
  Status s = slice.decode();
  if (s.ok()) {
    reset(*slice, true, slice.file_number());
  } else {
    storage_ = s.ToString();
    meta_ = &meta_impl;
  }
}

void FutureSlice::reset(const LazySlice& slice, Slice pinned_user_key) {
  if (slice.meta() == nullptr ||
      !slice.meta()->to_future(slice, pinned_user_key, this).ok()) {
    reset(slice);
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

LazySlice LazySliceWrapper(const LazySlice* slice) {
  class SliceMetaImpl : public LazySliceMeta{
    Status decode(const Slice& /*raw*/, void* /*arg*/, const void* const_arg,
                  void*& /*temp*/, Slice* value) const override {
      const LazySlice& slice_ref =
          *reinterpret_cast<const LazySlice*>(const_arg);
      auto s = slice_ref.decode();
      if (s.ok()) {
        *value = *slice_ref;
      }
      return s;
    }
  };
  static SliceMetaImpl meta_impl;
  assert(slice != nullptr);
  return LazySlice(slice->raw(), &meta_impl, nullptr, slice,
                   slice->file_number());
}

FutureSlice FutureSliceWrapper(const FutureSlice* slice) {
  struct SliceMetaImpl : public FutureSliceMeta {
    LazySlice to_lazy_slice(const Slice& storage) const override {
      Slice input = storage;
      uint64_t slice_ptr;
      if (!GetFixed64(&input, &slice_ptr)) {
        return LazySlice();
      }
      const FutureSlice& slice_ref =
          *reinterpret_cast<const FutureSlice*>(slice_ptr);
      return slice_ref.get();
    }
  };
  static SliceMetaImpl meta_impl;
  assert(slice != nullptr);
  std::string storage;
  PutFixed64(&storage, reinterpret_cast<uint64_t>(slice));
  return FutureSlice(&meta_impl, std::move(storage));
}

FutureSlice FutureSliceRemoveSuffixWrapper(const FutureSlice* slice,
                                           size_t fixed_len) {
  struct SliceMetaImpl : public FutureSliceMeta, public LazySliceMeta {
    LazySlice to_lazy_slice(const Slice& storage) const override {
      Slice input = storage;
      uint64_t slice_ptr, len;
      if (!GetVarint64(&input, &slice_ptr) || !GetVarint64(&input, &len)) {
        return LazySlice();
      }
      const FutureSlice& slice_ref =
          *reinterpret_cast<const FutureSlice*>(slice_ptr);
      LazySlice* lazy_slice_ptr = new LazySlice(slice_ref.get());
      return LazySlice(lazy_slice_ptr->raw(), this, lazy_slice_ptr,
                       reinterpret_cast<void*>(len),
                       lazy_slice_ptr->file_number());
    }
    void destroy(Slice& /*raw*/, void* arg, const void* /*const_arg*/,
                 void* /*temp*/) const override {
      assert(arg != nullptr);
      delete reinterpret_cast<LazySlice*>(arg);
    }
    Status decode(const Slice& /*raw*/, void* arg, const void* const_arg,
                  void*& /*temp*/, Slice* value) const override {
      LazySlice& lazy_slice = *reinterpret_cast<LazySlice*>(arg);
      uint64_t len = reinterpret_cast<uint64_t>(const_arg);
      auto s = lazy_slice.decode();
      if (!s.ok()) {
        return s;
      }
      if (lazy_slice->size() < len) {
        return Status::Corruption(
            "Error: Could not remove suffix from value.");
      }
      *value = Slice(lazy_slice->data(), lazy_slice->size() - len);
      return s;
    }
  };
  static SliceMetaImpl meta_impl;
  assert(slice != nullptr);
  std::string storage;
  PutVarint64Varint64(&storage, reinterpret_cast<uint64_t>(slice), fixed_len);
  return FutureSlice(&meta_impl, std::move(storage));
}

FutureSlice LazySliceToFutureSliceWrapper(const LazySlice* slice) {
  struct SliceMetaImpl : public FutureSliceMeta, public LazySliceMeta {
    LazySlice to_lazy_slice(const Slice& storage) const override {
      uint64_t slice_ptr;
      Slice input = storage;
      if (!GetFixed64(&input, &slice_ptr)) {
        return LazySlice();
      }
      const LazySlice& slice_ref =
          *reinterpret_cast<const LazySlice*>(slice_ptr);
      return LazySlice(slice_ref.raw(), this, nullptr, &slice_ref,
                       slice_ref.file_number());
    }
    Status decode(const Slice& /*raw*/, void* /*arg0*/,
                  const void* const_arg, void*& /*temp*/,
                  Slice* value) const override {
      const LazySlice& slice_ref =
          *reinterpret_cast<const LazySlice*>(const_arg);
      auto s = slice_ref.decode();
      if (s.ok()) {
        *value = *slice_ref;
      }
      return s;
    }
  };
  static SliceMetaImpl meta_impl;
  assert(slice != nullptr);
  std::string storage;
  PutFixed64(&storage, reinterpret_cast<uint64_t>(slice));
  return FutureSlice(&meta_impl, std::move(storage));
}

}  // namespace rocksdb
