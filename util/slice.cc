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

// arg       -> pointer to free
// const_arg -> if not null, pointer to error message
class DefaultLazySliceMetaImpl : public LazySliceMeta{
  void destroy(const Slice& raw, void* arg, const void* /*const_arg*/,
               void* temp) const override {
    assert(!raw.valid()); (void)raw;
    assert(temp == nullptr); (void)temp;
    if (arg != nullptr) {
      free(arg);
    }
  }
  void detach(Slice& value, const LazySliceMeta*& meta, Slice& raw,
              void*& arg, const void*& const_arg,
              void*& temp) const override {
    assert(meta == this); (void)meta;
    assert(!raw.valid()); (void)raw;
    assert(temp == nullptr); (void)temp;
    if (value.empty() || arg != nullptr || const_arg != nullptr) {
      return;
    }
    value = copy_value(arg, const_arg, value);
  }
  Status decode(const Slice& raw, void* arg, const void* const_arg,
                void*& temp, Slice* value) const override {
    assert(!raw.valid()); (void)raw;
    assert(temp == nullptr); (void)temp;
    if (const_arg != nullptr) {
      return Status::Corruption(reinterpret_cast<const char*>(const_arg));
    }
    return Status::OK();
  }
 public:
  static Slice copy_value(void*& arg, const void*& const_arg,
                          const Slice& value) {
    void* ptr = malloc(value.size());
    if (ptr == nullptr) {
      arg = nullptr;
      const_arg = "bad alloc error";
      return Slice::Invalid();
    } else {
      memcpy(ptr, value.data(), value.size());
      arg = ptr;
      const_arg = nullptr;
      return Slice(reinterpret_cast<char*>(ptr), value.size());
    }
  }
  static void copy_status(void*& arg, const void*& const_arg,
                          Status& status) {
    char* err_msg =
        strdup(status.getState() != nullptr
                   ? status.getState()
                   : status.ToString().c_str());
    if (err_msg == nullptr) {
      arg = nullptr;
      const_arg = "LazySlice decode fail & error msg strdup fail";
    } else {
      arg = err_msg;
      const_arg = err_msg;
    }
  }
};

// arg       -> pointer to std::string
// const_arg -> if nullptr, need delete arg
struct BufferLazySliceMetaImpl : public LazySliceMeta {
  void destroy(const Slice& raw, void* arg, const void* const_arg,
               void* temp) const override {
    assert(!raw.valid()); (void)raw;
    assert(temp == nullptr); (void)temp;
    assert(arg != nullptr);
    if (const_arg == nullptr) {
      delete reinterpret_cast<std::string*>(arg);
    }
  }
  void detach(Slice& value, const LazySliceMeta*& meta, Slice& raw,
              void*& arg, const void*& const_arg,
              void*& temp) const override {
    assert(meta == this); (void)meta;
    assert(!raw.valid()); (void)raw;
    assert(temp == nullptr); (void)temp;
    if (const_arg == nullptr) {
      return;
    }
    value = *reinterpret_cast<const std::string*>(arg);
    meta = default_meta();
    if (!value.empty()) {
      value = DefaultLazySliceMetaImpl::copy_value(arg, const_arg, value);
    } else {
      arg = nullptr;
      const_arg = nullptr;
    }
  }
  Status decode(const Slice& raw, void* arg, const void* /*const_arg*/,
                void*& temp, Slice* value) const override {
    assert(!raw.valid()); (void)raw;
    assert(temp == nullptr); (void)temp;
    *value = *reinterpret_cast<const std::string*>(arg);
    return Status::OK();
  }
};

// const_arg -> pointer to slice
struct ReferenceLazySliceMetaImpl : public LazySliceMeta {
  void destroy(const Slice& /*raw*/, void* arg, const void* const_arg,
               void* temp) const override {
    assert(arg == nullptr); (void)arg;
    assert(const_arg != nullptr); (void)const_arg;
    assert(temp == nullptr); (void)temp;
  }
  Status decode(const Slice& raw, void* arg, const void* const_arg,
                void*& temp, Slice* value) const override {
    assert(!raw.valid()); (void)raw;
    assert(const_arg == nullptr); (void)const_arg;
    assert(temp == nullptr); (void)temp;
    const LazySlice& slice_ref =
        *reinterpret_cast<const LazySlice*>(const_arg);
    auto s = slice_ref.decode();
    if (s.ok()) {
      *value = *slice_ref;
    }
    return s;
  }
};

void LazySliceMeta::detach(Slice& value, const LazySliceMeta*& meta,
                           Slice& raw, void*& arg, const void*& const_arg,
                           void*& temp) const {
  assert(meta == this); (void)meta;
  assert(const_arg != nullptr);
  if (value.valid()) {
    value = DefaultLazySliceMetaImpl::copy_value(arg, const_arg, value);
  } else {
    Status s = decode(raw, arg, const_arg, temp, &value);
    if (s.ok()) {
      value = DefaultLazySliceMetaImpl::copy_value(arg, const_arg, value);
    } else {
      value = Slice::Invalid();
      DefaultLazySliceMetaImpl::copy_status(arg, const_arg, s);
    }
  }
  meta = default_meta();
  raw = Slice::Invalid();
  temp = nullptr;
}

const LazySliceMeta* LazySliceMeta::default_meta() {
  static DefaultLazySliceMetaImpl meta_impl;
  return &meta_impl;
}

const LazySliceMeta* LazySliceMeta::reference_meta() {
  static ReferenceLazySliceMetaImpl meta_impl;
  return &meta_impl;
}

const LazySliceMeta* LazySliceMeta::buffer_meta() {
  static BufferLazySliceMetaImpl meta_impl;
  return &meta_impl;
}

void LazySlice::reset(const Slice& _value, bool copy, uint64_t _file_number) {
  destroy();
  if (copy && !_value.empty()) {
    value_ = DefaultLazySliceMetaImpl::copy_value(arg_, const_arg_, _value);
  } else {
    value_ = _value;
    arg_ = nullptr;
    const_arg_ = nullptr;
  }
  meta_ = LazySliceMeta::default_meta();
  raw_ = Slice::Invalid();
  temp_ = nullptr;
  file_number_ = _file_number;
}

LazySlice LazySliceReference(const LazySlice& slice) {
  return LazySlice(LazySliceMeta::reference_meta(), slice.raw(), nullptr,
                   &slice, slice.file_number());
}

void LazySliceCopy(LazySlice& dst, const LazySlice& src) {
  dst = LazySliceReference(src);
  dst.detach();
}

LazySlice LazySliceRemoveSuffix(const LazySlice* slice, size_t fixed_len) {
  // arg       -> fixed_len
  // const_arg -> pointer to slice
  struct LazySliceMetaImpl : public LazySliceMeta {
    void destroy(const Slice& /*raw*/, void* /*arg*/,
                 const void* const_arg, void* temp) const override {
      assert(const_arg != nullptr); (void)const_arg;
      assert(temp == nullptr); (void)temp;
    }
    Status decode(const Slice& /*raw*/, void* arg,
                  const void* const_arg, void*& /*temp*/,
                  Slice* value) const override {
      const LazySlice& slice_ref =
          *reinterpret_cast<const LazySlice*>(const_arg);
      uint64_t len = reinterpret_cast<uint64_t>(arg);
      auto s = slice_ref.decode();
      if (!s.ok()) {
        return s;
      }
      if (slice_ref->size() < len) {
        return Status::Corruption(
            "Error: Could not remove suffix.");
      }
      *value = Slice(slice_ref->data(), slice_ref->size() - len);
      return s;
    }
  };
  static LazySliceMetaImpl meta_impl;
  assert(slice != nullptr);
  return LazySlice(&meta_impl, slice->raw(),
                   reinterpret_cast<void*>(fixed_len), slice,
                   slice->file_number());
}

bool LazySliceTransToBuffer(LazySlice& slice) {
  if (slice.is_buffer()) {
    return true;
  }
  if (!slice.decode().ok()) {
    return false;
  }
  LazySlice buffer;
  buffer.reset_to_buffer();
  buffer.get_buffer()->assign(slice->data(), slice->size());
  buffer.swap(slice);
}

}  // namespace rocksdb
