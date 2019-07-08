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

namespace {
template<class T, class S>
T* union_cast(S* src) {
  union {
    S* s;
    T* t;
  } u;
  u.s = src;
  return u.t;
}
}

class DefaultLazySliceMetaImpl : public LazySliceMeta {
 public:
  struct LazySliceRepPtr {
    char* ptr;
    uint64_t size;
    uint64_t cap;
    const char* err;
  };

  void lazy_slice_destroy(LazySliceRep* rep) const override {
    auto data = union_cast<LazySliceRepPtr>(rep);
    free(data->ptr);
  }
  void lazy_slice_detach(LazySlice* slice, LazySliceRep* rep) const override {
    auto data = union_cast<LazySliceRepPtr>(rep);
    if (slice->empty() || data->ptr != nullptr || data->err != nullptr) {
      return;
    }
    *slice = copy_value(data, *slice);
  }
  Status lazy_slice_decode(const LazySliceRep* rep, Slice* value) const override {
    auto data = union_cast<const LazySliceRepPtr>(rep);
    if (data->err != nullptr) {
      return Status::Corruption(data->err);
    }
    if (!value->valid()) {
      return Status::Corruption("Invalid Slice");
    }
    return Status::OK();
  }
  static Slice copy_value(LazySliceRepPtr* data, const Slice& value) {
    if (data->cap < value.size()) {
       auto ptr = (char*)realloc(data->ptr, value.size());
       if (ptr == nullptr) {
         data->err = "DefaultLazySliceMetaImpl: bad alloc";
         return Slice::Invalid();
       }
       data->ptr = ptr;
       data->cap = value.size();
    }
    memcpy(data->ptr, value.data(), value.size());
    data->size = value.size();
    data->err = nullptr;
    return Slice(data->ptr, data->size);
  }
};

struct BufferLazySliceMetaImpl : public LazySliceMeta {
 public:
  struct LazySliceRepPtr {
    std::string* buffer;
    uint64_t is_owner;
    char* ptr;
    const char* err;
  };
  void lazy_slice_destroy(LazySliceRep* rep) const override {
    auto data = union_cast<LazySliceRepPtr>(rep);
    if (data->is_owner) {
      delete data->buffer;
    }
    free(data->ptr);
  }
  void lazy_slice_detach(LazySlice* slice, LazySliceRep* rep) const override {
    auto data = union_cast<LazySliceRepPtr>(rep);
    if (!data->is_owner) {
      LazySliceMeta::lazy_slice_detach(slice, rep);
    }
  }
  Status lazy_slice_decode(const LazySliceRep* rep,
                           Slice* value) const override {
    auto data = union_cast<const LazySliceRepPtr>(rep);
    if (data->err != nullptr) {
      return Status::Corruption(data->err);
    } else {
      *value = *data->buffer;
    }
    return Status::OK();
  }
};

// const_arg -> pointer to slice
struct ReferenceLazySliceMetaImpl : public LazySliceMeta {
  void lazy_slice_destroy(LazySliceRep* /*rep*/) const override {}
  Status lazy_slice_decode(const LazySliceRep* rep,
                           Slice* value) const override {
    const LazySlice& slice_ref =
        *reinterpret_cast<const LazySlice*>(rep->data[0]);
    auto s = slice_ref.decode();
    if (s.ok()) {
      *value = static_cast<const Slice&>(slice_ref);
    }
    return s;
  }
};

void LazySliceMeta::lazy_slice_detach(LazySlice* slice,
                                      LazySliceRep* rep) const {
  LazySliceRep new_rep = {};
  auto data = union_cast<DefaultLazySliceMetaImpl::LazySliceRepPtr>(&new_rep);
  Slice value = Slice::Invalid();
  if (slice->valid()) {
    value = DefaultLazySliceMetaImpl::copy_value(data, *slice);
  } else {
    Status s = lazy_slice_decode(rep, slice);
    if (s.ok()) {
      value = DefaultLazySliceMetaImpl::copy_value(data, *slice);
    } else {
      DefaultLazySliceMetaImpl::copy_value(data, s.ToString());
      if (data->err != nullptr) {
        data->err = "LazySlice::detach decode fail & bad alloc";
      }
    }
  }
  slice->reset(default_meta(), new_rep, slice->file_number());
  *slice = value;
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

void LazySlice::reset(const Slice& _value, bool _copy, uint64_t _file_number) {
  file_number_ = _file_number;
  if (!_copy) {
    *this = _value;
    return;
  }
  if (meta_ == LazySliceMeta::buffer_meta()) {
    auto data = union_cast<BufferLazySliceMetaImpl::LazySliceRepPtr>(&rep_);
    data->buffer->assign(_value.data(), _value.size());
    data->err = nullptr;
    return;
  }
  if (meta_ != LazySliceMeta::default_meta()) {
    destroy();
    meta_ = LazySliceMeta::default_meta();
    rep_ = {};
  }
  auto data = union_cast<DefaultLazySliceMetaImpl::LazySliceRepPtr>(&rep_);
  *this = DefaultLazySliceMetaImpl::copy_value(data, _value);
}

void LazySlice::reset_to_buffer(std::string* _buffer) {
  auto data = union_cast<BufferLazySliceMetaImpl::LazySliceRepPtr>(&rep_);
  if (meta_ == LazySliceMeta::buffer_meta()) {
    if (_buffer != nullptr) {
      if (data->buffer != _buffer) {
        if (data->is_owner) {
          delete data->buffer;
          data->is_owner = 0;
        }
        data->buffer = _buffer;
      }
    } else {
      if (!data->is_owner) {
        data->buffer = new std::string;
        data->is_owner = 1;
      }
    }
    data->err = nullptr;
    return;
  }
  destroy();
  if (_buffer != nullptr) {
    data->buffer = _buffer;
    data->is_owner = 0;
  } else {
    data->buffer = new std::string;
    data->is_owner = 1;
  }
  data->ptr = nullptr;
  data->err = nullptr;
}


std::string* LazySlice::trans_to_buffer() {
  auto data = union_cast<BufferLazySliceMetaImpl::LazySliceRepPtr>(&rep_);
  if (meta_ == LazySliceMeta::buffer_meta()) {
    slice() = Slice::Invalid();
    return data->buffer;
  }
  auto s = decode();
  if (s.ok()) {
    auto buffer = new std::string(data_, size_);
    destroy();
    *this = Slice::Invalid();
    meta_ = LazySliceMeta::buffer_meta();
    data->buffer = buffer;
    data->is_owner = 1;
    data->ptr = nullptr;
    data->err = nullptr;
    return buffer;
  }
  destroy();
  auto buffer = new std::string();
  *this = Slice::Invalid();
  meta_ = LazySliceMeta::buffer_meta();
  data->buffer = buffer;
  data->is_owner = 1;
  std::string err_msg = s.ToString();
  data->ptr = (char*)malloc(err_msg.size() + 1);
  if (data->ptr == nullptr) {
    data->err = "LazySlice::trans_to_buffer decode fail & bad alloc";
  } else {
    memcpy(data->ptr, err_msg.c_str(), err_msg.size() + 1);
    data->err = data->ptr;
  }
  return buffer;
}

LazySlice LazySliceReference(const LazySlice& slice) {
  return LazySlice(LazySliceMeta::reference_meta(), slice.raw(), nullptr,
                   &slice, slice.file_number());
}

LazySlice LazySliceCopy(const LazySlice& src) {
  LazySlice dst = LazySliceReference(src);
  dst.detach();
  return dst;
}

void LazySliceCopy(LazySlice& dst, const LazySlice& src) {
  dst = LazySliceReference(src);
  dst.detach();
}

LazySlice LazySliceRemoveSuffix(const LazySlice* slice, size_t fixed_len) {
  // arg       -> fixed_len
  // const_arg -> pointer to slice
  struct LazySliceMetaImpl : public LazySliceMeta {
    void lazy_slice_destroy(const Slice& /*raw*/, void* /*arg*/,
                            const void* const_arg, void* temp) const override {
      assert(const_arg != nullptr); (void)const_arg;
      assert(temp == nullptr); (void)temp;
    }
    Status lazy_slice_decode(const Slice& raw, void* arg,
                             const void* const_arg, void*& temp,
                             Slice* value) const override {
      assert(arg == nullptr); (void)arg;
      assert(temp == nullptr); (void)temp;
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
      *value = Slice(slice_ref.data(), slice_ref.size() - len);
      return s;
    }
  };
  static LazySliceMetaImpl meta_impl;
  assert(slice != nullptr);
  return LazySlice(&meta_impl, slice->raw(),
                   reinterpret_cast<void*>(fixed_len), slice,
                   slice->file_number());
}

}  // namespace rocksdb
