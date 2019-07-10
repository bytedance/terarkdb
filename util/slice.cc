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
  static_assert(sizeof(T) == sizeof(S), "");
  union { S* s; T* t; } u;
  u.s = src;
  return u.t;
}
}

class DefaultLazySliceMetaImpl : public LazySliceMeta {
 public:
  struct DefaultLazySliceRep {
    char* ptr;
    uint64_t size;
    uint64_t cap;
    const char* err;
  };

  void meta_destroy(LazySliceRep* _rep) const override {
    auto rep = union_cast<DefaultLazySliceRep>(_rep);
    free(rep->ptr);
  }
  void meta_pin_resource(LazySlice* slice, LazySliceRep* _rep) const override {
    auto rep = union_cast<DefaultLazySliceRep>(_rep);
    if (slice->empty() || rep->ptr != nullptr || rep->err != nullptr) {
      return;
    }
    *slice = copy_value(rep, *slice);
  }
  Status meta_decode_destructive(LazySlice* slice, LazySliceRep* _rep,
                                 LazySlice* target) const override {
    if (!slice->valid()) {
      auto s = DefaultLazySliceMetaImpl::meta_inplace_decode(slice, _rep);
      if (!s.ok()) {
        return s;
      }
    }
    auto rep = union_cast<DefaultLazySliceRep>(_rep);
    if (rep->ptr == slice->data()) {
      *target = std::move(*slice);
    } else {
      target->reset(*slice, true, slice->file_number());
      slice->reset();
    }
    return Status::OK();
  }
  Status meta_inplace_decode(LazySlice* slice,
                             LazySliceRep* _rep) const override {
    auto rep = union_cast<const DefaultLazySliceRep>(_rep);
    if (rep->err != nullptr) {
      return Status::Corruption(rep->err);
    }
    if (!slice->valid()) {
      return Status::Corruption("Invalid Slice");
    }
    return Status::OK();
  }

  static Slice copy_value(DefaultLazySliceRep* rep, const Slice& value) {
    if (rep->cap < value.size()) {
      auto ptr = (char*)realloc(rep->ptr, value.size());
      if (ptr == nullptr) {
        rep->err = "DefaultLazySliceMetaImpl: bad alloc";
        return Slice::Invalid();
      }
      rep->ptr = ptr;
      rep->cap = value.size();
    }
    memcpy(rep->ptr, value.data(), value.size());
    rep->size = value.size();
    rep->err = nullptr;
    return Slice(rep->ptr, rep->size);
  }
};

struct BufferLazySliceMetaImpl : public LazySliceMeta {
 public:
  struct BufferLazySliceRep {
    std::string* buffer;
    uint64_t is_owner;
    char* ptr;
    const char* err;
  };
  void meta_destroy(LazySliceRep* _rep) const override {
    auto rep = union_cast<BufferLazySliceRep>(_rep);
    if (rep->is_owner) {
      delete rep->buffer;
    }
    free(rep->ptr);
  }
  void meta_assign(LazySlice* slice, LazySliceRep* _rep,
                   Slice value) const override {
    auto rep = union_cast<BufferLazySliceRep>(_rep);
    rep->buffer->assign(value.data(), value.size());
    rep->err = nullptr;
    *slice = *rep->buffer;
  }
  void meta_pin_resource(LazySlice* /*slice*/,
                         LazySliceRep* /*rep*/) const override {}
  Status meta_decode_destructive(LazySlice* slice, LazySliceRep* _rep,
                                 LazySlice* target) const override {
    if (!slice->valid()) {
      auto s = BufferLazySliceMetaImpl::meta_inplace_decode(slice, _rep);
      if (!s.ok()) {
        return s;
      }
    }
    auto rep = union_cast<const BufferLazySliceRep>(_rep);
    if (rep->buffer->data() == slice->data() && rep->is_owner) {
      *target = std::move(*slice);
    } else {
      target->reset(*slice, true, slice->file_number());
      slice->reset();
    }
    return Status::OK();
  }
  Status meta_inplace_decode(LazySlice* slice,
                             LazySliceRep* _rep) const override {
    auto rep = union_cast<const BufferLazySliceRep>(_rep);
    if (rep->err != nullptr) {
      return Status::Corruption(rep->err);
    } else {
      *slice = *rep->buffer;
    }
    return Status::OK();
  }
};

// 0 -> pointer to slice
struct ReferenceLazySliceMetaImpl : public LazySliceMeta {
  void meta_destroy(LazySliceRep* /*rep*/) const override {}
  Status meta_inplace_decode(LazySlice* slice,
                             LazySliceRep* rep) const override {
    const LazySlice& slice_ref =
        *reinterpret_cast<const LazySlice*>(rep->data[0]);
    auto s = slice_ref.inplace_decode();
    if (s.ok()) {
      *slice = static_cast<const Slice&>(slice_ref);
    }
    return s;
  }
};

struct CleanableLazySliceMetaImpl : public LazySliceMeta {
  void meta_destroy(LazySliceRep* rep) const override {
    union_cast<Cleanable>(rep)->Reset();
  }
  void meta_pin_resource(LazySlice* /*slice*/,
                         LazySliceRep* /*rep*/) const override {}
  Status meta_decode_destructive(LazySlice* slice, LazySliceRep* /*rep*/,
                                 LazySlice* target) const override {
    *target = std::move(*slice);
    return Status::OK();
  }
  Status meta_inplace_decode(LazySlice* /*slice*/,
                             LazySliceRep* /*rep*/) const override {
    return Status::OK();
  }
};

void LazySliceMeta::meta_assign(LazySlice* slice, LazySliceRep* _rep,
                                Slice value) const {
  if (slice->meta() == default_meta()) {
    auto rep = union_cast<DefaultLazySliceMetaImpl::DefaultLazySliceRep>(_rep);
    *slice = DefaultLazySliceMetaImpl::copy_value(rep, value);
  } else {
    LazySliceRep new_rep = {};
    auto rep =
        union_cast<DefaultLazySliceMetaImpl::DefaultLazySliceRep>(&new_rep);
    value = DefaultLazySliceMetaImpl::copy_value(rep, value);
    slice->reset(default_meta(), new_rep, slice->file_number());
    *slice = value;
  }
}

void LazySliceMeta::meta_pin_resource(LazySlice* slice, LazySliceRep* /*rep*/) const{
  LazySlice pinned_slice;
  pinned_slice.assign(*slice);
  slice->swap(pinned_slice);
}

Status LazySliceMeta::meta_decode_destructive(LazySlice* /*slice*/,
                                              LazySliceRep* /*rep*/,
                                              LazySlice* /*target*/) const {
  return Status::NotSupported();
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

const LazySliceMeta* LazySliceMeta::cleanable_meta() {
  static CleanableLazySliceMetaImpl meta_impl;
  return &meta_impl;
}

void LazySlice::reset(std::string* _buffer) {
  assert(_buffer != nullptr);
  *this = *_buffer;
  auto rep = union_cast<BufferLazySliceMetaImpl::BufferLazySliceRep>(&rep_);
  if (meta_ == LazySliceMeta::buffer_meta()) {
    if (rep->buffer != _buffer) {
      if (rep->is_owner) {
        delete rep->buffer;
        rep->is_owner = 0;
      }
      rep->buffer = _buffer;
    }
    free(rep->ptr);
    rep->ptr = nullptr;
    rep->err = nullptr;
    return;
  } else {
    destroy();
    meta_ = LazySliceMeta::buffer_meta();
    rep->buffer = _buffer;
    rep->is_owner = 0;
    rep->ptr = nullptr;
    rep->err = nullptr;
  }
}

void LazySlice::assign(const LazySlice& _source) {
  Status s = _source.inplace_decode();
  if (s.ok()) {
    reset(_source, true, _source.file_number());
    return;
  }
  *this = Slice::Invalid();
  if (meta_ != LazySliceMeta::default_meta()) {
    destroy();
    meta_ = LazySliceMeta::default_meta();
    rep_ = {};
  }
  auto rep = union_cast<DefaultLazySliceMetaImpl::DefaultLazySliceRep>(&rep_);
  DefaultLazySliceMetaImpl::copy_value(rep, s.ToString());
  if (rep->err != nullptr) {
    rep->err = "LazySlice::assign decode fail & bad alloc";
  }
}

std::string* LazySlice::trans_to_buffer() {
  assert(meta_ != nullptr);
  auto rep = union_cast<BufferLazySliceMetaImpl::BufferLazySliceRep>(&rep_);
  if (meta_ == LazySliceMeta::buffer_meta()) {
    if (valid()) {
      if (data_ != rep->buffer->data() || size_ != rep->buffer->size()) {
        rep->buffer->assign(data_, size_);
      }
      *this = Slice::Invalid();
    }
    return rep->buffer;
  }
  auto s = inplace_decode();
  if (s.ok()) {
    auto buffer = new std::string(data_, size_);
    destroy();
    *this = Slice::Invalid();
    meta_ = LazySliceMeta::buffer_meta();
    rep->buffer = buffer;
    rep->is_owner = 1;
    rep->ptr = nullptr;
    rep->err = nullptr;
    return buffer;
  }
  destroy();
  auto buffer = new std::string();
  *this = Slice::Invalid();
  meta_ = LazySliceMeta::buffer_meta();
  rep->buffer = buffer;
  rep->is_owner = 1;
  std::string err_msg = s.ToString();
  rep->ptr = (char*)realloc(rep->ptr, err_msg.size() + 1);
  if (rep->ptr == nullptr) {
    rep->err = "LazySlice::trans_to_buffer decode fail & bad alloc";
  } else {
    memcpy(rep->ptr, err_msg.c_str(), err_msg.size() + 1);
    rep->err = rep->ptr;
  }
  return buffer;
}

LazySlice LazySliceReference(const LazySlice& slice) {
  return LazySlice(LazySliceMeta::reference_meta(),
                   {reinterpret_cast<uint64_t>(&slice)}, slice.file_number());
}

LazySlice LazySliceRemoveSuffix(const LazySlice* slice, size_t fixed_len) {
  struct LazySliceMetaImpl : public LazySliceMeta {
    void meta_destroy(LazySliceRep* /*rep*/) const override {}
    Status meta_inplace_decode(LazySlice* slice,
                               LazySliceRep* rep) const override {
      const LazySlice& slice_ref =
          *reinterpret_cast<const LazySlice*>(rep->data[0]);
      uint64_t len = rep->data[1];
      auto s = slice_ref.inplace_decode();
      if (!s.ok()) {
        return s;
      }
      if (slice_ref.size() < len) {
        return Status::Corruption(
            "Error: Could not remove suffix");
      }
      *slice = Slice(slice_ref.data(), slice_ref.size() - len);
      return s;
    }
  };
  static LazySliceMetaImpl meta_impl;
  assert(slice != nullptr);
  return LazySlice(&meta_impl, {reinterpret_cast<uint64_t>(slice), fixed_len},
                   slice->file_number());
}

}  // namespace rocksdb
