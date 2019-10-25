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
  static const char hextab[] = "0123456789ABCDEF";
  if (hex && size_) {
    result.resize(2 * size_);
    auto beg = &result[0];
    for (size_t i = 0; i < size_; ++i) {
      unsigned char c = data_[i];
      beg[i * 2 + 0] = hextab[c >> 4];
      beg[i * 2 + 1] = hextab[c & 0xf];
    }
  } else {
    result.assign(data_, size_);
  }
  return result;
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
  static_assert(alignof(T) == alignof(S), "");

  union { S* s; T* t; } u;
  u.s = src;
  return u.t;
}
}

class DefaultLazySliceControllerImpl : public LazySliceController {
public:
  struct alignas(LazySliceRep) Rep {
    char data[sizeof(LazySliceRep) - 1];
    uint8_t size;
  };

  void destroy(LazySliceRep* /*rep*/) const override {}
  void pin_resource(LazySlice* slice,
                    LazySliceRep* _rep) const override {
    auto rep = union_cast<const Rep>(_rep);
    if (slice->data() < rep->data || slice->data() >= rep->data + rep->size) {
      LazySliceController::assign(slice, _rep, *slice);
    }
  }
  Status decode_destructive(LazySlice* slice, LazySliceRep* /*rep*/,
                            LazySlice* target) const override {
    target->reset(*slice, true, slice->file_number());
    return Status::OK();
  }
  Status inplace_decode(LazySlice* /*slice*/,
                        LazySliceRep* /*rep*/) const override {
    assert(false);
    return Status::Corruption("Invalid Slice");
  }

  static Slice save_slice(LazySliceRep* _rep, const Slice& value) {
    auto rep = union_cast<Rep>(_rep);
    assert(value.size() <= sizeof rep->data);
    memmove(rep->data, value.data(), value.size());
    rep->size = value.size();
    return Slice(rep->data, rep->size);
  }
};

class MallocLazySliceControllerImpl : public LazySliceController {
 public:
  struct Rep {
    char* ptr;
    uint64_t size;
    uint64_t cap;
    const char* err;
  };

  void destroy(LazySliceRep* _rep) const override {
    auto rep = union_cast<Rep>(_rep);
    free(rep->ptr);
  }
  void assign(LazySlice* slice, LazySliceRep* rep, Slice value) const override {
    assign_slice(*slice, save_slice(rep, value));
  }
  void pin_resource(LazySlice* slice, LazySliceRep* _rep) const override {
    auto rep = union_cast<const Rep>(_rep);
    if (slice->valid() && (slice->data() < rep->ptr ||
                           slice->data() >= rep->ptr + rep->size)) {
      MallocLazySliceControllerImpl::assign(slice, _rep, *slice);
    }
  }
  Status decode_destructive(LazySlice* slice, LazySliceRep* _rep,
                            LazySlice* target) const override {
    auto rep = union_cast<const Rep>(_rep);
    if (!slice->valid()) {
      auto s = MallocLazySliceControllerImpl::inplace_decode(slice, _rep);
      if (!s.ok()) {
        return s;
      }
    } else if (slice->data() < rep->ptr ||
               slice->data() >= rep->ptr + rep->size) {
      target->reset(*slice, true, slice->file_number());
      return Status::OK();
    }
    *target = std::move(*slice);
    return Status::OK();
  }
  Status inplace_decode(LazySlice* slice, LazySliceRep* _rep) const override {
    assert(!slice->valid()); (void)slice;
    auto rep = union_cast<const Rep>(_rep);
    assert(rep->err != nullptr);
    return Status::Corruption(rep->err);
  }

  static Slice save_slice(LazySliceRep* _rep, const Slice& value) {
    auto rep = union_cast<Rep>(_rep);
    if (rep->cap < value.size()) {
      auto ptr = (char*)realloc(rep->ptr, value.size());
      if (ptr == nullptr) {
        rep->err = "MallocLazySliceControllerImpl::assign : bad alloc";
        return Slice::Invalid();
      }
      rep->ptr = ptr;
      rep->cap = value.size();
    }
    memmove(rep->ptr, value.data(), value.size());
    rep->size = value.size();
    rep->err = nullptr;
    return Slice(rep->ptr, rep->size);
  }
};

struct BufferLazySliceControllerImpl : public LazySliceController {
 public:
  struct Rep {
    std::string* buffer;
    uint64_t is_owner;
    char* ptr;
    const char* err;
  };
  void destroy(LazySliceRep* _rep) const override {
    auto rep = union_cast<Rep>(_rep);
    if (rep->is_owner) {
      delete rep->buffer;
    }
    free(rep->ptr);
  }
  void assign(LazySlice* slice, LazySliceRep* _rep,
              Slice value) const override {
    auto rep = union_cast<Rep>(_rep);
    rep->buffer->assign(value.data(), value.size());
    rep->err = nullptr;
    assign_slice(*slice, *rep->buffer);
  }
  void pin_resource(LazySlice* slice,
                    LazySliceRep* _rep) const override {
    auto buffer = union_cast<Rep>(_rep)->buffer;
    if (slice->valid() && (slice->data() < buffer->data() ||
                           slice->data() >= buffer->data() + buffer->size())) {
      buffer->assign(slice->data(), slice->size());
      assign_slice(*slice, *buffer);
    }
  }
  Status decode_destructive(LazySlice* slice, LazySliceRep* _rep,
                            LazySlice* target) const override {
    auto rep = union_cast<Rep>(_rep);
    auto buffer = rep->buffer;
    if (!slice->valid()) {
      auto s = BufferLazySliceControllerImpl::inplace_decode(slice, _rep);
      if (!s.ok()) {
        return s;
      }
    } else if (!rep->is_owner || slice->data() < buffer->data() ||
               slice->data() >= buffer->data() + buffer->size()) {
      target->reset(*slice, true, slice->file_number());
      return Status::OK();
    }
    *target = std::move(*slice);
    return Status::OK();
  }
  Status inplace_decode(LazySlice* slice, LazySliceRep* _rep) const override {
    assert(!slice->valid());
    auto rep = union_cast<const Rep>(_rep);
    if (rep->err != nullptr) {
      return Status::Corruption(rep->err);
    } else {
      assign_slice(*slice, *rep->buffer);
    }
    return Status::OK();
  }
};

// 0 -> pointer to slice
struct ReferenceLazySliceControllerImpl : public LazySliceController {
  void destroy(LazySliceRep* /*rep*/) const override {}
  Status inplace_decode(LazySlice* slice, LazySliceRep* rep) const override {
    assert(!slice->valid());
    const LazySlice& slice_ref =
        *reinterpret_cast<const LazySlice*>(rep->data[0]);
    auto s = slice_ref.inplace_decode();
    if (s.ok()) {
      assign_slice(*slice, slice_ref);
    }
    return s;
  }
};

struct CleanableLazySliceControllerImpl : public LazySliceController {
  void destroy(LazySliceRep* rep) const override {
    union_cast<Cleanable>(rep)->Reset();
  }
  void pin_resource(LazySlice* /*slice*/,
                    LazySliceRep* /*rep*/) const override {}
  Status decode_destructive(LazySlice* slice, LazySliceRep* /*rep*/,
                            LazySlice* target) const override {
    *target = std::move(*slice);
    return Status::OK();
  }
  Status inplace_decode(LazySlice* /*slice*/,
                        LazySliceRep* /*rep*/) const override {
    return Status::OK();
  }
};

void LazySliceController::assign(LazySlice* slice, LazySliceRep* rep,
                                 Slice value) const {
  assert(value.valid());
  if (value.size() < sizeof(LazySliceRep)) {
    slice->reset(default_coltroller(), {}, slice->file_number());
    slice->assign_slice(DefaultLazySliceControllerImpl::save_slice(rep, value));
  } else {
    slice->reset(malloc_coltroller(), {}, slice->file_number());
    slice->assign_slice(MallocLazySliceControllerImpl::save_slice(rep, value));
  }
}

void LazySliceController::pin_resource(LazySlice* slice,
                                       LazySliceRep* /*rep*/) const{
  LazySlice pinned_slice;
  auto s = slice->decode_destructive(pinned_slice);
  if (s.ok()) {
    *slice = std::move(pinned_slice);
  } else {
    slice->assign_error(s.ToString());
  }
}

Status LazySliceController::decode_destructive(LazySlice* slice,
                                               LazySliceRep* rep,
                                               LazySlice* target) const {
  if (!slice->slice_.valid()) {
    auto s = inplace_decode(slice, rep);
    if (!s.ok()) {
      return s;
    }
  }
  assign(target, &target->rep_, *slice);
  target->file_number_ = slice->file_number_;
  slice->clear();
  return Status::OK();
}

const LazySliceController* LazySliceController::default_coltroller() {
  static DefaultLazySliceControllerImpl controller_impl;
  return &controller_impl;
}

const LazySliceController* LazySliceController::malloc_coltroller() {
  static MallocLazySliceControllerImpl controller_impl;
  return &controller_impl;
}

const LazySliceController* LazySliceController::reference_controller() {
  static ReferenceLazySliceControllerImpl controller_impl;
  return &controller_impl;
}

const LazySliceController* LazySliceController::buffer_controller() {
  static BufferLazySliceControllerImpl controller_impl;
  return &controller_impl;
}

const LazySliceController* LazySliceController::cleanable_controller() {
  static CleanableLazySliceControllerImpl controller_impl;
  return &controller_impl;
}

void LazySlice::assign_error(const Slice& error) {
  if (controller_ != LazySliceController::malloc_coltroller()) {
    reset(LazySliceController::malloc_coltroller(), {}, file_number_);
  } else {
    slice_ = Slice::Invalid();
  }
  MallocLazySliceControllerImpl::save_slice(&rep_, error);
  auto rep = union_cast<MallocLazySliceControllerImpl::Rep>(&rep_);
  if (rep->err == nullptr) {
    rep->err = rep->ptr;
  } else {
    rep->err = "LazySlice unknow error & bad alloc";
  }
}

void LazySlice::fix_default_coltroller(const LazySlice& other) {
  assert(controller_ == LazySliceController::default_coltroller());
  if (other.slice_.size() < sizeof(LazySliceRep)) {
    assign_slice(DefaultLazySliceControllerImpl::save_slice(&rep_,
                                                            other.slice_));
  }
}

void LazySlice::reset(std::string* buffer) {
  assert(buffer != nullptr);
  assign_slice(*buffer);
  auto rep = union_cast<BufferLazySliceControllerImpl::Rep>(&rep_);
  if (controller_ == LazySliceController::buffer_controller()) {
    if (rep->buffer != buffer) {
      if (rep->is_owner) {
        delete rep->buffer;
        rep->is_owner = 0;
      }
      rep->buffer = buffer;
    }
    free(rep->ptr);
    rep->ptr = nullptr;
    rep->err = nullptr;
    return;
  } else {
    destroy();
    controller_ = LazySliceController::buffer_controller();
    rep->buffer = buffer;
    rep->is_owner = 0;
    rep->ptr = nullptr;
    rep->err = nullptr;
  }
}

void LazySlice::assign(const LazySlice& source) {
  Status s = source.inplace_decode();
  if (s.ok()) {
    reset(source, true, source.file_number());
  } else {
    assign_error(s.ToString());
  }
}

std::string* LazySlice::trans_to_buffer() {
  assert(controller_ != nullptr);
  auto rep = union_cast<BufferLazySliceControllerImpl::Rep>(&rep_);
  file_number_ = uint64_t(-1);
  if (controller_ == LazySliceController::buffer_controller()) {
    if (slice_.valid()) {
      if (slice_.data() != rep->buffer->data() ||
          slice_.size() != rep->buffer->size()) {
        rep->buffer->assign(slice_.data(), slice_.size());
      }
      assign_slice(Slice::Invalid());
    }
    return rep->buffer;
  }
  auto s = inplace_decode();
  if (s.ok()) {
    auto buffer = new std::string(slice_.data(), slice_.size());
    destroy();
    assign_slice(Slice::Invalid());
    controller_ = LazySliceController::buffer_controller();
    rep->buffer = buffer;
    rep->is_owner = 1;
    rep->ptr = nullptr;
    rep->err = nullptr;
    return buffer;
  }
  destroy();
  auto buffer = new std::string();
  assign_slice(Slice::Invalid());
  controller_ = LazySliceController::buffer_controller();
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
  return LazySlice(LazySliceController::reference_controller(),
                   {reinterpret_cast<uint64_t>(&slice)}, slice.file_number());
}

LazySlice LazySliceRemoveSuffix(const LazySlice* slice, size_t fixed_len) {
  struct LazySliceControllerImpl : public LazySliceController {
    void destroy(LazySliceRep* /*rep*/) const override {}
    Status inplace_decode(LazySlice* slice, LazySliceRep* rep) const override {
      assert(!slice->valid());
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
      assign_slice(*slice, Slice(slice_ref.data(), slice_ref.size() - len));
      return s;
    }
  };
  static LazySliceControllerImpl controller_impl;
  assert(slice != nullptr);
  return LazySlice(&controller_impl, {reinterpret_cast<uint64_t>(slice),
                                      fixed_len},
                   slice->file_number());
}

}  // namespace rocksdb
