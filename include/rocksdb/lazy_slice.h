// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Slice is a simple structure containing a pointer into some external
// storage and a size.  The user of a Slice must ensure that the slice
// is not used after the corresponding external storage has been
// deallocated.
//
// Multiple threads can invoke const methods on a Slice without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Slice must use
// external synchronization.

#pragma once

#include <utility>
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace rocksdb {

class LazySliceMeta {
public:
  virtual void destroy(const Slice& raw, void* arg,
                       const void* const_arg, void* temp) const = 0;
  virtual void detach(Slice& value, const LazySliceMeta*& meta, Slice& raw,
                      void*& arg, const void*& const_arg,
                      void*& temp) const;
  virtual Status decode(const Slice& raw, void* arg, const void* const_arg,
                        void*& temp, Slice* value) const = 0;
  virtual ~LazySliceMeta() = default;

  static const LazySliceMeta* default_meta();
  static const LazySliceMeta* buffer_meta();
  static const LazySliceMeta* reference_meta();
};

extern const LazySliceMeta* EmptyLazySliceMeta();

class LazySlice {
protected:
  mutable Slice value_;
  const LazySliceMeta* meta_;
  Slice raw_;
  void *arg_;
  const void *const_arg_;
  mutable void *temp_;
  uint64_t file_number_;

  void destroy();

public:
  LazySlice() noexcept;
  LazySlice(LazySlice&& _slice) noexcept;
  LazySlice(const LazySlice& _slice) = delete;
  explicit LazySlice(const Slice& _value, bool copy = true,
                     uint64_t _file_number = uint64_t(-1));
  LazySlice(const LazySliceMeta* _meta, const Slice& _raw,
            void* _arg = nullptr, const void* _const_arg = nullptr,
            uint64_t _file_number = uint64_t(-1)) noexcept;
  ~LazySlice() {
    destroy();
  }

  LazySlice& operator = (LazySlice&& _slice) noexcept {
    reset(std::move(_slice), _slice.file_number_);
    return *this;
  }
  LazySlice& operator = (const LazySlice& _slice) = delete;

  void reset();
  void reset(const Slice& _value, bool copy = true,
             uint64_t _file_number = uint64_t(-1));
  void reset(const LazySliceMeta* _meta, const Slice& _raw,
             void* arg, const void* _const_arg,
             uint64_t _file_number = uint64_t(-1));
  void reset(LazySlice&& _slice, uint64_t _file_number);

  void reset_to_buffer(std::string* _buffer = nullptr);
  std::string* get_buffer();

  void clear() { reset(); }
  bool is_buffer() const { return meta_ == LazySliceMeta::buffer_meta(); }
  bool valid() const { return meta_ != nullptr; }
  const Slice& raw() const { return raw_; }
  const LazySliceMeta *meta() const { return meta_; }
  void* arg() const { return arg_; }
  const void* const_arg() const { return const_arg_; }
  uint64_t file_number() const { return file_number_; }

  const Slice* get() const { assert(value_.valid()); return &value_; }
  const Slice& operator*() const { assert(value_.valid()); return value_; }
  const Slice* operator->() const { assert(value_.valid()); return &value_; }

  void swap(LazySlice& slice) noexcept;
  void detach();
  Status decode() const;
};

inline LazySlice::LazySlice() noexcept
    : value_(),
      meta_(LazySliceMeta::default_meta()),
      raw_(Slice::Invalid()),
      arg_(nullptr),
      const_arg_(nullptr),
      temp_(nullptr),
      file_number_(uint64_t(-1)) {}

inline LazySlice::LazySlice(LazySlice&& _slice) noexcept
    : value_(_slice.value_),
      meta_(_slice.meta_),
      raw_(_slice.raw_),
      arg_(_slice.arg_),
      const_arg_(_slice.const_arg_),
      temp_(_slice.temp_),
      file_number_(_slice.file_number_) {
  _slice.meta_ = nullptr;
}

inline LazySlice::LazySlice(const Slice& _value, bool copy, uint64_t _file_number)
    : value_(_value),
      meta_(LazySliceMeta::default_meta()),
      raw_(Slice::Invalid()),
      arg_(nullptr),
      const_arg_(nullptr),
      temp_(nullptr),
      file_number_(_file_number) {
  if (copy) {
    reset(_value, true, _file_number);
  }
}

inline LazySlice::LazySlice(const LazySliceMeta* _meta, const Slice& _raw,
                            void* _arg, const void* _const_arg,
                            uint64_t _file_number) noexcept
    : value_(Slice::Invalid()),
      meta_(_meta),
      raw_(_raw),
      arg_(_arg),
      const_arg_(_const_arg),
      temp_(nullptr),
      file_number_(_file_number) {
  assert(_meta != nullptr);
}

inline void LazySlice::destroy() {
  if (meta_ != nullptr) {
    meta_->destroy(raw_, arg_, const_arg_, temp_);
  }
}

inline void LazySlice::reset() {
  destroy();
  value_ = Slice();
  meta_ = LazySliceMeta::default_meta();
  raw_ = Slice::Invalid();
  arg_ = nullptr;
  const_arg_ = nullptr;
  temp_ = nullptr;
  file_number_ = uint64_t(-1);
}

inline void LazySlice::reset(const LazySliceMeta* _meta, const Slice& _raw,
                             void* _arg, const void* _const_arg,
                             uint64_t _file_number) {
  assert(_meta != nullptr);
  destroy();
  value_ = Slice::Invalid();
  meta_ = _meta;
  raw_ = _raw;
  arg_ = _arg;
  const_arg_ = _const_arg;
  temp_ = nullptr;
  file_number_ = _file_number;
}

inline void LazySlice::reset(LazySlice&& _slice, uint64_t _file_number) {
  assert(this != &_slice);
  destroy();
  value_ = _slice.value_;
  meta_ = _slice.meta_;
  raw_ = _slice.raw_;
  arg_ = _slice.arg_;
  const_arg_ = _slice.const_arg_;
  temp_ = _slice.temp_;
  file_number_ = _file_number;
  _slice.meta_ = nullptr;
}

inline void LazySlice::reset_to_buffer(std::string* _buffer) {
  if (meta_ == LazySliceMeta::buffer_meta() || arg_ == _buffer ||
      _buffer == nullptr) {
    return;
  }
  destroy();
  value_ = Slice::Invalid();
  meta_ = LazySliceMeta::buffer_meta();
  raw_ = Slice::Invalid();
  if (_buffer == nullptr) {
    const_arg_ = arg_ = new std::string();
  } else {
    arg_ = new std::string();
    const_arg_ = nullptr;
  }
  temp_ = nullptr;
  file_number_ = uint64_t(-1);
}

inline std::string* LazySlice::get_buffer() {
  assert(meta_ == LazySliceMeta::buffer_meta());
  return reinterpret_cast<std::string*>(arg_);
}

inline void LazySlice::swap(LazySlice& _slice) noexcept {
  std::swap(value_, _slice.value_);
  std::swap(meta_, _slice.meta_);
  std::swap(raw_, _slice.raw_);
  std::swap(arg_, _slice.arg_);
  std::swap(const_arg_, _slice.const_arg_);
  std::swap(temp_, _slice.temp_);
  std::swap(file_number_, _slice.file_number_);
}

inline void LazySlice::detach() {
  meta_->detach(value_, meta_, raw_, arg_, const_arg_, temp_);
}

inline Status LazySlice::decode() const {
  if (value_.valid()) {
    return Status::OK();
  }
  if (meta_ == nullptr) {
    return Status::Corruption("Invalid LazySlice");
  }
  return meta_->decode(raw_, arg_, const_arg_, temp_, &value_);
}

extern LazySlice LazySliceReference(const LazySlice& slice);
extern void LazySliceCopy(LazySlice& dst, const LazySlice& src);
extern LazySlice LazySliceRemoveSuffix(const LazySlice* slice,
                                       size_t fixed_len);
extern bool LazySliceTransToBuffer(LazySlice& slice);

}  // namespace rocksdb