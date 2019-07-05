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

class LazySliceMeta;
class FutureSliceMeta;

extern const LazySliceMeta* EmptyLazySliceMeta();

class LazySlice {
protected:
  mutable Slice slice_;
  mutable void *temp_;
  Slice raw_;
  const LazySliceMeta* meta_;
  void *arg_;
  const void *const_arg_;
  uint64_t file_number_;

  void destroy();

public:
  LazySlice();
  explicit LazySlice(const Slice& _slice,
                     uint64_t _file_number = uint64_t(-1));
  LazySlice(const Slice& _raw, const LazySliceMeta* _meta,
            void* _arg = nullptr, const void* _const_arg = nullptr,
            uint64_t _file_number = uint64_t(-1));
  LazySlice(LazySlice&& _slice, uint64_t _file_number);
  LazySlice(LazySlice&& _slice) noexcept ;
  LazySlice(const LazySlice&) = delete;
  ~LazySlice() {
    destroy();
  }

  LazySlice& operator = (LazySlice&& _slice) noexcept {
    reset(std::move(_slice), _slice.file_number_);
    return *this;
  }
  LazySlice& operator = (const LazySlice&) = delete;

  void reset();
  void reset(const Slice& _slice, uint64_t _file_number = uint64_t(-1));
  void reset(const Slice& _raw, const LazySliceMeta* _meta,
             void* arg, const void* _const_arg,
             uint64_t _file_number = uint64_t(-1));
  void reset(LazySlice&& _slice, uint64_t _file_number);

  void clear() { reset(Slice()); }
  bool valid() const { return meta_ != nullptr; }
  const Slice& raw() const { return raw_; }
  const LazySliceMeta *meta() const { return meta_; }
  void* arg() const { return arg_; }
  const void* const_arg() const { return const_arg_; }
  uint64_t file_number() const { return file_number_; }

  const Slice* get() const { assert(slice_.valid()); return &slice_; }
  const Slice& operator*() const { assert(slice_.valid()); return slice_; }
  const Slice* operator->() const { assert(slice_.valid()); return &slice_; }

  Status decode() const;
};

class FutureSliceMeta {
public:
  virtual LazySlice to_lazy_slice(const Slice& storage) const = 0;
  virtual ~FutureSliceMeta() = default;
};

class FutureSlice {
protected:
  const FutureSliceMeta* meta_;
  std::string storage_;
public:
  FutureSlice() : meta_(nullptr) {}
  explicit FutureSlice(const Slice& slice, bool copy = true,
                       uint64_t file_number = uint64_t(-1))
      : meta_(nullptr) {
    reset(slice, copy, file_number);
  }
  explicit FutureSlice(const LazySlice& slice, bool copy = false)
      : meta_(nullptr) {
    reset(slice, copy);
  }
  FutureSlice(const FutureSliceMeta* meta, std::string &&storage)
      : meta_(meta), storage_(std::move(storage)) {}
  FutureSlice(FutureSlice&&) = default;
  FutureSlice(const FutureSlice&) = default;

  FutureSlice& operator = (FutureSlice&&) = default;
  FutureSlice& operator = (const FutureSlice&) = default;

  void reset() { storage_.clear(); meta_ = nullptr; }
  void reset(const Slice& slice, bool copy = true,
             uint64_t file_number = uint64_t(-1));
  void reset(const LazySlice& slice, bool copy = false);
  void reset(const FutureSliceMeta* meta, std::string &&storage) {
    meta_ = meta;
    storage_ = std::move(storage);
  }

  std::string* buffer();
  void clear() { buffer()->clear(); }
  bool valid() const { return meta_ != nullptr; }

  LazySlice get() const{
    assert(valid());
    return meta_->to_lazy_slice(storage_);
  }
};

class LazySliceMeta {
public:
  virtual void destroy(Slice& /*raw*/, void* /*arg*/,
                       const void* /*const_arg*/, void* /*temp*/) const {}
  virtual Status decode(const Slice& raw, void* arg, const void* const_arg,
                        void*& /*temp*/, Slice* value) const = 0;
  virtual Status to_future(const LazySlice& /*slice*/,
                           FutureSlice* /*future_slice*/) const {
    return Status::NotSupported();
  }
  virtual ~LazySliceMeta() = default;
};

inline LazySlice::LazySlice()
    : slice_(Slice::Invalid()),
      temp_(nullptr),
      raw_(Slice::Invalid()),
      meta_(EmptyLazySliceMeta()),
      arg_(nullptr),
      const_arg_(nullptr),
      file_number_(uint64_t(-1)) {}

inline LazySlice::LazySlice(const Slice& _slice, uint64_t _file_number)
    : slice_(_slice),
      temp_(nullptr),
      raw_(Slice::Invalid()),
      meta_(EmptyLazySliceMeta()),
      arg_(nullptr),
      const_arg_(nullptr),
      file_number_(_file_number) {}

inline LazySlice::LazySlice(const Slice& _raw, const LazySliceMeta* _meta,
                            void* _arg, const void* _const_arg,
                            uint64_t _file_number)
    : slice_(Slice::Invalid()),
      temp_(nullptr),
      raw_(_raw),
      meta_(_meta),
      arg_(_arg),
      const_arg_(_const_arg),
      file_number_(_file_number) {
  assert(_meta != nullptr);
}

inline LazySlice::LazySlice(LazySlice&& _slice, uint64_t _file_number)
    : slice_(_slice.slice_),
      temp_(_slice.temp_),
      raw_(_slice.raw_),
      meta_(_slice.meta_),
      arg_(_slice.arg_),
      const_arg_(_slice.const_arg_),
      file_number_(_file_number) {
  _slice.meta_ = nullptr;
}

inline LazySlice::LazySlice(LazySlice&& _slice) noexcept
    : slice_(_slice.slice_),
      temp_(_slice.temp_),
      raw_(_slice.raw_),
      meta_(_slice.meta_),
      arg_(_slice.arg_),
      const_arg_(_slice.const_arg_),
      file_number_(_slice.file_number_) {
  _slice.meta_ = nullptr;
}

inline void LazySlice::destroy() {
  if (meta_ != nullptr) {
    meta_->destroy(raw_, arg_, const_arg_, temp_);
  }
}

inline void LazySlice::reset() {
  destroy();
  slice_ = Slice::Invalid();
  temp_ = nullptr;
  raw_ = Slice::Invalid();
  arg_ = nullptr;
  const_arg_ = nullptr;
  meta_ = nullptr;
  file_number_ = uint64_t(-1);
}

inline void LazySlice::reset(const Slice& _slice, uint64_t _file_number) {
  destroy();
  slice_ = _slice;
  temp_ = nullptr;
  raw_ = Slice::Invalid();
  meta_ = EmptyLazySliceMeta();
  arg_ = nullptr;
  const_arg_ = nullptr;
  file_number_ = _file_number;
}

inline void LazySlice::reset(const Slice& _raw, const LazySliceMeta* _meta,
                             void* _arg, const void* _const_arg,
                             uint64_t _file_number) {
  assert(_meta != nullptr);
  destroy();
  slice_ = Slice::Invalid();
  temp_ = nullptr;
  raw_ = _raw;
  meta_ = _meta;
  arg_ = _arg;
  const_arg_ = _const_arg;
  file_number_ = _file_number;
}

inline void LazySlice::reset(LazySlice&& _slice, uint64_t _file_number) {
  assert(this != &_slice);
  destroy();
  slice_ = _slice.slice_;
  temp_ = _slice.temp_;
  raw_ = _slice.raw_;
  meta_ = _slice.meta_;
  arg_ = _slice.arg_;
  const_arg_ = _slice.const_arg_;
  file_number_ = _file_number;
  _slice.meta_ = nullptr;
}

inline Status LazySlice::decode() const {
  if (slice_.valid()) {
    return Status::OK();
  }
  if (meta_ == nullptr) {
    return Status::Corruption("Invalid LazySlice");
  }
  return meta_->decode(raw_, arg_, const_arg_, temp_, &slice_);
}

extern LazySlice MakeReferenceOfLazySlice(const LazySlice& slice);
extern FutureSlice MakeReferenceOfFutureSlice(const FutureSlice& slice);
extern FutureSlice MakeRemoveSuffixReferenceOfFutureSlice(
    const FutureSlice& slice, size_t fixed_len);
extern FutureSlice MakeFutureSliceWrapperOfLazySlice(const LazySlice& slice);

}  // namespace rocksdb