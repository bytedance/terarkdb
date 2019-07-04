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

extern const LazySliceMeta* InvalidLazySliceMeta();

class LazySlice {
protected:
  mutable Slice slice_;
  Slice raw_;
  const LazySliceMeta* meta_;
  const void *arg0_, *arg1_;
  uint64_t file_number_;

  void destroy();

public:
  LazySlice();
  explicit LazySlice(const Slice& _slice,
                     uint64_t _file_number = uint64_t(-1));
  LazySlice(const Slice& _raw, const LazySliceMeta* _meta,
            const void* _arg0 = nullptr, const void* _arg1 = nullptr,
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
             const void* _arg0, const void* _arg1,
             uint64_t _file_number = uint64_t(-1));
  void reset(LazySlice&& _slice, uint64_t _file_number);

  bool valid() const { return meta_ != InvalidLazySliceMeta(); }
  const Slice& raw() const { return raw_; }
  const LazySliceMeta *meta() const { return meta_; }
  std::pair<const void*, const void*> args() const {
    return {arg0_, arg1_};
  }
  uint64_t file_number() const { return file_number_; }

  const Slice* get() const { assert(slice_.valid()); return &slice_; }
  const Slice& operator*() const { assert(slice_.valid()); return slice_; }
  const Slice* operator->() const { assert(slice_.valid()); return &slice_; }

  Status decode() const;
};

extern LazySlice MakeLazySliceReference(const LazySlice& slice);

class FutureSliceMeta {
public:
  virtual LazySlice to_lazy_slice(const Slice& storage) const = 0;
  virtual ~FutureSliceMeta() = default;
};

class FutureSlice {
protected:
  std::string storage_;
  const FutureSliceMeta* meta_;
public:
  FutureSlice() : meta_(nullptr) {}
  explicit FutureSlice(const Slice& slice, bool copy = true,
                       uint64_t file_number = uint64_t(-1))
      : meta_(nullptr) {
    reset(slice, copy, file_number);
  }
  explicit FutureSlice(const LazySlice& slice, bool copy = true)
      : meta_(nullptr) {
    reset(slice, copy);
  }
  FutureSlice(std::string &&storage, const FutureSliceMeta* meta)
      : storage_(std::move(storage)), meta_(meta) {}
  FutureSlice(FutureSlice&&) = default;
  FutureSlice(const FutureSlice&) = default;

  FutureSlice& operator = (FutureSlice&&) = default;
  FutureSlice& operator = (const FutureSlice&) = default;

  void reset() { storage_.clear(); meta_ = nullptr; }
  void reset(const Slice& slice, bool pinned = false,
             uint64_t file_number = uint64_t(-1));
  void reset(const LazySlice& slice, bool copy = true);
  void reset(std::string &&storage, const FutureSliceMeta* meta) {
    storage_ = std::move(storage);
    meta_ = meta;
  }

  std::string* buffer();
  void clear() { *this = FutureSlice(Slice(), false); }
  bool valid() const { return meta_ != nullptr; }

  LazySlice get() const{
    assert(valid());
    return meta_->to_lazy_slice(storage_);
  }
};

class LazySliceMeta {
public:
  virtual void destroy(Slice& /*raw*/, const void* /*_arg0*/,
                       const void* /*_arg1*/) const {}
  virtual Status decode(const Slice& raw, const void* _arg0,
                        const void* _arg1, Slice* value) const = 0;
  virtual Status to_future(const LazySlice& /*slice*/,
                           FutureSlice* /*future_slice*/) const {
    return Status::NotSupported("LazySlice to FutureSlice");
  }
  virtual ~LazySliceMeta() = default;
};

inline LazySlice::LazySlice()
    : slice_(Slice::Invalid()),
      raw_(Slice::Invalid()),
      meta_(InvalidLazySliceMeta()),
      arg0_(nullptr),
      arg1_(nullptr),
      file_number_(uint64_t(-1)) {}

inline LazySlice::LazySlice(const Slice& _slice, uint64_t _file_number)
    : slice_(_slice),
      raw_(Slice::Invalid()),
      meta_(nullptr),
      arg0_(nullptr),
      arg1_(nullptr),
      file_number_(_file_number) {}

inline LazySlice::LazySlice(const Slice& _raw,
                            const LazySliceMeta* _meta,
                            const void* _arg0, const void* _arg1,
                            uint64_t _file_number)
    : slice_(Slice::Invalid()),
      raw_(_raw),
      meta_(_meta),
      arg0_(_arg0),
      arg1_(_arg1),
      file_number_(_file_number) {}

inline LazySlice::LazySlice(LazySlice&& _slice, uint64_t _file_number)
    : slice_(_slice.slice_),
      raw_(_slice.raw_),
      meta_(_slice.meta_),
      arg0_(_slice.arg0_),
      arg1_(_slice.arg1_),
      file_number_(_file_number) {
  _slice.meta_ = nullptr;
}

inline LazySlice::LazySlice(LazySlice&& _slice) noexcept
    : slice_(_slice.slice_),
      raw_(_slice.raw_),
      meta_(_slice.meta_),
      arg0_(_slice.arg0_),
      arg1_(_slice.arg1_),
      file_number_(_slice.file_number_) {
  _slice.meta_ = nullptr;
}

inline void LazySlice::destroy() {
  if (meta_ != nullptr) {
    meta_->destroy(raw_, arg0_, arg1_);
  }
}

inline void LazySlice::reset() {
  destroy();
  slice_ = Slice::Invalid();
  raw_ = Slice::Invalid();
  arg0_ = arg1_ = nullptr;
  meta_ = InvalidLazySliceMeta();
  file_number_ = uint64_t(-1);
}

inline void LazySlice::reset(const Slice& _slice, uint64_t _file_number) {
  destroy();
  slice_ = _slice;
  raw_ = Slice::Invalid();
  meta_ = nullptr;
  arg0_ = arg1_ = nullptr;
  file_number_ = _file_number;
}

inline void LazySlice::reset(const Slice& _raw,
                             const LazySliceMeta* _meta,
                             const void* _arg0, const void* _arg1,
                             uint64_t _file_number) {
  destroy();
  slice_ = Slice::Invalid();
  raw_ = _raw;
  meta_ = _meta;
  arg0_ = _arg0;
  arg1_ = _arg1;
  file_number_ = _file_number;
}

inline void LazySlice::reset(LazySlice&& _slice, uint64_t _file_number) {
  assert(this != &_slice);
  destroy();
  slice_ = _slice.slice_;
  raw_ = _slice.raw_;
  meta_ = _slice.meta_;
  arg0_ = _slice.arg0_;
  arg1_ = _slice.arg1_;
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
  return meta_->decode(raw_, arg0_, arg1_, &slice_);
}

}  // namespace rocksdb