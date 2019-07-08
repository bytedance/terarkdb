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

class LazySlice;

struct LazySliceRep {
  uint64_t data[4];
};

class LazySliceMeta {
public:
  virtual void lazy_slice_destroy(LazySliceRep* rep) const = 0;
  virtual void lazy_slice_detach(LazySlice* slice, LazySliceRep* rep) const;
  virtual Status lazy_slice_decode(const LazySliceRep* rep,
                                   Slice* value) const = 0;
  virtual ~LazySliceMeta() = default;

  // 0 -> mem ptr
  // 1 -> mem size
  // 2 -> mem cap
  // 3 -> error
  static const LazySliceMeta* default_meta();
  // 0 -> std::string ptr
  // 1 -> is owner
  // 2 -> mem ptr
  // 3 -> error
  static const LazySliceMeta* buffer_meta();
  // 0 -> ptr to lazy slice
  static const LazySliceMeta* reference_meta();
};

extern const LazySliceMeta* EmptyLazySliceMeta();

class LazySlice : public Slice {
  friend class LazySliceMeta;
 private:
  using Slice::data_;
  using Slice::size_;
  const LazySliceMeta* meta_;
  LazySliceRep rep_;
  uint64_t file_number_;

  void destroy();
  Slice& slice() const { return *const_cast<LazySlice*>(this); }

 public:
  LazySlice() noexcept;
  LazySlice(LazySlice&& _slice) noexcept;
  LazySlice(const LazySlice& _slice) = delete;
  LazySlice(const Slice& _value, bool copy = false,
            uint64_t _file_number = uint64_t(-1));
  LazySlice(std::string* _buffer) noexcept;
  LazySlice(const LazySliceMeta* _meta, const LazySliceRep& _rep,
            uint64_t _file_number = uint64_t(-1)) noexcept;
  ~LazySlice() {
    destroy();
  }

  LazySlice& operator = (LazySlice&& _slice) noexcept {
    reset(std::move(_slice), _slice.file_number_);
    return *this;
  }
  LazySlice& operator = (const LazySlice& _slice) = delete;
  LazySlice& operator = (const Slice& _slice) {
    reset(_slice, false);
    return *this;
  }

  void reset(const Slice& _value, bool _copy = false,
             uint64_t _file_number = uint64_t(-1));
  void reset(const LazySliceMeta* _meta, const LazySliceRep& _rep,
             uint64_t _file_number = uint64_t(-1));
  void reset(LazySlice&& _slice, uint64_t _file_number);

  void reset_to_buffer(std::string* _buffer = nullptr);
  std::string* trans_to_buffer();
  Status save_to_buffer(std::string* _buffer) const;

  bool is_buffer() const { return meta_ == LazySliceMeta::buffer_meta(); }
  uint64_t file_number() const { return file_number_; }

  void release();
  void swap(LazySlice& _slice) noexcept;
  void detach();
  Status decode() const;
};

inline LazySlice::LazySlice() noexcept
    : Slice(),
      meta_(LazySliceMeta::default_meta()),
      rep_({}),
      file_number_(uint64_t(-1)) {}

inline LazySlice::LazySlice(LazySlice&& _slice) noexcept
    : Slice(_slice),
      meta_(_slice.meta_),
      rep_(_slice.rep_),
      file_number_(_slice.file_number_) {
  _slice.meta_ = nullptr;
}

inline LazySlice::LazySlice(const Slice& _value, bool _copy, uint64_t _file_number)
    : Slice(_value),
      meta_(LazySliceMeta::default_meta()),
      rep_({}),
      file_number_(_file_number) {
  if (_copy) {
    reset(_value, true, _file_number);
  }
}

inline LazySlice::LazySlice(std::string* _buffer) noexcept
    : Slice(Slice::Invalid()),
      meta_(LazySliceMeta::buffer_meta()),
      rep_({reinterpret_cast<uint64_t>(_buffer)}),
      file_number_(uint64_t(-1)) {
  assert(_buffer != nullptr);
}

inline LazySlice::LazySlice(const LazySliceMeta* _meta,
                            const LazySliceRep& _rep,
                            uint64_t _file_number) noexcept
    : Slice(Slice::Invalid()),
      meta_(_meta),
      rep_(_rep),
      file_number_(_file_number) {
  assert(_meta != nullptr);
}

inline void LazySlice::destroy() {
  if (meta_ != nullptr) {
    meta_->lazy_slice_destroy(&rep_);
  }
}

inline void LazySlice::reset(const LazySliceMeta* _meta,
                             const LazySliceRep& _rep,
                             uint64_t _file_number) {
  assert(_meta != nullptr);
  destroy();
  *this = Slice::Invalid();
  meta_ = _meta;
  rep_ = _rep;
  file_number_ = _file_number;
}

inline void LazySlice::reset(LazySlice&& _slice, uint64_t _file_number) {
  assert(this != &_slice);
  destroy();
  data_ = _slice.data_;
  size_ = _slice.size_;
  rep_ = _slice.rep_;
  file_number_ = _file_number;
  _slice.meta_ = nullptr;
}

inline Status LazySlice::save_to_buffer(std::string* buffer) const {
  if (!Slice::valid()) {
    auto s = decode();
    if (!s.ok()) {
      return s;
    }
  }
  if (!is_buffer() || reinterpret_cast<std::string*>(rep_.data[0]) != buffer) {
    buffer->assign(data_, size_);
  }
  return Status::OK();
}

inline void LazySlice::release() {
  destroy();
  *this = Slice();
  meta_ = LazySliceMeta::default_meta();
  rep_ = {};
  file_number_ = uint64_t(-1);
}

inline void LazySlice::swap(LazySlice& _slice) noexcept {
  std::swap(data_, _slice.data_);
  std::swap(size_, _slice.size_);
  std::swap(rep_, _slice.rep_);
}

inline void LazySlice::detach() {
  meta_->lazy_slice_detach(this, &rep_);
}

inline Status LazySlice::decode() const {
  if (Slice::valid()) {
    return Status::OK();
  }
  if (meta_ == nullptr) {
    return Status::Corruption("Invalid LazySlice");
  }
  return meta_->lazy_slice_decode(&rep_, &slice());
}

extern LazySlice LazySliceReference(const LazySlice& slice);

extern LazySlice LazySliceCopy(const LazySlice& src);
extern void LazySliceCopy(LazySlice& dst, const LazySlice& src);

extern LazySlice LazySliceRemoveSuffix(const LazySlice* slice,
                                       size_t fixed_len);

}  // namespace rocksdb