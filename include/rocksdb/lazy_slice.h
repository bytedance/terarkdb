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

#include <string>
#include <utility>
#include "rocksdb/cleanable.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace rocksdb {

class LazySlice;

struct LazySliceRep {
  uint64_t data[4];
};

class LazySliceController {
public:
  // release resource
  virtual void destroy(LazySliceRep* rep) const = 0;

  // save value into slice
  virtual void assign(LazySlice* slice, LazySliceRep* rep, Slice value) const;

  // pin the slice, turn the controller into editable
  virtual void pin_resource(LazySlice* slice, LazySliceRep* rep) const;

  // decode slice and save to target, the slice will be destroyed
  virtual Status decode_destructive(LazySlice* slice, LazySliceRep* rep,
                                    LazySlice* target) const;

  // inplace decode slice
  virtual Status inplace_decode(LazySlice* slice, LazySliceRep* rep) const = 0;

  virtual ~LazySliceController() = default;

  // data -> 31 bytes local storage
  static const LazySliceController* default_coltroller();
  // data[0] -> mem ptr
  // data[1] -> mem size
  // data[2] -> mem cap
  // data[3] -> error
  static const LazySliceController* malloc_coltroller();
  // data[0] -> std::string ptr
  // data[1] -> is owner
  // data[2] -> mem ptr
  // data[3] -> error
  static const LazySliceController* buffer_controller();
  // data[0] -> ptr to lazy slice
  static const LazySliceController* reference_controller();
  // rep -> Cleanable
  static const LazySliceController* cleanable_controller();

  static void assign_slice(LazySlice& lazy_slice, const Slice& slice);
};

class LazySlice {
  friend class LazySliceController;
 private:
  Slice slice_;
  const LazySliceController* controller_;
  LazySliceRep rep_;
  uint64_t file_number_;

  void destroy();

  void assign_slice(const Slice& _slice) { slice_ = _slice; }

  void assign_error(const Slice& error);

  void fix_default_coltroller(const LazySlice& other);

 public:

  LazySlice() noexcept;

  LazySlice(LazySlice&& _slice) noexcept;

  // non copyable
  LazySlice(const LazySlice& _slice) = delete;

  // init with slice from copying or referring
  explicit LazySlice(const Slice& _value, bool _copy = false,
                     uint64_t _file_number = uint64_t(-1));

  // init with outer buffer, DO NOT take life cycle of buffer
  explicit LazySlice(std::string* _buffer) noexcept;

  // init from cleanup function for slice
  LazySlice(const Slice& _value, Cleanable::CleanupFunction _func, void* _arg1,
            void* _arg2, uint64_t _file_number = uint64_t(-1)) noexcept;

  // init from cleanup function for slice
  LazySlice(const Slice& _value, Cleanable* _cleanable,
            uint64_t _file_number = uint64_t(-1)) noexcept;

  // init from user defined controller
  LazySlice(const LazySliceController* _controller, const LazySliceRep& _rep,
            uint64_t _file_number = uint64_t(-1)) noexcept;

  ~LazySlice() {
    destroy();
  }

  LazySlice& operator = (LazySlice&& _slice) noexcept {
    reset(std::move(_slice), _slice.file_number_);
    return *this;
  }

  // non copyable
  LazySlice& operator = (const LazySlice& _slice) = delete;

  // assign from a slice
  LazySlice& operator = (const Slice& _slice) {
    reset(_slice);
    return *this;
  }

  operator const Slice&() const { assert(valid()); return slice_; }

  const Slice& slice_ref() const { assert(valid()); return slice_; }
  const Slice* slice_ptr() const { assert(valid()); return &slice_; }

  // Return a pointer to the beginning of the referenced data
  const char* data() const { assert(valid()); return slice_.data(); }

  // Return the length (in bytes) of the referenced data
  size_t size() const { assert(valid()); return slice_.size(); }

  // Return true iff the length of the referenced data is zero
  bool empty() const { assert(valid()); return slice_.empty(); }

  // Return true if Slice valid
  bool valid() const { return slice_.valid(); }

  // Return an invalid Slice
  static Slice Invalid() { return Slice::Invalid(); }

  // Return the ith byte in the referenced data.
  // REQUIRES: n < size()
  char operator[](size_t n) const { assert(valid()); return slice_[n]; }

  // Change this slice to refer to an empty array
  void clear();

  // Drop the first "n" bytes from this slice.
  void remove_prefix(size_t n) { assert(valid()); slice_.remove_prefix(n); }

  // Drop the last "n" bytes from this slice.
  void remove_suffix(size_t n) { assert(valid()); slice_.remove_suffix(n); }

  // Return a string that contains the copy of the referenced data.
  // when hex is true, returns a string of twice the length hex encoded (0-9A-F)
  std::string ToString(bool hex = false) const {
    assert(valid());
    return slice_.ToString(hex);
  }

#ifdef __cpp_lib_string_view
  // Return a string_view that references the same data as this slice.
  std::string_view ToStringView() const {
    assert(valid());
    return slice_.ToStringView();
  }
#endif

  // Decodes the current slice interpreted as an hexadecimal string into result,
  // if successful returns true, if this isn't a valid hex string
  // (e.g not coming from Slice::ToString(true)) DecodeHex returns false.
  // This slice is expected to have an even number of 0-9A-F characters
  // also accepts lowercase (a-f)
  bool DecodeHex(std::string* result) const {
    assert(valid());
    return slice_.DecodeHex(result);
  }

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "b",
  //   == 0 iff "*this" == "b",
  //   >  0 iff "*this" >  "b"
  int compare(const Slice& b) const {
    assert(valid());
    return slice_.compare(b);
  }

  // Return true iff "x" is a prefix of "*this"
  bool starts_with(const Slice& x) const {
    assert(valid());
    return slice_.starts_with(x);
  }

  bool ends_with(const Slice& x) const {
    assert(valid());
    return slice_.ends_with(x);
  }

  // Compare two slices and returns the first byte where they differ
  size_t difference_offset(const Slice& b) const {
    assert(valid());
    return slice_.difference_offset(b);
  }

  // reset slice from copying or referring
  void reset(const Slice& _value, bool _copy = false,
             uint64_t _file_number = uint64_t(-1));

  // move assign slice with file number
  void reset(LazySlice&& _slice, uint64_t _file_number);

  // reset outer buffer, DO NOT take life cycle of buffer
  void reset(std::string* _buffer);

  // reset cleanup function for slice
  void reset(const Slice& _value, Cleanable::CleanupFunction _func,
             void* _arg1, void* _arg2, uint64_t _file_number = uint64_t(-1));

  // reset cleanup function for slice
  void reset(const Slice& _value, Cleanable* _cleanable,
             uint64_t _file_number = uint64_t(-1));

  // reset to user defined controller
  void reset(const LazySliceController* _controller, const LazySliceRep& _rep,
             uint64_t _file_number = uint64_t(-1));

  void reset_file_number() { file_number_ = uint64_t(-1); }

  // decode source and copy it
  void assign(const LazySlice& _source);

  // trans this to buffer for modification
  std::string* trans_to_buffer();

  // save data to buffer
  Status save_to_buffer(std::string* _buffer) const;

  // return the certain file number of SST, -1 for unknown
  uint64_t file_number() const { return file_number_; }

  const LazySliceController* controller() const { return controller_; }

  // pin this slice, turn the controller into editable
  void pin_resource();

  // decode this slice and save to target, this slice will be destroyed
  Status decode_destructive(LazySlice& _target);

  // decode this slice inplace
  Status inplace_decode() const;
};

inline void LazySliceController::assign_slice(LazySlice& lazy_slice,
                                              const Slice& slice) {
  lazy_slice.assign_slice(slice);
}

inline void LazySlice::destroy() {
  if (controller_ != nullptr) {
    controller_->destroy(&rep_);
  }
}

inline LazySlice::LazySlice() noexcept
    : controller_(LazySliceController::default_coltroller()),
      rep_{},
      file_number_(uint64_t(-1)) {}

inline LazySlice::LazySlice(LazySlice&& _slice) noexcept
    : slice_(_slice),
      controller_(_slice.controller_),
      rep_(_slice.rep_),
      file_number_(_slice.file_number_) {
  if (controller_ == LazySliceController::default_coltroller()) {
    fix_default_coltroller(_slice);
  }
  _slice.slice_ = Slice::Invalid();
  _slice.controller_ = nullptr;
}

inline LazySlice::LazySlice(const Slice& _value, bool _copy,
                            uint64_t _file_number)
    : slice_(_value),
      controller_(LazySliceController::default_coltroller()),
      rep_{},
      file_number_(_file_number) {
  assert(_value.valid());
  if (_copy) {
    controller_->assign(this, &rep_, _value);
  }
}

inline LazySlice::LazySlice(std::string* _buffer) noexcept
    : slice_(*_buffer),
      controller_(LazySliceController::buffer_controller()),
      rep_{reinterpret_cast<uint64_t>(_buffer)},
      file_number_(uint64_t(-1)) {
  assert(_buffer != nullptr);
}

inline LazySlice::LazySlice(const Slice& _value,
                            Cleanable::CleanupFunction _func,
                            void* _arg1, void* _arg2,
                            uint64_t _file_number) noexcept
    : slice_(_value),
      controller_(LazySliceController::cleanable_controller()),
      rep_{reinterpret_cast<uint64_t>(_func),
           reinterpret_cast<uint64_t>(_arg1),
           reinterpret_cast<uint64_t>(_arg2)},
      file_number_(_file_number) {
  assert(_value.valid());
  assert(_func != nullptr);
}

inline LazySlice::LazySlice(const Slice& _value, Cleanable* _cleanable,
                            uint64_t _file_number) noexcept
    : slice_(_value),
      controller_(LazySliceController::cleanable_controller()),
      rep_{},
      file_number_(_file_number) {
  assert(_value.valid());
  assert(_cleanable != nullptr);
  ::new(&rep_) Cleanable(std::move(*_cleanable));
}

inline LazySlice::LazySlice(const LazySliceController* _controller,
                            const LazySliceRep& _rep,
                            uint64_t _file_number) noexcept
    : slice_(Slice::Invalid()),
      controller_(_controller),
      rep_(_rep),
      file_number_(_file_number) {
  assert(_controller != nullptr);
}

inline void LazySlice::clear() {
  controller_->assign(this, &rep_, Slice());
  file_number_ = uint64_t(-1);
}

inline void LazySlice::reset(const Slice& _value, bool _copy,
                             uint64_t _file_number) {
  assert(_value.valid());
  if (_copy) {
    controller_->assign(this, &rep_, _value);
  } else {
    controller_->assign(this, &rep_, Slice());
    assign_slice(_value);
  }
  file_number_ = _file_number;
}

inline void LazySlice::reset(LazySlice&& _slice, uint64_t _file_number) {
  if (this != &_slice) {
    destroy();
    slice_ = _slice.slice_;
    controller_ = _slice.controller_;
    rep_ = _slice.rep_;
    if (controller_ == LazySliceController::default_coltroller()) {
      fix_default_coltroller(_slice);
    }
    _slice.slice_ = Slice::Invalid();
    _slice.controller_ = nullptr;
  }
  file_number_ = _file_number;
}

inline void LazySlice::reset(const Slice& _value,
                             Cleanable::CleanupFunction _func, void* _arg1,
                             void* _arg2, uint64_t _file_number) {
  assert(_value.valid());
  destroy();
  controller_ = LazySliceController::cleanable_controller();
  slice_ = _value;
  rep_ = {reinterpret_cast<uint64_t>(_func),
          reinterpret_cast<uint64_t>(_arg1),
          reinterpret_cast<uint64_t>(_arg2)};
  file_number_ = _file_number;
}

inline void LazySlice::reset(const Slice& _value, Cleanable* _cleanable,
                             uint64_t _file_number) {
  assert(_value.valid());
  destroy();
  controller_ = LazySliceController::cleanable_controller();
  slice_ = _value;
  new(&rep_) Cleanable(std::move(*_cleanable));
  file_number_ = _file_number;
}

inline void LazySlice::reset(const LazySliceController* _controller,
                             const LazySliceRep& _rep,
                             uint64_t _file_number) {
  assert(_controller != nullptr);
  destroy();
  slice_ = Slice::Invalid();
  controller_ = _controller;
  rep_ = _rep;
  file_number_ = _file_number;
}

inline Status LazySlice::save_to_buffer(std::string* buffer) const {
  assert(controller_ != nullptr);
  if (!slice_.valid()) {
    auto s = inplace_decode();
    if (!s.ok()) {
      return s;
    }
  }
  if (controller_ != LazySliceController::buffer_controller() ||
      reinterpret_cast<std::string*>(rep_.data[0]) != buffer ||
      slice_.data() != buffer->data()) {
    buffer->assign(slice_.data(), slice_.size());
  } else if (slice_.size() < buffer->size()) {
    buffer->resize(slice_.size());
  } else {
    assert(slice_.size() == buffer->size());
  }
  return Status::OK();
}

inline void LazySlice::pin_resource() {
  assert(controller_ != nullptr);
  return controller_->pin_resource(this, &rep_);
}

inline Status LazySlice::decode_destructive(LazySlice& _target) {
  assert(controller_ != nullptr);
  return controller_->decode_destructive(this, &rep_, &_target);
}

inline Status LazySlice::inplace_decode() const {
  assert(controller_ != nullptr);
  if (slice_.valid()) {
    return Status::OK();
  }
  auto self = const_cast<LazySlice*>(this);
  return controller_->inplace_decode(self, &self->rep_);
}

// make a slice reference
extern LazySlice LazySliceReference(const LazySlice& slice);

// make a slice reference, drop the last "fixed_len" bytes from the slice.
extern LazySlice LazySliceRemoveSuffix(const LazySlice* slice,
                                       size_t fixed_len);

}  // namespace rocksdb
