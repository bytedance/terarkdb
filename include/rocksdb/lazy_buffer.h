// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.


#pragma once

#include <assert.h>
#include <string>
#include <utility>
#include "rocksdb/cleanable.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace rocksdb {

class LazyBuffer;
class LazyBufferEditor;

struct LazyBufferRep {
  uint64_t data[4];
};

struct LazyBufferCustomizeBuffer {
  void* handle;
  void* (*uninitialized_resize)(void* handle, size_t size);
};

class LazyBufferController {
 public:
  virtual ~LazyBufferController() = default;

  // Release resource
  virtual void destroy(LazyBuffer* buffer) const = 0;

  // Resize buffer
  virtual void uninitialized_resize(LazyBuffer* _buffer, size_t size) const;

  // Save slice into buffer
  virtual void assign_slice(LazyBuffer* buffer, const Slice& slice) const;

  // Save error into buffer
  virtual void assign_error(LazyBuffer* buffer, Status&& status) const;

  // Pin the buffer, turn the controller into editable
  virtual void pin_buffer(LazyBuffer* buffer) const;

  // Fetch buffer and dump to target, the buffer may be destroyed
  virtual Status dump_buffer(LazyBuffer* buffer, LazyBuffer* target) const;

  // Fetch buffer
  virtual Status fetch_buffer(LazyBuffer* buffer) const = 0;

  // Use LazyBufferRep as local storage
  // data -> 32 bytes
  static const LazyBufferController* light_controller();

  // Use LazyBufferRep as buffer
  // data[0]     -> handle
  // data[1]     -> uninitialized_resize call
  // data[2 - 3] -> Status
  static const LazyBufferController* buffer_controller();

  // Use LazyBufferRep as string holder
  // data[0] -> string ptr
  // data[1] -> is owner
  // data[2 - 3] -> Status
  static const LazyBufferController* string_controller();

  // Use LazyBufferRep as LazuBuffer reference
  // data[0] -> ptr to LazyBuffer
  static const LazyBufferController* reference_controller();

  // Use LazyBufferRep as Cleanable
  // rep -> Cleanable
  static const LazyBufferController* cleanable_controller();

  // Set buffer->slice_ = slice
  static void set_slice(LazyBuffer* buffer, const Slice& slice);

  // Get &buffer->rep_
  static LazyBufferRep* get_rep(LazyBuffer* buffer);
};

class LazyBuffer {
  friend LazyBufferController;
 protected:
  union {
    struct {
      char* data_;
      size_t size_;
    };
    Slice slice_ = Slice();
  };
  const LazyBufferController* controller_;
  LazyBufferRep rep_;
  uint64_t file_number_;

  // Call LazyBufferController::destroy if controller_ not nullptr
  void destroy();

  // Call LazyBufferController::assign_error if _status not ok
  void assign_error(Status&& _status);

  // Fix light_controller local storage
  void fix_light_controller(const LazyBuffer& other);

public:

  // Empty buffer
  LazyBuffer() noexcept;

  // Init a buffer & uninitialized resize.
  explicit LazyBuffer(size_t _size) noexcept;

  // Move constructor
  LazyBuffer(LazyBuffer&& _buffer) noexcept;

  // Non copyable
  LazyBuffer(const LazyBuffer& _buffer) = delete;

  // Init with slice from copying or referring
  explicit LazyBuffer(const Slice& _slice, bool _copy = false,
                      uint64_t _file_number = uint64_t(-1));

  // Init with Status
  explicit LazyBuffer(Status&& _status) : LazyBuffer() {
    assign_error(std::move(_status));
  }

  // Init a buffer with customize buffer
  LazyBuffer(LazyBufferCustomizeBuffer _buffer) noexcept;

  // Init a buffer with outer string, DO NOT take life cycle of string
  explicit LazyBuffer(std::string* _string) noexcept;

  // Init from cleanup function for slice
  LazyBuffer(const Slice& _slice, Cleanable&& _cleanable,
             uint64_t _file_number = uint64_t(-1)) noexcept;

  // Init from customize controller
  LazyBuffer(const LazyBufferController* _controller,
             const LazyBufferRep& _rep, const Slice& _slice = Slice::Invalid(),
             uint64_t _file_number = uint64_t(-1)) noexcept;

  ~LazyBuffer() {
    destroy();
  }

  // Move assign
  LazyBuffer& operator = (LazyBuffer&& _buffer) noexcept {
    reset(std::move(_buffer));
    return *this;
  }

  // Non copyable
  LazyBuffer& operator = (const LazyBuffer& _buffer) = delete;

  // Get inner slice
  // REQUIRES: valid()
  const Slice& get_slice() const { assert(valid()); return slice_; }

  // Return a pointer to the beginning of the referenced data
  // REQUIRES: valid()
  const char* data() const { assert(valid()); return data_; }

  // Return the length (in bytes) of the referenced data
  // REQUIRES: valid()
  size_t size() const { assert(valid()); return size_; }

  // Return true iff the length of the referenced data is zero
  // REQUIRES: valid()
  bool empty() const { assert(valid()); return slice_.empty(); }

  // Return true if Slice valid
  bool valid() const { return slice_.valid(); }

  // Return the ith byte in the referenced data.
  // REQUIRES: n < size()
  // REQUIRES: valid()
  char operator[](size_t n) const { assert(valid()); return slice_[n]; }

  // Return a string that contains the copy of the referenced data.
  // when hex is true, returns a string of twice the length hex encoded (0-9A-F)
  // REQUIRES: valid()
  std::string ToString(bool hex = false) const {
    assert(valid());
    return slice_.ToString(hex);
  }

#ifdef __cpp_lib_string_view
  // Return a string_view that references the same data as this slice.
  // REQUIRES: valid()
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
  // REQUIRES: valid()
  bool DecodeHex(std::string* result) const {
    assert(valid());
    return slice_.DecodeHex(result);
  }

  // Return true iff "x" is a prefix of "*this"
  // REQUIRES: valid()
  bool starts_with(const Slice& x) const {
    assert(valid());
    return slice_.starts_with(x);
  }

  // Return true iff "x" is a prefix of "*this"
  // REQUIRES: valid()
  bool ends_with(const Slice& x) const {
    assert(valid());
    return slice_.ends_with(x);
  }

  // Change this slice to refer to an empty array
  void clear();

  // Move assign other buffer
  void reset(LazyBuffer&& _buffer);

  // Reset buffer from copying or referring
  void reset(const Slice& _slice, bool _copy = false,
             uint64_t _file_number = uint64_t(-1));

  // Reset with Status
  void reset(Status&& _status) { assign_error(std::move(_status)); }

  // Reset with customize buffer
  void reset(LazyBufferCustomizeBuffer _buffer);

  // Reset outer string, DO NOT take life cycle of string
  void reset(std::string* _string);

  // Reset cleanup function for slice
  void reset(const Slice& _slice, Cleanable&& _cleanable,
             uint64_t _file_number = uint64_t(-1));

  // Reset to customize controller
  void reset(const LazyBufferController* _controller, const LazyBufferRep& _rep,
             const Slice& _slice = Slice::Invalid(),
             uint64_t _file_number = uint64_t(-1));

  // Fetch source and copy it
  void assign(const LazyBuffer& _source);

  // Trans to editable buffer and get editor
  LazyBufferEditor* get_editor();

  // Trans buffer to string for modification
  std::string* trans_to_string();

  // Return the certain file number of SST, -1 for unknown
  uint64_t file_number() const { return file_number_; }

  // Pin this buffer, turn the controller into editable
  void pin();

  // Dump buffer to customize buffer
  Status dump(LazyBufferCustomizeBuffer _buffer) &&;

  // Dump buffer to string
  Status dump(std::string* _string) &&;

  // Dump buffer to other buffer
  Status dump(LazyBuffer& _buffer) &&;

  // Fetch buffer
  Status fetch() const;

  // For test
  const LazyBufferController* TEST_controller() const { return controller_; }

  // For test
  const LazyBufferRep* TEST_rep() const { return &rep_; }
};

class LazyBufferEditor : private LazyBuffer {
 public:
  char* data() const { return data_; }
  using LazyBuffer::size;
  using LazyBuffer::fetch;

  // If returns false, some error happens, call fetch to get details
  // grow bytes set 0
  bool resize(size_t _size);

  // If returns false, some error happens, call fetch to get details
  // grow byte uninitialized
  bool uninitialized_resize(size_t _size);
};

inline void LazyBufferController::set_slice(LazyBuffer* buffer,
                                            const Slice& slice) {
  buffer->slice_ = slice;
}

inline LazyBufferRep* LazyBufferController::get_rep(LazyBuffer* buffer) {
  return &buffer->rep_;
}

inline LazyBuffer::LazyBuffer() noexcept
    : controller_(LazyBufferController::light_controller()),
      rep_{},
      file_number_(uint64_t(-1)) {}

inline LazyBuffer::LazyBuffer(LazyBuffer&& _buffer) noexcept
    : slice_(_buffer.slice_),
      controller_(_buffer.controller_),
      rep_(_buffer.rep_),
      file_number_(_buffer.file_number_) {
  if (controller_ == LazyBufferController::light_controller() &&
      _buffer.size_ <= sizeof(LazyBufferRep)) {
    fix_light_controller(_buffer);
  }
  _buffer.slice_ = Slice::Invalid();
  _buffer.controller_ = nullptr;
}

inline LazyBuffer::LazyBuffer(const Slice& _slice, bool _copy,
                              uint64_t _file_number)
    : slice_(_slice),
      controller_(LazyBufferController::light_controller()),
      rep_{},
      file_number_(_file_number) {
  assert(_slice.valid());
  if (_copy) {
    controller_->assign_slice(this, _slice);
  }
}

inline LazyBuffer::LazyBuffer(const Slice& _slice, Cleanable&& _cleanable,
                              uint64_t _file_number) noexcept
    : slice_(_slice),
      controller_(LazyBufferController::cleanable_controller()),
      rep_{},
      file_number_(_file_number) {
  assert(_slice.valid());
  static_assert(sizeof _cleanable == sizeof rep_, "");
  static_assert(alignof(Cleanable) == alignof(LazyBufferRep), "");
  ::new(&rep_) Cleanable(std::move(_cleanable));
}

inline LazyBuffer::LazyBuffer(const LazyBufferController* _controller,
                              const LazyBufferRep& _rep, const Slice& _slice,
                              uint64_t _file_number) noexcept
    : slice_(_slice),
      controller_(_controller),
      rep_(_rep),
      file_number_(_file_number) {
  assert(_controller != nullptr);
}

inline void LazyBuffer::destroy() {
  if (controller_ != nullptr) {
    controller_->destroy(this);
  }
}

inline void LazyBuffer::assign_error(Status&& _status) {
  if (_status.ok()) {
    controller_->assign_slice(this, Slice());
  } else {
    controller_->assign_error(this, std::move(_status));
    assert(!slice_.valid());
  }
  file_number_ = uint64_t(-1);
}

inline void LazyBuffer::clear() {
  controller_->assign_slice(this, Slice());
  assert(size_ == 0);
  file_number_ = uint64_t(-1);
}

inline void LazyBuffer::reset(LazyBuffer&& _buffer) {
  if (this != &_buffer) {
    destroy();
    slice_ = _buffer.slice_;
    controller_ = _buffer.controller_;
    rep_ = _buffer.rep_;
    file_number_ = _buffer.file_number_;
    if (controller_ == LazyBufferController::light_controller() &&
        _buffer.size_ <= sizeof(LazyBufferRep)) {
      fix_light_controller(_buffer);
    }
    _buffer.slice_ = Slice::Invalid();
    _buffer.controller_ = nullptr;
  }
}

inline void LazyBuffer::reset(const Slice& _slice, bool _copy,
                              uint64_t _file_number) {
  assert(_slice.valid());
  if (_copy) {
    controller_->assign_slice(this, _slice);
    assert(slice_ == _slice);
  } else {
    destroy();
    slice_ = _slice;
    controller_ = LazyBufferController::light_controller();
    rep_ = {};
  }
  file_number_ = _file_number;
}

inline void LazyBuffer::reset(const Slice& _slice, Cleanable&& _cleanable,
                              uint64_t _file_number) {
  assert(_slice.valid());
  destroy();
  controller_ = LazyBufferController::cleanable_controller();
  slice_ = _slice;
  new(&rep_) Cleanable(std::move(_cleanable));
  file_number_ = _file_number;
}

inline void LazyBuffer::reset(const LazyBufferController* _controller,
                              const LazyBufferRep& _rep, const Slice& _slice,
                              uint64_t _file_number) {
  assert(_controller != nullptr);
  destroy();
  slice_ = _slice;
  controller_ = _controller;
  rep_ = _rep;
  file_number_ = _file_number;
}

inline void LazyBuffer::pin() {
  assert(controller_ != nullptr);
  return controller_->pin_buffer(this);
}

inline Status LazyBuffer::dump(LazyBuffer& _target) && {
  assert(controller_ != nullptr);
  assert(this != &_target);
  return controller_->dump_buffer(this, &_target);
}

inline Status LazyBuffer::fetch() const {
  assert(controller_ != nullptr);
  if (slice_.valid()) {
    return Status::OK();
  }
  return controller_->fetch_buffer(const_cast<LazyBuffer*>(this));
}

// make a slice reference
extern LazyBuffer LazyBufferReference(const LazyBuffer& buffer);

// make a slice reference, drop the last "fixed_len" bytes from the slice.
extern LazyBuffer LazyBufferRemoveSuffix(const LazyBuffer* buffer,
                                         size_t fixed_len);

}  // namespace rocksdb
