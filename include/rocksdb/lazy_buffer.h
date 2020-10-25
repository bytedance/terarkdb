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

// State pattern

class LazyBuffer;
class LazyBufferBuilder;

enum class LazyBufferPinLevel {
  Internal,  // detach life cycle from iterator or temporary object, still
             // depend SuperVersion
  DB,        // detach life cycle from SuperVersion, still depend DB
};

struct LazyBufferContext {
  uint64_t data[4];
};

struct LazyBufferCustomizeBuffer {
  void* handle;
  void* (*uninitialized_resize)(void* handle, size_t size);
};

class LazyBufferState {
 public:
  virtual ~LazyBufferState() = default;

  // Release resource
  virtual void destroy(LazyBuffer* buffer) const = 0;

  // Resize buffer
  virtual void uninitialized_resize(LazyBuffer* _buffer, size_t size) const;

  // Save slice into buffer
  virtual void assign_slice(LazyBuffer* buffer, const Slice& slice) const;

  // Save error into buffer
  virtual void assign_error(LazyBuffer* buffer, Status&& status) const;

  // Pin the buffer, turn the state into editable
  virtual Status pin_buffer(LazyBuffer* buffer) const;

  // Fetch buffer and dump to target, the buffer may be destroyed
  virtual Status dump_buffer(LazyBuffer* buffer, LazyBuffer* target) const;

  // Fetch buffer
  virtual Status fetch_buffer(LazyBuffer* buffer) const = 0;

  // Use LazyBufferContext as local storage
  // data -> 32 bytes
  static const LazyBufferState* light_state();

  // Use LazyBufferContext as buffer
  // data[0]     -> handle
  // data[1]     -> uninitialized_resize call
  // data[2 - 3] -> Status
  static const LazyBufferState* buffer_state();

  // Use LazyBufferContext as string holder
  // data[0] -> string ptr
  // data[1] -> is owner
  // data[2 - 3] -> Status
  static const LazyBufferState* string_state();

  // Use LazyBufferContext as LazuBuffer reference
  // data[0] -> ptr to LazyBuffer
  static const LazyBufferState* reference_state();

  // Use LazyBufferContext as Cleanable
  // context -> Cleanable
  static const LazyBufferState* cleanable_state();

  // Reserve buffer capacity
  static bool reserve_buffer(LazyBuffer* buffer, size_t size);

  // Set buffer->slice_ = slice
  static void set_slice(LazyBuffer* buffer, const Slice& slice);

  // Get &buffer->context_
  static LazyBufferContext* get_context(LazyBuffer* buffer);
};

class LazyBufferStateWrapper : public LazyBufferState {
  // if LazyBuffer use StateWrapper as its state, buffer can replace it state
  // with another, see CompactionSeparateHelper which inherit this class , store
  // its origin value and replace original lazybuffer's state
 public:
  void set_state(const LazyBufferState* state) { state_ = state; }
  const LazyBufferState* get_state() const { return state_; }

 protected:
  const LazyBufferState* state_ = nullptr;

  void destroy(LazyBuffer* buffer) const override {
    if (buffer != nullptr) {
      state_->destroy(buffer);
    }
  }

  void uninitialized_resize(LazyBuffer* buffer, size_t size) const override;

  void assign_slice(LazyBuffer* buffer, const Slice& slice) const override;

  void assign_error(LazyBuffer* buffer, Status&& status) const override;

  Status pin_buffer(LazyBuffer* buffer) const override;

  Status dump_buffer(LazyBuffer* buffer, LazyBuffer* target) const override;

  Status fetch_buffer(LazyBuffer* buffer) const override;
};

class LazyBuffer {
  friend LazyBufferState;

 protected:
  union {
    struct {
      char* data_;
      size_t size_;
    };
    Slice slice_ = Slice();
  };
  const LazyBufferState* state_;
  LazyBufferContext context_;
  uint64_t file_number_;

  // Call LazyBufferState::destroy if state_ not nullptr
  void destroy();

  // Call LazyBufferState::assign_error if _status not ok
  void assign_error(Status&& _status);

  // Fix light_state local storage
  void fix_light_state(const LazyBuffer& other);

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

  // Init with slice parts from copying
  explicit LazyBuffer(const SliceParts& _slice_parts,
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

  // Init from customize state
  LazyBuffer(const LazyBufferState* _state, const LazyBufferContext& _context,
             const Slice& _slice = Slice::Invalid(),
             uint64_t _file_number = uint64_t(-1)) noexcept;

  ~LazyBuffer() { destroy(); }

  // Move assign
  LazyBuffer& operator=(LazyBuffer&& _buffer) noexcept {
    reset(std::move(_buffer));
    return *this;
  }

  // Non copyable
  LazyBuffer& operator=(const LazyBuffer& _buffer) = delete;

  // Get inner slice
  // REQUIRES: valid()
  const Slice& slice() const {
    assert(valid());
    return slice_;
  }

  // Return a pointer to the beginning of the referenced data
  // REQUIRES: valid()
  const char* data() const {
    assert(valid());
    return data_;
  }

  // Return the length (in bytes) of the referenced data
  // REQUIRES: valid()
  size_t size() const {
    assert(valid());
    return size_;
  }

  // Return true iff the length of the referenced data is zero
  // REQUIRES: valid()
  bool empty() const {
    assert(valid());
    return slice_.empty();
  }

  // Return true if Slice valid
  bool valid() const { return slice_.valid(); }

  // Return the ith byte in the referenced data.
  // REQUIRES: n < size()
  // REQUIRES: valid()
  char operator[](size_t n) const {
    assert(valid());
    return slice_[n];
  }

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

  // Change this to invalid
  void reset();

  // Move assign other buffer
  void reset(LazyBuffer&& _buffer);

  // Reset buffer from copying or referring
  void reset(const Slice& _slice, bool _copy = false,
             uint64_t _file_number = uint64_t(-1));

  // Reset buffer from copying
  void reset(const SliceParts& _slice_parts,
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

  // Reset to customize state
  void reset(const LazyBufferState* _state, const LazyBufferContext& _context,
             const Slice& _slice = Slice::Invalid(),
             uint64_t _file_number = uint64_t(-1));

  // Fetch source and copy it
  void assign(const LazyBuffer& _source);

  // Trans to editable buffer and get builder
  LazyBufferBuilder* get_builder();

  // Trans buffer to string for modification
  std::string* trans_to_string();

  // Return the certain file number of SST, -1 for unknown
  uint64_t file_number() const { return file_number_; }

  // Pin this buffer, detach life cycle from some object
  void pin(LazyBufferPinLevel level = LazyBufferPinLevel::DB);

  // Dump buffer to customize buffer
  Status dump(LazyBufferCustomizeBuffer _buffer) &&;

  // Dump buffer to string
  Status dump(std::string* _string) &&;

  // Dump buffer to other buffer
  Status dump(LazyBuffer& _buffer) &&;

  // Fetch buffer
  Status fetch() const;

  // Wrap current state
  const LazyBufferState* wrap_state(LazyBufferStateWrapper* wrapper);

  // Unwrap current state
  void unwrap_state(LazyBufferStateWrapper* wrapper) {
    assert(state_ == wrapper);
    state_ = wrapper->get_state();
  }

  // For test
  const LazyBufferState* state() const { return state_; }

  // For test
  const LazyBufferContext* TEST_context() const { return &context_; }
};

class LazyBufferBuilder : private LazyBuffer {
 public:
  char* data() const { return data_; }
  using LazyBuffer::size;

  using LazyBuffer::fetch;
  using LazyBuffer::valid;

  // return valid()
  // If returns false, some error happens, call fetch to get details
  // grow bytes set 0
  bool resize(size_t _size);

  // return valid()
  // If returns false, some error happens, call fetch to get details
  // grow byte uninitialized
  bool uninitialized_resize(size_t _size);
};

inline void LazyBufferState::set_slice(LazyBuffer* buffer, const Slice& slice) {
  buffer->slice_ = slice;
}

inline LazyBufferContext* LazyBufferState::get_context(LazyBuffer* buffer) {
  return &buffer->context_;
}

inline void LazyBufferStateWrapper::uninitialized_resize(LazyBuffer* buffer,
                                                         size_t size) const {
  buffer->unwrap_state(const_cast<LazyBufferStateWrapper*>(this));
  state_->uninitialized_resize(buffer, size);
  destroy(nullptr);
}

inline void LazyBufferStateWrapper::assign_slice(LazyBuffer* buffer,
                                                 const Slice& slice) const {
  buffer->unwrap_state(const_cast<LazyBufferStateWrapper*>(this));
  state_->assign_slice(buffer, slice);
  destroy(nullptr);
}

inline void LazyBufferStateWrapper::assign_error(LazyBuffer* buffer,
                                                 Status&& status) const {
  buffer->unwrap_state(const_cast<LazyBufferStateWrapper*>(this));
  state_->assign_error(buffer, std::move(status));
  assert(buffer->file_number() == uint64_t(-1));
  destroy(nullptr);
}

inline Status LazyBufferStateWrapper::pin_buffer(LazyBuffer* buffer) const {
  auto self = const_cast<LazyBufferStateWrapper*>(this);
  buffer->unwrap_state(self);
  auto s = state_->pin_buffer(buffer);
  if (s.IsNotSupported()) {
    LazyBuffer tmp;
    s = state_->dump_buffer(buffer, &tmp);
    if (s.ok()) {
      *buffer = std::move(tmp);
    }
  }
  if (!s.ok() || buffer->wrap_state(self) == nullptr) {
    destroy(nullptr);
  }
  return s;
}

inline Status LazyBufferStateWrapper::dump_buffer(LazyBuffer* buffer,
                                                  LazyBuffer* target) const {
  buffer->unwrap_state(const_cast<LazyBufferStateWrapper*>(this));
  auto s = state_->dump_buffer(buffer, target);
  destroy(nullptr);
  buffer->reset();
  return s;
}

inline Status LazyBufferStateWrapper::fetch_buffer(LazyBuffer* buffer) const {
  auto self = const_cast<LazyBufferStateWrapper*>(this);
  buffer->unwrap_state(self);
  auto s = state_->fetch_buffer(buffer);
  if (!s.ok() || buffer->wrap_state(self) == nullptr) {
    destroy(nullptr);
  }
  return s;
}

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#endif

inline LazyBuffer::LazyBuffer() noexcept
    : state_(LazyBufferState::light_state()),
      context_{},
      file_number_(uint64_t(-1)) {}

inline LazyBuffer::LazyBuffer(LazyBuffer&& _buffer) noexcept
    : slice_(_buffer.slice_),
      state_(_buffer.state_),
      context_(_buffer.context_),
      file_number_(_buffer.file_number_) {
  if (state_ == LazyBufferState::light_state() &&
      _buffer.size_ <= sizeof(LazyBufferContext)) {
    fix_light_state(_buffer);
  }
  _buffer.slice_ = Slice::Invalid();
  _buffer.state_ = nullptr;
}

inline LazyBuffer::LazyBuffer(const Slice& _slice, bool _copy,
                              uint64_t _file_number)
    : slice_(_slice),
      state_(LazyBufferState::light_state()),
      context_{},
      file_number_(_file_number) {
  assert(_slice.valid());
  if (_copy) {
    state_->assign_slice(this, _slice);
  }
}

inline LazyBuffer::LazyBuffer(const Slice& _slice, Cleanable&& _cleanable,
                              uint64_t _file_number) noexcept
    : slice_(_slice),
      state_(LazyBufferState::cleanable_state()),
      context_{},
      file_number_(_file_number) {
  assert(_slice.valid());
  static_assert(sizeof _cleanable == sizeof context_, "");
  static_assert(alignof(Cleanable) == alignof(LazyBufferContext), "");
  ::new (&context_) Cleanable(std::move(_cleanable));
}

inline LazyBuffer::LazyBuffer(const LazyBufferState* _state,
                              const LazyBufferContext& _context,
                              const Slice& _slice,
                              uint64_t _file_number) noexcept
    : slice_(_slice),
      state_(_state),
      context_(_context),
      file_number_(_file_number) {
  assert(_state != nullptr);
}

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

inline void LazyBuffer::destroy() {
  if (state_ != nullptr) {
    state_->destroy(this);
    state_ = nullptr;
  }
}

inline void LazyBuffer::assign_error(Status&& _status) {
  if (_status.ok()) {
    state_->assign_slice(this, Slice());
  } else {
    state_->assign_error(this, std::move(_status));
    assert(!slice_.valid());
  }
  file_number_ = uint64_t(-1);
}

inline void LazyBuffer::clear() {
  if (state_ == nullptr) {
    state_ = LazyBufferState::light_state();
  }
  state_->assign_slice(this, Slice());
  assert(size_ == 0);
  file_number_ = uint64_t(-1);
}

inline void LazyBuffer::reset() {
  destroy();
  slice_ = Slice::Invalid();
  file_number_ = uint64_t(-1);
}

inline void LazyBuffer::reset(LazyBuffer&& _buffer) {
  if (this != &_buffer) {
    destroy();
    slice_ = _buffer.slice_;
    state_ = _buffer.state_;
    context_ = _buffer.context_;
    file_number_ = _buffer.file_number_;
    if (state_ == LazyBufferState::light_state() &&
        _buffer.size_ <= sizeof(LazyBufferContext)) {
      fix_light_state(_buffer);
    }
    _buffer.slice_ = Slice::Invalid();
    _buffer.state_ = nullptr;
  }
}

inline void LazyBuffer::reset(const Slice& _slice, bool _copy,
                              uint64_t _file_number) {
  assert(_slice.valid());
  if (_copy) {
    state_->assign_slice(this, _slice);
    assert(slice_ == _slice);
  } else {
    destroy();
    slice_ = _slice;
    state_ = LazyBufferState::light_state();
  }
  file_number_ = _file_number;
}

inline void LazyBuffer::reset(const Slice& _slice, Cleanable&& _cleanable,
                              uint64_t _file_number) {
  assert(_slice.valid());
  destroy();
  state_ = LazyBufferState::cleanable_state();
  slice_ = _slice;
  new (&context_) Cleanable(std::move(_cleanable));
  file_number_ = _file_number;
}

inline void LazyBuffer::reset(const LazyBufferState* _state,
                              const LazyBufferContext& _context,
                              const Slice& _slice, uint64_t _file_number) {
  assert(_state != nullptr);
  destroy();
  slice_ = _slice;
  state_ = _state;
  context_ = _context;
  file_number_ = _file_number;
}

inline Status LazyBuffer::fetch() const {
  assert(state_ != nullptr);
  if (slice_.valid()) {
    return Status::OK();
  }
  return state_->fetch_buffer(const_cast<LazyBuffer*>(this));
}

inline const LazyBufferState* LazyBuffer::wrap_state(
    LazyBufferStateWrapper* wrapper) {
  if (state_ == nullptr) {
    return nullptr;
  }
  auto state_backup = state_;
  wrapper->set_state(state_);
  state_ = wrapper;
  return state_backup;
}

// make a slice reference
extern LazyBuffer LazyBufferReference(const LazyBuffer& buffer);

// make a slice reference, drop the last "fixed_len" bytes from the slice.
extern LazyBuffer LazyBufferRemoveSuffix(const LazyBuffer* buffer,
                                         size_t fixed_len);

}  // namespace rocksdb
