//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/lazy_buffer.h"

#include <stdlib.h>
#include <string.h>

#include <algorithm>

namespace rocksdb {

namespace {
template <class T, class S>
T* union_cast(S* src) {
  static_assert(sizeof(T) == sizeof(S), "");
  static_assert(alignof(T) == alignof(S), "");

  union {
    S* s;
    T* t;
  } u;
  u.s = src;
  return u.t;
}
}  // namespace

class LightLazyBufferState : public LazyBufferState {
 public:
  struct alignas(LazyBufferContext) Context {
    char data[sizeof(LazyBufferContext)];
  };

  void destroy(LazyBuffer* /*buffer*/) const override {}

  void uninitialized_resize(LazyBuffer* buffer, size_t size) const override {
    auto context = union_cast<Context>(get_context(buffer));
    if (size <= sizeof(Context)) {
      size_t copy_size = std::min(buffer->size(), size);
      if (buffer->data() != context->data && copy_size > 0) {
        ::memmove(context->data, buffer->data(), copy_size);
      }
      set_slice(buffer, Slice(context->data, size));
    } else {
      LazyBufferState::uninitialized_resize(buffer, size);
    }
  }

  void assign_slice(LazyBuffer* buffer, const Slice& slice) const override {
    auto context = union_cast<Context>(get_context(buffer));
    if (slice.size() <= sizeof(Context)) {
      if (!slice.empty()) {
        ::memmove(context->data, slice.data(), slice.size());
      }
      set_slice(buffer, Slice(context->data, slice.size()));
    } else {
      LazyBufferState::assign_slice(buffer, slice);
    }
  }

  Status pin_buffer(LazyBuffer* buffer) const override {
    auto context = union_cast<Context>(get_context(buffer));
    if (buffer->size() <= sizeof(Context)) {
      if (buffer->data() != context->data && buffer->size() > 0) {
        ::memmove(context->data, buffer->data(), buffer->size());
      }
      set_slice(buffer, Slice(context->data, buffer->size()));
      return Status::OK();
    }
    return Status::NotSupported();
  }

  Status dump_buffer(LazyBuffer* buffer, LazyBuffer* target) const override {
    target->reset(buffer->slice(), true, buffer->file_number());
    return Status::OK();
  }

  Status fetch_buffer(LazyBuffer* /*slice*/) const override {
    return Status::OK();
  }
};

class BufferLazyBufferState : public LazyBufferState {
 public:
  struct alignas(LazyBufferContext) Context {
    LazyBufferCustomizeBuffer buffer;
    Status status;
  };

  void destroy(LazyBuffer* buffer) const override {
    auto context = union_cast<Context>(get_context(buffer));
    if (context->buffer.uninitialized_resize == nullptr) {
      ::free(context->buffer.handle);
    }
    context->status.~Status();
  }

  void uninitialized_resize(LazyBuffer* buffer, size_t size) const override {
    auto context = union_cast<Context>(get_context(buffer));
    if (size == 0) {
      if (context->buffer.uninitialized_resize != nullptr) {
        context->buffer.uninitialized_resize(context->buffer.handle, 0);
      }
      context->status = Status::OK();
      set_slice(buffer, Slice());
      assert(buffer->data() != nullptr);
    } else if (context->status.ok()) {
      void* ptr;
      if (context->buffer.uninitialized_resize == nullptr) {
        ptr = ::realloc(context->buffer.handle, size);
        if (ptr != nullptr) {
          context->buffer.handle = ptr;
        }
      } else {
        ptr =
            context->buffer.uninitialized_resize(context->buffer.handle, size);
      }
      if (ptr != nullptr) {
        set_slice(buffer, Slice(reinterpret_cast<char*>(ptr), size));
      } else {
        context->status = Status::BadAlloc();
        set_slice(buffer, Slice::Invalid());
      }
    } else {
      assert(!buffer->valid());
    }
  }

  void assign_slice(LazyBuffer* buffer, const Slice& slice) const override {
    if (buffer->valid() && slice.data() >= buffer->data() &&
        slice.data() < buffer->data() + buffer->size()) {
      // Self assign
      assert(slice.data() + slice.size() <= buffer->data() + buffer->size());
      char* ptr = (char*)buffer->data();
      ::memmove(ptr, slice.data(), slice.size());
      set_slice(buffer, Slice(ptr, slice.size()));
    } else {
      auto context = union_cast<Context>(get_context(buffer));
      context->status = Status::OK();
      BufferLazyBufferState::uninitialized_resize(buffer, slice.size());
      if (context->status.ok() && !slice.empty()) {
        assert(buffer->data() != nullptr);
        assert(buffer->size() == slice.size());
        char* ptr = (char*)buffer->data();
        ::memcpy(ptr, slice.data(), slice.size());
      }
    }
  }

  void assign_error(LazyBuffer* buffer, Status&& status) const override {
    auto context = union_cast<Context>(get_context(buffer));
    context->status = std::move(status);
    set_slice(buffer, Slice::Invalid());
  }

  Status pin_buffer(LazyBuffer* /*buffer*/) const override {
    return Status::OK();
  }

  Status dump_buffer(LazyBuffer* buffer, LazyBuffer* target) const override {
    auto context = union_cast<Context>(get_context(buffer));
    if (context->status.ok()) {
      assert(buffer->valid());
      target->reset(buffer->slice(), true, buffer->file_number());
      return Status::OK();
    } else {
      assert(!buffer->valid());
      return std::move(context->status);
    }
  }

  Status fetch_buffer(LazyBuffer* buffer) const override {
    return union_cast<const Context>(get_context(buffer))->status;
  }
};

struct StringLazyBufferState : public LazyBufferState {
 public:
  struct alignas(LazyBufferContext) Context {
    std::string* string;
    uint64_t is_owner;
    Status status;
  };

  void destroy(LazyBuffer* buffer) const override {
    auto context = union_cast<Context>(get_context(buffer));
    if (context->is_owner) {
      delete context->string;
    }
    context->status.~Status();
  }

  void uninitialized_resize(LazyBuffer* buffer, size_t size) const override {
    auto context = union_cast<Context>(get_context(buffer));
    if (context->status.ok() || size == 0) {
      try {
        context->string->resize(size);
        set_slice(buffer, *context->string);
        context->status = Status::OK();
        return;
      } catch (const std::bad_alloc&) {
        context->status = Status::BadAlloc();
      } catch (const std::exception& ex) {
        context->status = Status::Aborted(ex.what());
      }
      set_slice(buffer, Slice::Invalid());
    } else {
      assert(!buffer->valid());
    }
  }

  void assign_slice(LazyBuffer* buffer, const Slice& slice) const override {
    auto context = union_cast<Context>(get_context(buffer));
    try {
      context->string->assign(slice.data(), slice.size());
      set_slice(buffer, *context->string);
      context->status = Status::OK();
      return;
    } catch (const std::bad_alloc&) {
      context->status = Status::BadAlloc();
    } catch (const std::exception& ex) {
      context->status = Status::Aborted(ex.what());
    }
    set_slice(buffer, Slice::Invalid());
  }

  void assign_error(LazyBuffer* buffer, Status&& status) const override {
    auto context = union_cast<Context>(get_context(buffer));
    context->status = std::move(status);
    set_slice(buffer, Slice::Invalid());
  }

  Status pin_buffer(LazyBuffer* /*buffer*/) const override {
    return Status::OK();
  }

  Status dump_buffer(LazyBuffer* buffer, LazyBuffer* target) const override {
    auto context = union_cast<Context>(get_context(buffer));
    if (context->status.ok()) {
      target->reset(*context->string, true, buffer->file_number());
      return Status::OK();
    } else {
      assert(!buffer->valid());
      return std::move(context->status);
    }
  }

  Status fetch_buffer(LazyBuffer* buffer) const override {
    auto context = union_cast<const Context>(get_context(buffer));
    if (context->status.ok()) {
      set_slice(buffer, *context->string);
      return Status::OK();
    } else {
      return context->status;
    }
  }
};

// 0 -> pointer to slice
struct ReferenceLazyBufferState : public LazyBufferState {
 public:
  void destroy(LazyBuffer* /*buffer*/) const override {}

  Status fetch_buffer(LazyBuffer* buffer) const override {
    const LazyBuffer& buffer_ref =
        *reinterpret_cast<const LazyBuffer*>(get_context(buffer)->data[0]);
    auto s = buffer_ref.fetch();
    if (s.ok()) {
      set_slice(buffer, buffer_ref.slice());
    }
    return s;
  }
};

struct CleanableLazyBufferState : public LazyBufferState {
 public:
  void destroy(LazyBuffer* buffer) const override {
    union_cast<Cleanable>(get_context(buffer))->Reset();
  }

  Status pin_buffer(LazyBuffer* /*buffer*/) const override {
    return Status::OK();
  }

  Status dump_buffer(LazyBuffer* buffer, LazyBuffer* target) const override {
    target->reset(buffer->slice(), true, buffer->file_number());
    union_cast<Cleanable>(get_context(buffer))->Reset();
    return Status::OK();
  }

  Status fetch_buffer(LazyBuffer* /*buffer*/) const override {
    assert(false);
    return Status::OK();
  }
};

void LazyBufferState::uninitialized_resize(LazyBuffer* buffer,
                                           size_t size) const {
  assert(buffer->valid());
  size_t copy_size = std::min(buffer->size_, size);
  if (size <= sizeof(LazyBufferContext)) {
    LightLazyBufferState::Context tmp_context{};
    ::memcpy(tmp_context.data, buffer->data_, copy_size);
    buffer->destroy();
    buffer->state_ = light_state();
    auto context = union_cast<LightLazyBufferState::Context>(&buffer->context_);
    *context = tmp_context;
    buffer->data_ = context->data;
    buffer->size_ = size;
  } else {
    LazyBuffer tmp(size);
    auto context = union_cast<BufferLazyBufferState::Context>(&tmp.context_);
    if (context->status.ok() && copy_size > 0) {
      ::memcpy(tmp.data_, buffer->data_, copy_size);
    }
    buffer->destroy();
    buffer->slice_ = tmp.slice_;
    buffer->state_ = tmp.state_;
    tmp.state_ = nullptr;
    buffer->context_ = tmp.context_;
  }
}

void LazyBufferState::assign_slice(LazyBuffer* buffer,
                                   const Slice& slice) const {
  buffer->destroy();
  auto data = slice.data();
  auto size = slice.size();
  if (reserve_buffer(buffer, size)) {
    ::memcpy(buffer->data_, data, size);
  }
}

void LazyBufferState::assign_error(LazyBuffer* buffer, Status&& status) const {
  destroy(buffer);
  buffer->state_ = buffer_state();
  auto context = union_cast<BufferLazyBufferState::Context>(&buffer->context_);
  context->buffer.handle = nullptr;
  context->buffer.uninitialized_resize = nullptr;
  ::new (&context->status) Status(std::move(status));
  buffer->slice_ = Slice::Invalid();
}

Status LazyBufferState::pin_buffer(LazyBuffer* /*buffer*/) const {
  return Status::NotSupported();
}

Status LazyBufferState::dump_buffer(LazyBuffer* buffer,
                                    LazyBuffer* target) const {
  if (!buffer->valid()) {
    auto s = fetch_buffer(buffer);
    if (!s.ok()) {
      return s;
    }
  }
  target->state_->assign_slice(target, buffer->slice_);
  assert(target->slice_ == buffer->slice_);
  target->file_number_ = buffer->file_number_;
  buffer->destroy();
  return Status::OK();
}

const LazyBufferState* LazyBufferState::light_state() {
  static LightLazyBufferState static_state;
  return &static_state;
}

const LazyBufferState* LazyBufferState::buffer_state() {
  static BufferLazyBufferState static_state;
  return &static_state;
}

const LazyBufferState* LazyBufferState::string_state() {
  static StringLazyBufferState static_state;
  return &static_state;
}

const LazyBufferState* LazyBufferState::reference_state() {
  static ReferenceLazyBufferState static_state;
  return &static_state;
}

const LazyBufferState* LazyBufferState::cleanable_state() {
  static CleanableLazyBufferState static_state;
  return &static_state;
}

bool LazyBufferState::reserve_buffer(LazyBuffer* buffer, size_t size) {
  if (size <= sizeof(LazyBufferContext)) {
    buffer->state_ = light_state();
    auto context = union_cast<LightLazyBufferState::Context>(&buffer->context_);
    buffer->data_ = context->data;
    buffer->size_ = size;
    return true;
  } else {
    buffer->state_ = buffer_state();
    auto context =
        union_cast<BufferLazyBufferState::Context>(&buffer->context_);
    context->buffer.handle = ::malloc(size);
    context->buffer.uninitialized_resize = nullptr;
    if (context->buffer.handle == nullptr) {
      ::new (&context->status) Status(Status::BadAlloc());
      buffer->slice_ = Slice::Invalid();
      return false;
    } else {
      ::new (&context->status) Status;
      buffer->data_ = (char*)context->buffer.handle;
      buffer->size_ = size;
      return true;
    }
  }
}

void LazyBuffer::fix_light_state(const LazyBuffer& other) {
  assert(state_ == LazyBufferState::light_state());
  assert(other.size_ <= sizeof(LazyBufferContext));
  data_ = union_cast<LightLazyBufferState::Context>(&context_)->data;
  size_ = other.size_;
  if (!other.empty()) {
    ::memmove(data_, other.data_, size_);
  }
}

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#endif

LazyBuffer::LazyBuffer(size_t _size) noexcept
    : context_{}, file_number_(uint64_t(-1)) {
  LazyBufferState::reserve_buffer(this, _size);
}

LazyBuffer::LazyBuffer(const SliceParts& _slice_parts, uint64_t _file_number)
    : state_(LazyBufferState::light_state()),
      context_{},
      file_number_(_file_number) {
  size_t size = 0;
  for (int i = 0; i < _slice_parts.num_parts; ++i) {
    size += _slice_parts.parts[i].size();
  }
  if (LazyBufferState::reserve_buffer(this, size)) {
    char* dst = data_;
    for (int i = 0; i < _slice_parts.num_parts; ++i) {
      ::memcpy(dst, _slice_parts.parts[i].data(), _slice_parts.parts[i].size());
      dst += _slice_parts.parts[i].size();
    }
  }
}

LazyBuffer::LazyBuffer(LazyBufferCustomizeBuffer _buffer) noexcept
    : state_(LazyBufferState::buffer_state()),
      context_{reinterpret_cast<uint64_t>(_buffer.handle),
               reinterpret_cast<uint64_t>(_buffer.uninitialized_resize)},
      file_number_(uint64_t(-1)) {
  assert(_buffer.handle != nullptr);
  assert(_buffer.uninitialized_resize != nullptr);
  ::new (&union_cast<BufferLazyBufferState::Context>(&context_)->status) Status;
}

LazyBuffer::LazyBuffer(std::string* _string) noexcept
    : slice_(*_string),
      state_(LazyBufferState::string_state()),
      context_{reinterpret_cast<uint64_t>(_string)},
      file_number_(uint64_t(-1)) {
  assert(_string != nullptr);
  ::new (&union_cast<StringLazyBufferState::Context>(&context_)->status) Status;
}

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

void LazyBuffer::reset(const SliceParts& _slice_parts, uint64_t _file_number) {
  destroy();
  size_t size = 0;
  for (int i = 0; i < _slice_parts.num_parts; ++i) {
    size += _slice_parts.parts[i].size();
  }
  if (LazyBufferState::reserve_buffer(this, size)) {
    char* dst = data_;
    for (int i = 0; i < _slice_parts.num_parts; ++i) {
      ::memcpy(dst, _slice_parts.parts[i].data(), _slice_parts.parts[i].size());
      dst += _slice_parts.parts[i].size();
    }
    file_number_ = _file_number;
  } else {
    file_number_ = uint64_t(-1);
  }
}

void LazyBuffer::reset(LazyBufferCustomizeBuffer _buffer) {
  assert(_buffer.handle != nullptr);
  assert(_buffer.uninitialized_resize != nullptr);
  destroy();
  state_ = LazyBufferState::buffer_state();
  auto context = union_cast<BufferLazyBufferState::Context>(&context_);
  context->buffer = _buffer;
  ::new (&context->status) Status;
  slice_ = Slice();
  file_number_ = uint64_t(-1);
}

void LazyBuffer::reset(std::string* _string) {
  assert(_string != nullptr);
  slice_ = *_string;
  auto context = union_cast<StringLazyBufferState::Context>(&context_);
  if (state_ == LazyBufferState::string_state()) {
    if (context->string != _string) {
      if (context->is_owner) {
        delete context->string;
        context->is_owner = 0;
      }
      context->string = _string;
    }
    context->status = Status::OK();
  } else {
    destroy();
    state_ = LazyBufferState::string_state();
    context->string = _string;
    context->is_owner = 0;
    ::new (&context->status) Status;
  }
  slice_ = *context->string;
  file_number_ = uint64_t(-1);
}

void LazyBuffer::assign(const LazyBuffer& source) {
  Status s;
  if (source.valid() || (s = source.fetch()).ok()) {
    reset(source.slice(), true, source.file_number());
  } else {
    state_->assign_error(this, std::move(std::move(s)));
    assert(!slice_.valid());
  }
}

LazyBufferBuilder* LazyBuffer::get_builder() {
  assert(state_ != nullptr);
  file_number_ = uint64_t(-1);
  auto s = state_->fetch_buffer(this);
  if (s.ok()) {
    state_->uninitialized_resize(this, size_);
  } else {
    state_->assign_error(this, std::move(s));
  }
  return reinterpret_cast<LazyBufferBuilder*>(this);
}

std::string* LazyBuffer::trans_to_string() {
  assert(state_ != nullptr);
  file_number_ = uint64_t(-1);
  if (state_ == LazyBufferState::string_state()) {
    auto context = union_cast<StringLazyBufferState::Context>(&context_);
    assert(!slice_.valid() || (data_ == context->string->data() &&
                               size_ == context->string->size()));
    slice_ = Slice::Invalid();
    return context->string;
  } else {
    LazyBuffer tmp(LazyBufferState::string_state(),
                   {reinterpret_cast<uint64_t>(new std::string), 1});
    auto s = state_->dump_buffer(this, &tmp);
    destroy();
    slice_ = Slice::Invalid();
    state_ = tmp.state_;
    tmp.state_ = nullptr;
    context_ = tmp.context_;
    auto context = union_cast<StringLazyBufferState::Context>(&context_);
    if (!s.ok()) {
      context->status = std::move(s);
    }
    return context->string;
  }
}

void LazyBuffer::pin(LazyBufferPinLevel level) {
  if (state_ == nullptr) {
    return;
  }
  Status s = Status::NotSupported();
  if (level == LazyBufferPinLevel::Internal) {
    s = state_->pin_buffer(this);
    if (s.ok()) {
      return;
    }
  }
  if (s.IsNotSupported()) {
    LazyBuffer tmp;
    s = state_->dump_buffer(this, &tmp);
    if (s.ok()) {
      *this = std::move(tmp);
      assert(valid());
      return;
    }
  }
  assign_error(std::move(s));
  assert(!valid());
}

Status LazyBuffer::dump(LazyBufferCustomizeBuffer _buffer) && {
  assert(state_ != nullptr);
  if (slice_.valid()) {
    if (state_ != LazyBufferState::buffer_state() ||
        reinterpret_cast<std::string*>(context_.data[0]) != _buffer.handle) {
      void* ptr = _buffer.uninitialized_resize(_buffer.handle, slice_.size());
      if (!slice_.empty()) {
        if (ptr == nullptr) {
          return Status::BadAlloc();
        }
        ::memcpy(ptr, slice_.data(), slice_.size());
      }
    } else {
      assert(context_.data[1] ==
             reinterpret_cast<uint64_t>(_buffer.uninitialized_resize));
    }
  } else {
    assert(state_ != LazyBufferState::buffer_state());
    LazyBuffer buffer(_buffer);
    auto s = state_->dump_buffer(this, &buffer);
    if (!s.ok()) {
      return s;
    }
    assert(buffer.state_ == LazyBufferState::buffer_state());
    assert(buffer.context_.data[0] ==
           reinterpret_cast<uint64_t>(_buffer.handle));
    assert(buffer.context_.data[1] ==
           reinterpret_cast<uint64_t>(_buffer.uninitialized_resize));
  }
  return Status::OK();
}

Status LazyBuffer::dump(std::string* _string) && {
  assert(state_ != nullptr);
  if (state_ == LazyBufferState::string_state()) {
    auto context = union_cast<StringLazyBufferState::Context>(&context_);
    if (!context->status.ok()) {
      return std::move(context->status);
    }
    assert(!valid() || (data_ == context->string->data() &&
                        size_ == context->string->size()));
    if (context->string != _string) {
      *_string = std::move(*context->string);
    }
  } else if (slice_.valid()) {
    try {
      _string->assign(slice_.data(), slice_.size());
    } catch (const std::bad_alloc&) {
      return Status::BadAlloc();
    } catch (const std::exception& ex) {
      return Status::Aborted(ex.what());
    }
  } else {
    LazyBuffer buffer(_string);
    auto s = state_->dump_buffer(this, &buffer);
    if (!s.ok()) {
      return s;
    }
    assert(buffer.state_ == LazyBufferState::string_state());
    assert(reinterpret_cast<std::string*>(buffer.context_.data[0]) == _string);
  }
  return Status::OK();
}

Status LazyBuffer::dump(LazyBuffer& _target) && {
  assert(state_ != nullptr);
  assert(this != &_target);
  auto s = state_->pin_buffer(this);
  if (s.ok()) {
    _target.reset(std::move(*this));
    s = _target.fetch();
  } else if (s.IsNotSupported()) {
    s = state_->dump_buffer(this, &_target);
    assert(!s.ok() || _target.valid());
  }
  return s;
}

bool LazyBufferBuilder::resize(size_t _size) {
  size_t old_size = slice_.valid() ? size_ : 0;
  state_->uninitialized_resize(this, _size);
  assert(size_ != 0 || data_ != nullptr);
  if (data_ == nullptr) {
    assert(size_ == size_t(-1));
    return false;
  }
  if (_size > old_size) {
    ::memset(data_ + old_size, 0, _size - old_size);
  }
  return true;
}

bool LazyBufferBuilder::uninitialized_resize(size_t _size) {
  state_->uninitialized_resize(this, _size);
  assert(size_ != 0 || data_ != nullptr);
  if (data_ == nullptr) {
    assert(size_ == size_t(-1));
    return false;
  }
  return true;
}

LazyBuffer LazyBufferReference(const LazyBuffer& buffer) {
  if (buffer.valid()) {
    return LazyBuffer(buffer.slice(), false, buffer.file_number());
  } else {
    return LazyBuffer(LazyBufferState::reference_state(),
                      {reinterpret_cast<uint64_t>(&buffer)}, Slice::Invalid(),
                      buffer.file_number());
  }
}

LazyBuffer LazyBufferRemoveSuffix(const LazyBuffer* buffer, size_t fixed_len) {
  struct RemoveSuffixLazyBufferState : public LazyBufferState {
    void destroy(LazyBuffer* /*buffer*/) const override {}

    Status fetch_buffer(LazyBuffer* buffer) const override {
      auto context = get_context(buffer);
      const LazyBuffer& buffer_ref =
          *reinterpret_cast<const LazyBuffer*>(context->data[0]);
      uint64_t len = context->data[1];
      auto s = buffer_ref.fetch();
      if (!s.ok()) {
        return s;
      }
      if (buffer_ref.size() < len) {
        return Status::Corruption("Error: Could not remove suffix");
      }
      buffer->reset(Slice(buffer_ref.data(), buffer_ref.size() - len));
      return s;
    }
  };
  static RemoveSuffixLazyBufferState static_state;
  assert(buffer != nullptr);
  if (buffer->valid()) {
    return LazyBuffer(Slice(buffer->data(), buffer->size() - fixed_len));
  } else {
    return LazyBuffer(&static_state,
                      {reinterpret_cast<uint64_t>(buffer), fixed_len},
                      Slice::Invalid(), uint64_t(-1));
  }
}

}  // namespace rocksdb
