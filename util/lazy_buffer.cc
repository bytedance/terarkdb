//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/lazy_buffer.h"
#include <algorithm>
#include <string.h>
#include <stdlib.h>

namespace rocksdb {

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

class DefaultLazyBufferControllerImpl : public LazyBufferController {
public:
  struct alignas(LazyBufferRep) Rep {
    char data[sizeof(LazyBufferRep)];
  };

  void destroy(LazyBuffer* /*buffer*/) const override {}

  void uninitialized_resize(LazyBuffer* buffer, size_t size) const override {
    auto rep = union_cast<Rep>(get_rep(buffer));
    if (size <= sizeof(Rep)) {
      if (buffer->data() != rep->data) {
        ::memmove(rep->data, buffer->data(), std::min(buffer->size(), size));
      }
      set_slice(buffer, Slice(rep->data, size));
    } else {
      LazyBufferController::uninitialized_resize(buffer, size);
    }
  }

  void assign_slice(LazyBuffer* buffer, const Slice& slice) const override {
    auto rep = union_cast<Rep>(get_rep(buffer));
    if (slice.size() <= sizeof(Rep)) {
      ::memmove(rep->data, slice.data(), slice.size());
      set_slice(buffer, Slice(rep->data, slice.size()));
    } else {
      LazyBufferController::assign_slice(buffer, slice);
    }
  }

  void pin_buffer(LazyBuffer* buffer) const override {
    auto rep = union_cast<Rep>(get_rep(buffer));
    if (buffer->size() > sizeof(Rep)) {
      LazyBufferController::assign_slice(buffer, buffer->get_slice());
    } else if (buffer->data() != rep->data) {
      ::memmove(rep->data, buffer->data(), buffer->size());
      set_slice(buffer, Slice(rep->data, buffer->size()));
    }
  }

  Status dump_buffer(LazyBuffer* buffer, LazyBuffer* target) const override {
    target->reset(buffer->get_slice(), true, buffer->file_number());
    return Status::OK();
  }

  Status fetch_buffer(LazyBuffer* /*slice*/) const override {
    assert(false);
    return Status::Corruption("Invalid buffer");
  }
};

class BufferLazyBufferControllerImpl : public LazyBufferController {
 public:
  struct alignas(LazyBufferRep) Rep {
    LazyBufferCustomizeBuffer buffer;
    Status status;
  };

  void destroy(LazyBuffer* buffer) const override {
    auto rep = union_cast<Rep>(get_rep(buffer));
    if (rep->buffer.uninitialized_resize == nullptr &&
        rep->buffer.handle != nullptr) {
      ::free(rep->buffer.handle);
    }
    rep->status.~Status();
  }

  void uninitialized_resize(LazyBuffer* buffer, size_t size) const override {
    auto rep = union_cast<Rep>(get_rep(buffer));
    if (rep->buffer.uninitialized_resize == nullptr) {
      char* ptr = (char*)::realloc(rep->buffer.handle, size);
      if (ptr != nullptr || size == 0) {
        rep->buffer.handle = ptr;
        rep->status = Status::OK();
        set_slice(buffer, Slice(ptr, size));
      } else {
        rep->status = Status::BadAlloc();
        set_slice(buffer, Slice::Invalid());
      }
    } else {
      char* ptr =
          (char*)rep->buffer.uninitialized_resize(rep->buffer.handle, size);
      if (ptr != nullptr || size == 0) {
        rep->status = Status::OK();
        set_slice(buffer, Slice(ptr, size));
      } else {
        rep->status = Status::BadAlloc();
        set_slice(buffer, Slice::Invalid());
      }
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
      BufferLazyBufferControllerImpl::uninitialized_resize(buffer,
                                                           slice.size());
      auto rep = union_cast<Rep>(get_rep(buffer));
      if (rep->status.ok() && slice.empty()) {
        assert(buffer->data() != nullptr);
        assert(buffer->size() == slice.size());
        char* ptr = (char*)buffer->data();
        ::memcpy(ptr, slice.data(), slice.size());
      }
    }
  }

  void assign_error(LazyBuffer* buffer, Status&& status) const override {
    auto rep = union_cast<Rep>(get_rep(buffer));
    rep->status = std::move(status);
    set_slice(buffer, Slice::Invalid());
  }

  void pin_buffer(LazyBuffer* /*buffer*/) const override {}

  Status dump_buffer(LazyBuffer* buffer, LazyBuffer* target) const override {
    auto rep = union_cast<Rep>(get_rep(buffer));
    if (rep->status.ok()) {
      assert(buffer->valid());
      target->reset(buffer->get_slice(), true, buffer->file_number());
      return Status::OK();
    } else {
      assert(!buffer->valid());
      return std::move(rep->status);
    }
  }

  Status fetch_buffer(LazyBuffer* buffer) const override {
    auto rep = union_cast<const Rep>(get_rep(buffer));
    assert(!rep->status.ok());
    return rep->status;
  }
};

struct StringLazyBufferControllerImpl : public LazyBufferController {
 public:
  struct alignas(LazyBufferRep) Rep {
    std::string* string;
    uint64_t is_owner;
    Status status;
  };

  void destroy(LazyBuffer* buffer) const override {
    auto rep = union_cast<Rep>(get_rep(buffer));
    if (rep->is_owner) {
      delete rep->string;
    }
    rep->status.~Status();
  }

  void uninitialized_resize(LazyBuffer* buffer, size_t size) const override {
    auto rep = union_cast<Rep>(get_rep(buffer));
    try {
      rep->string->resize(size);
      set_slice(buffer, *rep->string);
      rep->status = Status::OK();
      return;
    } catch (const std::bad_alloc&) {
      rep->status = Status::BadAlloc();
    } catch (const std::exception& ex) {
      rep->status = Status::Aborted(ex.what());
    }
    set_slice(buffer, Slice::Invalid());
  }

  void assign_slice(LazyBuffer* buffer, const Slice& slice) const override {
    auto rep = union_cast<Rep>(get_rep(buffer));
    rep->string->assign(slice.data(), slice.size());
    rep->status = Status::OK();
    set_slice(buffer, *rep->string);
  }

  void assign_error(LazyBuffer* buffer, Status&& status) const override {
    auto rep = union_cast<Rep>(get_rep(buffer));
    rep->status = std::move(status);
    set_slice(buffer, Slice::Invalid());
  }

  void pin_buffer(LazyBuffer* /*buffer*/) const override {}

  Status dump_buffer(LazyBuffer* buffer, LazyBuffer* target) const override {
    auto rep = union_cast<Rep>(get_rep(buffer));
    if (rep->status.ok()) {
      target->reset(*rep->string, true, buffer->file_number());
      return Status::OK();
    } else {
      assert(!buffer->valid());
      return std::move(rep->status);
    }
  }

  Status fetch_buffer(LazyBuffer* buffer) const override {
    auto rep = union_cast<const Rep>(get_rep(buffer));
    if (rep->status.ok()) {
      set_slice(buffer, *rep->string);
      return Status::OK();
    } else {
      return rep->status;
    }
  }
};

// 0 -> pointer to slice
struct ReferenceLazyBufferControllerImpl : public LazyBufferController {

  void destroy(LazyBuffer* /*buffer*/) const override {}

  Status fetch_buffer(LazyBuffer* buffer) const override {
    const LazyBuffer& buffer_ref =
        *reinterpret_cast<const LazyBuffer*>(get_rep(buffer)->data[0]);
    auto s = buffer_ref.fetch();
    if (s.ok()) {
      set_slice(buffer, buffer_ref.get_slice());
    }
    return s;
  }
};

struct CleanableLazyBufferControllerImpl : public LazyBufferController {

  void destroy(LazyBuffer* buffer) const override {
    union_cast<Cleanable>(get_rep(buffer))->Reset();
  }

  void pin_buffer(LazyBuffer* /*buffer*/) const override {}

  Status dump_buffer(LazyBuffer* buffer, LazyBuffer* target) const override {
    target->reset(buffer->get_slice(), true, buffer->file_number());
    union_cast<Cleanable>(get_rep(buffer))->Reset();
    return Status::OK();
  }

  Status fetch_buffer(LazyBuffer* /*buffer*/) const override {
    assert(false);
    return Status::OK();
  }
};

void LazyBufferController::uninitialized_resize(LazyBuffer* buffer,
                                                size_t size) const {
  if (buffer->valid() && size <= sizeof(LazyBufferRep)) {
    DefaultLazyBufferControllerImpl::Rep tmp_rep{};
    ::memcpy(tmp_rep.data, buffer->data_, std::min(buffer->size_, size));
    destroy(buffer);
    buffer->controller_ = light_controller();
    auto rep =
        union_cast<DefaultLazyBufferControllerImpl::Rep>(&buffer->rep_);
    *rep = tmp_rep;
    buffer->data_ = rep->data;
    buffer->size_ = size;
  } else {
    LazyBuffer tmp(size);
    assert(tmp.controller_ == buffer_controller());
    auto rep = union_cast<BufferLazyBufferControllerImpl::Rep>(&tmp.rep_);
    if (rep->status.ok()) {
      auto s = dump_buffer(buffer, &tmp);
      assert(tmp.controller_ == buffer_controller());
      if (!s.ok()) {
        rep->status = std::move(s);
        set_slice(&tmp, Slice::Invalid());
      }
    }
    destroy(buffer);
    buffer->slice_ = tmp.slice_;
    buffer->controller_ = tmp.controller_;
    tmp.controller_ = nullptr;
    buffer->rep_ = tmp.rep_;
  }
}

void LazyBufferController::assign_slice(LazyBuffer* buffer,
                                        const Slice& slice) const {
  buffer->controller_->destroy(buffer);
  if (slice.size() <= sizeof(LazyBufferRep)) {
    buffer->controller_ = light_controller();
    auto rep =
        union_cast<DefaultLazyBufferControllerImpl::Rep>(&buffer->rep_);
    ::memcpy(rep->data, slice.data(), slice.size());
    buffer->data_ = rep->data;
    buffer->size_ = slice.size();
  } else {
    buffer->controller_ = buffer_controller();
    auto rep = union_cast<BufferLazyBufferControllerImpl::Rep>(&buffer->rep_);
    rep->buffer.handle = ::malloc(slice.size());
    rep->buffer.uninitialized_resize = nullptr;
    if (rep->buffer.handle == nullptr) {
      ::new(&rep->status) Status(Status::BadAlloc());
    } else {
      ::new(&rep->status) Status;
      ::memcpy(rep->buffer.handle, slice.data(), slice.size());
      buffer->data_ = (char*)rep->buffer.handle;
      buffer->size_ = slice.size();
    }
  }
}

void LazyBufferController::assign_error(LazyBuffer* buffer,
                                        Status&& status) const {
  destroy(buffer);
  buffer->controller_ = buffer_controller();
  auto rep = union_cast<BufferLazyBufferControllerImpl::Rep>(&buffer->rep_);
  rep->buffer.handle = nullptr;
  rep->buffer.uninitialized_resize = nullptr;
  ::new(&rep->status) Status(std::move(status));
  buffer->slice_ = Slice::Invalid();
}

void LazyBufferController::pin_buffer(LazyBuffer* buffer) const {
  LazyBuffer tmp;
  auto s = dump_buffer(buffer, &tmp);
  if (s.ok()) {
    *buffer = std::move(tmp);
  } else {
    assign_error(buffer, std::move(s));
    assert(!buffer->valid());
  }
}

Status LazyBufferController::dump_buffer(LazyBuffer* buffer,
                                         LazyBuffer* target) const {
  if (!buffer->valid()) {
    auto s = fetch_buffer(buffer);
    if (!s.ok()) {
      return s;
    }
  }
  target->controller_->assign_slice(target, buffer->slice_);
  assert(target->slice_ == buffer->slice_);
  target->file_number_ = buffer->file_number_;
  destroy(buffer);
  return Status::OK();
}

const LazyBufferController* LazyBufferController::light_controller() {
  static DefaultLazyBufferControllerImpl controller_impl;
  return &controller_impl;
}

const LazyBufferController* LazyBufferController::buffer_controller() {
  static BufferLazyBufferControllerImpl controller_impl;
  return &controller_impl;
}

const LazyBufferController* LazyBufferController::string_controller() {
  static StringLazyBufferControllerImpl controller_impl;
  return &controller_impl;
}

const LazyBufferController* LazyBufferController::reference_controller() {
  static ReferenceLazyBufferControllerImpl controller_impl;
  return &controller_impl;
}

const LazyBufferController* LazyBufferController::cleanable_controller() {
  static CleanableLazyBufferControllerImpl controller_impl;
  return &controller_impl;
}

LazyBuffer::LazyBuffer(size_t _size) noexcept
    : rep_{},
      file_number_(uint64_t(-1)) {
  if (_size <= sizeof(LazyBufferRep)) {
    controller_ = LazyBufferController::light_controller();
    data_ = union_cast<DefaultLazyBufferControllerImpl::Rep>(&rep_)->data;
    size_ = _size;
  } else {
    controller_ = LazyBufferController::buffer_controller();
    auto rep = union_cast<BufferLazyBufferControllerImpl::Rep>(&rep_);
    rep->buffer.handle = ::malloc(_size);
    if (rep->buffer.handle == nullptr) {
      ::new(&rep->status) Status(Status::BadAlloc());
      slice_ = Slice::Invalid();
    } else {
      ::new(&rep->status) Status;
      data_ = (char*)rep->buffer.handle;
      size_ = _size;
    }
  }
}

void LazyBuffer::fix_light_controller(const LazyBuffer& other) {
  assert(controller_ == LazyBufferController::light_controller());
  assert(other.size_ <= sizeof(LazyBufferRep));
  data_ = union_cast<DefaultLazyBufferControllerImpl::Rep>(&rep_)->data;
  size_ = other.size_;
  ::memmove(data_, other.data_, size_);
}

LazyBuffer::LazyBuffer(LazyBufferCustomizeBuffer _buffer) noexcept
    : controller_(LazyBufferController::buffer_controller()),
      rep_{reinterpret_cast<uint64_t>(_buffer.handle),
           reinterpret_cast<uint64_t>(_buffer.uninitialized_resize)},
      file_number_(uint64_t(-1)) {
  assert(_buffer.handle != nullptr);
  assert(_buffer.uninitialized_resize != nullptr);
  ::new(&union_cast<BufferLazyBufferControllerImpl::Rep>(&rep_)->status) Status;
}

LazyBuffer::LazyBuffer(std::string* _string) noexcept
    : slice_(*_string),
      controller_(LazyBufferController::string_controller()),
      rep_{reinterpret_cast<uint64_t>(_string)},
      file_number_(uint64_t(-1)) {
  assert(_string != nullptr);
  ::new(&union_cast<StringLazyBufferControllerImpl::Rep>(&rep_)->status) Status;
}

void LazyBuffer::reset(LazyBufferCustomizeBuffer _buffer) {
  assert(_buffer.handle != nullptr);
  assert(_buffer.uninitialized_resize != nullptr);
  destroy();
  controller_ = LazyBufferController::buffer_controller();
  auto rep = union_cast<BufferLazyBufferControllerImpl::Rep>(&rep_);
  rep->buffer = _buffer;
  ::new(&rep->status) Status;
  slice_ = Slice();
  file_number_ = uint64_t(-1);
}

void LazyBuffer::reset(std::string* _string) {
  assert(_string != nullptr);
  slice_ = *_string;
  auto rep = union_cast<StringLazyBufferControllerImpl::Rep>(&rep_);
  if (controller_ == LazyBufferController::string_controller()) {
    if (rep->string != _string) {
      if (rep->is_owner) {
        delete rep->string;
        rep->is_owner = 0;
      }
      rep->string = _string;
    }
    rep->status = Status::OK();
  } else {
    destroy();
    controller_ = LazyBufferController::string_controller();
    rep->string = _string;
    rep->is_owner = 0;
    ::new(&rep->status) Status;
  }
  slice_ = *rep->string;
}

void LazyBuffer::assign(const LazyBuffer& source) {
  Status s;
  if (source.valid() || (s = source.fetch()).ok()) {
    reset(source.get_slice(), true, source.file_number());
  } else {
    controller_->assign_error(this, std::move(std::move(s)));
    assert(!slice_.valid());
  }
}

LazyBufferEditor* LazyBuffer::get_editor() {
  assert(controller_ != nullptr);
  file_number_ = uint64_t(-1);
  controller_->uninitialized_resize(this, size_);
  return reinterpret_cast<LazyBufferEditor*>(this);
}

std::string* LazyBuffer::trans_to_string() {
  assert(controller_ != nullptr);
  file_number_ = uint64_t(-1);
  if (controller_ == LazyBufferController::string_controller()) {
    auto rep = union_cast<StringLazyBufferControllerImpl::Rep>(&rep_);
    assert(!slice_.valid() || (data_ == rep->string->data() &&
                               size_ == rep->string->size()));
    slice_ = Slice::Invalid();
    return rep->string;
  } else {
    LazyBuffer tmp(LazyBufferController::string_controller(),
                   {reinterpret_cast<uint64_t>(new std::string), 1});
    auto s = controller_->dump_buffer(this, &tmp);
    destroy();
    slice_ = Slice::Invalid();
    controller_ = tmp.controller_;
    tmp.controller_ = nullptr;
    rep_ = tmp.rep_;
    auto rep = union_cast<StringLazyBufferControllerImpl::Rep>(&rep_);
    if (!s.ok()) {
      rep->status = std::move(s);
    }
    return rep->string;
  }
}

Status LazyBuffer::dump(LazyBufferCustomizeBuffer _buffer) && {
  assert(controller_ != nullptr);
  if (slice_.valid()) {
    if (controller_ != LazyBufferController::buffer_controller() ||
        reinterpret_cast<std::string*>(rep_.data[0]) != _buffer.handle) {
      void* ptr = _buffer.uninitialized_resize(_buffer.handle, slice_.size());
      if (ptr == nullptr) {
        return Status::BadAlloc();
      }
      ::memcpy(ptr, slice_.data(), slice_.size());
    } else {
      assert(rep_.data[1] ==
             reinterpret_cast<uint64_t>(_buffer.uninitialized_resize));
    }
  } else {
    LazyBuffer buffer(_buffer);
    auto s = controller_->dump_buffer(this, &buffer);
    if (!s.ok()) {
      return s;
    }
    assert(buffer.controller_ == LazyBufferController::buffer_controller());
    assert(buffer.rep_.data[0] == reinterpret_cast<uint64_t>(_buffer.handle));
    assert(buffer.rep_.data[1] ==
           reinterpret_cast<uint64_t>(_buffer.uninitialized_resize));
  }
  return Status::OK();
}

Status LazyBuffer::dump(std::string* _string) && {
  assert(controller_ != nullptr);
  if (slice_.valid()) {
    if (controller_ != LazyBufferController::string_controller() ||
        reinterpret_cast<std::string*>(rep_.data[0]) != _string) {
      _string->assign(slice_.data(), slice_.size());
    } else {
      assert(slice_.data() == _string->data());
      assert(slice_.size() == _string->size());
    }
  } else {
    LazyBuffer buffer(_string);
    auto s = controller_->dump_buffer(this, &buffer);
    if (!s.ok()) {
      return s;
    }
    assert(buffer.controller_ == LazyBufferController::string_controller());
    assert(reinterpret_cast<std::string*>(buffer.rep_.data[0]) == _string);
  }
  return Status::OK();
}

bool LazyBufferEditor::resize(size_t _size) {
  size_t old_size = slice_.valid() ? size_ : 0;
  controller_->uninitialized_resize(this, _size);
  if (data_ == nullptr) {
    return false;
  }
  if (_size > old_size) {
    ::memset(data_ + old_size, 0, _size - old_size);
  }
  return true;
}

bool LazyBufferEditor::uninitialized_resize(size_t _size) {
  controller_->uninitialized_resize(this, _size);
  return data_ != nullptr;
}

LazyBuffer LazyBufferReference(const LazyBuffer& buffer) {
  if (buffer.valid()) {
    return LazyBuffer(buffer.get_slice(), false, buffer.file_number());
  } else {
    return LazyBuffer(LazyBufferController::reference_controller(),
                      {reinterpret_cast<uint64_t>(&buffer)}, Slice::Invalid(),
                      buffer.file_number());
  }
}

LazyBuffer LazyBufferRemoveSuffix(const LazyBuffer* buffer, size_t fixed_len) {
  struct LazyBufferControllerImpl : public LazyBufferController {
    void destroy(LazyBuffer* /*buffer*/) const override {}

    Status fetch_buffer(LazyBuffer* buffer) const override {
      auto rep = get_rep(buffer);
      const LazyBuffer& buffer_ref =
          *reinterpret_cast<const LazyBuffer*>(rep->data[0]);
      uint64_t len = rep->data[1];
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
  static LazyBufferControllerImpl controller_impl;
  assert(buffer != nullptr);
  if (buffer->valid()) {
    return LazyBuffer(Slice(buffer->data(), buffer->size() - fixed_len));
  } else {
    return LazyBuffer(&controller_impl, {reinterpret_cast<uint64_t>(buffer),
                                         fixed_len},
                      Slice::Invalid(), uint64_t(-1));
  }
}

}  // namespace rocksdb
