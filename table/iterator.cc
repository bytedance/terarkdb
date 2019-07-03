//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/iterator.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "util/arena.h"

namespace rocksdb {

Cleanable::Cleanable() {
  cleanup_.function = nullptr;
  cleanup_.next = nullptr;
}

Cleanable::~Cleanable() { DoCleanup(); }

Cleanable::Cleanable(Cleanable&& other) {
  *this = std::move(other);
}

Cleanable& Cleanable::operator=(Cleanable&& other) {
  if (this != &other) {
    cleanup_ = other.cleanup_;
    other.cleanup_.function = nullptr;
    other.cleanup_.next = nullptr;
  }
  return *this;
}

// If the entire linked list was on heap we could have simply add attach one
// link list to another. However the head is an embeded object to avoid the cost
// of creating objects for most of the use cases when the Cleanable has only one
// Cleanup to do. We could put evernything on heap if benchmarks show no
// negative impact on performance.
// Also we need to iterate on the linked list since there is no pointer to the
// tail. We can add the tail pointer but maintainin it might negatively impact
// the perforamnce for the common case of one cleanup where tail pointer is not
// needed. Again benchmarks could clarify that.
// Even without a tail pointer we could iterate on the list, find the tail, and
// have only that node updated without the need to insert the Cleanups one by
// one. This however would be redundant when the source Cleanable has one or a
// few Cleanups which is the case most of the time.
// TODO(myabandeh): if the list is too long we should maintain a tail pointer
// and have the entire list (minus the head that has to be inserted separately)
// merged with the target linked list at once.
void Cleanable::DelegateCleanupsTo(Cleanable* other) {
  assert(other != nullptr);
  if (cleanup_.function == nullptr) {
    return;
  }
  Cleanup* c = &cleanup_;
  other->RegisterCleanup(c->function, c->arg1, c->arg2);
  c = c->next;
  while (c != nullptr) {
    Cleanup* next = c->next;
    other->RegisterCleanup(c);
    c = next;
  }
  cleanup_.function = nullptr;
  cleanup_.next = nullptr;
}

void Cleanable::RegisterCleanup(Cleanable::Cleanup* c) {
  assert(c != nullptr);
  if (cleanup_.function == nullptr) {
    cleanup_.function = c->function;
    cleanup_.arg1 = c->arg1;
    cleanup_.arg2 = c->arg2;
    delete c;
  } else {
    c->next = cleanup_.next;
    cleanup_.next = c;
  }
}

void Cleanable::RegisterCleanup(CleanupFunction func, void* arg1, void* arg2) {
  assert(func != nullptr);
  Cleanup* c;
  if (cleanup_.function == nullptr) {
    c = &cleanup_;
  } else {
    c = new Cleanup;
    c->next = cleanup_.next;
    cleanup_.next = c;
  }
  c->function = func;
  c->arg1 = arg1;
  c->arg2 = arg2;
}

Status Iterator::GetProperty(std::string /*prop_name*/, std::string* prop) {
  if (prop == nullptr) {
    return Status::InvalidArgument("prop is nullptr");
  }
  return Status::InvalidArgument("Unidentified property.");
}

void LazyValue::reset() {
  raw_ = Slice::Invalid();
  value_ = Slice::Invalid();
  arg0_ = arg1_ = nullptr;
  decoder_ = nullptr;
  buffer_ = nullptr;
  file_number_ = uint64_t(-1);
}

void LazyValue::reset(const Slice& _value, uint64_t _file_number) {
  raw_ = Slice::Invalid();
  value_ = _value;
  arg0_ = arg1_ = nullptr;
  decoder_ = nullptr;
  buffer_ = nullptr;
  file_number_ = _file_number;
}

void LazyValue::reset(const Slice& _raw, void* _arg0, void* _arg1,
                      const LazuValueDecoder* _decoder, std::string* _buffer,
                      uint64_t _file_number) {
  raw_ = _raw;
  value_ = Slice::Invalid();
  arg0_ = _arg0;
  arg1_ = _arg1;
  decoder_ = _decoder;
  buffer_ = _buffer;
  file_number_ = _file_number;
}

void LazyValue::reset(const LazyValue& _value, uint64_t _file_number) {
  raw_ = _value.raw_;
  value_ = _value.value_;
  arg0_ = _value.arg0_;
  arg1_ = _value.arg1_;
  decoder_ = _value.decoder_;
  buffer_ = _value.buffer_;
  file_number_ = _file_number;
}

Status LazyValue::decode() const {
  if (value_.data() != nullptr) {
    return Status::OK();
  }
  assert(raw_.valid() && decoder_ != nullptr);
  return decoder_->decode_value(raw_, arg0_, arg1_, &value_, buffer_);
}

FutureValue::FutureValue(const LazyValue& value) {
  struct ValueDecoderImpl
      : public FutureValueDecoder, public LazuValueDecoder {
    LazyValue decode_lazy_value(const Slice& storage) const override {
      return LazyValue(storage, nullptr, nullptr, this, nullptr);
    }
    Status decode_value(const Slice& raw, void* arg0, void* arg1, Slice* value,
                        std::string* /*buffer*/) const override {
      if (raw.empty()) {
        return Status::Corruption("LazyValue decode fail");
      }
      if (raw[raw.size() - 1]) {
        *value = Slice(raw.data(), raw.size() - 1);
        return Status::OK();
      }
      return Status::Corruption(Slice(raw.data(), raw.size() - 1));
    }
  };
  static ValueDecoderImpl decoder;
  auto s = value.decode();
  if (s.ok()) {
    PutVarint64(&storage_, value.get().size());
    storage_.append(value.get().data(), value.get().size());
  } else {
    auto err_msg = s.ToString();
    PutVarint64(&storage_, err_msg.size());
    storage_.append(err_msg.data(), err_msg.size());
  }
  storage_.push_back(s.ok());
  decoder_ = &decoder;
}

FutureValue::FutureValue(const Slice& value, bool pinned, uint64_t file_number) {
  struct ValueDecoderImpl : public FutureValueDecoder {
    LazyValue decode_lazy_value(const Slice& storage) const override {
      Slice input = storage;
      uint64_t file_number, value_size;
      if (!GetVarint64(&input, &file_number) ||
          !GetVarint64(&input, &value_size)) {
        return LazyValue();
      }
      char flag = input[input.size() - 1];
      if (!flag) {
        return LazyValue(Slice(input.data(), value_size));
      }
      uint64_t value_ptr;
      if (!GetVarint64(&input, &value_ptr)) {
        return LazyValue();
      }
      return LazyValue(Slice(reinterpret_cast<const char*>(value_ptr),
                             value_size));
    }
  };
  static ValueDecoderImpl decoder;

  PutVarint64Varint64(&storage_, file_number, value.size());
  if (pinned) {
    PutFixed64(&storage_, reinterpret_cast<uint64_t>(value.data()));
  } else {
    storage_.append(value.data(), value.size());
  }
  storage_.push_back(pinned);
  decoder_ = &decoder;
}

namespace {
class EmptyIterator : public Iterator {
 public:
  explicit EmptyIterator(const Status& s) : status_(s) {}
  virtual bool Valid() const override { return false; }
  virtual void Seek(const Slice& /*target*/) override {}
  virtual void SeekForPrev(const Slice& /*target*/) override {}
  virtual void SeekToFirst() override {}
  virtual void SeekToLast() override {}
  virtual void Next() override { assert(false); }
  virtual void Prev() override { assert(false); }
  Slice key() const override {
    assert(false);
    return Slice();
  }
  Slice value() const override {
    assert(false);
    return Slice();
  }
  virtual Status status() const override { return status_; }

 private:
  Status status_;
};

template <class TValue = Slice>
class EmptyInternalIteratorBase : public InternalIteratorBase<TValue> {
 public:
  explicit EmptyInternalIteratorBase(const Status& s) : status_(s) {}
  virtual bool Valid() const override { return false; }
  virtual void Seek(const Slice& /*target*/) override {}
  virtual void SeekForPrev(const Slice& /*target*/) override {}
  virtual void SeekToFirst() override {}
  virtual void SeekToLast() override {}
  virtual void Next() override { assert(false); }
  virtual void Prev() override { assert(false); }
  Slice key() const override {
    assert(false);
    return Slice();
  }
  TValue value() const override {
    assert(false);
    return TValue();
  }
  virtual Status status() const override { return status_; }

 private:
  Status status_;
};
template <>
class EmptyInternalIteratorBase<Slice> : public InternalIteratorBase<Slice> {
public:
  explicit EmptyInternalIteratorBase(const Status& s) : status_(s) {}
  virtual bool Valid() const override { return false; }
  virtual void Seek(const Slice& /*target*/) override {}
  virtual void SeekForPrev(const Slice& /*target*/) override {}
  virtual void SeekToFirst() override {}
  virtual void SeekToLast() override {}
  virtual void Next() override { assert(false); }
  virtual void Prev() override { assert(false); }
  Slice key() const override {
    assert(false);
    return Slice();
  }
  LazyValue value() const override {
    assert(false);
    return LazyValue();
  }
  FutureValue future_value() const override {
    assert(false);
    return FutureValue();
  }
  virtual Status status() const override { return status_; }

private:
  Status status_;
};

}  // namespace

Iterator* NewEmptyIterator() { return new EmptyIterator(Status::OK()); }

Iterator* NewErrorIterator(const Status& status) {
  return new EmptyIterator(status);
}

template <class TValue>
InternalIteratorBase<TValue>* NewEmptyInternalIterator(Arena* arena) {
  using EmptyInternalIterator = EmptyInternalIteratorBase<TValue>;
  if (arena == nullptr) {
    return new EmptyInternalIterator(Status::OK());
  } else {
    auto mem = arena->AllocateAligned(sizeof(EmptyInternalIterator));
    return new (mem) EmptyInternalIterator(Status::OK());
  }
}
template InternalIteratorBase<BlockHandle>* NewEmptyInternalIterator(
    Arena* arena);
template InternalIteratorBase<Slice>* NewEmptyInternalIterator(Arena* arena);

template <class TValue>
InternalIteratorBase<TValue>* NewErrorInternalIterator(const Status& status,
                                                       Arena* arena) {
  using EmptyInternalIterator = EmptyInternalIteratorBase<TValue>;
  if (arena == nullptr) {
    return new EmptyInternalIterator(status);
  } else {
    auto mem = arena->AllocateAligned(sizeof(EmptyInternalIterator));
    return new (mem) EmptyInternalIterator(status);
  }
}
template InternalIteratorBase<BlockHandle>* NewErrorInternalIterator(
    const Status& status, Arena* arena);
template InternalIteratorBase<Slice>* NewErrorInternalIterator(
    const Status& status, Arena* arena);

}  // namespace rocksdb
