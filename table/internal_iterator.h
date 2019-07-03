// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <string>
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/status.h"
#include "table/format.h"

namespace rocksdb {

class LazuValueDecoder {
public:
  virtual Status decode_value(const Slice& raw, void* _arg0, void* _arg1,
                              Slice* value, std::string* buffer) const = 0;
  virtual ~LazuValueDecoder() {}
};

class LazyValue {
protected:
  mutable Slice value_;
  Slice raw_;
  void *arg0_, *arg1_;
  const LazuValueDecoder* decoder_;
  std::string* buffer_;
  uint64_t file_number_;

public:
  LazyValue() { reset(); }
  explicit LazyValue(const Slice& _value, uint64_t _file_number = uint64_t(-1)) {
    reset(_value, _file_number);
  }
  LazyValue(const Slice& _raw, void* _arg0, void* _arg1,
            const LazuValueDecoder* _decoder, std::string* _buffer,
            uint64_t _file_number = uint64_t(-1)) {
    reset(_raw, _arg0, _arg1, _decoder, _buffer, _file_number);
  }
  LazyValue(const LazyValue& _value, uint64_t _file_number) {
    reset(_value, _file_number);
  }

  void reset();
  void reset(const Slice& _value, uint64_t _file_number = uint64_t(-1));
  void reset(const Slice& _raw, void* _arg0, void* _arg1,
             const LazuValueDecoder* _decoder, std::string* _buffer,
             uint64_t _file_number = uint64_t(-1));
  void reset(const LazyValue& _value, uint64_t _file_number);

  const Slice& raw() const { return raw_; }
  const Slice& get() const { assert(value_.valid()); return value_; }
  uint64_t file_number() const { return file_number_; }
  const LazuValueDecoder *decoder() const { return decoder_; }

  Status decode() const;
};

class FutureValueDecoder {
public:
  virtual LazyValue decode_lazy_value(const Slice& storage) const = 0;
  virtual ~FutureValueDecoder() {}
};


class FutureValue {
protected:
  std::string storage_;
  const FutureValueDecoder* decoder_;
public:
  FutureValue() : decoder_(nullptr) {}
  FutureValue(std::string &&storage, const FutureValueDecoder* decoder)
      : storage_(std::move(storage)), decoder_(decoder) {}
  explicit FutureValue(const LazyValue& value);
  FutureValue(const Slice& value, bool pinned,
              uint64_t file_number = uint64_t(-1));

  FutureValue(FutureValue&&) = default;
  FutureValue(const FutureValue&) = default;
  FutureValue& operator = (FutureValue&&) = default;
  FutureValue& operator = (const FutureValue&) = default;

  void reset() { storage_.clear(); decoder_ = nullptr; }
  bool valid() const { return decoder_ != nullptr; }

  LazyValue value() const{
    assert(valid());
    return decoder_->decode_lazy_value(storage_);
  }
};

class InternalIteratorCommon : public Cleanable {
 public:
  InternalIteratorCommon() {}
  virtual ~InternalIteratorCommon() {}

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  // Always returns false if !status().ok().
  virtual bool Valid() const = 0;

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst() = 0;

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void SeekToLast() = 0;

  // Position at the first key in the source that at or past target
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  // All Seek*() methods clear any error status() that the iterator had prior to
  // the call; after the seek, status() indicates only the error (if any) that
  // happened during the seek, not any past errors.
  virtual void Seek(const Slice& target) = 0;

  // Position at the first key in the source that at or before target
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or before target.
  virtual void SeekForPrev(const Slice& target) = 0;

  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() = 0;

  // Moves to the previous entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the first entry in source.
  // REQUIRES: Valid()
  virtual void Prev() = 0;

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual Slice key() const = 0;

  // If an error has occurred, return it.  Else return an ok status.
  // If non-blocking IO is requested and this operation cannot be
  // satisfied without doing some IO, then this returns Status::Incomplete().
  virtual Status status() const = 0;

  // True if the iterator is invalidated because it is out of the iterator
  // upper bound
  virtual bool IsOutOfBound() { return false; }

  virtual Status GetProperty(std::string /*prop_name*/, std::string* /*prop*/) {
    return Status::NotSupported("");
  }

 protected:
  void SeekForPrevImpl(const Slice& target, const Comparator* cmp) {
    Seek(target);
    if (!Valid()) {
      SeekToLast();
    }
    while (Valid() && cmp->Compare(target, key()) < 0) {
      Prev();
    }
  }

 private:
  // No copying allowed
  InternalIteratorCommon(const InternalIteratorCommon&) = delete;
  InternalIteratorCommon& operator=(const InternalIteratorCommon&) = delete;
};

template <class TValue>
class InternalIteratorBase : public InternalIteratorCommon {
 public:

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual TValue value() const = 0;
};

template<>
class InternalIteratorBase<Slice> : public InternalIteratorCommon {
 public:
  // Return the value for the current entry. The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual LazyValue value() const = 0;

  // Return the key & value for the current entry. The underlying storage for
  // the returned slice is valid until version expired.
  // REQUIRES: Valid()
  virtual FutureValue future_value() const = 0;
};

using InternalIterator = InternalIteratorBase<Slice>;

// Return an empty iterator (yields nothing).
// allocated arena if not nullptr.
template <class TValue = Slice>
extern InternalIteratorBase<TValue>* NewEmptyInternalIterator(
    Arena* arena = nullptr);

// Return an empty iterator with the specified status.
// allocated arena if not nullptr.
template <class TValue = Slice>
extern InternalIteratorBase<TValue>* NewErrorInternalIterator(
    const Status& status, Arena* arena = nullptr);

}  // namespace rocksdb
