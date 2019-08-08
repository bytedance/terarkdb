//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <set>

#include "table/internal_iterator.h"

namespace rocksdb {

class SeparateHelper;

// A internal wrapper class with an interface similar to Iterator that caches
// the valid() and key() results for an underlying iterator.
// This can help avoid virtual function calls and also gives better
// cache locality.
template <class TValue = LazySlice>
class IteratorWrapperBase {
 public:
  IteratorWrapperBase() : iter_(nullptr), valid_(false) {}
  explicit IteratorWrapperBase(InternalIteratorBase<TValue>* _iter)
      : iter_(nullptr) {
    Set(_iter);
  }
  ~IteratorWrapperBase() {}
  InternalIteratorBase<TValue>* iter() const { return iter_; }

  // Set the underlying Iterator to _iter and return
  // previous underlying Iterator.
  InternalIteratorBase<TValue>* Set(InternalIteratorBase<TValue>* _iter) {
    InternalIteratorBase<TValue>* old_iter = iter_;

    iter_ = _iter;
    if (iter_ == nullptr) {
      valid_ = false;
    } else {
      Update();
    }
    return old_iter;
  }

  void SetValid(bool valid) { valid_ = valid; }

  void DeleteIter(bool is_arena_mode) {
    if (iter_) {
      if (!is_arena_mode) {
        delete iter_;
      } else {
        iter_->~InternalIteratorBase<TValue>();
      }
    }
  }

  // Iterator interface methods
  bool Valid() const        { return valid_; }
  Slice key() const         { assert(Valid()); return key_; }
  TValue value() const {
    assert(Valid());
    return iter_->value();
  }
  // Methods below require iter() != nullptr
  Status status() const     { assert(iter_); return iter_->status(); }
  void Next()               { assert(iter_); iter_->Next();        Update(); }
  void Prev()               { assert(iter_); iter_->Prev();        Update(); }
  void Seek(const Slice& k) { assert(iter_); iter_->Seek(k);       Update(); }
  void SeekForPrev(const Slice& k) {
    assert(iter_);
    iter_->SeekForPrev(k);
    Update();
  }
  void SeekToFirst()        { assert(iter_); iter_->SeekToFirst(); Update(); }
  void SeekToLast()         { assert(iter_); iter_->SeekToLast();  Update(); }

 protected:
  void Update() {
    valid_ = iter_->Valid();
    if (valid_) {
      key_ = iter_->key();
      assert(iter_->status().ok());
    }
  }

  InternalIteratorBase<TValue>* iter_;
  bool valid_;
  Slice key_;
};

class IteratorWrapper : public IteratorWrapperBase<LazySlice> {
  using IteratorWrapperBase<LazySlice>::iter_;
 public:
  using IteratorWrapperBase<LazySlice>::IteratorWrapperBase;

  LazySlice combined_value(const Slice& user_key) const {
    return iter_->combined_value(user_key);
  }
};

class CombinedInternalIterator : public InternalIterator {
 public:
  CombinedInternalIterator(InternalIterator* iter,
                           const SeparateHelper* separate_helper)
      : iter_(iter), separate_helper_(separate_helper) {}

  bool Valid() const { return iter_->Valid(); }
  Slice key() const { return iter_->key(); }
  LazySlice value() const { return iter_->value(); }
  LazySlice combined_value(const Slice& user_key) const;
  Status status() const { return iter_->status(); }
  void Next() { iter_->Next(); }
  void Prev() { iter_->Prev(); }
  void Seek(const Slice& k) { iter_->Seek(k); }
  void SeekForPrev(const Slice& k) { iter_->SeekForPrev(k); }
  void SeekToFirst() { iter_->SeekToFirst(); }
  void SeekToLast() { iter_->SeekToLast(); }

  InternalIterator* iter_;
  const SeparateHelper* separate_helper_;
};


}  // namespace rocksdb
