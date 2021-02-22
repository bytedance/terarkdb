//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <list>

#include "port/port.h"
#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

class PendingOutputLocker {
 protected:
  std::list<uint64_t> pending_output_locker_;

 public:
  class AutoUnlock {
    std::list<uint64_t>::iterator it_;
    PendingOutputLocker* lock_;

   public:
    AutoUnlock(std::list<uint64_t>::iterator _it, PendingOutputLocker* _lock)
        : it_(_it), lock_(_lock) {}

    AutoUnlock(AutoUnlock&& o) noexcept {
      it_ = o.it_;
      lock_ = o.lock_;
      o.lock_ = nullptr;
    }
    AutoUnlock& operator=(AutoUnlock&& o) noexcept {
      if (this != &o) {
        it_ = o.it_;
        lock_ = o.lock_;
        o.lock_ = nullptr;
      }
      return *this;
    }
    AutoUnlock(const AutoUnlock&) = delete;
    AutoUnlock& operator=(const AutoUnlock&) = delete;

    void Unlock() {
      if (lock_ != nullptr) {
        lock_->pending_output_locker_.erase(it_);
        lock_ = nullptr;
      }
    }

    ~AutoUnlock() { Unlock(); }
  };

  AutoUnlock Lock(uint64_t file_number) {
    // We need to remember the iterator of our insert, because after the
    // background job is done, we need to remove that element from
    // pending_outputs_.
    assert(pending_output_locker_.empty() ||
           file_number >= pending_output_locker_.back());
    pending_output_locker_.push_back(file_number);
    return AutoUnlock{--pending_output_locker_.end(), this};
  }

  uint64_t MinLockFileNumber() {
    return pending_output_locker_.empty() ? port::kMaxUint64
                                          : *pending_output_locker_.begin();
  }
};

}  // namespace TERARKDB_NAMESPACE
