//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <type_traits>

#include "rocksdb/env.h"
#include "rocksdb/statistics.h"
#include "rocksdb/thread_status.h"
#include "util/stop_watch.h"

namespace boost {
namespace fibers {
class mutex;
}  // namespace fibers
}  // namespace boost

namespace rocksdb {
class InstrumentedCondVar;

static const unsigned int kBoostFiberMutexSize = 32;
static const unsigned int kBoostFiberCondVarSize = 24;
static const unsigned int kBoostFiberIDSize = 8;

// A wrapper class for port::Mutex that provides additional layer
// for collecting stats and instrumentation.
class InstrumentedMutex {
 public:
  explicit InstrumentedMutex(bool adaptive = false);

  InstrumentedMutex(Statistics* stats, Env* env, int stats_code,
                    bool adaptive = false);

  ~InstrumentedMutex();

  void Lock();

  void Unlock();

  void AssertHeld();

 private:
  void LockInternal();
  friend class InstrumentedCondVar;
  typename std::aligned_storage<kBoostFiberMutexSize,
                                alignof(std::max_align_t)>::type mutex_;
  Statistics* stats_;
  Env* env_;
  int stats_code_;
#ifndef NDEBUG
  typename std::aligned_storage<kBoostFiberIDSize,
                                alignof(std::max_align_t)>::type owner_id_{};
#endif
};

// A wrapper class for port::Mutex that provides additional layer
// for collecting stats and instrumentation.
class InstrumentedMutexLock {
 public:
  explicit InstrumentedMutexLock(InstrumentedMutex* mutex) : mutex_(mutex) {
    mutex_->Lock();
  }

  ~InstrumentedMutexLock() { mutex_->Unlock(); }

 private:
  InstrumentedMutex* const mutex_;
  InstrumentedMutexLock(const InstrumentedMutexLock&) = delete;
  void operator=(const InstrumentedMutexLock&) = delete;
};

class InstrumentedCondVar {
 public:
  explicit InstrumentedCondVar(InstrumentedMutex* instrumented_mutex);

  ~InstrumentedCondVar();

  void Wait();

  bool TimedWait(uint64_t abs_time_us);

  void Signal();

  void SignalAll();

 private:
  void WaitInternal();
  bool TimedWaitInternal(uint64_t abs_time_us);
#ifndef NDEBUG
  InstrumentedMutex* instrumented_mutex_;
#endif
  typename std::aligned_storage<kBoostFiberCondVarSize,
                                alignof(std::max_align_t)>::type cond_;
  boost::fibers::mutex* mutex_;
  Statistics* stats_;
  Env* env_;
  int stats_code_;
};

}  // namespace rocksdb
