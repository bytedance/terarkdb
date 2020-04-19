//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "monitoring/instrumented_mutex.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#endif
#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/operations.hpp>
#ifdef __clang__
#pragma clang diagnostic pop
#endif
#include <stdexcept>

#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "util/sync_point.h"

#define AS_BOOST_FIBER_MUTEX(o) (*reinterpret_cast<boost::fibers::mutex*>(&(o)))
#define AS_BOOST_FIBER_COND_VAR(o) \
  (*reinterpret_cast<boost::fibers::condition_variable*>(&(o)))
#define AS_BOOST_FIBER_ID(o) \
  (*reinterpret_cast<boost::fibers::fiber::id*>(&(o)))

namespace rocksdb {
namespace {
Statistics* stats_for_report(Env* env, Statistics* stats) {
  if (env != nullptr && stats != nullptr &&
      stats->stats_level_ > kExceptTimeForMutex) {
    return stats;
  } else {
    return nullptr;
  }
}
}  // namespace

static_assert(sizeof(boost::fibers::mutex) == kBoostFiberMutexSize, "");
static_assert(sizeof(boost::fibers::condition_variable) ==
                  kBoostFiberCondVarSize,
              "");
static_assert(sizeof(boost::fibers::fiber::id) == kBoostFiberIDSize, "");

InstrumentedMutex::InstrumentedMutex(bool adaptive)
    : mutex_(), stats_(nullptr), env_(nullptr), stats_code_(0) {
  (void)adaptive;
  new (reinterpret_cast<void*>(&mutex_)) boost::fibers::mutex();
  new (reinterpret_cast<void*>(&owner_id_)) boost::fibers::fiber::id();
}

InstrumentedMutex::InstrumentedMutex(Statistics* stats, Env* env,
                                     int stats_code, bool adaptive)
    : mutex_(), stats_(stats), env_(env), stats_code_(stats_code) {
  (void)adaptive;
  new (reinterpret_cast<void*>(&mutex_)) boost::fibers::mutex();
  new (reinterpret_cast<void*>(&owner_id_)) boost::fibers::fiber::id();
}

InstrumentedMutex::~InstrumentedMutex() {
  AS_BOOST_FIBER_MUTEX(mutex_).~mutex();
  AS_BOOST_FIBER_ID(owner_id_).~id();
}

void InstrumentedMutex::Lock() {
  PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(
      db_mutex_lock_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
      stats_for_report(env_, stats_), stats_code_);
  LockInternal();
}

void InstrumentedMutex::Unlock() {
  AssertHeld();
  AS_BOOST_FIBER_ID(owner_id_) = boost::fibers::fiber::id{};
  AS_BOOST_FIBER_MUTEX(mutex_).unlock();
}

void InstrumentedMutex::AssertHeld() {
  if (AS_BOOST_FIBER_ID(owner_id_) != boost::this_fiber::get_id()) {
    // NOT Released
    // throw std::runtime_error("MutexNotHeld");
  }
}

void InstrumentedMutex::LockInternal() {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif
  AS_BOOST_FIBER_MUTEX(mutex_).lock();
  if (AS_BOOST_FIBER_ID(owner_id_) != boost::fibers::fiber::id{}) {
    // NOT Released
    // throw std::runtime_error("MutexAlreadyHeld");
  }
  AS_BOOST_FIBER_ID(owner_id_) = boost::this_fiber::get_id();
}

InstrumentedCondVar::InstrumentedCondVar(InstrumentedMutex* instrumented_mutex)
    : instrumented_mutex_(instrumented_mutex),
      cond_(),
      mutex_(
          reinterpret_cast<boost::fibers::mutex*>(&instrumented_mutex->mutex_)),
      stats_(instrumented_mutex->stats_),
      env_(instrumented_mutex->env_),
      stats_code_(instrumented_mutex->stats_code_) {
  new (reinterpret_cast<void*>(&cond_)) boost::fibers::condition_variable();
}

InstrumentedCondVar::~InstrumentedCondVar() {
  AS_BOOST_FIBER_COND_VAR(cond_).~condition_variable();
}

void InstrumentedCondVar::Wait() {
  PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(
      db_condition_wait_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
      stats_for_report(env_, stats_), stats_code_);
  WaitInternal();
}

void InstrumentedCondVar::WaitInternal() {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif
  std::unique_lock<boost::fibers::mutex> lk(*mutex_, std::adopt_lock);
  AS_BOOST_FIBER_COND_VAR(cond_).wait(lk);
  lk.release();
  AS_BOOST_FIBER_ID(instrumented_mutex_->owner_id_) =
      boost::this_fiber::get_id();
}

bool InstrumentedCondVar::TimedWait(uint64_t abs_time_us) {
  PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(
      db_condition_wait_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
      stats_for_report(env_, stats_), stats_code_);
  return TimedWaitInternal(abs_time_us);
}

bool InstrumentedCondVar::TimedWaitInternal(uint64_t abs_time_us) {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif
  TEST_SYNC_POINT_CALLBACK("InstrumentedCondVar::TimedWaitInternal",
                           &abs_time_us);
  std::unique_lock<boost::fibers::mutex> lk(*mutex_, std::adopt_lock);
  bool r = AS_BOOST_FIBER_COND_VAR(cond_).wait_for(
               lk, std::chrono::microseconds(abs_time_us)) !=
           boost::fibers::cv_status::timeout;
  lk.release();
  if (r) {
    AS_BOOST_FIBER_ID(instrumented_mutex_->owner_id_) =
        boost::this_fiber::get_id();
  }
  return r;
}

void InstrumentedCondVar::Signal() {
  AS_BOOST_FIBER_COND_VAR(cond_).notify_one();
}

void InstrumentedCondVar::SignalAll() {
  AS_BOOST_FIBER_COND_VAR(cond_).notify_all();
}

}  // namespace rocksdb
