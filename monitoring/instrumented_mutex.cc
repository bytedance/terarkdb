//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "monitoring/instrumented_mutex.h"

#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "rocksdb/terark_namespace.h"
#include "util/sync_point.h"
#include "util/util.h"

#if MUTEX_DEBUG_MILLISECONDS
#include <boost/stacktrace.hpp>
#endif

namespace TERARKDB_NAMESPACE {
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

void InstrumentedMutex::Lock() {
#if MUTEX_DEBUG_MILLISECONDS
  auto start = std::chrono::high_resolution_clock::now();
  auto stacktrace = new boost::stacktrace::stacktrace();
#endif
  PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(
      db_mutex_lock_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
      stats_for_report(env_, stats_), stats_code_);
  LockInternal();
#if MUTEX_DEBUG_MILLISECONDS
  assert(start_stacktrace_ == nullptr);
  start_stacktrace_ = stacktrace;
  wait_start_ = start;
  lock_start_ = std::chrono::high_resolution_clock::now();
#endif
}

void InstrumentedMutex::Unlock() {
#if MUTEX_DEBUG_MILLISECONDS
  int64_t lock_dur =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::high_resolution_clock::now() - lock_start_)
          .count();
  assert(start_stacktrace_ != nullptr);
  auto stacktrace =
      static_cast<boost::stacktrace::stacktrace*>(start_stacktrace_);
  start_stacktrace_ = nullptr;
#endif
  AssertHeld();
  mutex_.Unlock();
#if MUTEX_DEBUG_MILLISECONDS
  if (lock_dur > MUTEX_DEBUG_MILLISECONDS * 1000) {
    int64_t wait_dur = std::chrono::duration_cast<std::chrono::microseconds>(
                           lock_start_ - wait_start_)
                           .count();
    fprintf(
        stderr,
        "InstrumentedMutex Trace: Lock = %" PRIi64 "us Wait = %" PRIi64
        "us\n--------\n%s--------\n%s--------\n",
        lock_dur, wait_dur, boost::stacktrace::to_string(*stacktrace).c_str(),
        boost::stacktrace::to_string(boost::stacktrace::stacktrace()).c_str());
  }
  delete stacktrace;
#endif
}

void InstrumentedMutex::LockInternal() {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif
  mutex_.Lock();
}

void InstrumentedCondVar::Wait() {
  PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(
      db_condition_wait_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
      stats_for_report(env_, stats_), stats_code_);
  WaitInternal();
}

void InstrumentedCondVar::WaitInternal() {
#if MUTEX_DEBUG_MILLISECONDS
  int64_t lock_dur = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::high_resolution_clock::now() -
                         instrumented_mutex_->lock_start_)
                         .count();
  assert(instrumented_mutex_->start_stacktrace_ != nullptr);
  auto stacktrace = static_cast<boost::stacktrace::stacktrace*>(
      instrumented_mutex_->start_stacktrace_);
  instrumented_mutex_->start_stacktrace_ = nullptr;
  if (lock_dur > MUTEX_DEBUG_MILLISECONDS * 1000) {
    int64_t wait_dur =
        std::chrono::duration_cast<std::chrono::microseconds>(
            instrumented_mutex_->lock_start_ - instrumented_mutex_->wait_start_)
            .count();
    fprintf(
        stderr,
        "InstrumentedMutex Trace: Lock = %" PRIi64 "us Wait = %" PRIi64
        "us\n--------\n%s--------\n%s--------\n",
        lock_dur, wait_dur, boost::stacktrace::to_string(*stacktrace).c_str(),
        boost::stacktrace::to_string(boost::stacktrace::stacktrace()).c_str());
  }
  call_destructor(stacktrace);
#endif
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif
  cond_.Wait();
#if MUTEX_DEBUG_MILLISECONDS
  auto start = std::chrono::high_resolution_clock::now();
  call_constructor(stacktrace);
  assert(instrumented_mutex_->start_stacktrace_ == nullptr);
  instrumented_mutex_->start_stacktrace_ = stacktrace;
  instrumented_mutex_->wait_start_ = start;
  instrumented_mutex_->lock_start_ = std::chrono::high_resolution_clock::now();
#endif
}

bool InstrumentedCondVar::TimedWait(uint64_t abs_time_us) {
  PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(
      db_condition_wait_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
      stats_for_report(env_, stats_), stats_code_);
  return TimedWaitInternal(abs_time_us);
}

bool InstrumentedCondVar::TimedWaitInternal(uint64_t abs_time_us) {
#if MUTEX_DEBUG_MILLISECONDS
  int64_t lock_dur = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::high_resolution_clock::now() -
                         instrumented_mutex_->lock_start_)
                         .count();
  assert(instrumented_mutex_->start_stacktrace_ != nullptr);
  auto stacktrace = static_cast<boost::stacktrace::stacktrace*>(
      instrumented_mutex_->start_stacktrace_);
  instrumented_mutex_->start_stacktrace_ = nullptr;
  if (lock_dur > MUTEX_DEBUG_MILLISECONDS * 1000) {
    int64_t wait_dur =
        std::chrono::duration_cast<std::chrono::microseconds>(
            instrumented_mutex_->lock_start_ - instrumented_mutex_->wait_start_)
            .count();
    fprintf(
        stderr,
        "InstrumentedMutex Trace: Lock = %" PRIi64 "us Wait = %" PRIi64
        "us\n--------\n%s--------\n%s--------\n",
        lock_dur, wait_dur, boost::stacktrace::to_string(*stacktrace).c_str(),
        boost::stacktrace::to_string(boost::stacktrace::stacktrace()).c_str());
  }
  call_destructor(stacktrace);
#endif
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif
  TEST_SYNC_POINT_CALLBACK("InstrumentedCondVar::TimedWaitInternal",
                           &abs_time_us);

  bool r = cond_.TimedWait(abs_time_us);
#if MUTEX_DEBUG_MILLISECONDS
  auto start = std::chrono::high_resolution_clock::now();
  call_constructor(stacktrace);
  assert(instrumented_mutex_->start_stacktrace_ == nullptr);
  instrumented_mutex_->start_stacktrace_ = stacktrace;
  instrumented_mutex_->wait_start_ = start;
  instrumented_mutex_->lock_start_ = std::chrono::high_resolution_clock::now();
#endif
  return r;
}

}  // namespace TERARKDB_NAMESPACE
