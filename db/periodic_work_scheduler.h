//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include "db/db_impl.h"
#include "rocksdb/terark_namespace.h"
#include "util/timer.h"

namespace TERARKDB_NAMESPACE {

// PeriodicWorkScheduler is a singleton object, which is scheduling/running
// DumpStats(), PersistStats(), and FlushInfoLog() for all DB instances. All DB
// instances use the same object from `Default()`.
//
// Internally, it uses a single threaded timer object to run the periodic work
// functions. Timer thread will always be started since the info log flushing
// cannot be disabled.
class PeriodicWorkScheduler {
 public:
  static PeriodicWorkScheduler* Default();

  PeriodicWorkScheduler() = delete;
  PeriodicWorkScheduler(const PeriodicWorkScheduler&) = delete;
  PeriodicWorkScheduler(PeriodicWorkScheduler&&) = delete;
  PeriodicWorkScheduler& operator=(const PeriodicWorkScheduler&) = delete;
  PeriodicWorkScheduler& operator=(PeriodicWorkScheduler&&) = delete;

  void Register(DBImpl* dbi, unsigned int stats_dump_period_sec,
                unsigned int stats_persist_period_sec);

  void Unregister(DBImpl* dbi);

  // Periodically flush info log out of application buffer at a low frequency.
  // This improves debuggability in case of RocksDB hanging since it ensures the
  // log messages leading up to the hang will eventually become visible in the
  // log.
  static const uint64_t kDefaultFlushInfoLogPeriodSec = 10;
  static const uint64_t kDefaultScheduleGCTTLPeriodSec = 10;
  static const uint64_t kDefaultScheduleZNSTTLPeriodSec = 1;
  static const uint64_t kDefaultScheduleZNSMetricsPeriodSec = 30;

 protected:
  std::unique_ptr<Timer> timer;
  // `timer_mu_` serves two purposes currently:
  // (1) to ensure calls to `Start()` and `Shutdown()` are serialized, as
  //     they are currently not implemented in a thread-safe way; and
  // (2) to ensure the `Timer::Add()`s and `Timer::Start()` run atomically, and
  //     the `Timer::Cancel()`s and `Timer::Shutdown()` run atomically.
  port::Mutex timer_mu_;

  explicit PeriodicWorkScheduler(Env* env);

 private:
  static std::string GetTaskName(DBImpl* dbi, const std::string& func_name);
};

#ifndef NDEBUG
// PeriodicWorkTestScheduler is for unittest, which can specify the Env like
// SafeMockTimeEnv. It also contains functions for unittest.
class PeriodicWorkTestScheduler : public PeriodicWorkScheduler {
 public:
  static PeriodicWorkTestScheduler* Default(Env* env);

  void TEST_WaitForRun(std::function<void()> callback) const;

  size_t TEST_GetValidTaskNum() const;

 private:
  explicit PeriodicWorkTestScheduler(Env* env);
};
#endif  // !NDEBUG

}  // namespace TERARKDB_NAMESPACE

#endif  // ROCKSDB_LITE
