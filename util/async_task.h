//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#endif
#ifdef WITH_BOOSTLIB
#include <boost/fiber/future.hpp>
#endif
#ifdef __clang__
#pragma clang diagnostic pop
#endif
#include <functional>

#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

template <typename T>
class AsyncTask {
 private:
#ifdef WITH_BOOSTLIB
  boost::fibers::promise<T> promise;
  boost::fibers::future<T> future;
#else
  std::promise<T> promise;
  std::future<T> future;
#endif
  std::function<T()> func;

 public:
  bool valid() const { return future.valid(); }
  void wait() { future.wait(); }
  T get() { return future.get(); }

  void operator()() {
    // DO NOT call `promise.set_value(std::move(func()));` directly
    auto safe_promise = std::move(promise);
    safe_promise.set_value(std::move(func()));
  }

  AsyncTask(std::function<T()>&& f)
      : promise(), future(promise.get_future()), func(std::move(f)) {}
};

}  // namespace TERARKDB_NAMESPACE