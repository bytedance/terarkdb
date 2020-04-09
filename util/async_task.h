//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <boost/fiber/future.hpp>
#include <functional>

namespace rocksdb {

  template <typename T>
  struct AsyncTask {
    boost::fibers::promise<T> promise;
    boost::fibers::future<T> future;
    std::function<T()> func;
    void operator()() {
      promise.set_value(std::move(func()));
    }

    AsyncTask(std::function<T()>&& f) : func(std::move(f)) {
      future = promise.get_future();
    }
  };
  
} // namespace rocksdb