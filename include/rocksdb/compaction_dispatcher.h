// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2013 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <functional>
#include <future>
#include <string>

#include "utilities/util/terark_boost.hpp"
#include "rocksdb/env.h"

namespace rocksdb {
// using terark_boost::noncopyable;
struct CompactionWorkerContext;
struct CompactionWorkerResult;

#ifdef BOOSTLIB
class CompactionDispatcher : boost::noncopyable {
#else
class CompactionDispatcher : terark_boost::noncopyable {
#endif
 public:
  virtual ~CompactionDispatcher() = default;

  virtual std::function<CompactionWorkerResult()> StartCompaction(
      const CompactionWorkerContext& context) = 0;

  virtual const char* Name() const = 0;
};

class RemoteCompactionDispatcher : public CompactionDispatcher {
 public:
  virtual std::function<CompactionWorkerResult()> StartCompaction(
      const CompactionWorkerContext& context) override;

  virtual const char* Name() const override;

  virtual std::future<std::string> DoCompaction(std::string data) = 0;
#ifdef BOOSTLIB
  class Worker : boost::noncopyable {
#else
  class Worker : terark_boost::noncopyable {
#endif
   public:
    Worker(EnvOptions env_options, Env* env);
    virtual ~Worker();
    virtual std::string GenerateOutputFileName(size_t file_index) = 0;
    std::string DoCompaction(Slice data);
    static void DebugSerializeCheckResult(Slice data);

   protected:
    struct Rep;
    Rep* rep_;
  };
};

extern std::shared_ptr<CompactionDispatcher> NewCommandLineCompactionDispatcher(
    std::string cmd);

}  // namespace rocksdb
