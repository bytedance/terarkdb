#pragma once
#ifndef CHEAPIS_EXECUTOR_H
#define CHEAPIS_EXECUTOR_H

#include <memory>
#include <string>

#include "server.h"
#include "string_view.hpp"
#include "util/autovector.h"

namespace cheapis {
class Executor {
 public:
  Executor() = default;

  virtual ~Executor() = default;

 public:
  virtual void Submit(const rocksdb::autovector<nonstd::string_view>& argv,
                      Client* c, int fd) = 0;

  virtual void Execute(size_t n, long curr_time, EventLoop<Client>* el) = 0;

  virtual size_t GetTaskCount() const = 0;
};

std::unique_ptr<Executor> OpenExecutorMem();
}  // namespace cheapis

#endif  // CHEAPIS_EXECUTOR_H