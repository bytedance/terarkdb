/**
 * A simple structure for exceptions in RedisLists.
 *
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#pragma once
#ifndef ROCKSDB_LITE
#include <exception>

#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

class RedisListException : public std::exception {
 public:
  const char* what() const throw() override {
    return "Invalid operation or corrupt data in Redis List.";
  }
};

}  // namespace TERARKDB_NAMESPACE
#endif
