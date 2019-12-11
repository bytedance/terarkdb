#pragma once

#include <cassert>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <map>
#include <memory>

#include <db/version_set.h>
#include <options/cf_options.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <table/block_based_table_builder.h>
#include <table/format.h>
#include <table/get_context.h>
#include <table/meta_blocks.h>
#include <table/plain_table_builder.h>
#include <table/table_builder.h>
#include <table/terark_zip_internal.h>
#include <table/terark_zip_table.h>

using namespace rocksdb;

namespace terark {

class TraceTest {
 private:
 public:
  TraceTest() = default;

  ~TraceTest() = default;

 public:
  void traceSeek();

  const std::string generate_text(size_t bytes);
};
}  // namespace terark
