// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace TERARKDB_NAMESPACE;

std::string kDBPath = "/tmp/rocksdb_simple_example";

int main() {
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // Put key-value
  s = db->Put(WriteOptions(), "key1", "value");
  assert(s.ok());
  std::string value;
  // get value
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  // atomically apply a set of updates
  {
    WriteBatch batch;
    batch.Delete("key1");
    batch.Put("key2", value);
    s = db->Write(WriteOptions(), &batch);
  }

  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.IsNotFound());

  db->Get(ReadOptions(), "key2", &value);
  assert(value == "value");

  // TerarkDB replaced the PinnableSlice family of interfaces with LazyBuffer,
  // which defers materialising the value until it is actually read.
  {
    LazyBuffer lazy_val;
    s = db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &lazy_val);
    assert(s.ok());
    // fetch() materialises the value; slice() is only valid once it succeeds.
    s = lazy_val.fetch();
    assert(s.ok());
    assert(lazy_val.slice() == "value");
  }

  {
    // A LazyBuffer can dump into a caller-owned string, which is the
    // equivalent of giving PinnableSlice an external buffer.
    std::string string_val;
    LazyBuffer lazy_val;
    s = db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &lazy_val);
    assert(s.ok());
    s = std::move(lazy_val).dump(&string_val);
    assert(s.ok());
    assert(string_val == "value");
  }

  LazyBuffer lazy_val;
  s = db->Get(ReadOptions(), db->DefaultColumnFamily(), "key1", &lazy_val);
  assert(s.IsNotFound());
  // Reset the buffer after each use and before each reuse.
  lazy_val.reset();
  s = db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &lazy_val);
  assert(s.ok());
  s = lazy_val.fetch();
  assert(s.ok());
  assert(lazy_val.slice() == "value");
  lazy_val.reset();
  // The Slice returned by lazy_val.slice() is not valid after this point.

  delete db;

  return 0;
}
