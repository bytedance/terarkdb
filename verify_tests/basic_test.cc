#include <iostream>

#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/db.h"

using namespace rocksdb;

int main() {
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  // open DB
  Status s = DB::Open(options, "/tmp/rocksdb_test", &db);
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

  {
    LazyBuffer lazy_val;
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &lazy_val);
    assert(lazy_val.slice() == "value");
  }

  {
    std::string string_val;
    // If it cannot pin the value, it copies the value to its internal buffer.
    // The intenral buffer could be set during construction.
    LazyBuffer lazy_val(&string_val);
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &lazy_val);
    assert(lazy_val.slice() == "value");
    // If the value is not pinned, the internal buffer must have the value.
    // assert(lazy_val.IsPinned() || string_val == "value");
  }

  LazyBuffer lazy_val;
  db->Get(ReadOptions(), db->DefaultColumnFamily(), "key1", &lazy_val);
  assert(s.IsNotFound());
  lazy_val.clear();
  db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &lazy_val);
  assert(lazy_val.slice() == "value");
  lazy_val.clear();
  // The Slice pointed by lazy_val is not valid after this point

  delete db;
  printf("basic test success\n");
  return 0; 
}
