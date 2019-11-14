//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#include <table/terark_zip_weak_function.h>

#include "db/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "table/meta_blocks.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

class TerarkZipTableDBTest : public testing::Test {
 private:
  std::string dbname_;
  Env* env_;
  DB* db_;

 public:
  TerarkZipTableDBTest() : env_(Env::Default()) {
    dbname_ = test::TmpDir() + "/terark_zip_table_db_test";
    EXPECT_OK(DestroyDB(dbname_, Options()));
    db_ = nullptr;
    Reopen();
  }

  ~TerarkZipTableDBTest() {
    delete db_;
    EXPECT_OK(DestroyDB(dbname_, Options()));
  }

  Options CurrentOptions() {
    TerarkZipTableOptions opt;
    Options options;
    std::shared_ptr<TableFactory> block_based_factory(NewBlockBasedTableFactory());
    options.table_factory.reset(NewTerarkZipTableFactory(opt, NewAdaptiveTableFactory(block_based_factory)));
    // options.table_factory.reset(NewTerarkZipTableFactory(opt, nullptr));
    options.allow_mmap_reads = true;
    options.create_if_missing = true;
    return options;
  }

  DBImpl* dbfull() {
    return reinterpret_cast<DBImpl*>(db_);
  }

  // The following util methods are copied from plain_table_db_test.
  void Reopen(Options* options = nullptr) {
    delete db_;
    db_ = nullptr;
    Options opts;
    if (options != nullptr) {
      opts = *options;
    } else {
      opts = CurrentOptions();
      opts.create_if_missing = true;
    }
    ASSERT_OK(DB::Open(opts, dbname_, &db_));
  }

  void Destroy(Options* options) {
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, *options));
  }

  Status Put(const Slice& k, const Slice& v) {
    return db_->Put(WriteOptions(), k, v);
  }

  Status Delete(const std::string& k) {
    return db_->Delete(WriteOptions(), k);
  }

  std::string Get(const std::string& k) {
    ReadOptions options;
    std::string result;
    Status s = db_->Get(options, k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  int NumTableFilesAtLevel(int level) {
    std::string property;
    EXPECT_TRUE(db_->GetProperty(
        "rocksdb.num-files-at-level" + NumberToString(level), &property));
    return atoi(property.c_str());
  }

  // Return spread of files per level
  std::string FilesPerLevel() {
    std::string result;
    size_t last_non_zero_offset = 0;
    for (int level = 0; level < db_->NumberLevels(); level++) {
      int f = NumTableFilesAtLevel(level);
      char buf[100];
      snprintf(buf, sizeof(buf), "%s%d", (level ? "," : ""), f);
      result += buf;
      if (f > 0) {
        last_non_zero_offset = result.size();
      }
    }
    result.resize(last_non_zero_offset);
    return result;
  }
};

TEST_F(TerarkZipTableDBTest, Flush) {
  // Try with empty DB first.
  ASSERT_TRUE(dbfull() != nullptr);
  ASSERT_EQ("NOT_FOUND", Get("key2"));

  // Add some values to db.
  Options options = CurrentOptions();
  Reopen(&options);

  ASSERT_OK(Put("key1", "v1"));
  ASSERT_OK(Put("key2", "v2"));
  ASSERT_OK(Put("key3", "v3"));
  dbfull()->TEST_FlushMemTable();

  TablePropertiesCollection ptc;
  reinterpret_cast<DB*>(dbfull())->GetPropertiesOfAllTables(&ptc);
  ASSERT_EQ(1U, ptc.size());
  ASSERT_EQ(3U, ptc.begin()->second->num_entries);
  ASSERT_EQ("1", FilesPerLevel());
 
  ASSERT_EQ("v1", Get("key1"));
  ASSERT_EQ("v2", Get("key2"));
  ASSERT_EQ("v3", Get("key3"));
  ASSERT_EQ("NOT_FOUND", Get("key4"));

  // Now add more keys and flush.
  ASSERT_OK(Put("key4", "v4"));
  ASSERT_OK(Put("key5", "v5"));
  ASSERT_OK(Put("key6", "v6"));
  dbfull()->TEST_FlushMemTable();
  reinterpret_cast<DB*>(dbfull())->GetPropertiesOfAllTables(&ptc);
  ASSERT_EQ(2U, ptc.size());
  auto row = ptc.begin();
  ASSERT_EQ(3U, row->second->num_entries);
  ASSERT_EQ(3U, (++row)->second->num_entries);
  ASSERT_EQ("2", FilesPerLevel());
  ASSERT_EQ("v1", Get("key1"));
  ASSERT_EQ("v2", Get("key2"));
  ASSERT_EQ("v3", Get("key3"));
  ASSERT_EQ("v4", Get("key4"));
  ASSERT_EQ("v5", Get("key5"));
  ASSERT_EQ("v6", Get("key6"));
  ASSERT_OK(Delete("key6"));
  ASSERT_OK(Delete("key5"));
  ASSERT_OK(Delete("key4"));
  dbfull()->TEST_FlushMemTable();
  reinterpret_cast<DB*>(dbfull())->GetPropertiesOfAllTables(&ptc);
  ASSERT_EQ(3U, ptc.size());
  row = ptc.begin();
  ASSERT_EQ(3U, row->second->num_entries);
  ASSERT_EQ(3U, (++row)->second->num_entries);
  ASSERT_EQ(3U, (++row)->second->num_entries);
  ASSERT_EQ("3", FilesPerLevel());
  ASSERT_EQ("v1", Get("key1"));
  ASSERT_EQ("v2", Get("key2"));
  ASSERT_EQ("v3", Get("key3"));
  ASSERT_EQ("NOT_FOUND", Get("key4"));
  ASSERT_EQ("NOT_FOUND", Get("key5"));
  ASSERT_EQ("NOT_FOUND", Get("key6"));
}

/*
TEST_F(TerarkZipTableDBTest, Iteratorseekfirstandlast) {
  Options options = CurrentOptions();
  Reopen(&options);

  ASSERT_OK(Put("1000000000foo002", "v_2"));
  ASSERT_OK(Put("0000000000000bar", "random"));
  ASSERT_OK(Put("1000000000foo001", "v1"));
  ASSERT_OK(Put("3000000000000bar", "bar_v"));
  ASSERT_OK(Put("1000000000foo003", "v__3"));
  ASSERT_OK(Put("1000000000foo004", "v__4"));
  ASSERT_OK(Put("1000000000foo005", "v__5"));
  ASSERT_OK(Put("1000000000foo007", "v__7"));
  ASSERT_OK(Put("1000000000foo008", "v__8"));
  dbfull()->TEST_FlushMemTable();

  ASSERT_EQ("v1", Get("1000000000foo001"));
  ASSERT_EQ("v__3", Get("1000000000foo003"));
  std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ReadOptions()));

  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("0000000000000bar", iter->key().ToString());
  ASSERT_EQ("random", iter->value().ToString());
  
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("3000000000000bar", iter->key().ToString());
  ASSERT_EQ("bar_v", iter->value().ToString());
}
*/

TEST_F(TerarkZipTableDBTest, Iteratorforward) {
  Options options = CurrentOptions();
  Reopen(&options);

  ASSERT_OK(Put("1000000000foo002", "v_2"));
  ASSERT_OK(Put("0000000000000bar", "random"));
  ASSERT_OK(Put("1000000000foo001", "v1"));
  ASSERT_OK(Put("3000000000000bar", "bar_v"));
  ASSERT_OK(Put("1000000000foo003", "v__3"));
  ASSERT_OK(Put("1000000000foo004", "v__4"));
  ASSERT_OK(Put("1000000000foo005", "v__5"));
  ASSERT_OK(Put("1000000000foo007", "v__7"));
  ASSERT_OK(Put("1000000000foo008", "v__8"));
  dbfull()->TEST_FlushMemTable();

  ASSERT_EQ("v1", Get("1000000000foo001"));
  ASSERT_EQ("v__3", Get("1000000000foo003"));
  std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ReadOptions()));

  iter->Seek("1000000000foo000");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo001", iter->key().ToString());
  ASSERT_EQ("v1", iter->value().ToString());

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo002", iter->key().ToString());
  ASSERT_EQ("v_2", iter->value().ToString());

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo003", iter->key().ToString());
  ASSERT_EQ("v__3", iter->value().ToString());

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo004", iter->key().ToString());
  ASSERT_EQ("v__4", iter->value().ToString());

  iter->Seek("3000000000000bar");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("3000000000000bar", iter->key().ToString());
  ASSERT_EQ("bar_v", iter->value().ToString());

  iter->Seek("1000000000foo000");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo001", iter->key().ToString());
  ASSERT_EQ("v1", iter->value().ToString());

  iter->Seek("1000000000foo005");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo005", iter->key().ToString());
  ASSERT_EQ("v__5", iter->value().ToString());

  iter->Seek("1000000000foo006");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo007", iter->key().ToString());
  ASSERT_EQ("v__7", iter->value().ToString());

  iter->Seek("1000000000foo008");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo008", iter->key().ToString());
  ASSERT_EQ("v__8", iter->value().ToString());
}

TEST_F(TerarkZipTableDBTest, Iteratorprev) {
  Options options = CurrentOptions();
  Reopen(&options);

  ASSERT_OK(Put("1000000000foo002", "v_2"));
  ASSERT_OK(Put("0000000000000bar", "random"));
  ASSERT_OK(Put("1000000000foo001", "v1"));
  ASSERT_OK(Put("3000000000000bar", "bar_v"));
  ASSERT_OK(Put("1000000000foo003", "v__3"));
  ASSERT_OK(Put("1000000000foo004", "v__4"));
  ASSERT_OK(Put("1000000000foo005", "v__5"));
  ASSERT_OK(Put("1000000000foo007", "v__7"));
  ASSERT_OK(Put("1000000000foo008", "v__8"));
  dbfull()->TEST_FlushMemTable();

  ASSERT_EQ("v1", Get("1000000000foo001"));
  ASSERT_EQ("v__3", Get("1000000000foo003"));

  std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ReadOptions()));
  iter->Seek("1000000000foo008");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo008", iter->key().ToString());
  ASSERT_EQ("v__8", iter->value().ToString());

  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo007", iter->key().ToString());
  ASSERT_EQ("v__7", iter->value().ToString());

  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo005", iter->key().ToString());
  ASSERT_EQ("v__5", iter->value().ToString());

  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo004", iter->key().ToString());
  ASSERT_EQ("v__4", iter->value().ToString());

  iter->Seek("3000000000000bar");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("3000000000000bar", iter->key().ToString());
  ASSERT_EQ("bar_v", iter->value().ToString());

  iter->Seek("1000000000foo000");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo001", iter->key().ToString());
  ASSERT_EQ("v1", iter->value().ToString());

  iter->Seek("1000000000foo005");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo005", iter->key().ToString());
  ASSERT_EQ("v__5", iter->value().ToString());

  iter->Seek("1000000000foo006");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo007", iter->key().ToString());
  ASSERT_EQ("v__7", iter->value().ToString());

  iter->Seek("1000000000foo008");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo008", iter->key().ToString());
  ASSERT_EQ("v__8", iter->value().ToString());
}


TEST_F(TerarkZipTableDBTest, FlushWithDuplicateKeys) {
  Options options = CurrentOptions();
  Reopen(&options);
  ASSERT_OK(Put("key1", "v1"));
  ASSERT_OK(Put("key2", "v2"));
  ASSERT_OK(Put("key1", "v3"));  // Duplicate
  dbfull()->TEST_FlushMemTable();

  TablePropertiesCollection ptc;
  reinterpret_cast<DB*>(dbfull())->GetPropertiesOfAllTables(&ptc);
  ASSERT_EQ(1U, ptc.size());
  ASSERT_EQ(2U, ptc.begin()->second->num_entries);
  ASSERT_EQ("1", FilesPerLevel());
  ASSERT_EQ("v3", Get("key1"));
  ASSERT_EQ("v2", Get("key2"));
}

namespace {
static std::string Key(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "key_______%06d", i);
  return std::string(buf);
}

static std::string RandomString(Random* rnd, int len) {
  std::string r;
  test::RandomString(rnd, len, &r);
  return r;
}
}  // namespace.

TEST_F(TerarkZipTableDBTest, CompactionTrigger) {
  Options options = CurrentOptions();
  options.write_buffer_size = 120 << 10;  // 100KB
  options.num_levels = 3;
  options.level0_file_num_compaction_trigger = 3;
  Reopen(&options);

  Random rnd(301);

  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
      num++) {
    std::vector<std::string> values;
    // Write 120KB (10 values, each 12K)
    for (int i = 0; i < 10; i++) {
      values.push_back(RandomString(&rnd, 12000));
      ASSERT_OK(Put(Key(i), values[i]));
    }
    ASSERT_OK(Put(Key(999), ""));
    dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(NumTableFilesAtLevel(0), num + 1);
  }

  //generate one more file in level-0, and should trigger level-0 compaction
  std::vector<std::string> values;
  for (int i = 0; i < 12; i++) {
    values.push_back(RandomString(&rnd, 10000));
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Put(Key(999), ""));
  dbfull()->TEST_WaitForCompact();

  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), 1);

}

TEST_F(TerarkZipTableDBTest, CompactRange) {
  // Create a big L0 file and check it compacts into multiple files in L1.
  Options options = CurrentOptions();
  options.write_buffer_size = 270 << 10;
  // Two SST files should be created, each containing 14 keys.
  // Number of buckets will be 16. Total size ~156 KB.
  options.target_file_size_base = 160 << 10;
  Reopen(&options);

  // Write 28 values, each 10016 B ~ 10KB
  for (int idx = 0; idx < 28; ++idx) {
    ASSERT_OK(Put(Key(idx), std::string(10000, 'a' + idx)));
  }
  dbfull()->TEST_WaitForFlushMemTable();
  ASSERT_EQ("1", FilesPerLevel());

  for (int idx = 28; idx < 40; ++idx) {
    ASSERT_OK(Put(Key(idx), std::string(10000, 'a' + idx)));
  }

  dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                              true /* disallow trivial move */);

  ASSERT_EQ("0,1", FilesPerLevel());

  for (int idx = 0; idx < 40; ++idx) {
    ASSERT_EQ(std::string(10000, 'a' + idx), Get(Key(idx)));
  }
}

TEST_F(TerarkZipTableDBTest, SameKeyInsertedInTwoDifferentFilesAndCompacted) {
  // Insert same key twice so that they go to different SST files. Then wait for
  // compaction and check if the latest value is stored and old value removed.
  Options options = CurrentOptions();
  options.write_buffer_size = 100 << 10;  // 100KB
  options.level0_file_num_compaction_trigger = 2;
  Reopen(&options);

  // Write 11 values, each 10016 B
  for (int idx = 0; idx < 11; ++idx) {
    ASSERT_OK(Put(Key(idx), std::string(10000, 'a')));
  }
  dbfull()->TEST_WaitForFlushMemTable();
  ASSERT_EQ("1", FilesPerLevel());

  // Generate one more file in level-0, and should trigger level-0 compaction
  for (int idx = 0; idx < 11; ++idx) {
    ASSERT_OK(Put(Key(idx), std::string(10000, 'a' + idx)));
  }
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_CompactRange(0, nullptr, nullptr);

  ASSERT_EQ("0,1", FilesPerLevel());
  for (int idx = 0; idx < 11; ++idx) {
    ASSERT_EQ(std::string(10000, 'a' + idx), Get(Key(idx)));
  }
}

TEST_F(TerarkZipTableDBTest, AdaptiveTable) { // there is some wrong with adaptive table factory
  Options options = CurrentOptions();
  
  // Write some keys using terarkzip table.
  TerarkZipTableOptions opt;
  std::shared_ptr<TableFactory> block_based_factory(NewBlockBasedTableFactory());
  options.table_factory.reset(NewTerarkZipTableFactory(opt, NewAdaptiveTableFactory(block_based_factory)));
  // options.table_factory.reset(NewTerarkZipTableFactory(opt, nullptr));

  Reopen(&options);

  ASSERT_OK(Put("ley1", "l1"));
  ASSERT_OK(Put("ley2", "l2"));
  ASSERT_OK(Put("ley3", "l3"));
  // sstable key range <ley1, ley3>
  dbfull()->TEST_FlushMemTable();
  ASSERT_EQ("l1", Get("ley1"));
  ASSERT_EQ("l2", Get("ley2"));
  ASSERT_EQ("l3", Get("ley3"));
  
  // Write some keys using cuckoo table.
  options.table_factory.reset(NewCuckooTableFactory());
  Reopen(&options);

  ASSERT_OK(Put("key1", "v1"));
  ASSERT_OK(Put("key2", "v2"));
  ASSERT_OK(Put("ley3", "m3"));  // update terarkzip's kv item
  // sstable key range <key1, ley3>
  dbfull()->TEST_FlushMemTable();
  ASSERT_EQ("v1", Get("key1"));
  ASSERT_EQ("v2", Get("key2"));
  ASSERT_EQ("m3", Get("ley3"));

  // Write some keys using plain table.
  options.create_if_missing = false;
  options.table_factory.reset(NewPlainTableFactory());
  Reopen(&options);
  ASSERT_OK(Put("key4", "v4"));
  ASSERT_OK(Put("key5", "v5"));
  ASSERT_OK(Put("key1", "v11"));  // update cuckoo's kv item
  // sstable key range <key1, key5>
  dbfull()->TEST_FlushMemTable();
  ASSERT_EQ("v4", Get("key4"));
  ASSERT_EQ("v5", Get("key5"));
  ASSERT_EQ("v11", Get("key1"));

  // Write some keys using block based table.
  // std::shared_ptr<TableFactory> block_based_factory(
  //    NewBlockBasedTableFactory());
  options.table_factory.reset(NewTerarkZipTableFactory(opt, NewAdaptiveTableFactory(block_based_factory)));
  Reopen(&options);
  ASSERT_OK(Put("key6", "v6"));
  ASSERT_OK(Put("key7", "v7"));
  ASSERT_OK(Put("key4", "v44")); // update plain's kv item
  // sstable key range <key4, key7>
  dbfull()->TEST_FlushMemTable();
  ASSERT_EQ("v6", Get("key6"));
  ASSERT_EQ("v7", Get("key7"));
  ASSERT_EQ("v44", Get("key4"));

  // Total Test 
  ASSERT_EQ("l1", Get("ley1"));
  ASSERT_EQ("l2", Get("ley2"));
  ASSERT_EQ("m3", Get("ley3"));
  ASSERT_EQ("v11", Get("key1"));
  ASSERT_EQ("v2", Get("key2"));
  ASSERT_EQ("v44", Get("key4"));
  ASSERT_EQ("v5", Get("key5"));
}

TEST_F(TerarkZipTableDBTest, Correctness) {
  Options options = CurrentOptions();

  // Write some keys using terarkzip table.
  TerarkZipTableOptions opt;
  std::shared_ptr<TableFactory> block_based_factory(NewBlockBasedTableFactory());
  options.table_factory.reset(NewTerarkZipTableFactory(opt, NewAdaptiveTableFactory(block_based_factory)));
  // options.table_factory.reset(NewTerarkZipTableFactory(opt, nullptr));

  Destroy(&options);
  Reopen(&options);

  size_t count = 10;
  size_t len = 32;

  Random rnd(301);
  for (size_t i = 0; i < count; ++i) {
    ASSERT_OK(Put("0000" + RandomString(&rnd, len), Key(i)));
    ASSERT_OK(Put("1111" + RandomString(&rnd, len), Key(i)));
    ASSERT_OK(Put("2222" + RandomString(&rnd, len), Key(i)));
    dbfull()->Flush(FlushOptions());
  }

  std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ReadOptions()));
  iter->Seek("1111");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString().substr(0, 4), std::string("1111"));
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString().substr(0, 4), std::string("0000"));
  iter->Seek("2222");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString().substr(0, 4), std::string("2222"));
  iter->Next();
  iter->Prev();
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString().substr(0, 4), std::string("1111"));
  iter->Prev();
  iter.reset(nullptr);

  Reopen(&options);

  rnd.Reset(301);
  for (size_t i = 0; i < count; ++i) {
    ASSERT_EQ(Get("0000" + RandomString(&rnd, len)), Key(i));
    ASSERT_EQ(Get("1111" + RandomString(&rnd, len)), Key(i));
    ASSERT_EQ(Get("2222" + RandomString(&rnd, len)), Key(i));
  }
}

TEST_F(TerarkZipTableDBTest, EmptyTable) {
  Options options = CurrentOptions();

  // Write some keys using terarkzip table.
  TerarkZipTableOptions opt;
  std::shared_ptr<TableFactory> block_based_factory(NewBlockBasedTableFactory());
  options.table_factory.reset(NewTerarkZipTableFactory(opt, NewAdaptiveTableFactory(block_based_factory)));
  // options.table_factory.reset(NewTerarkZipTableFactory(opt, nullptr));

  Destroy(&options);
  Reopen(&options);

  Random rnd(301);
  size_t len = 32;

  dbfull()->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
  dbfull()->Flush(rocksdb::FlushOptions());

  dbfull()->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
  dbfull()->Flush(rocksdb::FlushOptions());
  dbfull()->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
  ASSERT_OK(Put(RandomString(&rnd, len), Key(0)));
  ASSERT_OK(Delete(RandomString(&rnd, len)));
  dbfull()->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
  ASSERT_OK(Put(RandomString(&rnd, len), Key(0)));
  ASSERT_OK(Delete(RandomString(&rnd, len)));
  dbfull()->Flush(rocksdb::FlushOptions());
  dbfull()->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr, "SKIPPED as Cuckoo table is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE