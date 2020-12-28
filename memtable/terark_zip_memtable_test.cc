// Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License.

#include "terark_zip_memtable.h"

#include <inttypes.h>

#include <atomic>
#include <chrono>
#include <string>
#include <memory>

#include "db/dbformat.h"
#include "gtest/gtest.h"

namespace rocksdb {

class TerarkZipMemtableTest : public testing::Test {};

TEST_F(TerarkZipMemtableTest, SimpleTest) {
  std::shared_ptr<MemTable> mem_;
  Options options;
  options.memtable_factory =
      std::shared_ptr<MemTableRepFactory>(NewPatriciaTrieRepFactory());

  InternalKeyComparator cmp(BytewiseComparator());
  ImmutableCFOptions ioptions(options);
  WriteBufferManager wb(options.db_write_buffer_size);

  mem_ = std::shared_ptr<MemTable>(
      new MemTable(cmp, ioptions, MutableCFOptions(options),
                   /* needs_dup_key_check */ true, &wb, kMaxSequenceNumber,
                   0 /* column_family_id */));

  // Run some basic tests
  SequenceNumber seq = 123;
  bool res;
  res = mem_->Add(seq, kTypeValue, "key", "value2");
  ASSERT_TRUE(res);
  res = mem_->Add(seq, kTypeValue, "key", "value2");
  ASSERT_FALSE(res);
  // Changing the type should still cause the duplicatae key
  res = mem_->Add(seq, kTypeMerge, "key", "value2");
  ASSERT_FALSE(res);
  // Changing the seq number will make the key fresh
  res = mem_->Add(seq + 1, kTypeMerge, "key", "value2");
  ASSERT_TRUE(res);
  // Test with different types for duplicate keys
  res = mem_->Add(seq, kTypeDeletion, "key", "");
  ASSERT_FALSE(res);
  res = mem_->Add(seq, kTypeSingleDeletion, "key", "");
  ASSERT_FALSE(res);
}

// Test multi-threading insertion
TEST_F(TerarkZipMemtableTest, MultiThreadingTest) {
  MemTable* mem_;
  Options options;
  options.memtable_factory =
      std::shared_ptr<MemTableRepFactory>(NewPatriciaTrieRepFactory());

  InternalKeyComparator cmp(BytewiseComparator());
  ImmutableCFOptions ioptions(options);
  WriteBufferManager wb(options.db_write_buffer_size);

  mem_ = new MemTable(cmp, ioptions, MutableCFOptions(options),
                      /* needs_dup_key_check */ true, &wb, kMaxSequenceNumber,
                      0 /* column_family_id */);

  size_t records = 1 << 20;
  int thread_cnt = 10;

  SequenceNumber seq = 0;

  // Single Thread CSPPTrie
  auto start = std::chrono::system_clock::now();
  for (size_t i = 0; i < records; ++i) {
    std::string key("key " + std::to_string(i));
    std::string value("value " + std::to_string(i));
    auto ret = mem_->Add(seq, kTypeValue, key, value);
    ASSERT_TRUE(ret);
    seq++;
  }
  auto end = std::chrono::system_clock::now();
  auto dur =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  printf("[CSPPTrie] Single-Thread Time Cost: %" PRId64 ", mem_->size = %" PRId64 "\n",
         dur, mem_->num_entries());

  delete mem_;

  // Single thread SkipList
  options.memtable_factory =
      std::shared_ptr<MemTableRepFactory>(new SkipListFactory());
  ImmutableCFOptions ioptions2(options);
  WriteBufferManager wb2(options.db_write_buffer_size);
  mem_ = new MemTable(cmp, ioptions2, MutableCFOptions(options),true, &wb2, kMaxSequenceNumber, 0);
  start = std::chrono::system_clock::now();
  seq = 0;
  for (size_t i = 0; i < records; ++i) {
    std::string key("key " + std::to_string(i));
    std::string value("value " + std::to_string(i));
    auto ret = mem_->Add(seq, kTypeValue, key, value);
    ASSERT_TRUE(ret);
    seq++;
  }
  end = std::chrono::system_clock::now();
  dur = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  printf("[SkipList] Single-Thread Time Cost: %" PRId64 ", mem_->size = %" PRId64 "\n",
         dur, mem_->num_entries());

  delete mem_;

  // Multi Thread CSPPTrie
  options.allow_concurrent_memtable_write = true;
  options.memtable_factory =
      std::shared_ptr<MemTableRepFactory>(NewPatriciaTrieRepFactory());
  ImmutableCFOptions ioptions3(options);
  WriteBufferManager wb3(options.db_write_buffer_size);
  mem_ = new MemTable(cmp, ioptions3, MutableCFOptions(options), true, &wb3, kMaxSequenceNumber, 0);

  std::vector<std::thread> threads;
  // Each thread should has its own post_process_info 
  std::vector<MemTablePostProcessInfo> infos(thread_cnt); 
  std::atomic<SequenceNumber> atomic_seq{0};
  start = std::chrono::system_clock::now();

  for (int t = 0; t < thread_cnt; ++t) {
    threads.emplace_back(std::thread([&, t]() {
      int start = (records / thread_cnt) * t;
      int end = (records / thread_cnt) * (t + 1);
      for (size_t i = start; i < end; ++i) {
        std::string key("key " + std::to_string(i));
        std::string value("value " + std::to_string(i));
        auto ret = mem_->Add(atomic_seq++, kTypeValue, key, value, true, &infos[t]);
        ASSERT_TRUE(ret);
      }
    }));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  end = std::chrono::system_clock::now();
  dur = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  uint64_t total_size = 0;
  for(auto info : infos) {
    total_size += info.num_entries;
  }

  printf("[CSPPTrie] Multi-Thread Time Cost: %" PRId64 ", mem_->size = %" PRId64 "\n", dur, total_size);

  delete mem_;

  // Multi Thread SkipList
  options.allow_concurrent_memtable_write = true;
  options.memtable_factory =
      std::shared_ptr<MemTableRepFactory>(new SkipListFactory());
  ImmutableCFOptions ioptions4(options);
  WriteBufferManager wb4(options.db_write_buffer_size);

  mem_ = new MemTable(cmp, ioptions4, MutableCFOptions(options), true, &wb4, kMaxSequenceNumber, 0);
  threads.clear();
  infos.clear();
  infos.resize(thread_cnt);
  atomic_seq = {0};
  start = std::chrono::system_clock::now();
  for (int t = 0; t < thread_cnt; ++t) {
    threads.emplace_back(std::thread([&, t]() {
      int start = (records / thread_cnt) * t;
      int end = (records / thread_cnt) * (t + 1);
      for (size_t i = start; i < end; ++i) {
        std::string key("key " + std::to_string(i));
        std::string value("value " + std::to_string(i));
        auto ret = mem_->Add(atomic_seq++, kTypeValue, key, value, true, &infos[t]);
        ASSERT_TRUE(ret);
      }
    }));
  }

  for (auto& thread : threads) {
    thread.join();
  }
  end = std::chrono::system_clock::now();
  dur = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  total_size = 0;
  for(auto info : infos) {
    total_size += info.num_entries;
  }
  printf("[SkipList] Multi-Thread Time Cost: %" PRId64 ", mem_->size = %" PRId64 "\n", dur, total_size);
  delete mem_;
}
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
