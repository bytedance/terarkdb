// Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License.

#ifndef ROCKSDB_LITE

#include <algorithm>
#include <string>
#include <vector>

#include "db/db_impl.h"
#include "db/db_test_util.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/transaction_log.h"
#include "util/file_util.h"
#include "util/filename.h"
#include "util/string_util.h"

namespace rocksdb {
const std::string keys[] = {std::string("key1"), std::string("key2"),
                            std::string("key3"), std::string("key4")};
const std::string vals[] = {std::string("val1"), std::string("val2"),
                            std::string("val3"), std::string("val4")};
const std::string vals_old[] = {std::string("val01"), std::string("val02")};
class RepairTest2 : public DBTestBase {
 public:
  RepairTest2()
      : DBTestBase("./repair_test2"),
        mock_env_(new MockTimeEnv(Env::Default())),
        options_(SetRepairCurrentOptions()) {}
  Options SetRepairCurrentOptions() {
    Options options(CurrentOptions());
    options.disable_auto_compactions = true;
    options.enable_lazy_compaction = false;
    options.blob_size = 20;
    return options;
  }
  Options RepairCurrentOptions() const { return options_; }
  size_t GetSstCount() {
    if (true) {
      return GetSstFileCount(dbname_);
    }
    std::vector<std::string> filenames;
    Status status = mock_env_->GetChildren(dbname_, &filenames);
    uint64_t number;
    FileType type;
    size_t count = 0;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type == FileType::kTableFile) {
        count++;
      }
    }
    return count;
  }
  std::set<uint64_t> RestoreSstNumber() {
    std::vector<std::string> filenames;
    std::set<uint64_t> numbers;
    Status status = mock_env_->GetChildren(dbname_, &filenames);
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type == FileType::kTableFile) {
        numbers.emplace(number);
      }
    }
    return numbers;
  }

 protected:
  std::unique_ptr<rocksdb::MockTimeEnv> mock_env_;
  Options options_;
};

TEST_F(RepairTest2, NoMapSstTest) {
  // Close();
  // Reopen(RepairCurrentOptions());
  Put(keys[0], vals[0]);
  Flush();
  Put(keys[1], vals[1]);
  Flush();
  Put(keys[2], vals[2]);
  Flush();
  Put(keys[3], vals[3]);
  Flush();
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(GetSstCount(), 4);
  Close();
  ASSERT_OK(RepairDB(dbname_, RepairCurrentOptions()));
  ASSERT_EQ(GetSstCount(), 4);
}

TEST_F(RepairTest2, MapSstTest) {
  // Close();
  // Reopen(RepairCurrentOptions());
  Put(keys[0], vals_old[0]);
  Flush();
  Put(keys[1], vals_old[1]);
  Flush();
  Put(keys[2], vals[2]);
  Put(keys[0], vals[0]);
  Flush();
  Put(keys[3], vals[3]);
  Put(keys[1], vals[1]);
  Flush();
  ASSERT_EQ(GetSstCount(), 4);
  Close();
  ASSERT_OK(RepairDB(dbname_, RepairCurrentOptions()));
  ASSERT_EQ(GetSstCount(), 5);
  ASSERT_OK(RepairDB(dbname_, RepairCurrentOptions()));
  ASSERT_EQ(GetSstCount(), 5);
  Reopen(RepairCurrentOptions());
  ASSERT_EQ(Get(keys[0]), vals[0]);
  ASSERT_EQ(Get(keys[1]), vals[1]);
  ASSERT_EQ(Get(keys[2]), vals[2]);
  ASSERT_EQ(Get(keys[3]), vals[3]);
  Iterator* iterator = dbfull()->NewIterator(ReadOptions());
  iterator->SeekToFirst();
  for (size_t i = 0; i < 4; ++i) {
    ASSERT_TRUE(iterator->Valid());
    ASSERT_TRUE(keys[i] == iterator->key());
    ASSERT_TRUE(vals[i] == iterator->value());
    iterator->Next();
  }
  ASSERT_TRUE(!iterator->Valid());
  delete iterator;
}

TEST_F(RepairTest2, BlobSstTest) {
  Random r(0);
  const std::string vals_new[] = {RandomString(&r, 100), RandomString(&r, 100),
                                  vals[2], vals[3]};
  ASSERT_EQ(GetSstCount(), 0);
  Put(keys[0], vals_old[0]);
  Flush();
  Put(keys[1], vals_old[1]);
  Flush();
  Put(keys[2], vals_new[2]);
  Put(keys[0], vals_new[0]);
  Flush();
  Put(keys[3], vals_new[3]);
  Put(keys[1], vals_new[1]);
  Flush();
  ASSERT_EQ(GetSstCount(), 4);
  Close();
  Reopen(RepairCurrentOptions());
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  dbfull()->TEST_WaitForCompact();
  std::set<uint64_t> sstnumber = RestoreSstNumber();
  ASSERT_EQ(sstnumber.size(), 3);
  Close();
  ASSERT_OK(RepairDB(dbname_, RepairCurrentOptions()));
  Reopen(RepairCurrentOptions());
  sstnumber = RestoreSstNumber();
  ASSERT_EQ(sstnumber.size(), 4);
  ASSERT_EQ(Get(keys[0]), vals_new[0]);
  ASSERT_EQ(Get(keys[1]), vals_new[1]);
  ASSERT_EQ(Get(keys[2]), vals_new[2]);
  ASSERT_EQ(Get(keys[3]), vals_new[3]);
  Iterator* iterator = dbfull()->NewIterator(ReadOptions());
  iterator->SeekToFirst();
  for (size_t i = 0; i < 4; ++i) {
    ASSERT_TRUE(iterator->Valid());
    ASSERT_TRUE(keys[i] == iterator->key());
    ASSERT_TRUE(vals_new[i] == iterator->value());
    iterator->Next();
  }
  ASSERT_TRUE(!iterator->Valid());
  delete iterator;
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as RepairDB is not supported in ROCKSDB_LITE\n");
  return 0;
}
#endif