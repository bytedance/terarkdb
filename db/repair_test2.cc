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
        options_(SetRepairCurrentOptions()),
        r(0) {
    for (size_t i = 0; i < 4; ++i) {
      if (i < 2) {
        vals_new[i] = RandomString(&r, 100);
      } else {
        vals_new[i] = vals[i];
      }
    }
  }
  Options SetRepairCurrentOptions() {
    Options options(CurrentOptions());
    options.disable_auto_compactions = true;
    options.enable_lazy_compaction = false;
    return options;
  }
  void SetRepairOptionsForBlob() { options_.blob_size = 20; }
  Options RepairCurrentOptions() { return options_; }
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
  void RestoreSstNumber(std::set<uint64_t>* numbers) {
    numbers->clear();
    std::vector<std::string> filenames;
    Status status = mock_env_->GetChildren(dbname_, &filenames);
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type == FileType::kTableFile) {
        numbers->emplace(number);
      }
    }
  }
  static bool FileNameCmp(std::string& file1, std::string& file2) {
    uint64_t number1, number2;
    FileType type1, type2;
    bool ok1 = ParseFileName(file1, &number1, &type1);
    bool ok2 = ParseFileName(file2, &number2, &type2);
    if (ok2) {
      if (!ok1) {
        return true;
      } else {
        if (type2 == kTableFile) {
          if (type1 == kTableFile) {
            return number1 < number2;
          } else {
            return true;
          }
        }
      }
    }
    return false;
  }
  std::string GetNewestSstPath() {
    uint64_t manifest_size;
    std::vector<std::string> files;
    db_->GetLiveFiles(files, &manifest_size);
    auto sst_iter = std::max_element(files.begin(), files.end(), FileNameCmp);
    uint64_t number;
    FileType type;
    if (!ParseFileName(*sst_iter, &number, &type) || type != kTableFile) {
      return "";
    }
    return dbname_ + *sst_iter;
  }
  void BlobSstPrepare() {
    ReOpen();
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
  }
  void ReOpen() {
    Close();
    Reopen(RepairCurrentOptions());
  }
  // Status PutSequence(const Slice& k, const Slice& v, WriteOptions wo,
  //                    SequenceNumber seq) {
  //   return dbfull()->TEST_WriteSequence(wo, k, v, seq);
  // }

 protected:
  std::unique_ptr<rocksdb::MockTimeEnv> mock_env_;
  Options options_;
  Random r;
  std::string vals_new[4];
};

TEST_F(RepairTest2, NoMapSstTest1) {
  ReOpen();
  for (size_t i = 0; i < 4; ++i) {
    Put(keys[i], vals[i]);
    Flush();
  }
  ASSERT_EQ(GetSstCount(), 4);
  Close();
  ASSERT_OK(RepairDB(dbname_, RepairCurrentOptions()));
  ASSERT_EQ(GetSstCount(), 4);
}
TEST_F(RepairTest2, NoMapSstTest2) {
  Close();
  Options options = RepairCurrentOptions();
  options.comparator = ReverseBytewiseComparator();
  Reopen(options);
  for (size_t i = 0; i < 4; ++i) {
    Put(keys[i], vals[i]);
    Flush();
  }
  ASSERT_EQ(GetSstCount(), 4);
  Close();
  ASSERT_OK(RepairDB(dbname_, options));
  ASSERT_EQ(GetSstCount(), 4);
}

TEST_F(RepairTest2, MapSstTest) {
  ReOpen();
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
  ASSERT_OK(RepairDB(dbname_, RepairCurrentOptions()));  // Delete map sst
  ASSERT_EQ(GetSstCount(), 5);
  Reopen(RepairCurrentOptions());
  Iterator* iterator = dbfull()->NewIterator(ReadOptions());
  iterator->SeekToFirst();
  for (size_t i = 0; i < 4; ++i) {
    ASSERT_EQ(Get(keys[i]), vals[i]);
    ASSERT_TRUE(iterator->Valid());
    ASSERT_TRUE(keys[i] == iterator->key());
    ASSERT_TRUE(vals[i] == iterator->value());
    iterator->Next();
  }
  ASSERT_TRUE(!iterator->Valid());
  delete iterator;
}

// TEST_F(RepairTest2, UnorderMapSstTest1) {
//   ReOpen();
//   WriteOptions wo;
//   ASSERT_OK(PutSequence(keys[0], vals[0], wo, 1));
//   ASSERT_OK(PutSequence(keys[1], vals[1], wo, 4));
//   Flush();
//   MoveFilesToLevel(1, 0);
//   ASSERT_OK(PutSequence(keys[0], vals_new[0], wo, 2));
//   ASSERT_OK(PutSequence(keys[1], vals_new[1], wo, 3));
//   Flush();
//   ASSERT_EQ(GetSstCount(), 2);
//   ASSERT_EQ(Get(keys[0]), vals_new[0]);
//   ASSERT_EQ(Get(keys[1]), vals_new[1]);
//   Iterator* iterator = dbfull()->NewIterator(ReadOptions());
//   iterator->SeekToFirst();
//   ASSERT_TRUE(iterator->Valid());
//   ASSERT_TRUE(keys[0] == iterator->key());
//   ASSERT_TRUE(vals_new[0] == iterator->value());
//   iterator->Next();
//   ASSERT_TRUE(iterator->Valid());
//   ASSERT_TRUE(keys[1] == iterator->key());
//   ASSERT_TRUE(vals[1] == iterator->value());
//   iterator->Next();
//   ASSERT_TRUE(!iterator->Valid());
//   delete iterator;
//   Close();
//   ASSERT_OK(RepairDB(dbname_, RepairCurrentOptions()));
//   ASSERT_EQ(GetSstCount(), 3);
//   Reopen(RepairCurrentOptions());
//   ASSERT_EQ(Get(keys[0]), vals[0]);
//   ASSERT_EQ(Get(keys[1]), vals[1]);
//   iterator = dbfull()->NewIterator(ReadOptions());
//   iterator->SeekToFirst();
//   ASSERT_TRUE(iterator->Valid());
//   ASSERT_TRUE(keys[0] == iterator->key());
//   ASSERT_TRUE(vals_new[0] == iterator->value());
//   iterator->Next();
//   ASSERT_TRUE(iterator->Valid());
//   ASSERT_TRUE(keys[1] == iterator->key());
//   ASSERT_TRUE(vals[1] == iterator->value());
//   iterator->Next();
//   ASSERT_TRUE(!iterator->Valid());
//   delete iterator;
// }

// TEST_F(RepairTest2, UnorderMapSstTest2) {
//   ReOpen();
//   WriteOptions wo;
//   ASSERT_OK(PutSequence(keys[0], vals[0], wo, 1));
//   ASSERT_OK(PutSequence(keys[1], vals[1], wo, 4));
//   Flush();
//   MoveFilesToLevel(1, 0);
//   ASSERT_OK(PutSequence(keys[0], vals_new[0], wo, 2));
//   ASSERT_OK(PutSequence(keys[1], vals_new[1], wo, 3));
//   Flush();
//   dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
//   dbfull()->TEST_WaitForCompact();
//   ASSERT_EQ(GetSstCount(), 1);
//   ASSERT_EQ(Get(keys[0]), vals_new[0]);
//   ASSERT_EQ(Get(keys[1]), vals[1]);
//   Iterator* iterator = dbfull()->NewIterator(ReadOptions());
//   iterator->SeekToFirst();
//   ASSERT_TRUE(iterator->Valid());
//   ASSERT_TRUE(keys[0] == iterator->key());
//   ASSERT_TRUE(vals_new[0] == iterator->value());
//   iterator->Next();
//   ASSERT_TRUE(iterator->Valid());
//   ASSERT_TRUE(keys[1] == iterator->key());
//   ASSERT_TRUE(vals[1] == iterator->value());
//   iterator->Next();
//   ASSERT_TRUE(!iterator->Valid());
//   delete iterator;
// }

TEST_F(RepairTest2, BlobSstTest) {
  BlobSstPrepare();
  std::set<uint64_t> sstnumber;
  RestoreSstNumber(&sstnumber);
  ASSERT_EQ(sstnumber.size(), 4);
  SetRepairOptionsForBlob();
  ReOpen();
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  dbfull()->TEST_WaitForCompact();
  RestoreSstNumber(&sstnumber);
  ASSERT_EQ(sstnumber.size(), 3);  // Delete two sst
  Close();
  ASSERT_OK(RepairDB(dbname_, RepairCurrentOptions()));
  Reopen(RepairCurrentOptions());
  RestoreSstNumber(&sstnumber);
  ASSERT_EQ(sstnumber.size(), 4);
  Iterator* iterator = dbfull()->NewIterator(ReadOptions());
  iterator->SeekToFirst();
  for (size_t i = 0; i < 4; ++i) {
    ASSERT_EQ(Get(keys[i]), vals_new[i]);
    ASSERT_TRUE(iterator->Valid());
    ASSERT_TRUE(keys[i] == iterator->key());
    ASSERT_TRUE(vals_new[i] == iterator->value());
    iterator->Next();
  }
  ASSERT_TRUE(!iterator->Valid());
  delete iterator;
}

TEST_F(RepairTest2, DropDependence1BlobSstTest) {
  ReOpen();
  Put(keys[0], vals_old[0]);
  Flush();
  Put(keys[1], vals_old[1]);
  Flush();
  Put(keys[2], vals_new[2]);
  Put(keys[0], vals_new[0]);
  Flush();
  auto sst_dependence1 = GetNewestSstPath();
  Put(keys[3], vals_new[3]);
  Put(keys[1], vals_new[1]);
  Flush();
  ASSERT_EQ(GetSstCount(), 4);
  SetRepairOptionsForBlob();
  ReOpen();
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  dbfull()->TEST_WaitForCompact();
  std::set<uint64_t> sstnumber;
  RestoreSstNumber(&sstnumber);
  ASSERT_EQ(sstnumber.size(), 3);  // Delete two sst
  Close();
  ASSERT_OK(RepairDB(dbname_, RepairCurrentOptions()));
  Reopen(RepairCurrentOptions());
  RestoreSstNumber(&sstnumber);
  ASSERT_EQ(sstnumber.size(), 4);
  ASSERT_FALSE(sst_dependence1.empty());
  ASSERT_OK(env_->DeleteFile(sst_dependence1));
  Close();
  ASSERT_OK(RepairDB(dbname_, RepairCurrentOptions()));
  Reopen(RepairCurrentOptions());
  RestoreSstNumber(&sstnumber);
  ASSERT_EQ(sstnumber.size(), 3);
  for (size_t i = 0; i < 4; ++i) {
    if (i == 0) {
      ASSERT_EQ(Get(keys[i]), std::string("NOT_FOUND"));
    } else {
      ASSERT_EQ(Get(keys[i]), vals_new[i]);
    }
  }
  Iterator* iterator = dbfull()->NewIterator(ReadOptions());
  iterator->SeekToFirst();
  for (size_t i = 0; i < 3; i++) {
    ASSERT_TRUE(iterator->Valid());
    ASSERT_TRUE(keys[i + 1] == iterator->key());
    ASSERT_TRUE(vals_new[i + 1] == iterator->value());
    iterator->Next();
  }
  ASSERT_TRUE(!iterator->Valid());
  delete iterator;
}

TEST_F(RepairTest2, DropDependence2BlobSstTest) {
  ReOpen();
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
  auto sst_dependence2 = GetNewestSstPath();
  ASSERT_EQ(GetSstCount(), 4);
  SetRepairOptionsForBlob();
  ReOpen();
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  dbfull()->TEST_WaitForCompact();
  ASSERT_FALSE(sst_dependence2.empty());
  ASSERT_OK(env_->DeleteFile(sst_dependence2));
  Close();
  ASSERT_OK(RepairDB(dbname_, RepairCurrentOptions()));
  Reopen(RepairCurrentOptions());
  for (size_t i = 0; i < 4; ++i) {
    if (i == 1) {
      ASSERT_EQ(Get(keys[i]), std::string("NOT_FOUND"));
    } else {
      ASSERT_EQ(Get(keys[i]), vals_new[i]);
    }
  }
  Iterator* iterator = dbfull()->NewIterator(ReadOptions());
  iterator->SeekToFirst();
  for (size_t i = 0; i < 3; i++) {
    size_t temp = i == 0 ? i : i + 1;
    ASSERT_TRUE(iterator->Valid());
    ASSERT_TRUE(keys[temp] == iterator->key());
    ASSERT_TRUE(vals_new[temp] == iterator->value());
    iterator->Next();
  }
  ASSERT_TRUE(!iterator->Valid());
  delete iterator;
}

TEST_F(RepairTest2, DropBlobSstTest) {
  ReOpen();
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
  SetRepairOptionsForBlob();
  ReOpen();
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  dbfull()->TEST_WaitForCompact();
  auto sst_blob = GetNewestSstPath();
  ASSERT_FALSE(sst_blob.empty());
  ASSERT_OK(env_->DeleteFile(sst_blob));
  Close();
  ASSERT_OK(RepairDB(dbname_, RepairCurrentOptions()));
  Reopen(RepairCurrentOptions());
  Iterator* iterator = dbfull()->NewIterator(ReadOptions());
  iterator->SeekToFirst();
  for (size_t i = 0; i < 4; i++) {
    ASSERT_EQ(Get(keys[i]), vals_new[i]);
    ASSERT_TRUE(iterator->Valid());
    ASSERT_TRUE(keys[i] == iterator->key());
    ASSERT_TRUE(vals_new[i] == iterator->value());
    iterator->Next();
  }
  ASSERT_TRUE(!iterator->Valid());
  delete iterator;
}

TEST_F(RepairTest2, ParallelScanTest) {
  ReOpen();
  size_t keys = 10;
  std::string manykey[keys] = {std::string("key0"), std::string("key1"),
                               std::string("key2"), std::string("key3"),
                               std::string("key4"), std::string("key5"),
                               std::string("key6"), std::string("key7"),
                               std::string("key8"), std::string("key9")};
  std::string manyval[keys];
  for (int i = 0; i < keys; i++) {
    manyval[i] = RandomString(&r, 11);
    Put(manykey[i], manyval[i]);
    Flush();
  }
  Close();
  ASSERT_OK(RepairDB(dbname_, RepairCurrentOptions()));
  Reopen(RepairCurrentOptions());
  Iterator* iterator = dbfull()->NewIterator(ReadOptions());
  iterator->SeekToFirst();
  for (size_t i = 0; i < keys; i++) {
    ASSERT_EQ(Get(manykey[i]), manyval[i]);
    ASSERT_TRUE(iterator->Valid());
    ASSERT_TRUE(manykey[i] == iterator->key());
    ASSERT_TRUE(manyval[i] == iterator->value());
    iterator->Next();
  }
  ASSERT_TRUE(!iterator->Valid());
  delete iterator;
}

TEST_F(RepairTest2, RepairMultipleColumnFamiliesTest1) {
  // Verify repair logic associates SST files with their original column
  // families.
  const int kNumCfs = 3;
  const int kEntriesPerCf = 2;
  DestroyAndReopen(RepairCurrentOptions());
  CreateAndReopenWithCF({"pikachu1", "pikachu2"}, RepairCurrentOptions());
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      Put(i, "key" + ToString(j), "val" + ToString(j));
      if (j == kEntriesPerCf - 1 && i == kNumCfs - 1) {
        continue;
      }
      Flush(i);
    }
  }
  ASSERT_EQ(GetSstCount(), 5);
  Close();
  ASSERT_OK(RepairDB(dbname_, RepairCurrentOptions()));
  ReopenWithColumnFamilies({"default", "pikachu1", "pikachu2"},
                           RepairCurrentOptions());
  ASSERT_EQ(GetSstCount(), 6);
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      ASSERT_EQ(Get(i, "key" + ToString(j)), "val" + ToString(j));
    }
  }
}

TEST_F(RepairTest2, RepairMultipleColumnFamiliesTest2) {
  const int kNumCfs = 3;
  const int kEntriesPerCf = 2;
  const std::string keys_cf[] = {std::string("key1"), std::string("key2"),
                                 std::string("key3"), std::string("key4")};
  const std::string vals_cf_v1[] = {std::string("val1"), std::string("val2"),
                                    std::string("val3"), std::string("val4")};
  const std::string vals_cf_v2[] = {std::string("val01"), std::string("val02"),
                                    std::string("val03"), std::string("val04")};
  const std::string vals_cf_v3[] = {
      std::string("val001"), std::string("val002"), std::string("val003"),
      std::string("val004")};
  DestroyAndReopen(RepairCurrentOptions());
  CreateAndReopenWithCF({"pikachu1", "pikachu2"}, RepairCurrentOptions());
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      Put(i, keys_cf[j], vals_cf_v1[j]);
      if (j == kEntriesPerCf - 1) {
        Put(i, keys_cf[j - 1], vals_cf_v2[j - 1]);
        if (i == kNumCfs - 1) {
          continue;
        }
      } else {
        Put(i, keys_cf[j + 2], vals_cf_v3[j + 2]);
      }
      Flush(i);
    }
  }
  ASSERT_EQ(GetSstCount(), 5);
  Close();
  ASSERT_OK(RepairDB(dbname_, RepairCurrentOptions()));
  ReopenWithColumnFamilies({"default", "pikachu1", "pikachu2"},
                           RepairCurrentOptions());
  ASSERT_EQ(GetSstCount(), 9);
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      if (j == kEntriesPerCf - 2) {
        ASSERT_EQ(Get(i, keys_cf[j]), vals_cf_v2[j]);
      } else {
        ASSERT_EQ(Get(i, keys_cf[j]), vals_cf_v1[j]);
        ASSERT_EQ(Get(i, keys_cf[j + 1]), vals_cf_v3[j + 1]);
      }
    }
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as RepairDB_V2 is not supported in ROCKSDB_LITE\n");
  return 0;
}
#endif