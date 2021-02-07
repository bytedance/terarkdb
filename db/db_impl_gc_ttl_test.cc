// Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License.

#include "db/db_impl.h"
#include "db/db_test_util.h"
#include "db/periodic_work_scheduler.h"
#include "rocksdb/terark_namespace.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"

namespace TERARKDB_NAMESPACE {

class DBImplGCTTL_Test : public DBTestBase {
 public:
  DBImplGCTTL_Test()
      : DBTestBase("./db_GC_ttl_test"),
        mock_env_(new MockTimeEnv(Env::Default())) {}
  ~DBImplGCTTL_Test() override { Close(); }
  void init() {
    dbname = test::PerThreadDBPath("ttl_gc_test");
    DestroyDB(dbname, options);
    options.create_if_missing = true;
    options.ttl_gc_ratio = 0.50;
    options.ttl_max_scan_gap = 10;
    options.ttl_extractor_factory.reset(
        new test::TestTtlExtractorFactory(mock_env_.get()));
    options.level0_file_num_compaction_trigger = 8;
    options.enable_lazy_compaction = false;
    options.table_factory.reset(
        new BlockBasedTableFactory(BlockBasedTableOptions()));
  }

  void run() {
    int L0FilesNums = 4;
    options.env = mock_env_.get();
    SetUp();
    Reopen(options);
    char ts_string[8];
    EncodeFixed64(ts_string, ttl);
    int KeyEntrys = 800;
    for (int i = 0; i < L0FilesNums; i++) {
      for (int j = 0; j < KeyEntrys; j++) {
        std::string key = "key";
        std::string value = "value";
        AppendNumberTo(&key, j);
        AppendNumberTo(&value, j);
        value.append(ts_string, 8);
        dbfull()->Put(WriteOptions(), key, value);
      }
      dbfull()->Flush(FlushOptions());
    }
    dbfull()->TEST_WaitForStatsDumpRun(
        [&] { mock_env_->set_current_time(ttl); });
    ASSERT_TRUE(flag);
    ASSERT_EQ(L0FilesNums, mark);
    dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    dbfull()->TEST_WaitForCompact();
  }
  void read() {
    mark = 0;
    cnt = 0;
    dbfull()->TEST_WaitForStatsDumpRun(
        [&] { mock_env_->set_current_time(ttl + ttl); });
    ASSERT_EQ(mark, cnt);
  }

 protected:
  std::unique_ptr<TERARKDB_NAMESPACE::MockTimeEnv> mock_env_;
  Options options;
  std::string dbname;
  bool flag = false;
  int mark = 0;
  int cnt = 0;
  uint64_t ttl = 200;

  void SetUp() override {
    mock_env_->InstallTimedWaitFixCallback();
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::StartPeriodicWorkScheduler:Init", [&](void* arg) {
          auto* periodic_work_scheduler_ptr =
              reinterpret_cast<PeriodicWorkScheduler**>(arg);
          *periodic_work_scheduler_ptr =
              PeriodicWorkTestScheduler::Default(mock_env_.get());
        });
    TERARKDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "DBImpl:ScheduleTtlGC", [&](void* /*arg*/) {
          mark = 0;
          flag = true;
        });
    TERARKDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "DBImpl:ScheduleTtlGC-mark", [&](void* /*arg*/) { mark++; });
    TERARKDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "DBImpl:Exist-SST", [&](void* /*arg*/) { cnt++; });
  }
};

TEST_F(DBImplGCTTL_Test, BlockBasedTableTest) {
  init();
  run();
  read();
}
#ifdef TERARK_ZIP
TEST_F(DBImplGCTTL_Test, TerarkTableTest) {
  init();
  TerarkZipTableOptions terarkziptableoptions;
  options.table_factory.reset(TERARKDB_NAMESPACE::NewTerarkZipTableFactory(
      terarkziptableoptions, options.table_factory));
  run();
  read();
}
#endif

}  // namespace TERARKDB_NAMESPACE
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
