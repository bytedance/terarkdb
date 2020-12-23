// Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License.

#include "util/testharness.h"
#include "util/testutil.h"
#include "util/string_util.h"
#include "db/db_impl.h"
#include "util/sync_point.h"
#include "db/db_test_util.h"

namespace rocksdb {

class DBImplGCTTL_Test : public DBTestBase {
 public:
  DBImplGCTTL_Test() : DBTestBase("/db_GC_ttl_test"){
  }
  void init() {
    rocksdb::SyncPoint::GetInstance()->SetCallBack(
        "DBImpl:ScheduleGCTTL",
        [&](void* /*arg*/) { mark = 0;flag = true;});
    rocksdb::SyncPoint::GetInstance()->SetCallBack(
        "DBImpl:ScheduleGCTTL-mark",
        [&](void* /*arg*/) { mark++;});
    SyncPoint::GetInstance()->EnableProcessing();

    dbname = test::PerThreadDBPath("ttl_gc_test");
    ASSERT_OK(DestroyDB(dbname, options));

    options.create_if_missing = true;
    options.ttl_garbage_collection_percentage = 50.0;
    options.ttl_scan_gap = 10;
    options.ttl_extractor_factory.reset(new test::TestTtlExtractorFactory());
    options.level0_file_num_compaction_trigger = 8;
    options.stats_dump_period_sec = 10;
    options.table_factory.reset(new BlockBasedTableFactory(BlockBasedTableOptions()));

  }

 protected:
  Options options;
  std::string dbname;
  bool flag = false;
  int mark = 0;
};

TEST_F(DBImplGCTTL_Test, L0FileExpiredTest) {
  init();
  std::unique_ptr<rocksdb::MockTimeEnv> mock_env;
  mock_env.reset(new rocksdb::MockTimeEnv(env_));
  mock_env->set_current_time(0); // in seconds
  options.env = mock_env.get();
  Reopen(options);

  int L0FilesNums = 4;
  uint64_t ttl = 200;
  char ts_string[8];
  EncodeFixed64(ts_string, ttl);
  int KeyEntrys = 800;
  for(int i = 0; i < L0FilesNums; i++){
    for(int j = 0 ; j < KeyEntrys;j++ ){
      std::string key = "key";
      std::string value = "value";
      AppendNumberTo(&key,j);
      AppendNumberTo(&value,j);
      value.append(ts_string,8);
      dbfull()->Put(WriteOptions(),key,value);
    }
    dbfull()->Flush(FlushOptions());
  }

  ASSERT_EQ(10, dbfull()->GetDBOptions().stats_dump_period_sec);
  dbfull()->TEST_WaitForTimedTaskRun([&] { mock_env->set_current_time(ttl); });
  ASSERT_TRUE(flag);
  ASSERT_EQ(L0FilesNums,mark);
  dbfull()->TEST_WaitForCompact();


}
}  // namespace rocksdb
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}