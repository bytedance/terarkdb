// Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License.

#include "table/block_based_table_builder.h"

#include "include/rocksdb/ttl_extractor.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb{

class DBImplGCTTL_Test : public testing::Test {
  //

 private:

};

TEST_F(DBImplGCTTL_Test,OpenTest) {

}
}
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}