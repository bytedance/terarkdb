#include "rocksdb/assert.h"
#include "rocksdb/terark_namespace.h"
#include "util/random.h"
#include "util/testharness.h"

namespace TERARKDB_NAMESPACE {
class AssertTest : public testing::Test {};
TEST_F(AssertTest, GenneralTest) {
    terark_assert(true);
    terark_assert(1 == 1);
    terark_assert(1 != 2);
}

}  // namespace TERARKDB_NAMESPACE
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
