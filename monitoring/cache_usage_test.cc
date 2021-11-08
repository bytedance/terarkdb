
#include "cache_usage.h"
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include <iostream>

namespace TERARKDB_NAMESPACE {
class CacheUsageTest : public DBTestBase {

    public:
    CacheUsageTest(): DBTestBase("/db_cache_usage_test") {}

};

TEST_F(CacheUsageTest, Basic){
    for(int i = 0 ;i < 1000 ;i++){
        CollectCacheUsage(i,i);
    }
    std::vector<std::pair<uint64_t, size_t>> usage;
    GetCacheUsage(&usage);
    for(auto& u : usage){
        assert(u.first == u.second);
    }
    for(int i = 0 ;i < 1000 ;i++){
        CollectCacheUsage(i,-i);
    }
    GetCacheUsage(&usage);
    
    assert(usage.size() == 0);
    
}
TEST_F(CacheUsageTest, MultiThreadTest){
    const int THREAD_NUM = 5;
    auto collect = [](){
        for(int i = 0; i < 1000;i++){
            CollectCacheUsage(i,i);
        }
    };
    std::vector<std::thread> threads;
    for(int i =0;i < THREAD_NUM -  1; i++){
        threads.emplace_back(collect);
    }
    collect();
    for(auto& thd: threads){
        thd.join();
    }
    std::vector<std::pair<uint64_t, size_t>> usage;
    GetCacheUsage(&usage);
    for(auto& u : usage){
        assert(u.second == u.first * THREAD_NUM);
    }
    uint64_t value;
    db_->GetIntProperty(DB::Properties::kBlockCacheGarbage, &value);
    assert(value ==  999 * 500 * THREAD_NUM);
}

}

int main(int argc, char** argv) {
  TERARKDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
