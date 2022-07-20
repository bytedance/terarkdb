//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "rocksdb/statistics.h"

#include <unordered_set>

#include "port/stack_trace.h"
#include "rocksdb/terark_namespace.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace TERARKDB_NAMESPACE {

class StatisticsTest : public testing::Test {};

// Sanity check to make sure that contents and order of TickersNameMap
// match Tickers enum
TEST_F(StatisticsTest, SanityTickers) {
  // EXPECT_EQ(static_cast<size_t>(Tickers::TICKER_ENUM_MAX),
  //          TickersNameMap.size() + CFTickersNameMap.size());

  std::unordered_map<uint32_t, std::string> db_tickers_map, cf_tickers_map;
  for (auto& db_tickers : TickersNameMap) {
    db_tickers_map[db_tickers.first] = db_tickers.second;
  }
  for (auto& cf_tickers : CFTickersNameMap) {
    cf_tickers_map[cf_tickers.first] = cf_tickers.second;
  }

  std::string pre_tickers_name;
  bool illegal = false;
  for (uint32_t t = 0; t < Tickers::TICKER_ENUM_MAX; t++) {
    auto db_tickers_iter = db_tickers_map.find(t);
    auto cf_tickers_iter = cf_tickers_map.find(t);
    if (db_tickers_iter == db_tickers_map.end() &&
        cf_tickers_iter == cf_tickers_map.end()) {
      std::cout << "Miss ticker name defined at " << t
                << ", preTickers=" << pre_tickers_name << std::endl;
      illegal = true;
    } else {
      if (db_tickers_iter != db_tickers_map.end() &&
          cf_tickers_iter != cf_tickers_map.end()) {
        std::cout << "Duplicate ticker name defined at " << t
                  << ", tickerName1=" << db_tickers_iter->second
                  << ", tickerName2=" << cf_tickers_iter->second << std::endl;
      }
      pre_tickers_name = (db_tickers_iter != db_tickers_map.end())
                             ? db_tickers_iter->second
                             : cf_tickers_iter->second;
    }
  }
  ASSERT_FALSE(illegal);
}

// Sanity check to make sure that contents and order of HistogramsNameMap
// match Tickers enum
TEST_F(StatisticsTest, SanityHistograms) {
  // EXPECT_EQ(static_cast<size_t>(Histograms::HISTOGRAM_ENUM_MAX),
  //          HistogramsNameMap.size() + CFHistogramsNameMap.size());

  std::unordered_map<uint32_t, std::string> db_hisgrams_map, cf_hisgrams_map;
  for (auto& db_hisgrams : HistogramsNameMap) {
    db_hisgrams_map[db_hisgrams.first] = db_hisgrams.second;
  }
  for (auto& cf_hisgrams : CFHistogramsNameMap) {
    cf_hisgrams_map[cf_hisgrams.first] = cf_hisgrams.second;
  }

  std::string pre_hisgrams_name;
  bool illegal = false;
  for (uint32_t h = 0; h < Histograms::HISTOGRAM_ENUM_MAX; h++) {
    auto db_hisgrams_iter = db_hisgrams_map.find(h);
    auto cf_hisgrams_iter = cf_hisgrams_map.find(h);
    if (db_hisgrams_iter == db_hisgrams_map.end() &&
        cf_hisgrams_iter == cf_hisgrams_map.end()) {
      std::cout << "Miss hisgrams name defined at " << h
                << ", preHisgramsName=" << pre_hisgrams_name << std::endl;
      illegal = true;
    } else {
      if (db_hisgrams_iter != db_hisgrams_map.end() &&
          cf_hisgrams_iter != cf_hisgrams_map.end()) {
        std::cout << "Duplicate hisgrams name defined at " << h
                  << ", hisgramsName1=" << db_hisgrams_iter->second
                  << ", hisgramsName2=" << cf_hisgrams_iter->second
                  << std::endl;
      }
      pre_hisgrams_name = (db_hisgrams_iter != db_hisgrams_map.end())
                              ? db_hisgrams_iter->second
                              : cf_hisgrams_iter->second;
    }
  }
  ASSERT_FALSE(illegal);
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  TERARKDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
