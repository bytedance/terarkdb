//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/lru_cache.h"

#include <string>
#include <vector>

#ifdef WITH_DIAGNOSE_CACHE
#include <terark/heap_ext.hpp>
#endif

#include "port/port.h"
#include "rocksdb/terark_namespace.h"
#include "util/testharness.h"

namespace TERARKDB_NAMESPACE {

class LRUCacheTest : public testing::Test,
                     virtual public testing::WithParamInterface<bool> {
 public:
  LRUCacheTest() {}
  ~LRUCacheTest() { DeleteCache(); }

  void DeleteCache() {
    if (cache_ != nullptr) {
      cache_->~CacheShard();
      port::cacheline_aligned_free(cache_);
      cache_ = nullptr;
    }
  }

#ifdef WITH_DIAGNOSE_CACHE
  void SetDiagnose(bool d) { is_diagnose_ = d; }
#else
  void SetDiagnose(bool d) {}
#endif

  void NewCache(size_t capacity, double high_pri_pool_ratio = 0.0) {
    DeleteCache();
#ifdef WITH_DIAGNOSE_CACHE
    if (is_diagnose_) {
      cache_ = reinterpret_cast<LRUCacheDiagnosableShard*>(
          port::cacheline_aligned_alloc(sizeof(LRUCacheDiagnosableShard)));
      LRUCacheDiagnosableMonitor::Options mo;
      mo.top_k = 10;
      new (cache_) LRUCacheDiagnosableShard(
          capacity, false /* strict_capcity_limit */, high_pri_pool_ratio, mo);
    } else
#endif
    {
      cache_ = reinterpret_cast<LRUCacheShard*>(
          port::cacheline_aligned_alloc(sizeof(LRUCacheShard)));
      new (cache_) LRUCacheShard(capacity, false /* strict_capcity_limit */,
                                 high_pri_pool_ratio, {});
    }
  }

  void Insert(const std::string& key,
              Cache::Priority priority = Cache::Priority::LOW) {
    cache_->Insert(key, 0 /*hash*/, nullptr /*value*/, 1 /*charge*/,
                   nullptr /*deleter*/, nullptr /*handle*/, priority);
  }

  void Insert(const std::string& key, size_t charge) {
    cache_->Insert(key, 0 /*hash*/, nullptr /*value*/, charge,
                   nullptr /*deleter*/, nullptr /*handle*/,
                   Cache::Priority::LOW);
  }

  void Insert(char key, Cache::Priority priority = Cache::Priority::LOW) {
    Insert(std::string(1, key), priority);
  }

  bool Lookup(const std::string& key) {
    auto handle = cache_->Lookup(key, 0 /*hash*/);
    if (handle) {
      cache_->Release(handle);
      return true;
    }
    return false;
  }

  bool Lookup(char key) { return Lookup(std::string(1, key)); }

  Cache::Handle* LookupNotRelease(const std::string& key) {
    return cache_->Lookup(key, 0 /*hash*/);
  }

  bool Erase(const std::string& key) { return cache_->Erase(key, 0 /*hash*/); }

  void ValidateLRUList(std::vector<std::string> keys,
                       size_t num_high_pri_pool_keys = 0) {
    LRUHandle* lru;
    LRUHandle* lru_low_pri;
#ifdef WITH_DIAGNOSE_CACHE
    if (is_diagnose_) {
      reinterpret_cast<LRUCacheDiagnosableShard*>(cache_)->TEST_GetLRUList(
          &lru, &lru_low_pri);
    } else
#endif
    {
      reinterpret_cast<LRUCacheShard*>(cache_)->TEST_GetLRUList(&lru,
                                                                &lru_low_pri);
    }

    LRUHandle* iter = lru;
    bool in_high_pri_pool = false;
    size_t high_pri_pool_keys = 0;
    if (iter == lru_low_pri) {
      in_high_pri_pool = true;
    }
    for (const auto& key : keys) {
      iter = iter->next;
      ASSERT_NE(lru, iter);
      ASSERT_EQ(key, iter->key().ToString());
      ASSERT_EQ(in_high_pri_pool, iter->InHighPriPool());
      if (in_high_pri_pool) {
        high_pri_pool_keys++;
      }
      if (iter == lru_low_pri) {
        ASSERT_FALSE(in_high_pri_pool);
        in_high_pri_pool = true;
      }
    }
    ASSERT_EQ(lru, iter->next);
    ASSERT_TRUE(in_high_pri_pool);
    ASSERT_EQ(num_high_pri_pool_keys, high_pri_pool_keys);
  }

#ifdef WITH_DIAGNOSE_CACHE
  using DataElement = LRUCacheDiagnosableMonitor::TopSet::DataElement;
  void ValidatePinnedElements(const std::vector<DataElement>& elements) {
    LRUCacheDiagnosableShard* monitor_cache =
        reinterpret_cast<LRUCacheDiagnosableShard*>(cache_);
    auto& topset = monitor_cache->TEST_get_pinned_set();
    auto& _elements_map = topset.TEST_get_elements_map();
    auto& _elements = topset.TEST_get_elements_storage();

    for (auto& e : elements) {
      auto findit = _elements_map.find(Slice(e.key));
      size_t data_idx = findit->second;
      ASSERT_TRUE(findit != _elements_map.end());
      ASSERT_TRUE(_elements[data_idx].count == e.count);
      ASSERT_TRUE(_elements[data_idx].total_charge == e.total_charge);
    }
  }
#endif

  CacheShard* cache() { return cache_; }

 private:
  CacheShard* cache_ = nullptr;
  bool is_diagnose_ = false;
};

#ifdef WITH_DIAGNOSE_CACHE
INSTANTIATE_TEST_CASE_P(LRUCacheTest, LRUCacheTest, ::testing::Bool());
#else
INSTANTIATE_TEST_CASE_P(LRUCacheTest, LRUCacheTest, ::testing::Values(false));
#endif

TEST_P(LRUCacheTest, BasicLRU) {
  SetDiagnose(GetParam());
  NewCache(5);
  for (char ch = 'a'; ch <= 'e'; ch++) {
    Insert(ch);
  }
  ValidateLRUList({"a", "b", "c", "d", "e"});
  for (char ch = 'x'; ch <= 'z'; ch++) {
    Insert(ch);
  }
  ValidateLRUList({"d", "e", "x", "y", "z"});
  ASSERT_FALSE(Lookup("b"));
  ValidateLRUList({"d", "e", "x", "y", "z"});
  ASSERT_TRUE(Lookup("e"));
  ValidateLRUList({"d", "x", "y", "z", "e"});
  ASSERT_TRUE(Lookup("z"));
  ValidateLRUList({"d", "x", "y", "e", "z"});
  Erase("x");
  ValidateLRUList({"d", "y", "e", "z"});
  ASSERT_TRUE(Lookup("d"));
  ValidateLRUList({"y", "e", "z", "d"});
  Insert("u");
  ValidateLRUList({"y", "e", "z", "d", "u"});
  Insert("v");
  ValidateLRUList({"e", "z", "d", "u", "v"});
}

TEST_P(LRUCacheTest, MidpointInsertion) {
  SetDiagnose(GetParam());
  // Allocate 2 cache entries to high-pri pool.
  NewCache(5, 0.45);

  Insert("a", Cache::Priority::LOW);
  Insert("b", Cache::Priority::LOW);
  Insert("c", Cache::Priority::LOW);
  Insert("x", Cache::Priority::HIGH);
  Insert("y", Cache::Priority::HIGH);
  ValidateLRUList({"a", "b", "c", "x", "y"}, 2);

  // Low-pri entries inserted to the tail of low-pri list (the midpoint).
  // After lookup, it will move to the tail of the full list.
  Insert("d", Cache::Priority::LOW);
  ValidateLRUList({"b", "c", "d", "x", "y"}, 2);
  ASSERT_TRUE(Lookup("d"));
  ValidateLRUList({"b", "c", "x", "y", "d"}, 2);

  // High-pri entries will be inserted to the tail of full list.
  Insert("z", Cache::Priority::HIGH);
  ValidateLRUList({"c", "x", "y", "d", "z"}, 2);
}

TEST_P(LRUCacheTest, EntriesWithPriority) {
  SetDiagnose(GetParam());
  // Allocate 2 cache entries to high-pri pool.
  NewCache(5, 0.45);

  Insert("a", Cache::Priority::LOW);
  Insert("b", Cache::Priority::LOW);
  Insert("c", Cache::Priority::LOW);
  ValidateLRUList({"a", "b", "c"}, 0);

  // Low-pri entries can take high-pri pool capacity if available
  Insert("u", Cache::Priority::LOW);
  Insert("v", Cache::Priority::LOW);
  ValidateLRUList({"a", "b", "c", "u", "v"}, 0);

  Insert("X", Cache::Priority::HIGH);
  Insert("Y", Cache::Priority::HIGH);
  ValidateLRUList({"c", "u", "v", "X", "Y"}, 2);

  // High-pri entries can overflow to low-pri pool.
  Insert("Z", Cache::Priority::HIGH);
  ValidateLRUList({"u", "v", "X", "Y", "Z"}, 2);

  // Low-pri entries will be inserted to head of low-pri pool.
  Insert("a", Cache::Priority::LOW);
  ValidateLRUList({"v", "X", "a", "Y", "Z"}, 2);

  // Low-pri entries will be inserted to head of high-pri pool after lookup.
  ASSERT_TRUE(Lookup("v"));
  ValidateLRUList({"X", "a", "Y", "Z", "v"}, 2);

  // High-pri entries will be inserted to the head of the list after lookup.
  ASSERT_TRUE(Lookup("X"));
  ValidateLRUList({"a", "Y", "Z", "v", "X"}, 2);
  ASSERT_TRUE(Lookup("Z"));
  ValidateLRUList({"a", "Y", "v", "X", "Z"}, 2);

  Erase("Y");
  ValidateLRUList({"a", "v", "X", "Z"}, 2);
  Erase("X");
  ValidateLRUList({"a", "v", "Z"}, 1);
  Insert("d", Cache::Priority::LOW);
  Insert("e", Cache::Priority::LOW);
  ValidateLRUList({"a", "v", "d", "e", "Z"}, 1);
  Insert("f", Cache::Priority::LOW);
  Insert("g", Cache::Priority::LOW);
  ValidateLRUList({"d", "e", "f", "g", "Z"}, 1);
  ASSERT_TRUE(Lookup("d"));
  ValidateLRUList({"e", "f", "g", "Z", "d"}, 2);
}

#ifdef WITH_DIAGNOSE_CACHE

TEST_F(LRUCacheTest, LRUCacheDiagnosableMonitor) {
  SetDiagnose(true);
  NewCache(5);
  Insert("a");
  Insert("b");
  Insert("c");
  Insert("d");

  LRUCacheDiagnosableShard* monitor_cache =
      reinterpret_cast<LRUCacheDiagnosableShard*>(cache());

  ASSERT_EQ(monitor_cache->TEST_get_lru_set().TEST_get_elements_map().size(),
            4);
  ASSERT_EQ(
      monitor_cache->TEST_get_lru_set().TEST_get_elements_storage().size(), 4);
  ASSERT_EQ(
      monitor_cache->TEST_get_highpri_set().TEST_get_elements_map().size(), 0);
  ASSERT_EQ(
      monitor_cache->TEST_get_highpri_set().TEST_get_elements_storage().size(),
      0);
  ASSERT_EQ(monitor_cache->TEST_get_total_set().TEST_get_elements_map().size(),
            4);
  ASSERT_EQ(
      monitor_cache->TEST_get_total_set().TEST_get_elements_storage().size(),
      4);

  ASSERT_EQ(monitor_cache->TEST_get_pinned_set().TEST_get_elements_map().size(),
            0);
  ASSERT_EQ(
      monitor_cache->TEST_get_pinned_set().TEST_get_elements_storage().size(),
      0);

  LRUHandle* h1 = reinterpret_cast<LRUHandle*>(LookupNotRelease("a"));
  ASSERT_EQ(h1->refs, 2);
  LRUHandle* h2 = reinterpret_cast<LRUHandle*>(LookupNotRelease("a"));
  ASSERT_EQ(h1->refs, 3);
  ASSERT_EQ(h2->refs, 3);
  LRUHandle* h3 = reinterpret_cast<LRUHandle*>(LookupNotRelease("a"));
  ASSERT_EQ(h1->refs, 4);
  ASSERT_EQ(h2->refs, 4);
  ASSERT_EQ(h3->refs, 4);

  ASSERT_EQ(monitor_cache->TEST_get_lru_set().TEST_get_elements_map().size(),
            3);
  ASSERT_EQ(
      monitor_cache->TEST_get_lru_set().TEST_get_elements_storage().size(), 3);
  ASSERT_EQ(monitor_cache->TEST_get_pinned_set().TEST_get_elements_map().size(),
            1);
  ASSERT_EQ(
      monitor_cache->TEST_get_pinned_set().TEST_get_elements_storage().size(),
      1);

  Erase("d");
  ASSERT_EQ(monitor_cache->TEST_get_total_set().TEST_get_elements_map().size(),
            3);
  ASSERT_EQ(
      monitor_cache->TEST_get_total_set().TEST_get_elements_storage().size(),
      3);
  ASSERT_EQ(monitor_cache->TEST_get_pinned_set().TEST_get_elements_map().size(),
            1);
  ASSERT_EQ(
      monitor_cache->TEST_get_pinned_set().TEST_get_elements_storage().size(),
      1);
  ASSERT_EQ(
      monitor_cache->TEST_get_lru_set().TEST_get_elements_storage().size(), 2);
  ASSERT_EQ(
      monitor_cache->TEST_get_lru_set().TEST_get_elements_storage().size(), 2);

  LookupNotRelease("b");
  ASSERT_EQ(monitor_cache->TEST_get_pinned_set().TEST_get_elements_map().size(),
            2);
  ASSERT_EQ(
      monitor_cache->TEST_get_pinned_set().TEST_get_elements_storage().size(),
      2);

  Insert("a", 3);
  LRUHandle* h4 = reinterpret_cast<LRUHandle*>(LookupNotRelease("a"));
  ASSERT_EQ(h4->refs, 2);
  ASSERT_EQ(h3->refs, 3);
  ASSERT_EQ(h2->refs, 3);
  ASSERT_EQ(h1->refs, 3);
  std::vector<DataElement> elements;

  elements.emplace_back("a", 4, 2, 0);
  elements.emplace_back("b", 1, 1, 0);
  ValidatePinnedElements(elements);
}

TEST_F(LRUCacheTest, TopSet) {
  using TopSet = LRUCacheDiagnosableMonitor::TopSet;
  auto new_lru_handle = [&](int id, size_t charge) {
    std::string keydata = "handle-" + std::to_string(id);
    LRUHandle* e = reinterpret_cast<LRUHandle*>(
        new char[sizeof(LRUHandle) - 1 + keydata.size()]);
    e->key_length = 13;
    memcpy(e->key_data, keydata.c_str(), keydata.size());

    e->charge = charge;
    return e;
  };

  TopSet ts(10);

  LRUHandle* h1 = new_lru_handle(1, 4);
  ts.Add(h1);
  LRUHandle* h2 = new_lru_handle(2, 3);
  ts.Add(h2);

  ts.Sub(h2);

  auto storage = ts.TEST_get_elements_storage();
  auto heap = ts.TEST_get_data_heap();
  auto map = ts.TEST_get_elements_map();
  ASSERT_EQ(map.size(), 1);
  ASSERT_EQ(storage.size(), 1);
  ASSERT_EQ(heap.size(), 1);

  ASSERT_EQ(storage[0].key.substr(0, 8), "handle-1");
  ASSERT_EQ(storage[0].total_charge, 4);
  ASSERT_EQ(storage[0].count, 1);
}
TEST_F(LRUCacheTest, TopSetAdd) {
  using TopSet = LRUCacheDiagnosableMonitor::TopSet;
  using DataIdx = LRUCacheDiagnosableMonitor::TopSet::DataIdx;
  auto new_lru_handle = [&](int id, size_t charge) {
    std::string keydata = "handle-" + std::to_string(id);
    size_t handle_size = sizeof(LRUHandle) - 1 + keydata.size();
    LRUHandle* e = reinterpret_cast<LRUHandle*>(new char[handle_size]);
    memset(e, 0, handle_size);
    e->key_length = 13;
    memcpy(e->key_data, keydata.c_str(), keydata.size());

    e->charge = charge;
    return e;
  };

  TopSet ts(10);

  LRUHandle* h1 = new_lru_handle(1, 1);
  ts.Add(h1);
  LRUHandle* h2 = new_lru_handle(2, 4);
  ts.Add(h2);
  LRUHandle* h3 = new_lru_handle(3, 10);
  ts.Add(h3);
  LRUHandle* h4 = new_lru_handle(4, 1);
  ts.Add(h4);
  LRUHandle* h5 = new_lru_handle(2, 3);
  ts.Add(h5);
  LRUHandle* h6 = new_lru_handle(5, 9);
  ts.Add(h6);

  auto storage = ts.TEST_get_elements_storage();
  auto heap = ts.TEST_get_data_heap();
  auto map = ts.TEST_get_elements_map();
  ASSERT_EQ(map.size(), 5);
  ASSERT_EQ(storage.size(), 5);
  ASSERT_EQ(heap.size(), 5);

  ASSERT_EQ(storage[0].key.substr(0, 8), "handle-1");
  ASSERT_EQ(storage[0].total_charge, 1);
  ASSERT_EQ(storage[0].count, 1);

  ASSERT_EQ(storage[1].key.substr(0, 8), "handle-2");
  ASSERT_EQ(storage[1].total_charge, 7);
  ASSERT_EQ(storage[1].count, 2);

  ASSERT_EQ(storage[2].key.substr(0, 8), "handle-3");
  ASSERT_EQ(storage[2].total_charge, 10);
  ASSERT_EQ(storage[2].count, 1);

  ASSERT_EQ(storage[3].key.substr(0, 8), "handle-4");
  ASSERT_EQ(storage[3].total_charge, 1);
  ASSERT_EQ(storage[3].count, 1);

  ASSERT_EQ(storage[4].key.substr(0, 8), "handle-5");
  ASSERT_EQ(storage[4].total_charge, 9);
  ASSERT_EQ(storage[4].count, 1);

  terark::pop_heap_keep_top(heap.begin(), heap.size(),
                            ts.TEST_get_charge_cmp());
  DataIdx top_idx = heap.back();
  ASSERT_EQ(top_idx, 2);
  for (size_t i = 1; i < heap.size(); i++) {
    ASSERT_TRUE(ts.VerifyIdx());
    terark::pop_heap_keep_top(heap.begin(), heap.size() - i,
                              ts.TEST_get_charge_cmp());
    ASSERT_TRUE(ts.VerifyIdx());
    auto cur_top_idx = heap.back();
    ASSERT_LE(storage[cur_top_idx].total_charge, storage[top_idx].total_charge);
  }
}

TEST_F(LRUCacheTest, TopSetSub) {
  using TopSet = LRUCacheDiagnosableMonitor::TopSet;
  using DataIdx = LRUCacheDiagnosableMonitor::TopSet::DataIdx;
  auto new_lru_handle = [&](int id, size_t charge) {
    std::string keydata = "handle-" + std::to_string(id);
    size_t handle_size = sizeof(LRUHandle) - 1 + keydata.size();
    LRUHandle* e = reinterpret_cast<LRUHandle*>(new char[handle_size]);
    memset(e, 0, handle_size);
    e->key_length = 13;
    memcpy(e->key_data, keydata.c_str(), keydata.size());

    e->charge = charge;
    return e;
  };

  TopSet ts(10);

  LRUHandle* h1 = new_lru_handle(1, 1);
  ts.Add(h1);
  LRUHandle* h2 = new_lru_handle(2, 4);
  ts.Add(h2);
  LRUHandle* h3 = new_lru_handle(3, 10);
  ts.Add(h3);
  LRUHandle* h4 = new_lru_handle(4, 1);
  ts.Add(h4);
  LRUHandle* h5 = new_lru_handle(2, 3);
  ts.Add(h5);
  LRUHandle* h6 = new_lru_handle(5, 9);
  ts.Add(h6);

  auto& storage = ts.TEST_get_elements_storage();
  auto& heap = ts.TEST_get_data_heap();
  auto& map = ts.TEST_get_elements_map();
  ASSERT_EQ(map.size(), 5);
  ASSERT_EQ(storage.size(), 5);
  ASSERT_EQ(heap.size(), 5);

  ASSERT_EQ(storage[0].key.substr(0, 8), "handle-1");
  ASSERT_EQ(storage[0].total_charge, 1);
  ASSERT_EQ(storage[0].count, 1);

  ASSERT_EQ(storage[1].key.substr(0, 8), "handle-2");
  ASSERT_EQ(storage[1].total_charge, 7);
  ASSERT_EQ(storage[1].count, 2);

  ASSERT_EQ(storage[2].key.substr(0, 8), "handle-3");
  ASSERT_EQ(storage[2].total_charge, 10);
  ASSERT_EQ(storage[2].count, 1);

  ASSERT_EQ(storage[3].key.substr(0, 8), "handle-4");
  ASSERT_EQ(storage[3].total_charge, 1);
  ASSERT_EQ(storage[3].count, 1);

  ASSERT_EQ(storage[4].key.substr(0, 8), "handle-5");
  ASSERT_EQ(storage[4].total_charge, 9);
  ASSERT_EQ(storage[4].count, 1);

  ts.Sub(h5);

  ASSERT_EQ(storage[1].key.substr(0, 8), "handle-2");
  ASSERT_EQ(storage[1].total_charge, 4);
  ASSERT_EQ(storage[1].count, 1);

  ts.Sub(h6);

  ASSERT_EQ(storage[4].key.substr(0, 8), "handle-5");
  ASSERT_EQ(storage[4].total_charge, 0);
  ASSERT_EQ(storage[4].count, 0);

  auto& storage1 = ts.TEST_get_elements_storage();
  auto heap1 = ts.TEST_get_data_heap();
  auto& map1 = ts.TEST_get_elements_map();
  ASSERT_EQ(map1.size(), 4);
  ASSERT_EQ(storage1.size(), 4);
  ASSERT_EQ(heap1.size(), 4);

  terark::pop_heap_keep_top(heap1.begin(), heap1.size(),
                            ts.TEST_get_charge_cmp());
  DataIdx top_idx = heap1.back();
  ASSERT_EQ(top_idx, 2);
  for (size_t i = 1; i < heap1.size(); i++) {
    ASSERT_TRUE(ts.VerifyIdx());
    terark::pop_heap_keep_top(heap1.begin(), heap1.size() - i,
                              ts.TEST_get_charge_cmp());
    ASSERT_TRUE(ts.VerifyIdx());
    auto cur_top_idx = heap1.back();
    ASSERT_LE(storage[cur_top_idx].total_charge, storage[top_idx].total_charge);
  }
}
#endif
}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
