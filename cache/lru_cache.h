//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <map>
#include <mutex>
#include <sstream>
#include <string>

#include "cache/sharded_cache.h"
#include "port/port.h"
#include "util/autovector.h"

namespace rocksdb {

// LRU cache implementation

// An entry is a variable length heap-allocated structure.
// Entries are referenced by cache and/or by any external entity.
// The cache keeps all its entries in table. Some elements
// are also stored on LRU list.
//
// LRUHandle can be in these states:
// 1. Referenced externally AND in hash table.
//  In that case the entry is *not* in the LRU. (refs > 1 && in_cache == true)
// 2. Not referenced externally and in hash table. In that case the entry is
// in the LRU and can be freed. (refs == 1 && in_cache == true)
// 3. Referenced externally and not in hash table. In that case the entry is
// in not on LRU and not in table. (refs >= 1 && in_cache == false)
//
// All newly created LRUHandles are in state 1. If you call
// LRUCacheShard::Release
// on entry in state 1, it will go into state 2. To move from state 1 to
// state 3, either call LRUCacheShard::Erase or LRUCacheShard::Insert with the
// same key.
// To move from state 2 to state 1, use LRUCacheShard::Lookup.
// Before destruction, make sure that no handles are in state 1. This means
// that any successful LRUCacheShard::Lookup/LRUCacheShard::Insert have a
// matching
// RUCache::Release (to move into state 2) or LRUCacheShard::Erase (for state 3)

struct LRUHandle {
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  uint32_t refs;  // a number of refs to this entry
                  // cache itself is counted as 1

  // Include the following flags:
  //   in_cache:    whether this entry is referenced by the hash table.
  //   is_high_pri: whether this entry is high priority entry.
  //   in_high_pri_pool: whether this entry is in high-pri pool.
  char flags;

  uint32_t hash;  // Hash of key(); used for fast sharding and comparisons

  char key_data[1];  // Beginning of key

  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) {
      return *(reinterpret_cast<Slice*>(value));
    } else {
      return Slice(key_data, key_length);
    }
  }

  bool InCache() { return flags & 1; }
  bool IsHighPri() { return flags & 2; }
  bool InHighPriPool() { return flags & 4; }
  bool HasHit() { return flags & 8; }

  void SetInCache(bool in_cache) {
    if (in_cache) {
      flags |= 1;
    } else {
      flags &= ~1;
    }
  }

  void SetPriority(Cache::Priority priority) {
    if (priority == Cache::Priority::HIGH) {
      flags |= 2;
    } else {
      flags &= ~2;
    }
  }

  void SetInHighPriPool(bool in_high_pri_pool) {
    if (in_high_pri_pool) {
      flags |= 4;
    } else {
      flags &= ~4;
    }
  }

  void SetHit() { flags |= 8; }

  void Free() {
    assert((refs == 1 && InCache()) || (refs == 0 && !InCache()));
    if (deleter) {
      (*deleter)(key(), value);
    }
    delete[] reinterpret_cast<char*>(this);
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class LRUHandleTable {
 public:
  LRUHandleTable();
  ~LRUHandleTable();

  LRUHandle* Lookup(const Slice& key, uint32_t hash);
  LRUHandle* Insert(LRUHandle* h);
  LRUHandle* Remove(const Slice& key, uint32_t hash);

  template <typename T>
  void ApplyToAllCacheEntries(T func) {
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        auto n = h->next_hash;
        assert(h->InCache());
        func(h);
        h = n;
      }
    }
  }

 private:
  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash);

  void Resize();

  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  LRUHandle** list_;
  uint32_t length_;
  uint32_t elems_;
};

// A single shard of sharded cache.
template <class CacheMonitor>
class ALIGN_AS(CACHE_LINE_SIZE) LRUCacheShardTemplate : public CacheMonitor,
                                                        public CacheShard {
  using CacheMonitor::high_pri_pool_usage_;
  using CacheMonitor::HighPriPoolUsageAdd;
  using CacheMonitor::HighPriPoolUsageSub;
  using CacheMonitor::lru_usage_;
  using CacheMonitor::LRUUsageAdd;
  using CacheMonitor::LRUUsageSub;
  using CacheMonitor::usage_;
  using CacheMonitor::UsageAdd;
  using CacheMonitor::UsageSub;

 public:
  using MonitorOptions = typename CacheMonitor::Options;
  LRUCacheShardTemplate(size_t capacity, bool strict_capacity_limit,
                        double high_pri_pool_ratio,
                        const typename CacheMonitor::Options& options);
  virtual ~LRUCacheShardTemplate();

  // Separate from constructor so caller can easily make an array of LRUCache
  // if current usage is more than new capacity, the function will attempt to
  // free the needed space
  virtual void SetCapacity(size_t capacity) override;

  // Set the flag to reject insertion if cache if full.
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override;

  // Set percentage of capacity reserved for high-pri cache entries.
  void SetHighPriorityPoolRatio(double high_pri_pool_ratio);

  // Like Cache methods, but with an extra "hash" parameter.
  virtual Status Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value),
                        Cache::Handle** handle,
                        Cache::Priority priority) override;
  virtual Cache::Handle* Lookup(const Slice& key, uint32_t hash) override;
  virtual bool Ref(Cache::Handle* handle) override;
  virtual bool Release(Cache::Handle* handle,
                       bool force_erase = false) override;
  virtual void Erase(const Slice& key, uint32_t hash) override;

  // Although in some platforms the update of size_t is atomic, to make sure
  // GetUsage() and GetPinnedUsage() work correctly under any platform, we'll
  // protect them with mutex_.

  virtual size_t GetUsage() const override;
  virtual size_t GetPinnedUsage() const override;

  virtual void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                      bool thread_safe) override;

  virtual void EraseUnRefEntries() override;

  virtual std::string GetPrintableOptions() const override;

  void TEST_GetLRUList(LRUHandle** lru, LRUHandle** lru_low_pri);

  //  Retrieves number of elements in LRU, for unit test purpose only
  //  not threadsafe
  size_t TEST_GetLRUSize();

  //  Retrives high pri pool ratio
  double GetHighPriPoolRatio();

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Insert(LRUHandle* e);

  // Overflow the last entry in high-pri pool to low-pri pool until size of
  // high-pri pool is no larger than the size specify by high_pri_pool_pct.
  void MaintainPoolSize();

  // Just reduce the reference count by 1.
  // Return true if last reference
  bool Unref(LRUHandle* e);

  // Free some space following strict LRU policy until enough space
  // to hold (usage_ + charge) is freed or the lru list is empty
  // This function is not thread safe - it needs to be executed while
  // holding the mutex_
  void EvictFromLRU(size_t charge, autovector<LRUHandle*>* deleted);

  // Initialized before use.
  size_t capacity_;

  // Whether to reject insertion if cache reaches its full capacity.
  bool strict_capacity_limit_;

  // Ratio of capacity reserved for high priority cache entries.
  double high_pri_pool_ratio_;

  // High-pri pool size, equals to capacity * high_pri_pool_ratio.
  // Remember the value to avoid recomputing each time.
  double high_pri_pool_capacity_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // LRU contains items which can be evicted, ie reference only by cache
  LRUHandle lru_;

  // Pointer to head of low-pri pool in LRU list.
  LRUHandle* lru_low_pri_;

  // ------------^^^^^^^^^^^^^-----------
  // Not frequently modified data members
  // ------------------------------------
  //
  // We separate data members that are updated frequently from the ones that
  // are not frequently updated so that they don't share the same cache line
  // which will lead into false cache sharing
  //
  // ------------------------------------
  // Frequently modified data members
  // ------------vvvvvvvvvvvvv-----------
  LRUHandleTable table_;

  // mutex_ protects the following state.
  // We don't count mutex_ as the cache's internal state so semantically we
  // don't mind mutex_ invoking the non-const actions.
  mutable port::Mutex mutex_;
};

class LRUCacheNoMonitor {
 public:
  struct Options {};

 protected:
  LRUCacheNoMonitor(const Options&)
      : high_pri_pool_usage_(0), usage_(0), lru_usage_(0) {}

  void HighPriPoolUsageAdd(const LRUHandle* h) {
    high_pri_pool_usage_ += h->charge;
  }
  void HighPriPoolUsageSub(const LRUHandle* h) {
    high_pri_pool_usage_ -= h->charge;
  }
  void UsageAdd(const LRUHandle* h) { usage_ += h->charge; }
  void UsageSub(const LRUHandle* h) { usage_ -= h->charge; }
  void LRUUsageAdd(const LRUHandle* h) { lru_usage_ += h->charge; }
  void LRUUsageSub(const LRUHandle* h) { lru_usage_ -= h->charge; }

  // Memory size for entries in high-pri pool.
  size_t high_pri_pool_usage_;
  // Memory size for entries residing in the cache
  size_t usage_;
  // Memory size for entries residing only in the LRU list
  size_t lru_usage_;
};

class LRUCacheDiagnosableMonitor {
 public:
  struct Options {
    size_t top_k;
  };

 protected:
  LRUCacheDiagnosableMonitor(const Options& opt)
      : high_pri_pool_usage_(0), usage_(0), lru_usage_(0), k_(opt.top_k) {}

  void HighPriPoolUsageAdd(const LRUHandle* h) {
    high_pri_pool_usage_ += h->charge;
    mu_.Lock();
    TopKAdd(topk_in_hpp_, h);
    mu_.Unlock();
    high_pri_add_count.fetch_add(1);
  }
  void HighPriPoolUsageSub(const LRUHandle* h) {
    high_pri_pool_usage_ -= h->charge;
    mu_.Lock();
    TopKSub(topk_in_hpp_, h);
    mu_.Unlock();
    high_pri_del_count.fetch_add(1);
  }
  void UsageAdd(const LRUHandle* h) {
    usage_ += h->charge;
    assert(h->refs > 0);
    mu_.Lock();
    TopKAdd(topk_in_all_, h);
    mu_.Unlock();
    element_new_count.fetch_add(1);
  }
  void UsageSub(const LRUHandle* h) {
    usage_ -= h->charge;
    assert(h->refs == 0);
    mu_.Lock();
    TopKSub(topk_in_hpp_, h);
    mu_.Unlock();
    element_del_count.fetch_add(1);
  }
  void LRUUsageAdd(const LRUHandle* h) {
    lru_usage_ += h->charge;
    mu_.Lock();
    TopKAdd(topk_in_lru_, h);
    mu_.Unlock();
    insert_lru_count.fetch_add(1);
  }
  void LRUUsageSub(const LRUHandle* h) {
    lru_usage_ -= h->charge;
    mu_.Lock();
    TopKSub(topk_in_lru_, h);
    mu_.Unlock();
    remove_lru_count.fetch_add(1);
  }

 public:
  std::string DumpDiagnoseInfo() {
    std::stringstream stat;
    stat << "total insert delta: "
         << GetDelta(element_new_count, last_element_new_count) << std::endl;
    stat << "total delete delta: "
         << GetDelta(element_del_count, last_element_del_count) << std::endl;
    stat << "lru   insert delta: "
         << GetDelta(insert_lru_count, last_insert_lru_count) << std::endl;
    stat << "lr    delete delta: "
         << GetDelta(remove_lru_count, last_remove_lru_count) << std::endl;
    stat << "highp insert delta: "
         << GetDelta(high_pri_add_count, last_high_pri_add_count) << std::endl;
    stat << "highp delete delta: "
         << GetDelta(high_pri_del_count, last_high_pri_del_count) << std::endl;
    stat << "topk in total: " << TopKInfo(topk_in_all_) << std::endl;
    stat << "topk in lru  : " << TopKInfo(topk_in_lru_) << std::endl;
    stat << "topk in highp: " << TopKInfo(topk_in_hpp_) << std::endl;
    return stat.str();
  }

 protected:
  void TopKAdd(std::map<size_t, size_t>& s, const LRUHandle* h) {
    auto findit = s.find(h->charge);
    if (findit != s.end()) {
      findit->second++;
    } else {
      s[h->charge] = 1;
    }
    if (s.size() > k_) {
      s.erase(s.begin());
    }
  }

  // since there is no unique-id for every charge_size in topkset, here just
  // delete one same charge_size if there are more than one
  // TODO  maybe use LRUHandle* as it unique_id, but LRUHandle* is a Pointer
  // that may be free before it remove from set, which may cause
  // head_use_after_free bug
  void TopKSub(std::map<size_t, size_t>& s, const LRUHandle* h) {
    auto findit = s.find(h->charge);
    if (findit == s.end()) return;
    findit->second--;
    if (findit->second == 0) {
      s.erase(findit);
    }
  }

  uint64_t GetDelta(std::atomic<uint64_t>& cur, std::atomic<uint64_t>& last) {
    auto tmp = cur.load();
    auto delta = tmp - last.load();
    last.store(tmp);
    return tmp;
  }
  std::string TopKInfo(std::map<size_t, size_t>& topk) {
    std::string res{"["};
    for (auto e : topk) {
      res.append("(" + std::to_string(e.first) + "," +
                 std::to_string(e.second) + ")" + ",");
    }
    if (res.back() == ',') res.pop_back();
    res.append("]");
    return res;
  }

  std::atomic<uint64_t> insert_lru_count{0};
  std::atomic<uint64_t> remove_lru_count{0};
  std::atomic<uint64_t> element_new_count{0};
  std::atomic<uint64_t> element_del_count{0};
  std::atomic<uint64_t> high_pri_add_count{0};
  std::atomic<uint64_t> high_pri_del_count{0};

  std::atomic<uint64_t> last_insert_lru_count{0};
  std::atomic<uint64_t> last_remove_lru_count{0};
  std::atomic<uint64_t> last_element_new_count{0};
  std::atomic<uint64_t> last_element_del_count{0};
  std::atomic<uint64_t> last_high_pri_add_count{0};
  std::atomic<uint64_t> last_high_pri_del_count{0};

  std::map<size_t, size_t> topk_in_hpp_;
  std::map<size_t, size_t> topk_in_all_;
  std::map<size_t, size_t> topk_in_lru_;

  // std::mutex mu_;  // for topK update
  mutable port::Mutex mu_;

  // Memory size for entries in high-pri pool.
  size_t high_pri_pool_usage_;
  // Memory size for entries residing in the cache
  size_t usage_;
  // Memory size for entries residing only in the LRU list
  size_t lru_usage_;

  const size_t k_;
};

template <class LRUCacheShardType>
class LRUCacheBase : public ShardedCache {
 public:
  LRUCacheBase(size_t capacity, int num_shard_bits, bool strict_capacity_limit,
               double high_pri_pool_ratio,
               const typename LRUCacheShardType::MonitorOptions& options,
               std::shared_ptr<MemoryAllocator> memory_allocator = nullptr);
  virtual ~LRUCacheBase();
  virtual const char* Name() const override;
  virtual CacheShard* GetShard(int shard) override;
  virtual const CacheShard* GetShard(int shard) const override;
  virtual void* Value(Handle* handle) override;
  virtual size_t GetCharge(Handle* handle) const override;
  virtual uint32_t GetHash(Handle* handle) const override;
  virtual void DisownData() override;
  virtual std::string DumpLRUCacheStatistics();

  //  Retrieves number of elements in LRU, for unit test purpose only
  size_t TEST_GetLRUSize();
  //  Retrives high pri pool ratio
  double GetHighPriPoolRatio() {
    double result = 0.0;
    if (num_shards_ > 0) {
      result = shards_[0].GetHighPriPoolRatio();
    }
    return result;
  }

 private:
  LRUCacheShardType* shards_ = nullptr;
  int num_shards_ = 0;
};

using LRUCacheShard = LRUCacheShardTemplate<LRUCacheNoMonitor>;
using LRUCacheDiagnosableShard =
    LRUCacheShardTemplate<LRUCacheDiagnosableMonitor>;

using LRUCache = LRUCacheBase<LRUCacheShard>;
using DiagnosableLRUCache = LRUCacheBase<LRUCacheDiagnosableShard>;

}  // namespace rocksdb
