//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <iostream>
#include <mutex>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>

#include "cache/sharded_cache.h"
#include "port/port.h"
#include "rocksdb/terark_namespace.h"
#include "util/autovector.h"
#include "util/mutexlock.h"

#ifdef WITH_DIAGNOSE_CACHE
#include <terark/heap_ext.hpp>
#endif  // !NDEBUG

namespace TERARKDB_NAMESPACE {

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
  virtual bool Erase(const Slice& key, uint32_t hash) override;

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

  std::string DumpDiagnoseInfo() {
    std::stringstream stat;
    stat << "usage in total: " << usage_ << std::endl;
    stat << "usage in lru  : " << lru_usage_ << std::endl;
    stat << "usage in highp: " << high_pri_pool_usage_ << std::endl;
    return stat.str();
  }

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

#ifdef WITH_DIAGNOSE_CACHE
class LRUCacheDiagnosableMonitor {
 public:
  struct Options {
    size_t top_k;
  };
  class TopSet {
   public:
    struct DataElement {
      DataElement(std::string&& k, size_t ch, size_t co, size_t idxheap)
          : key(k), total_charge(ch), count(co), idx_in_heap_vec(idxheap) {}
      std::string
          key;  // key from LRUHandle* can be deleted before we do not need it
      size_t total_charge;
      size_t count;  // key remove from hashtable, may not be deleted since its
                     // has refs
      size_t idx_in_heap_vec;
    };

    using DataIdx = size_t;

    struct ChargeCmp {
      ChargeCmp(std::deque<DataElement>& d) : data_(d) {}
      bool operator()(const DataIdx& l, const DataIdx& r) {
        // MaxHeap
        return data_[l].total_charge < data_[r].total_charge;
      }

     private:
      const std::deque<DataElement>& data_;
    };

    struct SyncIndex {
      SyncIndex(std::deque<DataElement>& d) : data_(d) {}
      void operator()(DataIdx di, size_t new_idx) {
        data_[di].idx_in_heap_vec = new_idx;
      }

     private:
      std::deque<DataElement>& data_;
    };

    TopSet(size_t k)
        : k_(k),
          data_storage_(),
          charge_cmp_(data_storage_),
          sync_idx_(data_storage_) {}

    void Add(const LRUHandle* h) {
      mu_.Lock();
      auto findit = key_map_.find(h->key());
      size_t heap_idx = size_t(-1);

      if (findit != key_map_.end()) {
        // same key merge into one item
        heap_idx = data_storage_[findit->second].idx_in_heap_vec;
        size_t data_idx = findit->second;
        data_storage_[data_idx].total_charge += h->charge;
        data_storage_[data_idx].count++;
      } else {
        // new key append to tail
        heap_idx = data_heap_.size();
        DataElement de{h->key().ToString(), h->charge, 1, heap_idx};
        DataIdx new_idx = data_storage_.size();
        data_storage_.push_back(de);
        bool ok = key_map_.insert({data_storage_.back().key, new_idx}).second;
        assert(ok);

        data_heap_.push_back(new_idx);
      }

      assert(VerifyIdx());
      terark::terark_heap_hole_up(data_heap_.begin(), heap_idx, size_t(0),
                                  data_heap_[heap_idx], charge_cmp_, sync_idx_);
      assert(VerifyIdx());

      assert(key_map_.size() == data_heap_.size() &&
             key_map_.size() == data_storage_.size());
      mu_.Unlock();
    }

    void Sub(const LRUHandle* h) {
      mu_.Lock();
      auto findit = key_map_.find(h->key());
      if (findit != key_map_.end()) {
        size_t data_idx = findit->second;
        data_storage_[data_idx].total_charge -= h->charge;
        data_storage_[data_idx].count--;

        if (data_storage_[data_idx].count == 0) {
          bool is_last_one = data_idx == data_storage_.size() - 1;
          // delete heap item, change deleted-item to backvalue, adjust heap and
          // pop back
          size_t delete_hole = data_storage_[data_idx].idx_in_heap_vec;
          assert(VerifyIdx());
          terark::terark_adjust_heap(data_heap_.begin(), delete_hole,
                                     data_heap_.size() - 1, data_heap_.back(),
                                     charge_cmp_, sync_idx_);
          data_heap_.pop_back();

          // delete key map item
          assert(findit->second == data_idx);
          key_map_.erase(findit);
          auto findlast = key_map_.find(data_storage_.back().key);
          assert(is_last_one || key_map_.empty() || findlast != key_map_.end());

          // delete storage item
          if (!is_last_one) {
            // if not last one, need swap with back, change heap item and
            // reload key_map's slice
            data_storage_[data_idx] = data_storage_.back();
            data_heap_[data_storage_[data_idx].idx_in_heap_vec] = data_idx;

            key_map_.erase(findlast);
            key_map_.insert({data_storage_[data_idx].key, data_idx});
          }
          data_storage_.pop_back();

          assert(VerifyIdx());
        }
      }
      assert(key_map_.size() == data_heap_.size() &&
             key_map_.size() == data_storage_.size());
      mu_.Unlock();
    }

    std::string Info() {
      mu_.Lock();
      std::string res{"["};
      size_t length = data_heap_.size();
      for (size_t i = 0; i < k_ && i < data_heap_.size(); i++) {
        assert(VerifyIdx());
        terark::pop_heap_keep_top(data_heap_.begin(), length - i, charge_cmp_,
                                  sync_idx_);
        assert(VerifyIdx());
        size_t last_idx = length - i - 1;
        auto& e = data_storage_[data_heap_[last_idx]];

        res.append("(" + Slice(e.key).ToString(true) + "," +
                   std::to_string(e.total_charge) + "," +
                   std::to_string(e.count) + ")" + ",");
        // FIXME code fail here when do recovery, it seems order in my heap is
        // wrong
        assert(i == 0 ||
               e.total_charge <=
                   data_storage_[data_heap_[last_idx + 1]].total_charge);
      }
      for (size_t i = 1; i <= k_ && i < data_heap_.size(); i++) {
        size_t heap_idx = data_heap_.size() - i;
        assert(VerifyIdx());
        terark::terark_heap_hole_up(data_heap_.begin(), heap_idx, size_t(0),
                                    data_heap_[heap_idx], charge_cmp_,
                                    sync_idx_);
        assert(VerifyIdx());
      }
      if (res.back() == ',') res.pop_back();
      res.append("]");

      mu_.Unlock();
      return res;
    }

    size_t Size() { return data_storage_.size(); }

    const std::deque<DataElement>& TEST_get_elements_storage() {
      return data_storage_;
    }
    const std::unordered_map<Slice, DataIdx, SliceHasher>&
    TEST_get_elements_map() {
      return key_map_;
    }
    const std::vector<DataIdx> TEST_get_data_heap() { return data_heap_; }
    const ChargeCmp TEST_get_charge_cmp() { return charge_cmp_; }
    bool VerifyIdx() {
      for (size_t data_idx = 0; data_idx < data_storage_.size(); data_idx++) {
        if (data_heap_[data_storage_[data_idx].idx_in_heap_vec] != data_idx) {
          return false;
        }
      }
      return true;
    }

    static bool CmpWrapper(const Slice& l, const Slice& r) {
      return l.compare(r) == 0;
    }

   private:
    size_t k_;
    std::unordered_map<Slice, DataIdx, SliceHasher> key_map_;
    std::deque<DataElement> data_storage_;
    std::vector<DataIdx> data_heap_;

    ChargeCmp charge_cmp_;
    SyncIndex sync_idx_;
    mutable port::Mutex mu_;
  };

  std::string DumpDiagnoseInfo() {
    std::stringstream stat;
    stat << "usage in total: " << usage_ << std::endl;
    stat << "usage in lru  : " << lru_usage_ << std::endl;
    stat << "usage in highp: " << high_pri_pool_usage_ << std::endl;
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
    stat << "topk in total: " << topk_in_all_.Info() << std::endl;
    stat << "topk in lru  : " << topk_in_lru_.Info() << std::endl;
    stat << "topk in highp: " << topk_in_hpp_.Info() << std::endl;
    stat << "topk pinned!!: " << topk_pinned_.Info() << std::endl;
    return stat.str();
  }

  LRUCacheDiagnosableMonitor(const Options& opt)
      : high_pri_pool_usage_(0),
        usage_(0),
        lru_usage_(0),
        topk_in_hpp_(opt.top_k),
        topk_in_all_(opt.top_k),
        topk_in_lru_(opt.top_k),
        topk_pinned_(opt.top_k) {}

  TopSet& TEST_get_pinned_set() { return topk_pinned_; }
  TopSet& TEST_get_highpri_set() { return topk_in_hpp_; }
  TopSet& TEST_get_lru_set() { return topk_in_lru_; }
  TopSet& TEST_get_total_set() { return topk_in_all_; }

 protected:
  void HighPriPoolUsageAdd(const LRUHandle* h) {
    high_pri_pool_usage_ += h->charge;
    topk_in_hpp_.Add(h);

    high_pri_add_count.fetch_add(1);
  }
  void HighPriPoolUsageSub(const LRUHandle* h) {
    high_pri_pool_usage_ -= h->charge;
    topk_in_hpp_.Sub(h);

    high_pri_del_count.fetch_add(1);
  }
  void UsageAdd(const LRUHandle* h) {
    usage_ += h->charge;
    assert(h->refs > 0);
    topk_in_all_.Add(h);

    element_new_count.fetch_add(1);
  }
  // only when last_reference is true, we call UsageSub and pinned value is also
  // removed
  void UsageSub(const LRUHandle* h) {
    usage_ -= h->charge;
    assert(h->refs == 0);
    topk_in_all_.Sub(h);
    topk_pinned_.Sub(h);

    element_del_count.fetch_add(1);
  }
  void LRUUsageAdd(const LRUHandle* h) {
    lru_usage_ += h->charge;
    topk_in_lru_.Add(h);

    insert_lru_count.fetch_add(1);
  }
  // value that has been lookup or ref increases its refs, when its refs equals
  // one, it removed from lru and increate it refs, so that it is pinned and
  // here we can increate pinned state whenever it remove from lru there are
  void LRUUsageSub(const LRUHandle* h) {
    lru_usage_ -= h->charge;
    topk_in_lru_.Sub(h);
    topk_pinned_.Add(h);

    remove_lru_count.fetch_add(1);
  }

  // Memory size for entries in high-pri pool.
  size_t high_pri_pool_usage_;
  // Memory size for entries residing in the cache
  size_t usage_;
  // Memory size for entries residing only in the LRU list
  size_t lru_usage_;

 private:
  // since using LRUHandle* as it unique_id can cause
  // head_use_after_free bug

  uint64_t GetDelta(std::atomic<uint64_t>& cur, std::atomic<uint64_t>& last) {
    auto tmp = cur.load();
    auto delta = tmp - last.load();
    last.store(tmp);
    return tmp;
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

  TopSet topk_in_hpp_;
  TopSet topk_in_all_;
  TopSet topk_in_lru_;
  TopSet topk_pinned_;
};
#else
using LRUCacheDiagnosableMonitor = LRUCacheNoMonitor;
#endif

template <class LRUCacheShardType>
class LRUCacheBase : public ShardedCache {
 public:
  LRUCacheBase(size_t capacity, int num_shard_bits, bool strict_capacity_limit,
               double high_pri_pool_ratio,
               const typename LRUCacheShardType::MonitorOptions& options = {},
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

}  // namespace TERARKDB_NAMESPACE
