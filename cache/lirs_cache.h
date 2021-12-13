#pragma once

#include <string>

#include "cache/sharded_cache.h"
#include "port/port.h"
#include "rocksdb/terark_namespace.h"
#include "util/autovector.h"

namespace TERARKDB_NAMESPACE {

struct LIRSHandle {
  void* value;
  void (*deleter)(const Slice&, void* value);
  LIRSHandle* next_hash;
  LIRSHandle* next_stack;
  LIRSHandle* prev_stack;
  LIRSHandle* next_queue;
  LIRSHandle* prev_queue;
  size_t charge;
  size_t key_length;
  uint32_t refs;
  uint32_t hash;  // Hash of key(); used for fast sharding and comparisons

  enum State { kRemote = 0, kLIR, kHIR, kNHIR, kInvalid } state;

  char key_data[1];  // Beginning of key

  Slice key() const { return Slice(key_data, key_length); }

  bool Remote() { return state == kRemote; }
  bool LIR() { return state == kLIR; }
  bool HIR() { return state == kHIR; }
  bool InCache() { return state == kLIR || state == kHIR || state == kNHIR; }
  bool NHIR() { return state == kNHIR; }
  bool Valid() { return state != kInvalid; }

  void SetRemote() { state = kRemote; }
  void SetLIR() { state = kLIR; }
  void SetHIR() { state = kHIR; }
  void SetNHIR() { state = kNHIR; }
  void SetInvalid() { state = kInvalid; }

  void Free() {
    assert((refs == 1 && InCache()) || (refs == 0 && !InCache()));
    if (deleter) {
      (*deleter)(key(), value);
    }
    delete[] reinterpret_cast<char*>(this);
  }
};

class LIRSHandleTable {
 public:
  LIRSHandleTable();
  ~LIRSHandleTable();

  LIRSHandle* Lookup(const Slice& key, uint32_t hash);
  LIRSHandle* Insert(LIRSHandle* h);
  LIRSHandle* Remove(const Slice& key, uint32_t hash);

  template <typename T>
  void ApplyToAllCacheEntries(T func) {
    for (uint32_t i = 0; i < length_; i++) {
      LIRSHandle* h = list_[i];
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
  LIRSHandle** FindPointer(const Slice& key, uint32_t hash);

  void Resize();

  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  LIRSHandle** list_;
  uint32_t length_;
  uint32_t elems_;
};

class ALIGN_AS(CACHE_LINE_SIZE) LIRSCacheShard : public CacheShard {
 public:
  LIRSCacheShard(size_t capacity, bool strict_capacity_limit,
                 double irr_ratio = 0.9);
  virtual ~LIRSCacheShard();

  virtual void SetCapacity(size_t capacity) override;
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override;

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

  virtual size_t GetUsage() const override;
  virtual size_t GetPinnedUsage() const override;

  virtual void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                      bool thread_safe) override;

  virtual void EraseUnRefEntries() override;

  virtual std::string GetPrintableOptions() const override;

 private:
  void PushToQueue(LIRSHandle* h);
  void RemoveFromQueue(LIRSHandle* h);
  void AdjustToQueueTail(LIRSHandle* h);
  void ShiftQueueHead();
  void PushToStack(LIRSHandle* h);
  void RemoveFromStack(LIRSHandle* h);
  void AdjustToStackTop(LIRSHandle* h);
  void AdjustStackBottom();
  void StackPruning();
  void LIRS_Remove(LIRSHandle* h);
  void LIRS_Insert(LIRSHandle* h);
  bool Unref(LIRSHandle* h);
  void EvictFromLIRS(size_t charge, autovector<LIRSHandle*>* deleted);

  size_t capacity_;
  size_t stack_capacity_;
  size_t usage_;
  size_t stack_usage_;
  double irr_ratio_;
  LIRSHandle cache_;
  LIRSHandleTable table_;
  bool strict_capacity_limit_;
  mutable port::Mutex mutex_;
};

class LIRSCache : public ShardedCache {
 public:
  LIRSCache(size_t capacity, int num_shard_bits, bool strict_capacity_limit,
            double irr_ratio,
            std::shared_ptr<MemoryAllocator> memory_allocator = nullptr);
  virtual ~LIRSCache();
  virtual const char* Name() const override { return "LIRSCache"; }
  virtual CacheShard* GetShard(int shard) override;
  virtual const CacheShard* GetShard(int shard) const override;
  virtual void* Value(Handle* handle) override;
  virtual size_t GetCharge(Handle* handle) const override;
  virtual uint32_t GetHash(Handle* handle) const override;
  virtual void DisownData() override;

 private:
  LIRSCacheShard* shards_ = nullptr;
  int num_shards_ = 0;
};

}  // namespace TERARKDB_NAMESPACE