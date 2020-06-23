#include "cache/lirs_cache.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include <string>

#include "util/mutexlock.h"

namespace rocksdb {

LIRSHandleTable::LIRSHandleTable() : list_(nullptr), length_(0), elems_(0) {
  Resize();
}

LIRSHandleTable::~LIRSHandleTable() {
  ApplyToAllCacheEntries([](LIRSHandle* h) {
    if (h->refs == 1) {
      h->Free();
    }
  });
  delete[] list_;
}

LIRSHandle* LIRSHandleTable::Lookup(const Slice& key, uint32_t hash) {
  return *FindPointer(key, hash);
}

LIRSHandle* LIRSHandleTable::Insert(LIRSHandle* h) {
  LIRSHandle** ptr = FindPointer(h->key(), h->hash);
  LIRSHandle* old = *ptr;
  h->next_hash = (old == nullptr ? nullptr : old->next_hash);
  *ptr = h;
  if (old == nullptr) {
    ++elems_;
    if (elems_ > length_) {
      Resize();
    }
  }
  return old;
}

LIRSHandle* LIRSHandleTable::Remove(const Slice& key, uint32_t hash) {
  LIRSHandle** ptr = FindPointer(key, hash);
  LIRSHandle* result = *ptr;
  if (result != nullptr) {
    *ptr = result->next_hash;
    --elems_;
  }
  return result;
}

LIRSHandle** LIRSHandleTable::FindPointer(const Slice& key, uint32_t hash) {
  LIRSHandle** ptr = &list_[hash & (length_ - 1)];
  while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
    ptr = &(*ptr)->next_hash;
  }
  return ptr;
}

void LIRSHandleTable::Resize() {
  uint32_t new_length = 16;
  while (new_length < elems_ * 1.5) {
    new_length *= 2;
  }
  LIRSHandle** new_list = new LIRSHandle*[new_length];
  memset(new_list, 0, sizeof(new_list[0]) * new_length);
  uint32_t count = 0;
  for (uint32_t i = 0; i < length_; i++) {
    LIRSHandle* h = list_[i];
    while (h != nullptr) {
      LIRSHandle* next = h->next_hash;
      uint32_t hash = h->hash;
      LIRSHandle** ptr = &new_list[hash & (new_length - 1)];
      h->next_hash = *ptr;
      *ptr = h;
      h = next;
      count++;
    }
  }
  assert(elems_ == count);
  delete[] list_;
  list_ = new_list;
  length_ = new_length;
}

LIRSCacheShard::LIRSCacheShard(size_t capacity, bool strict_capacity_limit,
                               double irr_ratio)
    : capacity_(0),
      strict_capacity_limit_(strict_capacity_limit),
      irr_ratio_(irr_ratio),
      usage_(0) {
  SetCapacity(capacity);
}

LIRSCacheShard::~LIRSCacheShard() {}

bool bool LIRSCacheShard::Unref(LIRSHandle* h) {
  assert(h->refs > 0);
  h->refs--;
  return h->refs == 0;
}

void LIRSCacheShard::EraseUnRefEntries() {
  autovector<LIRSHandle*> last_reference_list;
  {
    MutexLock l(&mutex_);
    while (head_.next_s != &LIRS_) {
      LIRSHandle* old = LIRS_.next;
      assert(old->InCache());
      assert(old->refs ==
             1);  // LIRS list contains elements which may be evicted
      LIRS_Remove(old);
      table_.Remove(old->key(), old->hash);
      old->SetInCache(false);
      Unref(old);
      usage_ -= old->charge;
      last_reference_list.push_back(old);
    }
  }

  for (auto entry : last_reference_list) {
    entry->Free();
  }
}

void LIRSCacheShard::ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                            bool thread_safe) {
  if (thread_safe) {
    mutex_.Lock();
  }
  table_.ApplyToAllCacheEntries(
      [callback](LIRSHandle* h) { callback(h->value, h->charge); });
  if (thread_safe) {
    mutex_.Unlock();
  }
}

bool LIRSCacheShard::Release(Cache::Handle* handle, bool force_erase) {
  if (handle == nullptr) {
    return false;
  }
  LIRSHandle* e = reinterpret_cast<LIRSHandle*>(handle);
  bool last_reference = false;
  {
    MutexLock l(&mutex_);
    last_reference = Unref(e);
    if (last_reference) {
      usage_ -= e->charge;
    }
    if (e->refs == 1 && e->InCache()) {
      // The item is still in cache, and nobody else holds a reference to it
      if (usage_ > capacity_ || force_erase) {
        // the cache is full
        // The LIRS list must be empty since the cache is full
        assert(!(usage_ > capacity_) || LIRS_.next == &LIRS_);
        // take this opportunity and remove the item
        table_.Remove(e->key(), e->hash);
        e->SetInCache(false);
        Unref(e);
        usage_ -= e->charge;
        last_reference = true;
      } else {
        // put the item on the list to be potentially freed
        LIRS_Insert(e);
      }
    }
  }

  // free outside of mutex
  if (last_reference) {
    e->Free();
  }
  return last_reference;
}

Status LIRSCacheShard::Insert(const Slice& key, uint32_t hash, void* value,
                              size_t charge,
                              void (*deleter)(const Slice& key, void* value),
                              Cache::Handle** handle,
                              Cache::Priority priority) {
  LIRSHandle* e = reinterpret_cast<LIRSHandle*>(
      new char[sizeof(LIRSHandle) - 1 + key.size()]);
  Status s;
  autovector<LIRSHandle*> last_reference_list;

  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->flags = 0;
  e->hash = hash;
  e->refs = (handle == nullptr ? 1 : 2);
  e->next = e->prev = nullptr;
  e->SetInCache(true);
  e->SetPriority(priority);
  memcpy(e->key_data, key.data(), key.size());

  {
    MutexLock l(&mutex_);
    EvictFromLIRS(charge, &last_reference_list);
    if (usage_ - lirs_usage_ + charge > capacity_ &&
        (strict_capacity_limit_ || handle == nullptr)) {
      if (handle == nullptr) {
        last_reference_list.push_back(e);
      } else {
        delete[] reinterpret_cast<char*>(e);
        *handle = nullptr;
        s = Status::Incomplete("Insert failed due to LIRS cache being full.");
      }
    } else {
      LIRSHandle* old = table_.Insert(e);
      usage_ += e->charge;
      if (old != nullptr) {
        old->SetInCache(false);
        if (Unref(old)) {
          usage_ -= old->charge;
          LIRS_Remove(old);
          last_reference_list.push_back(old);
        }
      }
      if (handle == nullptr) {
        LIRS_Insert(e);
      } else {
        *handle = reinterpret_cast<Cache::Handle*>(e);
      }
      s = Status::OK();
    }
  }
  for (auto entry : last_reference_list) {
    entry->Free();
  }

  return s;
}

void LIRSCacheShard::Erase(const Slice& key, uint32_t hash) {
  LIRSHandle* e;
  bool last_reference = false;
  {
    MutexLock l(&mutex_);
    e = table_.Remove(key, hash);
    if (e != nullptr) {
      last_reference = Unref(e);
      if (last_reference) {
        usage_ -= e->charge;
      }
      if (last_reference && e->InCache()) {
        LIRS_Remove(e);
      }
      e->SetInCache(false);
    }
  }

  // mutex not held here
  // last_reference will only be true if e != nullptr
  if (last_reference) {
    e->Free();
  }
}

size_t LIRSCacheShard::GetUsage() const {
  MutexLock l(&mutex_);
  return usage_;
}

size_t LIRSCacheShard::GetPinnedUsage() const {
  MutexLock l(&mutex_);
  assert(usage_ >= lirs_usage_);
  return usage_ - lirs_usage_;
}

std::string LIRSCacheShard::GetPrintableOptions() const {
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  {
    MutexLock l(&mutex_);
    snprintf(buffer, kBufferSize, "    irr_ratio : %.3lf\n", irr_ratio_);
  }
  return std::string(buffer);
}

LIRSCache::LIRSCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double irr_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator = nullptr) {
  num_shards_ = 1 << num_shard_bits;
  shards_ = reinterpret_cast<LIRSCacheShard*>(
      port::cacheline_aligned_alloc(sizeof(LIRSCacheShard) * num_shards_));
  size_t size_per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
  for (int i = 0; i < num_shards_; i++) {
    new (&shards_[i])
        LIRSCacheShard(size_per_shard, strict_capacity_limit, irr_ratio_);
  }
}

LIRSCache::~LIRSCache() {
  if (shards_ != nullptr) {
    assert(num_shards_ > 0);
    for (int i = 0; i < num_shards_; i++) {
      shards_[i].~LIRSCacheShard();
    }
    port::cacheline_aligned_free(shards_);
  }
}

CacheShard* LIRSCache::GetShard(int shard) {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

const CacheShard* LIRSCache::GetShard(int shard) const {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

void* LIRSCache::Value(Handle* handle) {
  return reinterpret_cast<const LIRSHandle*>(handle)->value;
}

size_t LIRSCache::GetCharge(Handle* handle) const {
  return reinterpret_cast<const LIRSHandle*>(handle)->charge;
}

uint32_t LIRSCache::GetHash(Handle* handle) const {
  return reinterpret_cast<const LIRSHandle*>(handle)->hash;
}

std::shared_ptr<Cache> NewLIRSCache(const LIRSCacheOptions& cache_opts) {
  return NewLIRSCache(cache_opts.capacity, cache_opts.num_shard_bits,
                      cache_opts.strict_capacity_limit, cache_opts.irr_ratio,
                      cache_opts.memory_allocator);
}

std::shared_ptr<Cache> NewLIRSCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double irr_ratio, std::shared_ptr<MemoryAllocator> memory_allocator) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  if (irr_ratio < 0.0 || irr_ratio > 1.0) {
    // invalid irr_ratio
    return nullptr;
  }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return std::make_shared<LIRSCache>(capacity, num_shard_bits,
                                     strict_capacity_limit, irr_ratio,
                                     std::move(memory_allocator));
}

}  // namespace rocksdb