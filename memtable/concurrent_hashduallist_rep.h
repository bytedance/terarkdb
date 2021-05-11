#pragma once
#ifndef ROCKSDB_LITE
#include <list>

#include "monitoring/instrumented_mutex.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

class ConcurrentHashDualListReqFactory : public MemTableRepFactory {
  using Pointer = std::atomic<void*>;

 public:
  struct BucketCleaner {
    BucketCleaner(ConcurrentHashDualListReqFactory* factory)
        : factory_(factory) {}

    void operator()(Pointer* ptr) {
      bool purge = false;
      if (factory_ && ptr) {
        for (size_t i = 0; i < factory_->bucket_count_; ++i) {
          ptr[i].store(nullptr, std::memory_order_relaxed);
        }
        InstrumentedMutexLock l(&factory_->mutex_);
        if (factory_->preallocated_buckets_.size() <
            factory_->num_hash_buckets_preallocated_) {
          factory_->preallocated_buckets_.emplace_back(ptr);
        } else {
          purge = true;
        }
      } else {
        purge = true;
      }
      if (purge) {
        delete[] ptr;
      }
    }

    ConcurrentHashDualListReqFactory* const factory_;
  };

  ConcurrentHashDualListReqFactory(size_t bucket_count,
                                   size_t huge_page_tlb_size,
                                   int bucket_entries_logging_threshold,
                                   size_t num_hash_buckets_preallocated,
                                   bool if_log_bucket_dist_when_flush)
      : bucket_count_(bucket_count),
        huge_page_tlb_size_(huge_page_tlb_size),
        bucket_entries_logging_threshold_(bucket_entries_logging_threshold),
        num_hash_buckets_preallocated_(num_hash_buckets_preallocated),
        if_log_bucket_dist_when_flush_(if_log_bucket_dist_when_flush),
        bucket_cleaner_(this) {
    for (size_t i = 0; i < num_hash_buckets_preallocated_; ++i) {
      Pointer* mem = new Pointer[bucket_count_];
      for (size_t j = 0; j < bucket_count_; ++j) {
        mem[j].store(nullptr, std::memory_order_relaxed);
      }
      preallocated_buckets_.emplace_back(mem);
    }
  }

  virtual bool IsInsertConcurrentlySupported() const override { return true; }

  virtual bool CanHandleDuplicatedKey() const override { return false; }

  virtual ~ConcurrentHashDualListReqFactory() = default;

  using MemTableRepFactory::CreateMemTableRep;
  virtual MemTableRep* CreateMemTableRep(
      const MemTableRep::KeyComparator& compare, bool needs_dup_key_check,
      Allocator* allocator, const SliceTransform* transform,
      Logger* logger) override;

  virtual const char* Name() const override {
    return "ConcurrentHashDualListReqFactory";
  }

  virtual bool IsPrefixExtractorRequired() const override { return true; }

 private:
  const size_t bucket_count_;
  const size_t huge_page_tlb_size_;
  int bucket_entries_logging_threshold_;
  const size_t num_hash_buckets_preallocated_;
  bool if_log_bucket_dist_when_flush_;
  BucketCleaner bucket_cleaner_;
  std::list<std::unique_ptr<Pointer[]>> preallocated_buckets_;
  InstrumentedMutex mutex_;
};

}  // namespace TERARKDB_NAMESPACE

#endif
