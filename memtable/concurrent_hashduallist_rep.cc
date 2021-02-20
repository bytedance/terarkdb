#ifndef ROCKSDB_LITE
#include "memtable/concurrent_hashduallist_rep.h"

#include <atomic>

#include "db/memtable.h"
#include "memtable/fulllist_iter.h"
#include "memtable/skiplist.h"
#include "monitoring/histogram.h"
#include "port/port.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/terark_namespace.h"
#include "util/arena.h"
#include "util/murmurhash.h"
#include "util/string_util.h"

namespace TERARKDB_NAMESPACE {
namespace {

class ConcurrentHashDualListRep : public MemTableRep {
  struct Node {
    static constexpr uintptr_t vertical_tag = 1ull;

    Node() {
      next_[0].store(nullptr, std::memory_order_relaxed);
      next_[1].store(nullptr, std::memory_order_relaxed);
    }

    void SetLevelNext(Node *x) { next_[0].store(x, std::memory_order_release); }

    void NoBarrier_SetLevelNext(Node *x) {
      next_[0].store(x, std::memory_order_relaxed);
    }

    void SetVerticalNext(Node *x) {
      next_[1].store(x, std::memory_order_release);
    }

    void NoBarrier_SetVerticalNext(Node *x) {
      next_[1].store(x, std::memory_order_relaxed);
    }

    bool CASLevelNext(Node *expected, Node *x) {
      return next_[0].compare_exchange_strong(expected, x,
                                              std::memory_order_relaxed);
    }

    bool CASVerticalNext(Node *expected, Node *x) {
      return next_[1].compare_exchange_strong(expected, x,
                                              std::memory_order_relaxed);
    }

    Node *LevelNext() const {
      auto vp =
          reinterpret_cast<uintptr_t>(next_[0].load(std::memory_order_acquire));
      return reinterpret_cast<Node *>(vp & (~1ull));
    }

    Node *NoBarrier_LevelNext() const {
      auto vp =
          reinterpret_cast<uintptr_t>(next_[0].load(std::memory_order_relaxed));
      return reinterpret_cast<Node *>(vp & (~1ull));
    }

    Node *VerticalNext() const {
      return next_[1].load(std::memory_order_acquire);
    }

    Node *NoBarrier_VerticalNext() const {
      return next_[1].load(std::memory_order_relaxed);
    }

    void MarkNodeVertical(bool concurrent) {
      while (NodeAtLevel()) {
        auto ovp = next_[0].load(std::memory_order_relaxed);
        auto ov = reinterpret_cast<uintptr_t>(ovp);
        auto nvp = reinterpret_cast<Node *>(ov | vertical_tag);
        if (!concurrent) {
          next_[0].store(nvp, std::memory_order_relaxed);
        } else {
          if (next_[0].compare_exchange_weak(ovp, nvp,
                                             std::memory_order_relaxed)) {
            break;
          }
        }
      }
    }

    bool NodeAtLevel() const {
      auto vp = next_[0].load(std::memory_order_acquire);
      auto v = reinterpret_cast<uintptr_t>(vp);
      return !(v & 1);
    }

    Node(const Node &) = delete;
    Node &operator=(const Node &) = delete;

    std::atomic<Node *> next_[2];

    char key[1];
  };

  class DuaLinkListIterator : public MemTableRep::Iterator {
   public:
    DuaLinkListIterator(const ConcurrentHashDualListRep *const memtable_rep,
                        Pointer *bucket = nullptr)
        : memtable_rep_(memtable_rep),
          bucket_(bucket),
          level_node_(nullptr),
          vertical_node_(nullptr) {}

    ~DuaLinkListIterator() = default;

    bool Valid() const override {
      return (bucket_ != nullptr && level_node_ != nullptr &&
              vertical_node_ != nullptr);
    }

    const char *EncodedKey() const override {
      assert(Valid());
      return vertical_node_->key;
    }

    void Next() override {
      assert(Valid());
      vertical_node_ = vertical_node_->VerticalNext();
      if (!vertical_node_) {
        level_node_ = level_node_->LevelNext();
        vertical_node_ = level_node_;
      }
    }

    void Prev() override { assert(false); }

    void Seek(const Slice &internal_key, const char *memtable_key) override {
      SeekToHead();
      while (memtable_rep_->UserKeyIsAfterNode(internal_key, level_node_)) {
        level_node_ = level_node_->LevelNext();
      }
      vertical_node_ = level_node_;
      while (memtable_rep_->KeyIsAfterNode(internal_key, vertical_node_)) {
        vertical_node_ = vertical_node_->VerticalNext();
      }
    }

    void SeekForPrev(const Slice &internal_key,
                     const char *memtable_key) override {
      assert(false);
    }

    void SeekToFirst() override { SeekToHead(); }

    void SeekToLast() override { assert(false); }

    bool IsSeekForPrevSupported() const override { return false; }

   protected:
    void Reset(Pointer *bucket) {
      if (bucket) {
        bucket_ = bucket;
        level_node_ = vertical_node_ =
            reinterpret_cast<Node *>(bucket_->load(std::memory_order_acquire));
      } else {
        level_node_ = vertical_node_ = nullptr;
      }
    }

   private:
    friend class ConcurrentHashDualListRep;
    const ConcurrentHashDualListRep *const memtable_rep_;
    Pointer *bucket_;
    Node *level_node_;
    Node *vertical_node_;

    void SeekToHead() { Reset(bucket_); }
  };

  class DynamicIterator : public DuaLinkListIterator {
   public:
    DynamicIterator(const ConcurrentHashDualListRep *const memtable_rep)
        : DuaLinkListIterator(memtable_rep), memtable_rep_(memtable_rep) {
      assert(memtable_rep_);
    }

    void Seek(const Slice &k, const char *memtable_key) override {
      assert(memtable_rep_);
      Pointer &bucket = memtable_rep_->GetBucket(k);
      Reset(&bucket);
      DuaLinkListIterator::Seek(k, memtable_key);
    }

    bool Valid() const override { return DuaLinkListIterator::Valid(); }

    const char *EncodedKey() const override {
      return DuaLinkListIterator::EncodedKey();
    }

    void Next() override { return DuaLinkListIterator::Next(); }

    bool IsSeekForPrevSupported() const override {
      return DuaLinkListIterator::IsSeekForPrevSupported();
    }

   protected:
    const ConcurrentHashDualListRep *const memtable_rep_;
  };

  using BucketCleaner = ConcurrentHashDualListReqFactory::BucketCleaner;

 public:
  ConcurrentHashDualListRep(const MemTableRep::KeyComparator &compare,
                            Allocator *allocator,
                            std::unique_ptr<Pointer[], BucketCleaner> &&_mem,
                            const SliceTransform *transform, size_t bucket_size,
                            size_t huge_page_tlb_size, Logger *logger,
                            int bucket_entries_logging_threshold,
                            bool if_log_bucket_dist_when_flush)
      : MemTableRep(allocator),
        bucket_size_(bucket_size),
        mem_(std::move(_mem)),
        transform_(transform),
        compare_(compare),
        huge_page_tlb_size_(huge_page_tlb_size),
        logger_(logger),
        // bucket_entries_logging_threshold_(bucket_entries_logging_threshold),
        if_log_bucket_dist_when_flush_(if_log_bucket_dist_when_flush) {
    assert(allocator_);
    if (mem_) {
      buckets_ = mem_.get();
    } else {
      char *mem = allocator_->AllocateAligned(sizeof(Pointer) * bucket_size_,
                                              huge_page_tlb_size_, logger_);

      buckets_ = new (mem) Pointer[bucket_size_];

      for (size_t i = 0; i < bucket_size_; ++i) {
        buckets_[i].store(nullptr, std::memory_order_relaxed);
      }
    }
  }

  KeyHandle Allocate(const size_t len, char **buf) override {
    char *mem = allocator_->AllocateAligned(len + sizeof(Node));
    Node *x = new (mem) Node();
    *buf = x->key;
    return static_cast<KeyHandle>(mem);
  }

  bool Contains(const Slice &internal_key) const override {
    Pointer &bucket = GetBucket(internal_key);
    Node *head = static_cast<Node *>(bucket.load(std::memory_order_acquire));
    Node *x = FindGreaterOrEqualInBucket(head, internal_key);
    return x != nullptr && Equal(internal_key, x->key);
  }

  void Insert(KeyHandle handle) override { return InsertImpl(handle, false); }

  void InsertConcurrently(KeyHandle handle) override {
    return InsertImpl(handle, true);
  }

  size_t ApproximateMemoryUsage() override { return 0; }

  void Get(const LookupKey &k, void *callback_args,
           bool (*callback_func)(void *arg, const Slice &key,
                                 const char *value)) override {
    auto internal_key = k.internal_key();
    Pointer &bucket = GetBucket(internal_key);
    if (!bucket.load(std::memory_order_acquire)) {
      return;
    }
    DuaLinkListIterator iter(this, &bucket);
    for (iter.Seek(k.internal_key(), nullptr);
         iter.Valid() && callback_func(callback_args, iter.key(), iter.value());
         iter.Next()) {
    }
  }

  MemTableRep::Iterator *GetIterator(Arena *arena = nullptr) override;

  MemTableRep::Iterator *GetDynamicPrefixIterator(
      Arena *alloc_arena = nullptr) override {
    if (alloc_arena == nullptr) {
      return new DynamicIterator(this);
    } else {
      auto mem = alloc_arena->AllocateAligned(sizeof(DynamicIterator));
      return new (mem) DynamicIterator(this);
    }
  }

 private:
  void InsertImpl(KeyHandle handle, bool concurrent) {
    Node *x = static_cast<Node *>(handle);
    assert(x && !Contains(GetLengthPrefixedSlice(x->key)));
    auto internal_key = GetLengthPrefixedSlice(x->key);
    Pointer &bucket = GetBucket(internal_key);
    Node *prev = nullptr;
    for (;;) {
      prev =
          prev == nullptr
              ? reinterpret_cast<Node *>(bucket.load(std::memory_order_acquire))
              : prev;
      prev = FindLessOrEqualInBucket(prev, internal_key);
      assert(prev == nullptr || KeyIsAfterNode(internal_key, prev));
      if (!UserKeyEqual(internal_key, prev)) {
        if (concurrent) {
          Node *next = nullptr;
          if (prev == nullptr) {
            next = reinterpret_cast<Node *>(
                bucket.load(std::memory_order_relaxed));
          } else {
            next = prev->NoBarrier_LevelNext();
          }
          if (UserKeyEqual(internal_key, next)) {
            x->NoBarrier_SetVerticalNext(next);
            next->MarkNodeVertical(true);
            x->NoBarrier_SetLevelNext(next->LevelNext());
          } else {
            x->NoBarrier_SetLevelNext(next);
          }
          if (!KeyIsAfterNode(internal_key, next)) {
            if (prev == nullptr) {
              void *vnext = next;
              if (bucket.compare_exchange_strong(vnext, (void *)x,
                                                 std::memory_order_relaxed)) {
                return;
              }
            } else if (prev->CASLevelNext(next, x)) {
              return;
            }
            if (prev && !prev->NodeAtLevel()) {
              prev = nullptr;
            }
          }
          port::AsmVolatilePause();
          continue;
        } else {
          Node *next = nullptr;
          if (prev == nullptr) {
            next = reinterpret_cast<Node *>(
                bucket.load(std::memory_order_relaxed));
          } else {
            next = prev->NoBarrier_LevelNext();
          }
          if (UserKeyEqual(internal_key, next)) {
            assert(next && next->NodeAtLevel());
            x->NoBarrier_SetVerticalNext(next);
            next->MarkNodeVertical(false);
            next = next->NoBarrier_LevelNext();
          }
          assert(!next || next->NodeAtLevel());
          x->NoBarrier_SetLevelNext(next);
          if (prev == nullptr) {
            bucket.store(x, std::memory_order_release);
          } else {
            prev->SetLevelNext(x);
          }
          return;
        }
      } else {
        Node *next = prev->NoBarrier_VerticalNext();
        assert(!next || !next->NodeAtLevel());
        x->NoBarrier_SetVerticalNext(next);
        if (concurrent) {
          if (!KeyIsAfterNode(internal_key, next) &&
              prev->CASVerticalNext(next, x)) {
            return;
          }
          port::AsmVolatilePause();
          continue;
        } else {
          prev->SetVerticalNext(x);
          return;
        }
      }
    }
  }

  Slice GetPrefix(const Slice &internal_key) const {
    assert(transform_);
    return transform_->Transform(ExtractUserKey(internal_key));
  }

  size_t GetHash(const Slice &key) const {
    return MurmurHash(key.data(), key.size(), 0) % bucket_size_;
  }

  Pointer &GetBucket(const Slice &internal_key) const {
    size_t i = GetHash(GetPrefix(internal_key));
    assert(i < bucket_size_);
    return buckets_[i];
  }

  Pointer &GetBucket(const char *key) const {
    assert(key != nullptr);
    return GetBucket(GetLengthPrefixedSlice(key));
  }

  Node *FindGreaterOrEqualInBucket(Node *x, const Slice &internal_key) const {
    while (true) {
      if (UserKeyIsAfterNode(internal_key, x)) {
        // Keep searching in this list
        assert(x);
        x = x->LevelNext();
      } else if (!UserKeyEqual(internal_key, x)) {
        return x;
      } else {
        break;
      }
    }
    Node *y = x;
    while (true) {
      assert(UserKeyEqual(internal_key, y));
      if (KeyIsAfterNode(internal_key, y)) {
        y = y->VerticalNext();
      } else {
        break;
      }
    }
    if (y == nullptr) {
      return x->LevelNext();
    } else {
      return y;
    }
  }

  Node *FindLessOrEqualInBucket(Node *prev, const Slice &internal_key) const {
    // assert(prev == nullptr || KeyIsAfterNode(internal_key, prev));
    Node *x = prev;
    prev = nullptr;
    while (true) {
      if (UserKeyIsAfterNode(internal_key, x)) {
        assert(x);
        prev = x;
        x = x->LevelNext();
      } else if (UserKeyEqual(internal_key, x)) {
        break;
      } else {
        return prev;
      }
    }
    assert(UserKeyEqual(internal_key, x));
    while (true) {
      // assert(UserKeyEqual(x, prev));
      if (KeyIsAfterNode(internal_key, x)) {
        prev = x;
        x = x->VerticalNext();
      } else if (x && Equal(internal_key, x->key)) {
        return x;
      } else {
        return prev;
      }
    }
  }

  bool KeyIsAfterNode(const Slice &internal_key, const Node *n) const {
    return n != nullptr && compare_(n->key, internal_key) < 0;
  }

  bool UserKeyIsAfterNode(const Slice &internal_key, const Node *n) const {
    return n != nullptr &&
           CompareUserKey(GetLengthPrefixedSlice(n->key), internal_key) < 0;
  }

  int CompareUserKey(const Slice &ileft, const Slice &iright) const {
    return compare_.icomparator()->user_comparator()->Compare(
        ExtractUserKey(ileft), ExtractUserKey(iright));
  }

  bool UserKeyEqual(const Slice &internal_key, const Node *n) const {
    return n != nullptr &&
           CompareUserKey(GetLengthPrefixedSlice(n->key), internal_key) == 0;
  }

  bool UserKeyEqual(const Node *l, const Node *r) const {
    if (l == nullptr || r == nullptr) {
      return l == r;
    }
    return CompareUserKey(GetLengthPrefixedSlice(l->key),
                          GetLengthPrefixedSlice(r->key)) == 0;
  }

  bool Equal(const Slice &a, const Key &b) const { return compare_(b, a) == 0; }

  size_t bucket_size_;

  Pointer *buckets_;

  std::unique_ptr<Pointer[], BucketCleaner> mem_;

  const SliceTransform *transform_;

  const MemTableRep::KeyComparator &compare_;

  size_t huge_page_tlb_size_;
  Logger *logger_;
  // int bucket_entries_logging_threshold_;  // not used ?
  bool if_log_bucket_dist_when_flush_;
};

MemTableRep::Iterator *ConcurrentHashDualListRep::GetIterator(
    Arena *alloc_arena) {
  Arena *new_arena = new Arena(allocator_->BlockSize());
  auto list = new MemtableSkipList(compare_, new_arena);
  HistogramImpl keys_per_bucket_hist;

  for (size_t i = 0; i < bucket_size_; ++i) {
    size_t count = 0;
    DuaLinkListIterator iter(this, buckets_ + i);
    for (iter.SeekToHead(); iter.Valid(); iter.Next()) {
      list->Insert(iter.EncodedKey());
      ++count;
    }
    if (if_log_bucket_dist_when_flush_) {
      keys_per_bucket_hist.Add(count);
    }
  }
  if (if_log_bucket_dist_when_flush_ && logger_ != nullptr) {
    Info(logger_, "ConcurrentHashDualList Entry distribution among buckets: %s",
         keys_per_bucket_hist.ToString().c_str());
  }
  if (alloc_arena == nullptr) {
    return new FullListIterator(list, new_arena);
  } else {
    auto mem = alloc_arena->AllocateAligned(sizeof(FullListIterator));
    return new (mem) FullListIterator(list, new_arena);
  }
}

}  // namespace

MemTableRep *ConcurrentHashDualListReqFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator &compare, bool /*needs_dup_key_check*/,
    Allocator *allocator, const SliceTransform *transform, Logger *logger) {
  std::unique_ptr<Pointer[], BucketCleaner> mem(nullptr, bucket_cleaner_);
  {
    InstrumentedMutexLock l(&mutex_);
    if (!preallocated_buckets_.empty()) {
      Pointer *ptr = preallocated_buckets_.front().release();
      preallocated_buckets_.pop_front();
      assert(ptr != nullptr);
      mem.reset(ptr);
    }
  }
  return new ConcurrentHashDualListRep(
      compare, allocator, std::move(mem), transform, bucket_count_,
      huge_page_tlb_size_, logger, bucket_entries_logging_threshold_,
      if_log_bucket_dist_when_flush_);
}

MemTableRepFactory *NewConcurrentHashDualListReqFactory(
    size_t bucket_count, size_t huge_page_tlb_size,
    int bucket_entries_logging_threshold, size_t num_hash_buckets_preallocated,
    bool if_log_bucket_dist_when_flush) {
  return new ConcurrentHashDualListReqFactory(
      bucket_count, huge_page_tlb_size, bucket_entries_logging_threshold,
      num_hash_buckets_preallocated, if_log_bucket_dist_when_flush);
}

MemTableRepFactory *NewConcurrentHashDualListReqFactory(
    const std::unordered_map<std::string, std::string> &options, Status *s) {
  auto f = options.begin();

  size_t bucket_count = 50000;
  f = options.find("bucket_count");
  if (options.end() != f) {
    bucket_count = ParseSizeT(f->second);
  }

  size_t huge_page_tlb_size = 0;
  f = options.find("huge_page_tlb_size");
  if (options.end() != f) {
    huge_page_tlb_size = ParseSizeT(f->second);
  }

  int bucket_entries_logging_threshold = 4096;
  f = options.find("bucket_entries_logging_threshold");
  if (options.end() != f) {
    bucket_entries_logging_threshold = ParseSizeT(f->second);
  }

  size_t num_hash_buckets_preallocated = 0;
  f = options.find("num_hash_buckets_preallocated");
  if (options.end() != f) {
    num_hash_buckets_preallocated = ParseSizeT(f->second);
  }

  bool if_log_bucket_dist_when_flush = true;
  f = options.find("if_log_bucket_dist_when_flush");
  if (options.end() != f) {
    try {
      if_log_bucket_dist_when_flush = ParseBoolean("", f->second);
    } catch (const std::exception &) {
      *s = Status::InvalidArgument("NewConcurrentHashDualListReqFactory",
                                   "if_log_bucket_dist_when_flush");
    }
  }

  return new ConcurrentHashDualListReqFactory(
      bucket_count, huge_page_tlb_size, bucket_entries_logging_threshold,
      num_hash_buckets_preallocated, if_log_bucket_dist_when_flush);
}

ROCKSDB_REGISTER_MEM_TABLE("dualhash_linklist",
                           ConcurrentHashDualListReqFactory);

}  // namespace TERARKDB_NAMESPACE

#endif  // ROCKSDB_LITE
