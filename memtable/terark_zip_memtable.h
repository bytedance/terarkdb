// chenchanglong@bytedance.com
// Oct. 21. 2019.

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdlib>
#include <iterator>
#include <memory>
#include <mutex>
#include <numeric>
#include <utility>
#include <vector>

#include "db/memtable.h"
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "rocksdb/memtablerep.h"
#include "table/terark_zip_internal.h"
#include "terark/fsa/cspptrie.inl"
#include "terark/heap_ext.hpp"
#include "terark/io/byte_swap.hpp"
#include "terark/thread/instance_tls_owner.hpp"
#include "util/arena.h"

namespace rocksdb {

// Write token pairing with MainPatricia
class MemWriterToken : public terark::Patricia::WriterToken {
  uint64_t tag_;
  Slice value_;

 public:
  uint64_t get_tag() { return tag_; }

  void reset_tag_value(uint64_t tag, const Slice& value) {
    tag_ = tag;
    value_ = value;
  }

 protected:
  bool init_value(void* valptr, size_t valsize) noexcept;
};

namespace terark_memtable_details {

typedef std::array<terark::MainPatricia*, 32> tries_t;

enum class ConcurrentType { Native, None };

enum class PatriciaKeyType { UserKey, FullKey };

enum class InsertResult { Success, Duplicated, Fail };

#pragma pack(push)
#pragma pack(4)
struct tag_vector_t {
  std::atomic<uint64_t> size_loc;
  struct data_t {
    uint64_t tag;
    uint32_t loc;
    operator uint64_t() const { return tag; }
  };
  static bool full(uint32_t size) { return !((size - 1) & size); }
};
#pragma pack(pop)

}  // namespace terark_memtable_details

// Patricia trie memtable rep
class PatriciaTrieRep : public MemTableRep {
  terark::Patricia::ConcurrentLevel concurrent_level_;
  terark_memtable_details::PatriciaKeyType patricia_key_type_;
  bool handle_duplicate_;
  std::atomic_bool immutable_;
  terark_memtable_details::tries_t trie_vec_;
  size_t trie_vec_size_;
  size_t overhead_;  // this overhead is for new memtable size check
  int64_t write_buffer_size_;
  static const int64_t size_limit_ = 1LL << 30;
  std::mutex mutex_;

 public:
  // Create a new patricia trie memtable rep with following options
  PatriciaTrieRep(terark_memtable_details::ConcurrentType concurrent_type,
                  terark_memtable_details::PatriciaKeyType patricia_key_type,
                  bool handle_duplicate, intptr_t write_buffer_size,
                  Allocator* allocator);

  ~PatriciaTrieRep();

  // Return approximate memory usage which is sum of memory usage from
  // all patricia trie handled by this rep.
  virtual size_t ApproximateMemoryUsage() override;

  // This approximition is not supported, return 0 instead.
  virtual uint64_t ApproximateNumEntries(const Slice& /*start_ikey*/,
                                         const Slice& /*end_ikey*/) override {
    return 0;
  }

  // Return true if this rep contains querying key.
  virtual bool Contains(const Slice& internal_key) const override;

  // Get with lazyslice feedback, parsing the value when truly needed.
  virtual void Get(const LookupKey& k, void* callback_args,
                   bool (*callback_func)(void* arg, const Slice& key,
                                         const char* value)) override;

  // Return iterator of this rep
  virtual MemTableRep::Iterator* GetIterator(Arena* arena) override;

  // Insert with keyhandle is not supported.
  virtual void Insert(KeyHandle /*handle*/) override { assert(false); }

  // Return true if insertion successed.
  virtual bool InsertKeyValue(const Slice& internal_key,
                              const Slice& value) override;

  // Concurrent write is supported by default.
  virtual bool InsertKeyValueConcurrently(const Slice& internal_key,
                                          const Slice& value) override {
    return InsertKeyValue(internal_key, value);
  }

  virtual void MarkReadOnly() override;
};

// Heap iterator for traversing multi tries simultaneously.
// Create a heap to merge iterators from all tries.
template <bool heap_mode>
class PatriciaRepIterator : public MemTableRep::Iterator,
                            boost::noncopyable {
  typedef terark::Patricia::ReaderToken token_t;

  // Inner iterator abstructiong for polymorphism
  class HeapItem : boost::noncopyable {
   public:
    terark::Patricia::IteratorPtr handle;
    uint64_t tag;
    size_t index;

    struct VectorData {
      size_t size;
      const typename terark_memtable_details::tag_vector_t::data_t* data;
    };

    HeapItem(terark::Patricia* trie) : tag(uint64_t(-1)), index(size_t(-1)) {
      handle.reset(trie->new_iter());
    }

    VectorData GetVector();
    uint32_t GetValue() const;
    void Seek(terark::fstring find_key, uint64_t find_tag);
    void SeekForPrev(terark::fstring find_key, uint64_t find_tag);
    void SeekToFirst();
    void SeekToLast();
    void Next();
    void Prev();
  };

  // union definition as unify interface for iterator polymorphism
  union {
    struct {
      HeapItem* array;
      size_t count;
      HeapItem** heap;
      size_t size;
    } multi_;
    HeapItem single_;
  };

  std::string buffer_;
  int direction_;

  // Return pointer of current heap item.
  const HeapItem* Current() const {
    return heap_mode ? *multi_.heap : &single_;
  }

  // Return pointer of current heap item.
  HeapItem* Current() { return heap_mode ? *multi_.heap : &single_; }

  // Return current key.
  terark::fstring CurrentKey() { return Current()->handle->word(); }

  // Return current tag.
  uint64_t CurrentTag() { return Current()->tag; }

  // Lexicographical order 3 way compartor
  struct ForwardComp {
    bool operator()(HeapItem* l, HeapItem* r) const {
      int c = terark::fstring_func::compare3()(l->handle->word(),
                                               r->handle->word());
      return c == 0 ? l->tag < r->tag : c > 0;
    }
  };

  // Reverse lexicographical order 3 way compartor
  struct BackwardComp {
    bool operator()(HeapItem* l, HeapItem* r) const {
      int c = terark::fstring_func::compare3()(l->handle->word(),
                                               r->handle->word());
      return c == 0 ? l->tag > r->tag : c < 0;
    }
  };

  // Rebuild heap for orderness
  template <int direction, class func_t>
  void Rebuild(func_t&& callback_func);

 public:
  PatriciaRepIterator(terark_memtable_details::tries_t& tries,
                      size_t tries_size);

  virtual ~PatriciaRepIterator();

  // Returns true iff the iterator is at valid position.
  virtual bool Valid() const override { return direction_ != 0; }

  // No needs to encode.
  virtual const char* EncodedKey() const override {
    assert(false);
    return nullptr;
  }

  // Return the key of current position
  // REQUIRES : Valid()
  virtual Slice key() const override {
    assert(direction_ != 0);
    return buffer_;
  }

  // Return the value of current position, parsing when truly needed.
  virtual const char* value() const override;

  // Advances to the next position.
  // REQUIRES: Valid()
  virtual void Next() override;

  // Advances to the previous position.
  // REQUIRES: Valid()
  virtual void Prev() override;

  // Advance to the first entry with a key >= target
  virtual void Seek(const Slice& user_key, const char* memtable_key) override;

  // Retreat to the last entry with a key <= target
  virtual void SeekForPrev(const Slice& user_key,
                           const char* memtable_key) override;

  // Position at the first entry in this rep.
  // Final state of iterator is Valid() iff this rep is not empty.
  virtual void SeekToFirst() override;

  // Position at the last entry in this rep.
  // Final state of iterator is Valid() iff this rep is not empty.
  virtual void SeekToLast() override;

  // Support by default
  virtual bool IsSeekForPrevSupported() const override { return true; }
};

class PatriciaTrieRepFactory : public MemTableRepFactory {
 private:
  std::shared_ptr<class MemTableRepFactory> fallback_;
  terark_memtable_details::ConcurrentType concurrent_type_;
  terark_memtable_details::PatriciaKeyType patricia_key_type_;
  int64_t write_buffer_size_;

 public:
  PatriciaTrieRepFactory(
      std::shared_ptr<class MemTableRepFactory>& fallback,
      terark_memtable_details::ConcurrentType concurrent_type =
          terark_memtable_details::ConcurrentType::Native,
      terark_memtable_details::PatriciaKeyType patricia_key_type =
          terark_memtable_details::PatriciaKeyType::UserKey,
      int64_t write_buffer_size = 512LL * 1048576)
      : fallback_(fallback),
        concurrent_type_(concurrent_type),
        patricia_key_type_(patricia_key_type),
        write_buffer_size_(write_buffer_size) {}

  virtual ~PatriciaTrieRepFactory() {}

  virtual MemTableRep* CreateMemTableRep(
      const MemTableRep::KeyComparator& key_cmp, bool needs_dup_key_check,
      Allocator* allocator, const SliceTransform* transform,
      Logger* logger) override;

  virtual MemTableRep* CreateMemTableRep(
      const MemTableRep::KeyComparator& key_cmp, bool needs_dup_key_check,
      Allocator* allocator, const SliceTransform* slice_transform,
      Logger* logger, uint32_t) override {
    return CreateMemTableRep(key_cmp, needs_dup_key_check, allocator,
                             slice_transform, logger);
  }

  virtual MemTableRep* CreateMemTableRep(
      const MemTableRep::KeyComparator& key_cmp, bool needs_dup_key_check,
      Allocator* allocator, const ImmutableCFOptions& ioptions,
      const MutableCFOptions& mutable_cf_options,
      uint32_t column_family_id) override;

  virtual const char* Name() const override { return "PatriciaTrieRepFactory"; }

  virtual bool IsInsertConcurrentlySupported() const override {
    return concurrent_type_ != terark_memtable_details::ConcurrentType::None;
  }

  virtual bool CanHandleDuplicatedKey() const override { return true; }
};

}  // namespace rocksdb
