// chenchanglong@bytedance.com
// Oct. 21. 2019.

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <iterator>
#include <memory>
#include <mutex>
#include <numeric>
#include <utility>
#include <vector>

#include "db/memtable.h"
#include "rocksdb/convenience.h"
#include "rocksdb/memtablerep.h"
#include "table/terark_zip_internal.h"
#include "util/arena.h"
#include "port/port.h"

#include "terark/fsa/cspptrie.inl"
#include "terark/heap_ext.hpp"
#include "terark/io/byte_swap.hpp"
#include "terark/thread/instance_tls_owner.hpp"

namespace rocksdb {

namespace terark_memtable_details {

enum class ConcurrentType { Native, None };

enum class PatriciaKeyType { UserKey, FullKey };

enum class InsertResult { Success, Duplicated, Fail };

struct tag_vector_t {
  uint32_t size;
  uint32_t loc;
  struct data_t {
    uint64_t tag;
    uint32_t loc;
    operator uint64_t() const { return tag; }
  };
  bool full() { return terark::fast_popcount(size) == 1; }
};

struct VectorData {
  size_t size;
  const typename tag_vector_t::data_t *data;
};

}

namespace detail = terark_memtable_details;

// data structure inheriting terark's cspptrie for supporting memtable
class MemPatricia : public terark::MainPatricia {
public:
  MemPatricia(size_t valsize,
              intptr_t maxMem = 512<<10,
              ConcurrentLevel level = OneWriteMultiRead,
              fstring fpath = "")
    : MainPatricia(valsize, maxMem, level, fpath) {}
};

// Write token pairing with MemPatricia
class MemWriterToken : public terark::Patricia::WriterToken {
  uint64_t tag_;
  Slice value_;
public:
  uint64_t get_tag() { return tag_; }
  MemWriterToken(MemPatricia *trie, uint64_t tag, const Slice &value)
        : terark::Patricia::WriterToken(trie), tag_(tag), value_(value) {};
protected:
  bool init_value(void *valptr, size_t valsize) noexcept;
};

// Patricia trie memtable rep
class PatriciaTrieRep : public MemTableRep {
  terark::Patricia::ConcurrentLevel concurrent_level_;
  detail::PatriciaKeyType patricia_key_type_;
  bool handle_duplicate_;
  std::atomic_bool immutable_;
  std::vector<MemPatricia *> trie_vec_;
  int64_t write_buffer_size_;
  static const int64_t size_limit_ = 1LL << 30;

public:
  // Create a new patricia trie memtable rep with following options
  PatriciaTrieRep(detail::ConcurrentType concurrent_type,
                  detail::PatriciaKeyType patricia_key_type,
                  bool handle_duplicate,
                  intptr_t write_buffer_size,
                  Allocator *allocator,
                  const MemTableRep::KeyComparator &compare);

  // Return approximate memory usage which is sum of memory usage from
  // all patricia trie handled by this rep.
  virtual size_t ApproximateMemoryUsage() override;

  // This approximition is not supported, return 0 instead.  
  virtual uint64_t ApproximateNumEntries(
    const Slice &start_ikey,
    const Slice &end_ikey) override { return 0; }

  // Return true if this rep contains querying key.
  virtual bool Contains(const Slice &internal_key) const override;

  // Get with lazyslice feedback, parsing the value when truly needed.
  virtual void Get(
    const LookupKey &k,
    void *callback_args,
    bool (*callback_func)(void *arg, const Slice& key,
                          LazyBuffer&& value)) override;

  // Return iterator of this rep
  virtual MemTableRep::Iterator *GetIterator(Arena *arena) override;

  // Insert with keyhandle is not supported. 
  virtual void Insert(KeyHandle handle) override { assert(false); }

  // Return true if insertion successed.
  virtual bool InsertKeyValue(
    const Slice &internal_key,
    const Slice &value) override;

  // Concurrent write is supported by default.
  virtual bool InsertKeyValueConcurrently(
    const Slice &internal_key,
    const Slice &value) override { return InsertKeyValue(internal_key, value); }

  virtual void MarkReadOnly() override {
    for (auto iter : trie_vec_) iter->set_readonly();
    immutable_ = true;
  }
};

// Heap iterator for traversing multi tries simultaneously. 
// Create a heap to merge iterators from all tries. 
template <bool heap_mode>
class PatriciaRepIterator :
    public MemTableRep::Iterator,
    public LazyBufferController,
    boost::noncopyable {
  typedef terark::Patricia::ReaderToken token_t;

  // Inner iterator abstructiong for polymorphism
  class HeapItem : boost::noncopyable {
  public:
    terark::MainPatricia::IterMem handle;
    uint64_t tag;
    size_t index;

    struct VectorData {
      size_t size;
      const typename detail::tag_vector_t::data_t *data;
    };

    HeapItem(terark::Patricia *trie) : tag(uint64_t(-1)), index(size_t(-1)) {
      handle.construct(trie);
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
      HeapItem *array;
      size_t count;
      HeapItem **heap;
      size_t size;
    } multi_;
    HeapItem single_;
  };

  std::string buffer_;
  PatriciaTrieRep *rep_;
  int direction_;

  // Return pointer of current heap item.
  const HeapItem *Current() const { return heap_mode ? *multi_.heap : &single_; }

  // Return pointer of current heap item.
  HeapItem *Current() { return heap_mode ? *multi_.heap : &single_; }

  // Return current key.
  terark::fstring CurrentKey() { return Current()->handle.iter()->word(); }

  // Return current tag.
  uint64_t CurrentTag() { return Current()->tag; }

  // Lexicographical order 3 way compartor
  struct ForwardComp {
    bool operator()(HeapItem *l, HeapItem *r) const {
      int c = terark::fstring_func::compare3()(l->handle.iter()->word(), r->handle.iter()->word());
      return c == 0 ? l->tag < r->tag : c > 0;
    }
  };

  // Reverse lexicographical order 3 way compartor
  struct BackwardComp {
    bool operator()(HeapItem *l, HeapItem *r) const {
      int c = terark::fstring_func::compare3()(l->handle.iter()->word(), r->handle.iter()->word());
      return c == 0 ? l->tag > r->tag : c < 0;
    }
  };

  // Rebuild heap for orderness
  template <int direction, class func_t> void Rebuild(func_t &&callback_func);

public:
  PatriciaRepIterator(std::vector<MemPatricia *> tries);

  virtual ~PatriciaRepIterator();

  // LazyBufferController override
  virtual void destroy(LazyBuffer* /*buffer*/) const override {}

  // LazyBufferController override
  virtual void pin_buffer(LazyBuffer* buffer) const override {
    if (!buffer->valid()) {
      buffer->reset(GetValue());
    }
  }

  // LazyBufferController override
  Status fetch_buffer(LazyBuffer* buffer) const override {
    set_slice(buffer, GetValue());
    return Status::OK();
  }

  // Returns true iff the iterator is at valid position. 
  virtual bool Valid() const override { return direction_ != 0; }

  // No needs to encode.
  virtual const char* EncodedKey() const override { assert(false); return nullptr; }

  // Return the key of current position
  // REQUIRES : Valid()
  virtual Slice key() const override { assert(direction_ != 0); return buffer_; }

  // Return the value of current position immediately
  // REQUIRES : Valid()
  virtual Slice GetValue() const;

  // Return the value of current position, parsing when truly needed.
  virtual LazyBuffer value() const override {
    assert(direction_ != 0);
    return LazyBuffer(this, {});
  }

  // Advances to the next position.
  // REQUIRES: Valid()
  virtual void Next() override;

  // Advances to the previous position.
  // REQUIRES: Valid()
  virtual void Prev() override;

  // Advance to the first entry with a key >= target
  virtual void Seek(const Slice &user_key, const char *memtable_key) override;

  // Retreat to the last entry with a key <= target
  virtual void SeekForPrev(const Slice &user_key,
                           const char *memtable_key) override;

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
  detail::ConcurrentType concurrent_type_;
  detail::PatriciaKeyType patricia_key_type_;
  int64_t write_buffer_size_;

public:
  PatriciaTrieRepFactory(
    std::shared_ptr<class MemTableRepFactory> &fallback,
    detail::ConcurrentType concurrent_type = detail::ConcurrentType::Native,
    detail::PatriciaKeyType patricia_key_type = detail::PatriciaKeyType::UserKey,
    int64_t write_buffer_size = 512LL * 1048576)
  : fallback_(fallback),
    concurrent_type_(concurrent_type),
    patricia_key_type_(patricia_key_type),
    write_buffer_size_(write_buffer_size) {}

  virtual ~PatriciaTrieRepFactory() {}

  virtual MemTableRep *
  CreateMemTableRep(const MemTableRep::KeyComparator &key_cmp,
                    bool needs_dup_key_check, Allocator *allocator,
                    const SliceTransform *transform, Logger *logger) override;

  virtual MemTableRep *
  CreateMemTableRep(const MemTableRep::KeyComparator& key_cmp, 
                    bool needs_dup_key_check, Allocator* allocator,
                    const SliceTransform* slice_transform, Logger* logger, uint32_t) override {
    return CreateMemTableRep(key_cmp, needs_dup_key_check, allocator, slice_transform, logger);
  }

  virtual MemTableRep *
  CreateMemTableRep(const MemTableRep::KeyComparator &key_cmp,
                    bool needs_dup_key_check,
                    Allocator *allocator,
                    const ImmutableCFOptions &ioptions,
                    const MutableCFOptions &mutable_cf_options,
                    uint32_t column_family_id) override;

  virtual const char *Name() const override {
    return "PatriciaTrieRepFactory";
  }

  virtual bool IsInsertConcurrentlySupported() const override {
    return concurrent_type_ != detail::ConcurrentType::None;
  }

  virtual bool CanHandleDuplicatedKey() const override { return true; }
};

MemTableRepFactory* NewPatriciaTrieRepFactory(
    std::shared_ptr<MemTableRepFactory> fallback);

MemTableRepFactory *NewPatriciaTrieRepFactory(
    const std::unordered_map<std::string, std::string> &options, Status *s);

}