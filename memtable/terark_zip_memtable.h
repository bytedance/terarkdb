//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

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
#include "rocksdb/memtablerep.h"
#include "terark_zip_internal.h"
#include "util/arena.h"
#include "port/port.h"
#include <rocksdb/convenience.h>
#include <terark/fsa/cspptrie.inl>
#include <terark/heap_ext.hpp>
#include <terark/io/byte_swap.hpp>
#include <terark/thread/instance_tls_owner.hpp>

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

static const char *build_key(terark::fstring user_key, uint64_t tag,
                               valvec<char> *buffer) {
    buffer->resize(0);
    buffer->reserve(user_key.size() + 8);
    buffer->append(user_key.data(), user_key.size());
    if (rocksdb::port::kLittleEndian) {
      buffer->append(const_cast<const char *>(reinterpret_cast<char *>(&tag)),
                     sizeof(tag));
    } else {
      char buf[sizeof(tag)];
      EncodeFixed64(buf, tag);
      buffer->append(buf, sizeof(buf));
    }
    return buffer->data();
  }

  static const char *build_key(terark::fstring user_key, uint64_t tag,
                               std::string *buffer) {
    buffer->resize(0);
    buffer->reserve(user_key.size() + 8);
    buffer->append(user_key.data(), user_key.size());
    PutFixed64(buffer, tag);
    return buffer->data();
  }

}

namespace detail = terark_memtable_details;

class MemPatricia : public terark::MainPatricia {
public:
  MemPatricia(size_t valsize,
              intptr_t maxMem = 512<<10,
              ConcurrentLevel level = OneWriteMultiRead,
              fstring fpath = "") : MainPatricia(valsize,
                                                 maxMem,
                                                 level,
                                                 fpath) {}
};

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

class PatriciaMemtableRep : public MemTableRep {
  terark::Patricia::ConcurrentLevel concurrent_level_;
  detail::PatriciaKeyType patricia_key_type_;
  bool handle_duplicate_;

  std::atomic_bool immutable_;

  std::vector<MemPatricia *> trie_vec_;
  MemPatricia * target_trie_;
  int64_t write_buffer_size_;
  static const int64_t size_limit_ = 1LL << 30;

public:
  PatriciaMemtableRep(detail::ConcurrentType concurrent_type,
                      detail::PatriciaKeyType patricia_key_type,
                      bool handle_duplicate,
                      intptr_t write_buffer_size,
                      Allocator *allocator,
                      const MemTableRep::KeyComparator &compare)
                    : MemTableRep(allocator){
    immutable_ = false;
    patricia_key_type_ = patricia_key_type;
    handle_duplicate_ = handle_duplicate;
    write_buffer_size_ = write_buffer_size;
    if (concurrent_type == detail::ConcurrentType::Native)
      concurrent_level_ = terark::Patricia::ConcurrentLevel::MultiWriteMultiRead;
    else
      concurrent_level_ = terark::Patricia::ConcurrentLevel::OneWriteMultiRead;
    trie_vec_.emplace_back(new MemPatricia(4, write_buffer_size_,
                                            concurrent_level_));             
  }

  ~PatriciaMemtableRep();

  virtual size_t ApproximateMemoryUsage() override;

  virtual uint64_t ApproximateNumEntries(
    const Slice &start_ikey,
    const Slice &end_ikey) override { return 0; }

  virtual bool Contains(const Slice &internal_key) const;

  virtual void Get(
    const LookupKey &k,
    void *callback_args,
    bool (*callback_func)(void *arg, const KeyValuePair *));

  virtual MemTableRep::Iterator *GetIterator(Arena *arena);

  virtual void Insert(KeyHandle handle) override { assert(false); }

  virtual bool InsertKeyValue(
    const Slice &internal_key,
    const Slice &value);

  virtual bool InsertKeyValueConcurrently(
    const Slice &internal_key,
    const Slice &value) override { return InsertKeyValue(internal_key, value); }

  virtual void MarkReadOnly() override {
    for (auto iter : trie_vec_)
      iter->set_readonly();
    immutable_ = true;
  }

  class HeapItem : boost::noncopyable {
  public:
    MemPatricia *trie;
    MemPatricia::IterMem handle;
    //char iter_buf[MainPatricia::ITER_SIZE];
    uint64_t tag;
    size_t index;

    HeapItem(MemPatricia *p_trie)
        : trie(p_trie), tag(uint64_t(-1)), index(size_t(-1)) {
      handle.construct(p_trie);      
    }

    detail::VectorData GetVector() {
      auto vector = (detail::tag_vector_t *)trie->mem_get(*(uint32_t *)handle.iter()->value());
      size_t size = vector->size;
      auto data = (typename detail::tag_vector_t::data_t *)trie->mem_get(vector->loc);
      return detail::VectorData{size, data};
    }

    uint32_t GetValue();

    void Seek(terark::fstring find_key, uint64_t find_tag);

    void SeekForPrev(terark::fstring find_key, uint64_t find_tag);

    void SeekToFirst();

    void SeekToLast();

    void Next();

    void Prev();
  };

  struct ForwardComp {
    bool operator()(HeapItem *l, HeapItem *r) const {
      int c = terark::fstring_func::compare3()(l->handle.iter()->word(), r->handle.iter()->word());
      return c == 0 ? l->tag < r->tag : c > 0;
    }
  };

  struct BackwardComp {
    bool operator()(HeapItem *l, HeapItem *r) const {
      int c = terark::fstring_func::compare3()(l->handle.iter()->word(), r->handle.iter()->word());
      return c == 0 ? l->tag > r->tag : c < 0;
    }
  };

  class HeapIterator : public MemTableRep::Iterator, boost::noncopyable {
    std::string buffer_;
    HeapItem *array;
    HeapItem **heap;
    size_t count;
    size_t size;
    int direction_;

  public:
    HeapIterator(PatriciaMemtableRep *rep);
    virtual ~HeapIterator();

    virtual Slice GetKey() const override {
      return buffer_;
    }

    virtual Slice GetValue() const override{
      return GetLengthPrefixedSlice(
        (const char *)(heap[0]->trie->mem_get(heap[0]->GetValue())));
    }

    virtual std::pair<Slice, Slice> GetKeyValue() const override {
      return {GetKey(), GetValue()};
    }

    virtual bool IsKeyPinned() const override { return false; }
    virtual bool IsSeekForPrevSupported() const { return true; }

    virtual const char *key() const override { assert(false); return nullptr; }

    virtual void Next() override;
    virtual void Prev() override;

    template <int direction, class func_t>
    void Rebuild(func_t &&callback_func);

    virtual void Seek(const Slice &user_key, const char *memtable_key) override;
    virtual void SeekForPrev(const Slice &user_key,
                             const char *memtable_key) override;

    virtual void SeekToFirst() override;
    virtual void SeekToLast() override;

    virtual bool Valid() const override { return direction_ != 0; }
  };
};

template <bool heap_mode>
class PatriciaRepIterator : public MemTableRep::Iterator, boost::noncopyable {
  typedef terark::Patricia::ReaderToken token_t;

  struct HeapItem : boost::noncopyable {
    terark::MainPatricia::IterMem handle;
    uint64_t tag;
    size_t index;

    HeapItem(terark::Patricia *trie)
        : tag(uint64_t(-1)), index(size_t(-1)) { handle.construct(trie); }

    struct VectorData {
      size_t size;
      const typename detail::tag_vector_t::data_t *data;
    };

    VectorData GetVector() {
      auto trie = static_cast<terark::MainPatricia *>(handle.iter()->trie());
      auto vector = (detail::tag_vector_t *)trie->mem_get(*(uint32_t *)handle.iter()->value());
      size_t size = vector->size;
      auto data = (typename detail::tag_vector_t::data_t *)trie->mem_get(vector->loc);
      return {size, data};
    }

    uint32_t GetValue() const {
      auto trie = static_cast<terark::MainPatricia *>(handle.iter()->trie());
      auto vector = (detail::tag_vector_t *)trie->mem_get(*(uint32_t *)handle.iter()->value());
      auto data = (detail::tag_vector_t::data_t *)trie->mem_get(vector->loc);
      return data[index].loc;
    }

    void Seek(terark::fstring find_key, uint64_t find_tag) {
      if (!handle.iter()->seek_lower_bound(find_key)) {
        index = size_t(-1);
        return;
      }
      auto vec = GetVector();
      if (handle.iter()->word() == find_key) {
        index = terark::upper_bound_0(vec.data, vec.size, find_tag) - 1;
        if (index != size_t(-1)) {
          tag = vec.data[index];
          return;
        }
        if (!handle.iter()->incr()) {
          assert(index == size_t(-1));
          return;
        }
        vec = GetVector();
      }
      assert(handle.iter()->word() > find_key);
      index = vec.size - 1;
      tag = vec.data[index].tag;
    }

    void SeekForPrev(terark::fstring find_key, uint64_t find_tag) {
      if (!handle.iter()->seek_rev_lower_bound(find_key)) {
        index = size_t(-1);
        return;
      }
      auto vec = GetVector();
      if (handle.iter()->word() == find_key) {
        index = terark::lower_bound_0(vec.data, vec.size, find_tag);
        if (index != vec.size) {
          tag = vec.data[index].tag;
          return;
        }
        if (!handle.iter()->decr()) {
          index = size_t(-1);
          return;
        }
        vec = GetVector();
      }
      assert(handle.iter()->word() < find_key);
      index = 0;
      tag = vec.data[index].tag;
    }

    void SeekToFirst() {
      if (!handle.iter()->seek_begin()) {
        index = size_t(-1);
        return;
      }
      auto vec = GetVector();
      index = vec.size - 1;
      tag = vec.data[index].tag;
    }

    void SeekToLast() {
      if (!handle.iter()->seek_end()) {
        index = size_t(-1);
        return;
      }
      auto vec = GetVector();
      index = 0;
      tag = vec.data[index].tag;
    }

    void Next() {
      assert(index != size_t(-1));
      if (index-- == 0) {
        if (!handle.iter()->incr()) {
          assert(index == size_t(-1));
          return;
        }
        auto vec = GetVector();
        index = vec.size - 1;
        tag = vec.data[index].tag;
      } else {
        auto vec = GetVector();
        tag = vec.data[index].tag;
      }
    }

    void Prev() {
      assert(index != size_t(-1));
      auto vec = GetVector();
      if (++index == vec.size) {
        if (!handle.iter()->decr()) {
          index = size_t(-1);
          return;
        }
        vec = GetVector();
        index = 0;
      }
      tag = vec.data[index].tag;
    }
  };

  std::string buffer_;
  PatriciaMemtableRep *rep_;
  union {
    struct {
      HeapItem *array;
      size_t count;
      HeapItem **heap;
      size_t size;
    } multi_;
    HeapItem single_;
  };
  int direction_;

  const HeapItem *Current() const {
    if (heap_mode) {
      return *multi_.heap;
    } else {
      return &single_;
    }
  }

  HeapItem *Current() {
    if (heap_mode) {
      return *multi_.heap;
    } else {
      return &single_;
    }
  }

  terark::fstring CurrentKey() { return Current()->handle.iter()->word(); }

  uint64_t CurrentTag() { return Current()->tag; }

  struct ForwardComp {
    bool operator()(HeapItem *l, HeapItem *r) const {
      int c = terark::fstring_func::compare3()(l->handle.iter()->word(), r->handle.iter()->word());
      if (c == 0) {
        return l->tag < r->tag;
      } else {
        return c > 0;
      }
    }
  };

  struct BackwardComp {
    bool operator()(HeapItem *l, HeapItem *r) const {
      int c = terark::fstring_func::compare3()(l->handle.iter()->word(), r->handle.iter()->word());
      if (c == 0) {
        return l->tag > r->tag;
      } else {
        return c < 0;
      }
    }
  };

  template <int direction, class func_t> void Rebuild(func_t &&callback_func) {
    static_assert(direction == 1 || direction == -1,
                  "direction must be 1 or -1");
    direction_ = direction;
    multi_.size = multi_.count;
    if (direction == 1) {
      for (size_t i = 0; i < multi_.size;) {
        if (callback_func(multi_.heap[i])) {
          ++i;
        } else {
          --multi_.size;
          std::swap(multi_.heap[i], multi_.heap[multi_.size]);
        }
      }
      std::make_heap(multi_.heap, multi_.heap + multi_.size, ForwardComp());
    } else {
      for (size_t i = 0; i < multi_.size;) {
        if (callback_func(multi_.heap[i])) {
          ++i;
        } else {
          --multi_.size;
          std::swap(multi_.heap[i], multi_.heap[multi_.size]);
        }
      }
      std::make_heap(multi_.heap, multi_.heap + multi_.size, BackwardComp());
    }
  }

public:
  PatriciaRepIterator(std::vector<MemPatricia *> tries) : direction_(0) {
    assert(tries.size() > 0);
    if (heap_mode) {
      // reserve m_tls_vec.size() for optimization!
      valvec<HeapItem> hitem(tries.size(), terark::valvec_reserve());
      valvec<HeapItem *> hptrs(tries.size(), terark::valvec_reserve());
      for (auto trie : tries) {
        hptrs.push_back(new (hitem.grow_no_init(1)) HeapItem(trie));
      }
      assert(hitem.size() == hptrs.size());
      multi_.count = hitem.size();
      multi_.array = hitem.risk_release_ownership();
      multi_.heap = hptrs.risk_release_ownership();
      multi_.size = 0;
    } else {
      new (&single_) HeapItem(tries.front());
    }
  }

  virtual ~PatriciaRepIterator() {
    if (heap_mode) {
      free(multi_.heap);
      for (size_t i = 0; i < multi_.count; ++i) {
        multi_.array[i].~HeapItem();
      }
      free(multi_.array);
    } else {
      single_.~HeapItem();
    }
  }

  // Returns true iff the iterator is positioned at a valid node.
  virtual bool Valid() const override { return direction_ != 0; }

  // Returns the key at the current position.
  // REQUIRES: Valid()
  virtual const char *key() const override {
    assert(false);
    return nullptr;
  }

  virtual Slice GetKey() const override { return buffer_; }

  virtual Slice GetValue() const override {
    const HeapItem *item = Current();
    uint32_t value_loc = item->GetValue();
    auto trie = static_cast<terark::MainPatricia *>(item->handle.iter()->trie());
    return GetLengthPrefixedSlice((const char *)trie->mem_get(value_loc));
  }

  virtual std::pair<Slice, Slice> GetKeyValue() const override {
    return {Iterator::GetKey(), Iterator::GetValue()};
  }

  // Advances to the next position.
  // REQUIRES: Valid()
  virtual void Next() override {
    if (heap_mode) {
      if (direction_ != 1) {
        terark::fstring find_key(buffer_.data(), buffer_.size() - 8);
        uint64_t tag = DecodeFixed64(find_key.end());
        Rebuild<1>([&](HeapItem *item) {
          item->Seek(find_key, tag);
          return item->index != size_t(-1);
        });
        if (multi_.size == 0) {
          direction_ = 0;
          return;
        }
      }
      multi_.heap[0]->Next();
      if (multi_.heap[0]->index == size_t(-1)) {
        std::pop_heap(multi_.heap, multi_.heap + multi_.size, ForwardComp());
        if (--multi_.size == 0) {
          direction_ = 0;
          return;
        }
      } else {
        terark::adjust_heap_top(multi_.heap, multi_.size, ForwardComp());
      }
    } else {
      single_.Next();
      if (single_.index == size_t(-1)) {
        direction_ = 0;
        return;
      }
    }
    detail::build_key(CurrentKey(), CurrentTag(), &buffer_);
  }

  // Advances to the previous position.
  // REQUIRES: Valid()
  virtual void Prev() override {
    if (heap_mode) {
      if (direction_ != -1) {
        terark::fstring find_key(buffer_.data(), buffer_.size() - 8);
        uint64_t tag = DecodeFixed64(find_key.end());
        Rebuild<-1>([&](HeapItem *item) {
          item->SeekForPrev(find_key, tag);
          return item->index != size_t(-1);
        });
        if (multi_.size == 0) {
          direction_ = 0;
          return;
        }
      }
      multi_.heap[0]->Prev();
      if (multi_.heap[0]->index == size_t(-1)) {
        std::pop_heap(multi_.heap, multi_.heap + multi_.size, BackwardComp());
        if (--multi_.size == 0) {
          direction_ = 0;
          return;
        }
      } else {
        terark::adjust_heap_top(multi_.heap, multi_.size, BackwardComp());
      }
    } else {
      single_.Prev();
      if (single_.index == size_t(-1)) {
        direction_ = 0;
        return;
      }
    }
    detail::build_key(CurrentKey(), CurrentTag(), &buffer_);
  }

  // Advance to the first entry with a key >= target
  virtual void Seek(const Slice &user_key, const char *memtable_key) override {
    terark::fstring find_key;
    if (memtable_key != nullptr) {
      Slice internal_key = GetLengthPrefixedSlice(memtable_key);
      find_key = terark::fstring(internal_key.data(), internal_key.size() - 8);
    } else {
      find_key = terark::fstring(user_key.data(), user_key.size() - 8);
    }
    uint64_t tag = DecodeFixed64(find_key.end());

    if (heap_mode) {
      Rebuild<1>([&](HeapItem *item) {
        item->Seek(find_key, tag);
        return item->index != size_t(-1);
      });
      if (multi_.size == 0) {
        direction_ = 0;
        return;
      }
    } else {
      single_.Seek(find_key, tag);
      if (single_.index == size_t(-1)) {
        direction_ = 0;
        return;
      }
      direction_ = 1;
    }
    detail::build_key(CurrentKey(), CurrentTag(), &buffer_);
  }

  // retreat to the first entry with a key <= target
  virtual void SeekForPrev(const Slice &user_key,
                           const char *memtable_key) override {
    terark::fstring find_key;
    if (memtable_key != nullptr) {
      Slice internal_key = GetLengthPrefixedSlice(memtable_key);
      find_key = terark::fstring(internal_key.data(), internal_key.size() - 8);
    } else {
      find_key = terark::fstring(user_key.data(), user_key.size() - 8);
    }
    uint64_t tag = DecodeFixed64(find_key.end());

    if (heap_mode) {
      Rebuild<-1>([&](HeapItem *item) {
        item->SeekForPrev(find_key, tag);
        return item->index != size_t(-1);
      });
      if (multi_.size == 0) {
        direction_ = 0;
        return;
      }
    } else {
      single_.SeekForPrev(find_key, tag);
      if (single_.index == size_t(-1)) {
        direction_ = 0;
        return;
      }
      direction_ = -1;
    }
    detail::build_key(CurrentKey(), CurrentTag(), &buffer_);
  }

  // Position at the first entry in list.
  // Final state of iterator is Valid() iff list is not empty.
  virtual void SeekToFirst() override {
    if (heap_mode) {
      Rebuild<1>([&](HeapItem *item) {
        item->SeekToFirst();
        return item->index != size_t(-1);
      });
      if (multi_.size == 0) {
        direction_ = 0;
        return;
      }
    } else {
      single_.SeekToFirst();
      if (single_.index == size_t(-1)) {
        direction_ = 0;
        return;
      }
      direction_ = 1;
    }
    detail::build_key(CurrentKey(), CurrentTag(), &buffer_);
  }

  // Position at the last entry in list.
  // Final state of iterator is Valid() iff list is not empty.
  virtual void SeekToLast() override {
    if (heap_mode) {
      Rebuild<-1>([&](HeapItem *item) {
        item->SeekToLast();
        return item->index != size_t(-1);
      });
      if (multi_.size == 0) {
        direction_ = 0;
        return;
      }
    } else {
      single_.SeekToLast();
      if (single_.index == size_t(-1)) {
        direction_ = 0;
        return;
      }
      direction_ = -1;
    }
    detail::build_key(CurrentKey(), CurrentTag(), &buffer_);
  }

  virtual bool IsKeyPinned() const override { return false; }

  virtual bool IsSeekForPrevSupported() const { return true; }
};

class PatriciaMemTableRepFactory : public MemTableRepFactory {
private:
  std::shared_ptr<class MemTableRepFactory> fallback_;
  detail::ConcurrentType concurrent_type_;
  detail::PatriciaKeyType patricia_key_type_;
  int64_t write_buffer_size_;

public:
  PatriciaMemTableRepFactory(
    std::shared_ptr<class MemTableRepFactory> &fallback,
    detail::ConcurrentType concurrent_type,
    detail::PatriciaKeyType patricia_key_type,
    int64_t write_buffer_size)
  : fallback_(fallback),
    concurrent_type_(concurrent_type),
    patricia_key_type_(patricia_key_type),
    write_buffer_size_(write_buffer_size) {}

  virtual ~PatriciaMemTableRepFactory() {}

  virtual MemTableRep *
  CreateMemTableRep(const MemTableRep::KeyComparator &key_cmp,
                    bool needs_dup_key_check, Allocator *allocator,
                    const SliceTransform *transform, Logger *logger) override;

  virtual MemTableRep *
  CreateMemTableRep(const MemTableRep::KeyComparator &key_cmp,
                    bool needs_dup_key_check,
                    Allocator *allocator,
                    const ImmutableCFOptions &ioptions,
                    const MutableCFOptions &mutable_cf_options,
                    uint32_t column_family_id) override;

  virtual const char *Name() const override {
    return "PatriciaMemTableRepFactory";
  }

  virtual bool IsInsertConcurrentlySupported() const override {
    return concurrent_type_ != detail::ConcurrentType::None;
  }

  virtual bool CanHandleDuplicatedKey() const override { return true; }
};

static MemTableRepFactory *
CreatePatriciaRepFactory(
    std::shared_ptr<class MemTableRepFactory> &fallback,
    detail::ConcurrentType concurrent_type,
    detail::PatriciaKeyType patricia_key_type,
    int64_t write_buffer_size) {
  if (!fallback) fallback.reset(new SkipListFactory());
  return new PatriciaMemTableRepFactory(
      fallback,
      concurrent_type,
      patricia_key_type,
      write_buffer_size);
}

MemTableRepFactory *NewPatriciaMemTableRepFactory(
    const std::unordered_map<std::string, std::string> &options, Status *s) {
  // default settings
  detail::ConcurrentType concurrent_type = detail::ConcurrentType::Native;
  int64_t write_buffer_size = 64 * 1024 * 1024;
  std::shared_ptr<class MemTableRepFactory> fallback;
  detail::PatriciaKeyType patricia_key_type = detail::PatriciaKeyType::UserKey;

  auto c = options.find("concurrent_type");
  if (c != options.end() && c->second == "none")
    concurrent_type = detail::ConcurrentType::None;

  auto u = options.find("use_virtual_mem");
  if (u != options.end() && u->second == "enable"){
#if defined(_WIN32) || defined(_WIN64)
    write_buffer_size = -1LL * 1073741824;
#else
    write_buffer_size = -16LL * 1073741824;
#endif
  }

  auto f = options.find("fallback");
  if (f != options.end() && f->second != "patricia") {
    fallback.reset(CreateMemTableRepFactory(f->second, options, s));
    if (!s->ok()) {
      *s = Status::InvalidArgument("NewPatriciaMemTableRepFactory", s->getState());
      return nullptr;
    }
  }
  
  auto p = options.find("key_catagory");
  if (p != options.end() && p->second == "full")
    patricia_key_type = detail::PatriciaKeyType::FullKey;

  return CreatePatriciaRepFactory(
      fallback,
      concurrent_type,
      patricia_key_type,
      write_buffer_size);
}

ROCKSDB_REGISTER_MEM_TABLE("patricia", PatriciaMemTableRepFactory);

}