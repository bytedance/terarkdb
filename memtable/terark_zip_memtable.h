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
#include <rocksdb/convenience.h>
#include <terark/fsa/cspptrie.inl>
#include <terark/heap_ext.hpp>
#include <terark/io/byte_swap.hpp>
#include <terark/thread/instance_tls_owner.hpp>

namespace rocksdb {
namespace {

using namespace terark;

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

class TlsWriterToken : public Patricia::WriterToken {
  uint64_t tag_;
  Slice value_;

public:
  TlsWriterToken(Patricia *trie, uint64_t tag, const Slice &value)
      : Patricia::WriterToken(trie), tag_(tag), value_(value) {}

  uint64_t get_tag() { return tag_; }

protected:
  bool init_value(void *valptr, size_t valsize) noexcept override;
};

class PatriciaMemtableRep : public MemTableRep {
  ConcurrentType concurrent_type_;
  PatriciaKeyType patricia_key_type_;
  bool handle_duplicate_;

  std::atomic_bool immutable_;
  port::RWMutex mutex_;

  std::vector<MainPatricia *> trie_vec_;
  MainPatricia * target_trie_;
  int64_t write_buffer_size_;
  static const int64_t size_limit_ = 1LL << 30;

public:
  PatriciaMemtableRep(ConcurrentType concurrent_type,
                      PatriciaKeyType patricia_key_type,
                      bool handle_duplicate,
                      intptr_t write_buffer_size,
                      Allocator *allocator,
                      const MemTableRep::KeyComparator &compare)
                    : concurrent_type_(concurrent_type),
                      patricia_key_type_(patricia_key_type),
                      handle_duplicate_(handle_duplicate),
                      write_buffer_size_(write_buffer_size), 
                      MemTableRep(allocator),
                      immutable_(false) {
    trie_vec_.emplace_back(new MainPatricia(4, write_buffer_size_,
                                            concurrent_type_, ""));
  }

  ~PatriciaMemtableRep();

  virtual size_t ApproximateMemoryUsage() override;

  virtual uint64_t ApproximateNumEntries(
    const Slice &start_ikey,
    const Slice &end_ikey) override { return 0; }

  virtual bool Contains(const Slice &internal_key) const override;

  virtual void Get(
    const LookupKey &k,
    void *callback_args,
    bool (*callback_func)(void *arg, const KeyValuePair *)) override;

  virtual MemTableRep::Iterator *GetIterator(Arena *arena) override;

  virtual void Insert(KeyHandle handle) override { assert(false); }

  virtual bool InsertKeyValue(
    const Slice &internal_key,
    const Slice &value) override;

  virtual bool InsertKeyValueConcurrently(
    const Slice &internal_key,
    const Slice &value) override { return InsertKeyValue(internal_key, value); }

  virtual void MarkReadOnly() override;

  class HeapIterator : public MemTableRep::Iterator, boost::noncopyable {

    class HeapItem : boost::noncopyable {
      terark::Patricia::Iterator iter;
      uint64_t tag;
      size_t index;

    public:
      HeapItem(terark::Patricia *trie)
          : iter(trie), tag(uint64_t(-1)), index(size_t(-1)) {}

      struct VectorData {
        size_t size;
        const typename tag_vector_t::data_t *data;
      };

      VectorData GetVector();

      uint32_t GetValue() const;

      void Seek(terark::fstring find_key, uint64_t find_tag);

      void SeekForPrev(terark::fstring find_key, uint64_t find_tag);

      void SeekToFirst();

      void SeekToLast();

      void Next();

      void Prev();
    };

    struct ForwardComp {
      bool operator()(HeapItem *l, HeapItem *r) const {
        int c = terark::fstring_func::compare3()(l->iter.word(), r->iter.word());
        return c == 0 ? l->tag < r->tag : c > 0;
      }
    };

    struct BackwardComp {
      bool operator()(HeapItem *l, HeapItem *r) const {
        int c = terark::fstring_func::compare3()(l->iter.word(), r->iter.word());
        return c == 0 ? l->tag > r->tag : c < 0;
      }
    };

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
        (const char *)*heap[0]->mem_get(*heap[0]->GetValue()));
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
  }

private:
  static const char *build_key(
    terark::fstring user_key,
    uint64_t tag,
    valvec<char> *buffer);

  static const char *build_key(
    terark::fstring user_key,
    uint64_t tag,
    std::string *buffer);
};

} // namespace

class PatriciaMemTableRepFactory : public MemTableRepFactory {
private:
  std::shared_ptr<class MemTableRepFactory> fallback_;
  ConcurrentType concurrent_type_;
  PatriciaKeyType patricia_key_type_;
  int64_t write_buffer_size_;

public:
  PatriciaMemTableRepFactory(
    std::shared_ptr<class MemTableRepFactory> &fallback,
    ConcurrentType concurrent_type,
    PatriciaKeyType patricia_key_type,
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
    return concurrent_type_ != ConcurrentType::none;
  }

  virtual bool CanHandleDuplicatedKey() const override { return true; }
};

static MemTableRepFactory *
CreatePatriciaRepFactory(
    std::shared_ptr<class MemTableRepFactory> &fallback,
    ConcurrentType concurrent_type,
    PatriciaKeyType patricia_key_type,
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
  ConcurrentType concurrent_type = ConcurrentType::Native;
  int64_t write_buffer_size = 64 * 1024 * 1024;
  std::shared_ptr<class MemTableRepFactory> fallback;
  PatriciaKeyType patricia_key_type = PatriciaKeyType::UserKey;

  auto c = options.find("concurrent_type");
  if (c != options.end() && c->second == "none")
    concurrent_type = ConcurrentType::None;

  auto u = options.find("use_virtual_mem");
  if (u != options.end() && u->second == "enable"){
#if defined(_WIN32) || defined(_WIN64)
    write_buffer_size = -1073741824;
#else
    write_buffer_size = -16 * 1073741824;
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
    patricia_key_type = PatriciaKeyType::FullKey;

  return CreatePatriciaRepFactory(
      fallback,
      concurrent_type,
      patricia_key_type,
      write_buffer_size);
}

ROCKSDB_REGISTER_MEM_TABLE("patricia", PatriciaMemTableRepFactory);

} // namespace rocksdb
