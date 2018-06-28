//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <utility>
#include <iterator>
#include <numeric>
#include <memory>
#include <cstdlib>
#include <vector>
#include <algorithm>
#include <atomic>

#include "db/memtable.h"
#include "rocksdb/memtablerep.h"
#include "util/arena.h"
#include "util/mutexlock.h"
#include "util/threaded_rbtree.h"
#include <terark/fsa/dynamic_patricia_trie.hpp>
#include <terark/heap_ext.hpp>
#include <terark/io/byte_swap.hpp>

namespace rocksdb {
namespace {

class PTrieRep : public MemTableRep {
  typedef size_t size_type;
  static size_type constexpr max_stack_depth = 2 * (sizeof(uintptr_t) * 8 - 1);

  typedef threaded_rbtree_node_t<uintptr_t, std::false_type> node_t;
  typedef threaded_rbtree_stack_t<node_t, max_stack_depth> stack_t;
  typedef threaded_rbtree_root_t<node_t, std::false_type, std::false_type> root_t;

  struct rep_node_t {
    node_t node;
    uint64_t tag;
    char prefixed_value[1];

    Slice value() { return GetLengthPrefixedSlice(prefixed_value); }
  };
  static size_type constexpr rep_node_size = sizeof(node_t) + sizeof(uint64_t);

  struct deref_node_t {
    node_t &operator()(size_type index) {
      return *(node_t*)index;
    }
  };
  struct deref_key_t {
    uint64_t operator()(size_type index) const {
      return ((rep_node_t*)index)->tag;
    }
  };
  typedef std::greater<uint64_t> key_compare_t;

  static const char* build_key(terark::fstring user_key, uintptr_t index, std::string* buffer) {
    rep_node_t* node = (rep_node_t*)index;
    buffer->resize(0);
    buffer->reserve(user_key.size() + 8);
    buffer->append(user_key.data(), user_key.size());
    PutFixed64(buffer, node->tag);
    return buffer->data();
  }

  static port::Mutex* sharding(const void* ptr, terark::valvec<port::Mutex>& mutex) {
    uintptr_t val = size_t(ptr);
    return &mutex[terark::byte_swap((val << 3) | (val >> 61)) % mutex.size()];
  }

private:
  mutable terark::valvec<std::unique_ptr<terark::PatriciaTrie>> trie_vec_;
  mutable terark::valvec<port::Mutex> mutex_;
  std::atomic_bool immutable_;
  std::atomic_size_t num_entries_;

public:
  explicit PTrieRep(const MemTableRep::KeyComparator &compare, Allocator *allocator,
                    const SliceTransform *, size_t sharding)
    : MemTableRep(allocator)
    , immutable_(false)
    , num_entries_(0) {
    assert(sharding > 0);
    mutex_.reserve(sharding);
    for (size_t i = 0; i < sharding; ++i) {
      mutex_.unchecked_emplace_back();
    }
    trie_vec_.reserve(32);
    trie_vec_.emplace_back(new terark::PatriciaTrie(sizeof(void*), allocator->BlockSize()));
  }

  virtual KeyHandle Allocate(const size_t len, char **buf) override {
    assert(false);
    return nullptr;
  }

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  virtual void Insert(KeyHandle handle) override {
    assert(false);
  }

  virtual void InsertKeyValue(const Slice& internal_key, const Slice& value) override {
    terark::fstring key(internal_key.data(), internal_key.data() + internal_key.size() - 8);
    uint64_t tag = DecodeFixed64(key.end());

    size_t node_size = rep_node_size + value.size() + VarintLength(value.size());
    rep_node_t* node = (rep_node_t*)allocator_->AllocateAligned(node_size);
    node->tag = tag;
    memcpy(EncodeVarint32(node->prefixed_value, (uint32_t)value.size()),
           value.data(), value.size());

    class Token : public terark::PatriciaTrie::WriterToken {
    public:
      Token(terark::PatriciaTrie* trie, rep_node_t* node, Allocator* allocator)
        : terark::PatriciaTrie::WriterToken(trie)
        , node_(node)
        , allocator_(allocator) {}
    protected:
      void init_value(void* dest, const void* src, size_t valsize) override {
        assert(src == nullptr);
        assert(valsize == sizeof(void*));
        stack_t stack;
        stack.height = 0;
        root_t* root = new(allocator_->AllocateAligned(sizeof(root_t))) root_t();
        threaded_rbtree_insert(*root, stack, deref_node_t(), (uintptr_t)node_);
        memcpy(dest, &root, sizeof(void*));
      }
    private:
      rep_node_t * node_;
      Allocator* allocator_;
    };
    
    for (size_t i = 0; ; ++i) {
      auto* trie = trie_vec_[i].get();
      Token token(trie, node, allocator_);
      if (!trie->insert(key, nullptr, &token)) {
        MutexLock _lock(sharding(token.value(), mutex_));
        root_t* root = *(root_t**)token.value();
        stack_t stack;
        threaded_rbtree_find_path_for_multi(*root, stack, deref_node_t(), tag,
                                            deref_key_t(), key_compare_t());
        threaded_rbtree_insert(*root, stack, deref_node_t(), (uintptr_t)node);
        break;
      } else if (token.value() != nullptr) {
        break;
      } else if (i == trie_vec_.size() - 1) {
        assert(i < trie_vec_.capacity());
        trie_vec_.unchecked_emplace_back(
            new terark::PatriciaTrie(sizeof(void*),
                                     allocator_->BlockSize() << trie_vec_.size()));
      }
    }
    ++num_entries_;
  }

  // Returns true iff an entry that compares equal to key is in the list.
  virtual bool Contains(const Slice& internal_key) const override {
    terark::fstring find_key(internal_key.data(), internal_key.size() - 8);
    uint64_t tag = DecodeFixed64(find_key.end());
    for (size_t i = 0; i < trie_vec_.size(); ++i) {
      auto trie = trie_vec_[i].get();
      terark::PatriciaTrie::ReaderToken token(trie);
      if (!trie->lookup(find_key, &token)) {
        continue;
      }
      auto contains_impl = [&] {
        root_t* root = *(root_t**)token.value();
        auto index = threaded_rbtree_equal_unique(*root, deref_node_t(), tag,
                                                  deref_key_t(), key_compare_t());
        return index != node_t::nil_sentinel;
      };
      if (immutable_) {
        return contains_impl();
      }
      else {
        MutexLock _lock(sharding(token.value(), mutex_));
        return contains_impl();
      }
    }
    return false;
  }

  virtual void MarkReadOnly() override {
    for (size_t i = 0; i < trie_vec_.size(); ++i) {
      trie_vec_[i]->set_readonly();
    }
    immutable_ = true;
  }

  virtual size_t ApproximateMemoryUsage() override {
    size_t mem_size = 0;
    for (size_t i = 0; i < trie_vec_.size(); ++i) {
      mem_size += trie_vec_[i]->mem_size();
    }
    return mem_size;
  }

  virtual uint64_t ApproximateNumEntries(const Slice& start_ikey,
    const Slice& end_ikey) override {
    return 0;
  }

  virtual void Get(const LookupKey &k, void *callback_args,
                   bool(*callback_func)(void *arg, const KeyValuePair*)) override {

    class Context : public KeyValuePair {
    public:
      virtual Slice GetKey() const override {
        return buffer;
      }
      virtual Slice GetValue() const override {
        return ((rep_node_t*)index)->value();
      }
      virtual std::pair<Slice, Slice> GetKeyValue() const override {
        return { GetKey(), GetValue() };
      }

      KeyValuePair* Update() {
        build_key(find_key, index, &buffer);
        return this;
      }

      terark::fstring find_key;
      size_t index;
      uint64_t tag;
      std::string buffer;
    } ctx;

    Slice internal_key = k.internal_key();
    ctx.find_key = terark::fstring(internal_key.data(), internal_key.size() - 8);
    ctx.tag = DecodeFixed64(ctx.find_key.end());


    for (size_t i = 0; i < trie_vec_.size(); ++i) {
      auto trie = trie_vec_[i].get();
      terark::PatriciaTrie::ReaderToken token(trie);
      if (!trie->lookup(ctx.find_key, &token)) {
        continue;
      }
      auto get_impl = [&] {
        root_t* root = *(root_t**)token.value();
        ctx.index = threaded_rbtree_lower_bound(*root, deref_node_t(), ctx.tag,
                                                 deref_key_t(), key_compare_t());
        while (ctx.index != node_t::nil_sentinel &&
               callback_func(callback_args, ctx.Update())) {
          ctx.index = threaded_rbtree_move_next(ctx.index, deref_node_t());
        }
      };
      if (immutable_) {
        get_impl();
      } else {
        MutexLock _lock(sharding(token.value(), mutex_));
        get_impl();
      }
      break;
    }
  }

  virtual ~PTrieRep() override {}

  // used for immutable
  struct DummyLock {
    template<class T> DummyLock(T const &) {}
  };

  template<bool heap_mode, class Lock>
  class Iterator : public MemTableRep::Iterator, boost::noncopyable {
    typedef terark::PatriciaTrie::ReaderToken token_t;
    friend class PTrieRep;
    static constexpr size_t num_words_update = 1024;

    struct Item : boost::noncopyable {
      terark::PatriciaTrie* trie;
      token_t token;
      terark::ADFA_LexIterator* iter;
      size_t num_words;

      Item(terark::PatriciaTrie* _trie)
        : trie(_trie)
        , token(_trie)
        , iter(_trie->adfa_make_iter())
        , num_words(trie->num_words()) {
      }
      ~Item() {
        delete iter;
      }
      bool Update() {
        if (trie->num_words() - num_words > num_words_update) {
          token.update();
          return true;
        }
        return false;
      }
    };
    std::string buffer_;
    PTrieRep* rep_;
    union {
      struct {
        Item* array;
        size_t count;
        Item** heap;
        size_t size;
      } multi_;
      Item single_;
    };
    uintptr_t where_;
    int direction_;

    Iterator(PTrieRep* rep)
      : rep_(rep)
      , where_(node_t::nil_sentinel)
      , direction_(0) {
      if (heap_mode) {
        multi_.count = rep_->trie_vec_.size();
        multi_.array = (Item*)malloc(sizeof(Item) * multi_.count);
        multi_.heap = (Item**)malloc(sizeof(void*) * multi_.count);
        for (size_t i = 0; i < multi_.count; ++i) {
          multi_.heap[i] =
              new(multi_.array + i) Item(rep_->trie_vec_[i].get());
        }
        multi_.size = 0;
      } else {
        new(&single_) Item(rep_->trie_vec_.front().get());
      }
    }

    Item& Current() {
      if (heap_mode) {
        return **multi_.heap;
      } else {
        return single_;
      }
    }

    const char* CurrentValue() {
      auto& item = Current();
      return (const char*)item.trie->get_valptr(item.iter->word_state());
    }

    struct ForwardComp {
      bool operator()(Item* l, Item* r) const {
        return l->iter->word() > r->iter->word();
      }
    };
    struct BackwardComp {
      bool operator()(Item* l, Item* r) const {
        return l->iter->word() < r->iter->word();
      }
    };

    void Rebuild(int direction, void *arg,
                 bool(*callback_func)(void *arg, terark::ADFA_LexIterator* iter)) {
      assert(std::abs(direction) == 1);
      direction_ = direction;
      multi_.size = multi_.count;
      if (direction == 1) {
        for (size_t i = 0; i < multi_.size; ) {
          multi_.heap[i]->Update();
          if (callback_func(arg, multi_.heap[i]->iter)) {
            ++i;
          } else {
            --multi_.size;
            std::swap(multi_.heap[i], multi_.heap[multi_.size]);
          }
        }
        std::make_heap(multi_.heap, multi_.heap + multi_.size, ForwardComp());
      } else {
        for (size_t i = 0; i < multi_.size; ) {
          multi_.heap[i]->Update();
          if (callback_func(arg, multi_.heap[i]->iter)) {
            ++i;
          }
          else {
            --multi_.size;
            std::swap(multi_.heap[i], multi_.heap[multi_.size]);
          }
        }
        std::make_heap(multi_.heap, multi_.heap + multi_.size, BackwardComp());
      }
    }

    void UpdateIterator() {
      if (Current().Update()) {
        terark::fstring find_key(buffer_.data(), buffer_.size() - 8);
        Current().iter->seek_lower_bound(find_key);
      }
    }

    bool ItemNext() {
      if (heap_mode) {
        if (direction_ != 1) {
          terark::fstring find_key(buffer_.data(), buffer_.size() - 8);
          Rebuild(1, &find_key, [](void *arg, terark::ADFA_LexIterator* iter) {
            return iter->seek_lower_bound(*(terark::fstring*)arg);
          });
          if (multi_.size == 0) {
            return false;
          }
        } else {
          UpdateIterator();
        }
        if (multi_.heap[0]->iter->incr()) {
          terark::adjust_heap_top(multi_.heap, multi_.size, ForwardComp());
        } else {
          std::pop_heap(multi_.heap, multi_.heap + multi_.size, ForwardComp());
          if (--multi_.size == 0) {
            return false;
          }
        }
      } else {
        UpdateIterator();
        if (!single_.iter->incr()) {
          return false;
        }
      }
      const char* value = CurrentValue();
      MutexLock _lock(sharding(value, rep_->mutex_));
      root_t* root = *(root_t**)value;
      where_ = root->get_most_left(deref_node_t());
      assert(where_ != node_t::nil_sentinel);
      return true;
    }

    bool ItemPrev() {
      if (heap_mode) {
        if (direction_ != -1) {
          terark::fstring find_key(buffer_.data(), buffer_.size() - 8);
          Rebuild(-1, &find_key, [](void *arg, terark::ADFA_LexIterator* iter) {
            return iter->seek_rev_lower_bound(*(terark::fstring*)arg);
          });
          if (multi_.size == 0) {
            return false;
          }
        } else {
          UpdateIterator();
        }
        if (multi_.heap[0]->iter->decr()) {
          terark::adjust_heap_top(multi_.heap, multi_.size, BackwardComp());
        } else {
          std::pop_heap(multi_.heap, multi_.heap + multi_.size, BackwardComp());
          if (--multi_.size == 0) {
            return false;
          }
        }
      } else {
        UpdateIterator();
        if (!single_.iter->decr()) {
          return false;
        }
      }
      const char* value = CurrentValue();
      MutexLock _lock(sharding(value, rep_->mutex_));
      root_t* root = *(root_t**)value;
      where_ = root->get_most_right(deref_node_t());
      assert(where_ != node_t::nil_sentinel);
      return true;
    }

  public:
    virtual ~Iterator() {
      if (heap_mode) {
        free(multi_.heap);
        for (size_t i = 0; i < multi_.count; ++i) {
          multi_.array[i].~Item();
        }
        free(multi_.array);
      } else {
        single_.~Item();
      }
    }

    // Returns true iff the iterator is positioned at a valid node.
    virtual bool Valid() const override {
      return where_ != node_t::nil_sentinel;
    }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    virtual const char *key() const override {
      assert(false);
      return nullptr;
    }

    virtual Slice GetKey() const override {
      return buffer_;
    }

    virtual Slice GetValue() const override {
      return ((rep_node_t*)where_)->value();
    }

    virtual std::pair<Slice, Slice> GetKeyValue() const override {
      return { GetKey(), GetValue() };
    }

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual void Next() override {
      {
        MutexLock _lock(sharding(CurrentValue(), rep_->mutex_));
        where_ = threaded_rbtree_move_next(where_, deref_node_t());
      }
      if (where_ == node_t::nil_sentinel && !ItemNext()) {
        return;
      }
      build_key(Current().iter->word(), where_, &buffer_);
    }

    // Advances to the previous position.
    // REQUIRES: Valid()
    virtual void Prev() override {
      {
        MutexLock _lock(sharding(CurrentValue(), rep_->mutex_));
        where_ = threaded_rbtree_move_prev(where_, deref_node_t());
      }
      if (where_ == node_t::nil_sentinel && !ItemPrev()) {
        return;
      }
      build_key(Current().iter->word(), where_, &buffer_);
    }

    // Advance to the first entry with a key >= target
    virtual void Seek(const Slice &user_key, const char *memtable_key)
      override {
      terark::fstring find_key;
      if (memtable_key != nullptr) {
        Slice internal_key = GetLengthPrefixedSlice(memtable_key);
        find_key = terark::fstring(internal_key.data(), internal_key.size() - 8);
      } else {
        find_key = terark::fstring(user_key.data(), user_key.size() - 8);
      }
      uint64_t tag = DecodeFixed64(find_key.end());

      if (heap_mode) {
        Rebuild(1, &find_key, [](void *arg, terark::ADFA_LexIterator* iter) {
          return iter->seek_lower_bound(*(terark::fstring*)arg);
        });
        if (multi_.size == 0) {
          where_ = node_t::nil_sentinel;
          return;
        }
      } else {
        single_.Update();
        if (!single_.iter->seek_lower_bound(find_key)) {
          where_ = node_t::nil_sentinel;
          return;
        }
      }
      const void* value = CurrentValue();
      if (Current().iter->word() == find_key) {
        {
          MutexLock _lock(sharding(value, rep_->mutex_));
          root_t* root = *(root_t**)value;
          where_ = threaded_rbtree_lower_bound(*root, deref_node_t(), tag,
                                               deref_key_t(), key_compare_t());
        }
        if (where_ == node_t::nil_sentinel && !ItemNext()) {
          return;
        }
      } else {
        assert(Current().iter->word() > find_key);
        MutexLock _lock(sharding(value, rep_->mutex_));
        root_t* root = *(root_t**)value;
        where_ = root->get_most_left(deref_node_t());
      }
      build_key(Current().iter->word(), where_, &buffer_);
    }

    // retreat to the first entry with a key <= target
    virtual void SeekForPrev(const Slice& user_key, const char* memtable_key)
      override {
      terark::fstring find_key;
      if (memtable_key != nullptr) {
        Slice internal_key = GetLengthPrefixedSlice(memtable_key);
        find_key = terark::fstring(internal_key.data(), internal_key.size() - 8);
      } else {
        find_key = terark::fstring(user_key.data(), user_key.size() - 8);
      }
      uint64_t tag = DecodeFixed64(find_key.end());

      if (heap_mode) {
        Rebuild(-1, &find_key, [](void *arg, terark::ADFA_LexIterator* iter) {
          return iter->seek_rev_lower_bound(*(terark::fstring*)arg);
        });
        if (multi_.size == 0) {
          where_ = node_t::nil_sentinel;
          return;
        }
      } else {
        single_.Update();
        if (!single_.iter->seek_rev_lower_bound(find_key)) {
          where_ = node_t::nil_sentinel;
          return;
        }
      }
      const void* value = CurrentValue();
      if (Current().iter->word() == find_key) {
        {
          MutexLock _lock(sharding(value, rep_->mutex_));
          root_t* root = *(root_t**)value;
          where_ = threaded_rbtree_reverse_lower_bound(*root, deref_node_t(), tag,
                                                       deref_key_t(), key_compare_t());
        }
        if (where_ == node_t::nil_sentinel && !ItemPrev()) {
          return;
        }
      } else {
        assert(Current().iter->word() < find_key);
        MutexLock _lock(sharding(value, rep_->mutex_));
        root_t* root = *(root_t**)value;
        where_ = root->get_most_right(deref_node_t());
      }
      build_key(Current().iter->word(), where_, &buffer_);
    }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToFirst() override {
      if (heap_mode) {
        Rebuild(1, nullptr, [](void *arg, terark::ADFA_LexIterator* iter) {
          return iter->seek_begin();
        });
        if (multi_.size == 0) {
          where_ = node_t::nil_sentinel;
          return;
        }
      } else {
        single_.Update();
        if (!single_.iter->seek_begin()) {
          where_ = node_t::nil_sentinel;
          return;
        }
      }
      const void* value = CurrentValue();
      {
        MutexLock _lock(sharding(value, rep_->mutex_));
        root_t* root = *(root_t**)value;
        where_ = root->get_most_left(deref_node_t());
        assert(where_ != node_t::nil_sentinel);
      }
      build_key(Current().iter->word(), where_, &buffer_);
    }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToLast() override {
      if (heap_mode) {
        Rebuild(-1, nullptr, [](void *arg, terark::ADFA_LexIterator* iter) {
          return iter->seek_end();
        });
        if (multi_.size == 0) {
          where_ = node_t::nil_sentinel;
          return;
        }
      } else {
        single_.Update();
        if (!single_.iter->seek_end()) {
          where_ = node_t::nil_sentinel;
          return;
        }
      }
      const void* value = CurrentValue();
      {
        MutexLock _lock(sharding(value, rep_->mutex_));
        root_t* root = *(root_t**)value;
        where_ = root->get_most_right(deref_node_t());
        assert(where_ != node_t::nil_sentinel);
      }
      build_key(Current().iter->word(), where_, &buffer_);
    }

    virtual bool IsKeyPinned() const override { return false; }

    virtual bool IsSeekForPrevSupported() const { return true; }
  };
  virtual MemTableRep::Iterator *GetIterator(Arena *arena = nullptr) override {
    if (immutable_) {
      if (trie_vec_.size() == 1) {
        typedef PTrieRep::Iterator<false, DummyLock> i_t;
        return arena ? new(arena->AllocateAligned(sizeof(i_t))) i_t(this)
                     : new i_t(this);
      } else {
        typedef PTrieRep::Iterator<true, DummyLock> i_t;
        return arena ? new(arena->AllocateAligned(sizeof(i_t))) i_t(this)
                     : new i_t(this);
      }
    } else {
      if (trie_vec_.size() == 1) {
        typedef PTrieRep::Iterator<false, MutexLock> i_t;
        return arena ? new(arena->AllocateAligned(sizeof(i_t))) i_t(this)
                     : new i_t(this);
      } else {
        typedef PTrieRep::Iterator<true, MutexLock> i_t;
        return arena ? new(arena->AllocateAligned(sizeof(i_t))) i_t(this)
                     : new i_t(this);
      }
    }
  }
};

class PTrieMemtableRepFactory : public MemTableRepFactory {
public:
  PTrieMemtableRepFactory(size_t sharding_count,
                          std::shared_ptr<class MemTableRepFactory> fallback)
    : sharding_count_(sharding_count)
    , fallback_(fallback) {}
  virtual ~PTrieMemtableRepFactory() {}

  using MemTableRepFactory::CreateMemTableRep;
  virtual MemTableRep *CreateMemTableRep(
      const MemTableRep::KeyComparator &compare, Allocator *allocator,
      const SliceTransform *transform, Logger *logger) override {
    auto key_comparator = compare.icomparator();
    auto user_comparator = key_comparator->user_comparator();
    if (strcmp(user_comparator->Name(), BytewiseComparator()->Name()) == 0) {
      return new PTrieRep(compare, allocator, transform, sharding_count_);
    } else {
      return fallback_->CreateMemTableRep(compare, allocator, transform, logger);
    }
  }

  virtual const char *Name() const override {
    return "PatriciaTrieRepFactory";
  }

  virtual bool IsInsertConcurrentlySupported() const override {
    return false;
  }

private:
  size_t sharding_count_;
  std::shared_ptr<class MemTableRepFactory> fallback_;
};

}

MemTableRepFactory* NewPatriciaTrieRepFactory(size_t sharding_count,
                                              std::shared_ptr<class MemTableRepFactory> fallback) {
  if (!fallback) {
    fallback.reset(new SkipListFactory());
  }
  if (sharding_count == 0) {
    sharding_count = std::thread::hardware_concurrency() * 2 + 3;
  }
  return new PTrieMemtableRepFactory(sharding_count, fallback);
}

} // namespace rocksdb
