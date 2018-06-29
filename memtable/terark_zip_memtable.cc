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
#include <mutex>

#include "db/memtable.h"
#include "rocksdb/memtablerep.h"
#include "util/arena.h"
#include "util/threaded_rbtree.h"
#include <terark/fsa/dynamic_patricia_trie.hpp>
#include <terark/heap_ext.hpp>
#include <terark/io/byte_swap.hpp>

namespace rocksdb {
namespace {

class PTrieRep : public MemTableRep {
  typedef size_t size_type;
  static size_type constexpr max_stack_depth = 2 * (sizeof(uint32_t) * 8 - 1);

  typedef threaded_rbtree_node_t<uint32_t> node_t;
  typedef threaded_rbtree_stack_t<node_t, max_stack_depth> stack_t;
  typedef threaded_rbtree_root_t<node_t, std::false_type, std::false_type> root_t;

#pragma pack(push)
#pragma pack(4)
  struct rep_node_t {
    node_t node;
    uint64_t tag;
    char prefixed_value[1];

    Slice value() { return GetLengthPrefixedSlice(prefixed_value); }
  };
#pragma pack(pop)

  static size_type constexpr rep_node_size = sizeof(node_t) + sizeof(uint64_t);

  struct deref_node_t {
    deref_node_t(terark::PatriciaTrie* _trie) : trie(_trie) {}
    node_t &operator()(size_type index) {
      return *(node_t*)trie->mem_get(index);
    }
    terark::PatriciaTrie* trie;
  };
  struct deref_key_t {
    deref_key_t(terark::PatriciaTrie* _trie) : trie(_trie) {}
    uint64_t operator()(size_type index) const {
      return ((rep_node_t*)trie->mem_get(index))->tag;
    }
    terark::PatriciaTrie* trie;
  };

  static size_t constexpr max_trie_count = 32;

  typedef std::greater<uint64_t> key_compare_t;

  static const char* build_key(terark::fstring user_key, uint64_t tag, std::string* buffer) {
    buffer->resize(0);
    buffer->reserve(user_key.size() + 8);
    buffer->append(user_key.data(), user_key.size());
    PutFixed64(buffer, tag);
    return buffer->data();
  }

  static std::mutex& sharding(const void* ptr, terark::valvec<std::mutex>& mutex) {
    uintptr_t val = size_t(ptr);
    return mutex[terark::byte_swap((val << 3) | (val >> 61)) % mutex.size()];
  }

private:
  mutable terark::valvec<std::unique_ptr<terark::PatriciaTrie>> trie_vec_;
  mutable terark::valvec<std::mutex> mutex_;
  std::atomic_bool immutable_;
  std::atomic_size_t num_entries_;
  size_t mem_size_;

public:
  explicit PTrieRep(const MemTableRep::KeyComparator &compare, Allocator *allocator,
                    const SliceTransform *, size_t sharding)
    : MemTableRep(allocator)
    , immutable_(false)
    , num_entries_(0)
    , mem_size_(0) {
    assert(sharding > 0);
    mutex_.reserve(sharding);
    for (size_t i = 0; i < sharding; ++i) {
      mutex_.unchecked_emplace_back();
    }
    trie_vec_.reserve(max_trie_count);
    trie_vec_.emplace_back(new terark::PatriciaTrie(sizeof(root_t), allocator->BlockSize()));
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
    auto insert_impl = [&](terark::PatriciaTrie* trie) {
      size_t node_index = trie->mem_alloc(node_size);
      if (node_index == terark::PatriciaTrie::mem_alloc_fail) {
        return false;
      }
      size_t root_index = trie->mem_alloc(sizeof(root_t));
      if (root_index == terark::PatriciaTrie::mem_alloc_fail) {
        trie->mem_free(node_index, node_size);
        return false;
      }
      rep_node_t* node = (rep_node_t*)trie->mem_get(node_index);
      node->tag = tag;
      memcpy(EncodeVarint32(node->prefixed_value, (uint32_t)value.size()),
             value.data(), value.size());
      root_t* root = new(trie->mem_get(root_index)) root_t();
      stack_t stack;
      stack.height = 0;
      threaded_rbtree_insert(*root, stack, deref_node_t(trie), node_index);

      terark::PatriciaTrie::WriterToken token(trie);
      uint32_t root_insert = (uint32_t)root_index;
      if (!trie->insert(key, &root_insert, &token)) {
        trie->mem_free(root_index, sizeof(root_t));
        root = (root_t*)trie->mem_get(*(uint32_t*)token.value());
        std::unique_lock<std::mutex> _lock(sharding(root, mutex_));
        threaded_rbtree_find_path_for_multi(*root, stack, deref_node_t(trie), tag,
                                            deref_key_t(trie), key_compare_t());
        threaded_rbtree_insert(*root, stack, deref_node_t(trie), node_index);
        return true;
      }
      else if (token.value() != nullptr) {
        return true;
      }
      trie->mem_free(root_index, sizeof(root_t));
      trie->mem_free(node_index, node_size);
      return false;
    };
    auto trie = trie_vec_.back().get();
    if (!insert_impl(trie)) {
      size_t new_mem_size = 0;
      for (size_t i = 0; i < trie_vec_.size(); ++i) {
        new_mem_size += trie_vec_[i]->mem_size();
      }
      size_t new_trie_size =
          std::max(trie->mem_size() * 2, key.size() + node_size) + 1024;
      trie = new terark::PatriciaTrie(sizeof(root_t), new_trie_size);
      trie_vec_.unchecked_emplace_back(trie);
      mem_size_ = new_mem_size;
      bool ok = insert_impl(trie);
      assert(ok); (void)ok;
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
      root_t* root = (root_t*)trie->mem_get(*(uint32_t*)token.value());
      auto contains_impl = [&] {
        auto index = threaded_rbtree_equal_unique(*root, deref_node_t(trie), tag,
                                                  deref_key_t(trie), key_compare_t());
        return index != node_t::nil_sentinel;
      };
      if (immutable_) {
        if (contains_impl()) {
          return true;
        }
      }
      else {
        std::unique_lock<std::mutex> _lock(sharding(root, mutex_));
        if (contains_impl()) {
          return true;
        }
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
    return mem_size_ + trie_vec_.back()->mem_size();
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
        return node->value();
      }
      virtual std::pair<Slice, Slice> GetKeyValue() const override {
        return { Context::GetKey(), Context::GetValue() };
      }

      KeyValuePair* Update(rep_node_t* _node) {
        node = _node;
        build_key(find_key, node->tag, &buffer);
        return this;
      }

      terark::fstring find_key;
      rep_node_t* node;
      std::string buffer;
    } ctx;

    Slice internal_key = k.internal_key();
    ctx.find_key = terark::fstring(internal_key.data(), internal_key.size() - 8);
    uint64_t tag = DecodeFixed64(ctx.find_key.end());

    struct HeapItem {
      uint64_t tag;
      terark::PatriciaTrie* trie;
      root_t* root;
      size_t index;
    };
    HeapItem heap[max_trie_count];
    size_t heap_size = 0;

    for (size_t i = 0; i < trie_vec_.size(); ++i) {
      auto trie = trie_vec_[i].get();
      terark::PatriciaTrie::ReaderToken token(trie);
      if (!trie->lookup(ctx.find_key, &token)) {
        continue;
      }
      root_t* root = (root_t*)trie->mem_get(*(uint32_t*)token.value());
      auto get_impl = [&] {
        size_t index = threaded_rbtree_lower_bound(*root, deref_node_t(trie), tag,
                                                   deref_key_t(trie), key_compare_t());
        if (index != node_t::nil_sentinel) {
          heap[heap_size++] = HeapItem{ deref_key_t(trie)(index), trie, root, index };
        }
      };
      if (immutable_) {
        get_impl();
      } else {
        std::unique_lock<std::mutex> _lock(sharding(root, mutex_));
        get_impl();
      }
    }
    auto heap_comp = [](const HeapItem& l, const HeapItem& r) {
      return l.tag < r.tag;
    };
    std::make_heap(heap, heap + heap_size, heap_comp);
    auto heap_next = [&]() {
      heap->index = threaded_rbtree_move_next(heap->index, deref_node_t(heap->trie));
      if (heap->index != node_t::nil_sentinel) {
        heap->tag = deref_key_t(heap->trie)(heap->index);
        terark::adjust_heap_top(heap, heap_size, heap_comp);
      } else {
        std::pop_heap(heap, heap + heap_size, heap_comp);
        --heap_size;
      }
    };
    while (heap_size > 0 &&
           callback_func(callback_args,
                         ctx.Update((rep_node_t*)heap->trie->mem_get(heap->index)))) {
      if (immutable_) {
        heap_next();
      } else {
        std::unique_lock<std::mutex> _lock(sharding(heap->root, mutex_));
        heap_next();
      }
    }
  }

  virtual ~PTrieRep() override {}

  // used for immutable
  struct dummy_lock {
    template<class T> dummy_lock(T const &) {}
  };

  template<bool heap_mode, class lock_t>
  class Iterator : public MemTableRep::Iterator, boost::noncopyable {
    typedef terark::PatriciaTrie::ReaderToken token_t;
    friend class PTrieRep;
    static constexpr size_t num_words_update = 1024;

    struct HeapItem : boost::noncopyable {
      token_t token;
      terark::ADFA_LexIterator* iter;
      size_t where;
      uint64_t tag;
      size_t num_words;

      HeapItem(terark::PatriciaTrie* trie)
        : token(trie)
        , iter(trie->adfa_make_iter())
        , where(node_t::nil_sentinel)
        , tag(0)
        , num_words(trie->num_words()) {
      }
      ~HeapItem() {
        delete iter;
      }
      bool Update() {
        if (token.trie()->num_words() - num_words > num_words_update) {
          token.update();
          num_words = token.trie()->num_words();
          return true;
        }
        return false;
      }
      root_t* Value() {
        auto trie = token.trie();
        size_t root_index = *(uint32_t*)trie->get_valptr(iter->word_state());
        return (root_t*)trie->mem_get(root_index);
      }
      void Seek(PTrieRep* rep, terark::fstring find_key, uint64_t find_tag) {
        Update();
        if (!iter->seek_lower_bound(find_key)) {
          where = node_t::nil_sentinel;
          return;
        }
        auto trie = token.trie();
        auto root = Value();
        if (iter->word() == find_key) {
          {
            lock_t _lock(sharding(root, rep->mutex_));
            where = threaded_rbtree_lower_bound(*root, deref_node_t(trie), find_tag,
                                                deref_key_t(trie), key_compare_t());
          }
          if (where != node_t::nil_sentinel) {
            tag = deref_key_t(trie)(where);
            return;
          }
          if (!iter->incr()) {
            return;
          }
          root = Value();
        }
        assert(iter->word() > find_key);
        lock_t _lock(sharding(root, rep->mutex_));
        where = root->get_most_left(deref_node_t(trie));
        tag = deref_key_t(trie)(where);
      }
      void SeekForPrev(PTrieRep* rep, terark::fstring find_key, uint64_t find_tag) {
        Update();
        if (!iter->seek_rev_lower_bound(find_key)) {
          where = node_t::nil_sentinel;
          return;
        }
        auto trie = token.trie();
        auto root = Value();
        if (iter->word() == find_key) {
          {
            lock_t _lock(sharding(root, rep->mutex_));
            where =
                threaded_rbtree_reverse_lower_bound(*root, deref_node_t(trie),
                                                    find_tag, deref_key_t(trie),
                                                    key_compare_t());
          }
          if (where != node_t::nil_sentinel) {
            tag = deref_key_t(trie)(where);
            return;
          }
          if (!iter->decr()) {
            return;
          }
          root = Value();
        }
        assert(iter->word() < find_key);
        lock_t _lock(sharding(root, rep->mutex_));
        where = root->get_most_right(deref_node_t(trie));
        tag = deref_key_t(trie)(where);
      }
      void SeekToFirst(PTrieRep* rep) {
        Update();
        if (!iter->seek_begin()) {
          where = node_t::nil_sentinel;
          return;
        }
        auto trie = token.trie();
        auto root = Value();
        lock_t _lock(sharding(root, rep->mutex_));
        where = root->get_most_left(deref_node_t(trie));
        tag = deref_key_t(trie)(where);
      }
      void SeekToLast(PTrieRep* rep) {
        Update();
        if (!iter->seek_end()) {
          where = node_t::nil_sentinel;
          return;
        }
        auto trie = token.trie();
        auto root = Value();
        lock_t _lock(sharding(root, rep->mutex_));
        where = root->get_most_right(deref_node_t(trie));
        tag = deref_key_t(trie)(where);
      }
      void Next(PTrieRep* rep, std::string& internal_key) {
        if (Update()) {
          terark::fstring find_key(internal_key.data(), internal_key.size() - 8);
          iter->seek_lower_bound(find_key);
        }
        auto trie = token.trie();
        where = threaded_rbtree_move_next(where, deref_node_t(trie));
        if (where == node_t::nil_sentinel) {
          if (!iter->incr()) {
            return;
          }
          auto root = Value();
          lock_t _lock(sharding(root, rep->mutex_));
          where = root->get_most_left(deref_node_t(trie));
        }
        tag = deref_key_t(trie)(where);
      }
      void Prev(PTrieRep* rep, std::string& internal_key) {
        if (Update()) {
          terark::fstring find_key(internal_key.data(), internal_key.size() - 8);
          iter->seek_lower_bound(find_key);
        }
        auto trie = token.trie();
        where = threaded_rbtree_move_prev(where, deref_node_t(trie));
        if (where == node_t::nil_sentinel) {
          if (!iter->decr()) {
            return;
          }
          auto root = Value();
          lock_t _lock(sharding(root, rep->mutex_));
          where = root->get_most_right(deref_node_t(trie));
        }
        tag = deref_key_t(trie)(where);
      }
    };
    std::string buffer_;
    PTrieRep* rep_;
    union {
      struct {
        HeapItem* array;
        size_t count;
        HeapItem** heap;
        size_t size;
      } multi_;
      HeapItem single_;
    };
    int direction_;

    Iterator(PTrieRep* rep)
      : rep_(rep)
      , direction_(0) {
      if (heap_mode) {
        multi_.count = rep_->trie_vec_.size();
        multi_.array = (HeapItem*)malloc(sizeof(HeapItem) * multi_.count);
        multi_.heap = (HeapItem**)malloc(sizeof(void*) * multi_.count);
        for (size_t i = 0; i < multi_.count; ++i) {
          multi_.heap[i] =
              new(multi_.array + i) HeapItem(rep_->trie_vec_[i].get());
        }
        multi_.size = 0;
      } else {
        new(&single_) HeapItem(rep_->trie_vec_.front().get());
      }
    }

    const HeapItem* Current() const {
      if (heap_mode) {
        return *multi_.heap;
      } else {
        return &single_;
      }
    }
    HeapItem* Current() {
      if (heap_mode) {
        return *multi_.heap;
      }
      else {
        return &single_;
      }
    }
    terark::fstring CurrentKey() {
      return Current()->iter->word();
    }
    uint64_t CurrentTag() {
      return Current()->tag;
    }

    struct ForwardComp {
      bool operator()(HeapItem* l, HeapItem* r) const {
        int c = terark::fstring_func::compare3()(l->iter->word(), r->iter->word());
        if (c == 0) {
          return l->tag < r->tag;
        } else {
          return c > 0;
        }
      }
    };
    struct BackwardComp {
      bool operator()(HeapItem* l, HeapItem* r) const {
        int c = terark::fstring_func::compare3()(l->iter->word(), r->iter->word());
        if (c == 0) {
          return l->tag > r->tag;
        } else {
          return c < 0;
        }
      }
    };

    template<int direction, class func_t>
    void Rebuild(func_t&& callback_func) {
      static_assert(direction == 1 || direction == -1, "direction must be 1 or -1");
      direction_ = direction;
      multi_.size = multi_.count;
      if (direction == 1) {
        for (size_t i = 0; i < multi_.size; ) {
          if (callback_func(multi_.heap[i])) {
            ++i;
          } else {
            --multi_.size;
            std::swap(multi_.heap[i], multi_.heap[multi_.size]);
          }
        }
        std::make_heap(multi_.heap, multi_.heap + multi_.size, ForwardComp());
      } else {
        for (size_t i = 0; i < multi_.size; ) {
          if (callback_func(multi_.heap[i])) {
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

  public:
    virtual ~Iterator() {
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
    virtual bool Valid() const override {
      return direction_ != 0;
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
      const HeapItem* item = Current();
      return ((rep_node_t*)item->token.trie()->mem_get(item->where))->value();
    }

    virtual std::pair<Slice, Slice> GetKeyValue() const override {
      return { Iterator::GetKey(), Iterator::GetValue() };
    }

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual void Next() override {
      if (heap_mode) {
        if (direction_ != 1) {
          terark::fstring find_key(buffer_.data(), buffer_.size() - 8);
          uint64_t tag = DecodeFixed64(find_key.end());
          Rebuild<1>([&](HeapItem* item) {
            item->Seek(rep_, find_key, tag);
            return item->where != node_t::nil_sentinel;
          });
          if (multi_.size == 0) {
            direction_ = 0;
            return;
          }
        }
        multi_.heap[0]->Next(rep_, buffer_);
        if (multi_.heap[0]->where == node_t::nil_sentinel) {
          std::pop_heap(multi_.heap, multi_.heap + multi_.size, ForwardComp());
          if (--multi_.size == 0) {
            direction_ = 0;
            return;
          }
        } else {
          terark::adjust_heap_top(multi_.heap, multi_.size, ForwardComp());
        }
      } else {
        single_.Next(rep_, buffer_);
        if (single_.where == node_t::nil_sentinel) {
          direction_ = 0;
          return;
        }
      }
      build_key(CurrentKey(), CurrentTag(), &buffer_);
    }

    // Advances to the previous position.
    // REQUIRES: Valid()
    virtual void Prev() override {
      if (heap_mode) {
        if (direction_ != -1) {
          terark::fstring find_key(buffer_.data(), buffer_.size() - 8);
          uint64_t tag = DecodeFixed64(find_key.end());
          Rebuild<-1>([&](HeapItem* item) {
            item->SeekForPrev(rep_, find_key, tag);
            return item->where != node_t::nil_sentinel;
          });
          if (multi_.size == 0) {
            direction_ = 0;
            return;
          }
        }
        multi_.heap[0]->Prev(rep_, buffer_);
        if (multi_.heap[0]->where == node_t::nil_sentinel) {
          std::pop_heap(multi_.heap, multi_.heap + multi_.size, BackwardComp());
          if (--multi_.size == 0) {
            direction_ = 0;
            return;
          }
        } else {
          terark::adjust_heap_top(multi_.heap, multi_.size, BackwardComp());
        }
      } else {
        single_.Prev(rep_, buffer_);
        if (single_.where == node_t::nil_sentinel) {
          direction_ = 0;
          return;
        }
      }
      build_key(CurrentKey(), CurrentTag(), &buffer_);
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
        Rebuild<1>([&](HeapItem* item) {
          item->Seek(rep_, find_key, tag);
          return item->where != node_t::nil_sentinel;
        });
        if (multi_.size == 0) {
          direction_ = 0;
          return;
        }
      } else {
        single_.Seek(rep_, find_key, tag);
        if (single_.where == node_t::nil_sentinel) {
          direction_ = 0;
          return;
        }
        direction_ = 1;
      }
      build_key(CurrentKey(), CurrentTag(), &buffer_);
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
        Rebuild<-1>([&](HeapItem* item) {
          item->SeekForPrev(rep_, find_key, tag);
          return item->where != node_t::nil_sentinel;
        });
        if (multi_.size == 0) {
          direction_ = 0;
          return;
        }
      } else {
        single_.SeekForPrev(rep_, find_key, tag);
        if (single_.where == node_t::nil_sentinel) {
          direction_ = 0;
          return;
        }
        direction_ = -1;
      }
      build_key(CurrentKey(), CurrentTag(), &buffer_);
    }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToFirst() override {
      if (heap_mode) {
        Rebuild<1>([&](HeapItem* item) {
          item->SeekToFirst(rep_);
          return item->where != node_t::nil_sentinel;
        });
        if (multi_.size == 0) {
          direction_ = 0;
          return;
        }
      } else {
        single_.SeekToFirst(rep_);
        if (single_.where != node_t::nil_sentinel) {
          direction_ = 0;
          return;
        }
        direction_ = 1;
      }
      build_key(CurrentKey(), CurrentTag(), &buffer_);
    }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToLast() override {
      if (heap_mode) {
        Rebuild<-1>([&](HeapItem* item) {
          item->SeekToLast(rep_);
          return item->where != node_t::nil_sentinel;
        });
        if (multi_.size == 0) {
          direction_ = 0;
          return;
        }
      } else {
        single_.SeekToLast(rep_);
        if (single_.where != node_t::nil_sentinel) {
          direction_ = 0;
          return;
        }
        direction_ = -1;
      }
      build_key(CurrentKey(), CurrentTag(), &buffer_);
    }

    virtual bool IsKeyPinned() const override { return false; }

    virtual bool IsSeekForPrevSupported() const { return true; }
  };
  virtual MemTableRep::Iterator *GetIterator(Arena *arena = nullptr) override {
    if (immutable_) {
      if (trie_vec_.size() == 1) {
        typedef PTrieRep::Iterator<false, dummy_lock> i_t;
        return arena ? new(arena->AllocateAligned(sizeof(i_t))) i_t(this)
                     : new i_t(this);
      } else {
        typedef PTrieRep::Iterator<true, dummy_lock> i_t;
        return arena ? new(arena->AllocateAligned(sizeof(i_t))) i_t(this)
                     : new i_t(this);
      }
    } else {
      if (trie_vec_.size() == 1) {
        typedef PTrieRep::Iterator<false, std::unique_lock<std::mutex>> i_t;
        return arena ? new(arena->AllocateAligned(sizeof(i_t))) i_t(this)
                     : new i_t(this);
      } else {
        typedef PTrieRep::Iterator<true, std::unique_lock<std::mutex>> i_t;
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
