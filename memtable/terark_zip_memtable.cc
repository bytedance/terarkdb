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
#include <terark/fsa/dynamic_patricia_trie.inl>
#include <terark/heap_ext.hpp>
#include <terark/io/byte_swap.hpp>

namespace rocksdb {
namespace {

class PTrieRep : public MemTableRep {

#pragma pack(push)
#pragma pack(4)
  struct tag_vector_t {
    uint32_t size;
    uint32_t loc;
    struct data_t {
      uint64_t tag;
      uint32_t loc;

      operator uint64_t() const {
        return tag;
      }
    };

    bool full() {
      return terark::fast_popcount(size) == 1;
    }
  };
#pragma pack(pop)

  static constexpr size_t max_trie_count = 32;

  static const char* build_key(terark::fstring user_key, uint64_t tag, std::string* buffer) {
    buffer->resize(0);
    buffer->reserve(user_key.size() + 8);
    buffer->append(user_key.data(), user_key.size());
    PutFixed64(buffer, tag);
    return buffer->data();
  }

private:
  struct trie_item_t {
    size_t accumulate_mem_size;
    std::unique_ptr<terark::MainPatricia> trie;
  };
  mutable trie_item_t trie_arr_[max_trie_count];
  trie_item_t* current_;
  std::atomic_bool immutable_;
  std::atomic_size_t num_entries_;
  size_t mem_size_;

public:
  explicit PTrieRep(size_t write_buffer_size, const MemTableRep::KeyComparator &compare,
                    Allocator *allocator, const SliceTransform *)
    : MemTableRep(allocator)
    , immutable_(false)
    , num_entries_(0)
    , mem_size_(0) {
    write_buffer_size = std::min(write_buffer_size, (size_t(1) << 34) - 64*1024);
    current_ = trie_arr_;
    current_->accumulate_mem_size = 0;
    current_->trie.reset(new terark::MainPatricia(sizeof(uint32_t), write_buffer_size,
                                                  terark::Patricia::OneWriteMultiRead));
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

  virtual bool InsertKeyValue(const Slice& internal_key, const Slice& value) override {
    terark::fstring key(internal_key.data(), internal_key.data() + internal_key.size() - 8);
    //uint64_t tag = DecodeFixed64(key.end());

    class Token : public terark::Patricia::WriterToken {
    public:
      Token(terark::Patricia* trie, uint64_t tag, const Slice& value)
        : terark::Patricia::WriterToken(trie)
        , tag_(tag)
        , value_(value) {}

      terark::MainPatricia* trie() {
        return static_cast<terark::MainPatricia*>(main());
      }

      uint64_t get_tag() {
        return tag_;
      }

    protected:
      bool init_value(void* valptr, size_t valsize) override {
        assert(valsize == sizeof(uint32_t));
        size_t vector_loc = terark::MainPatricia::mem_alloc_fail;
        size_t data_loc = terark::MainPatricia::mem_alloc_fail;
        size_t value_loc = terark::MainPatricia::mem_alloc_fail;
        size_t value_size = VarintLength(value_.size()) + value_.size();
        do {
          vector_loc = trie()->mem_alloc(sizeof(tag_vector_t));
          if (vector_loc == terark::MainPatricia::mem_alloc_fail) {
            break;
          }
          data_loc = trie()->mem_alloc(sizeof(tag_vector_t::data_t));
          if (data_loc == terark::MainPatricia::mem_alloc_fail) {
            break;
          }
          value_loc = trie()->mem_alloc(value_size);
          if (value_loc == terark::MainPatricia::mem_alloc_fail) {
            break;
          }

          char* value_dst = EncodeVarint32((char*)trie()->mem_get(value_loc),
                                           (uint32_t)value_.size());
          memcpy(value_dst, value_.data(), value_.size());
          auto* data = (tag_vector_t::data_t*)trie()->mem_get(data_loc);
          data->loc = (uint32_t)value_loc;
          data->tag = tag_;
          auto* vector = (tag_vector_t*)trie()->mem_get(vector_loc);
          vector->loc = (uint32_t)data_loc;
          vector->size = 1;

          uint32_t u32_vector_loc = vector_loc;
          memcpy(valptr, &u32_vector_loc, valsize);
          return true;
        } while (false);
        if (value_loc != terark::MainPatricia::mem_alloc_fail) {
          trie()->mem_free(value_loc, value_size);
        }
        if (data_loc != terark::MainPatricia::mem_alloc_fail) {
          trie()->mem_free(data_loc, sizeof(tag_vector_t::data_t));
        }
        if (vector_loc != terark::MainPatricia::mem_alloc_fail) {
          trie()->mem_free(vector_loc, sizeof(tag_vector_t));
        }
        return false;
      }
    private:
      uint64_t tag_;
      Slice value_;
    };

    enum class InsertResult {
      Success, Duplicated, Fail,
    };
    auto insert_impl = [&](terark::MainPatricia* trie) {
      Token token(trie, DecodeFixed64(key.end()), value);
      uint32_t value_storage;
      if (!trie->insert(key, &value_storage, &token)) {
        size_t vector_loc = *(uint32_t*)token.value();
        auto* vector = (tag_vector_t*)trie->mem_get(vector_loc);
        size_t data_loc = vector->loc;
        auto* data = (tag_vector_t::data_t*)trie->mem_get(data_loc);
        size_t size = vector->size;
        assert(size > 0);
        assert(token.get_tag() > data[size - 1].tag);
        if ((token.get_tag() >> 8) == (data[size - 1].tag >> 8)) {
          return InsertResult::Duplicated;
        }
        size_t value_size = VarintLength(value.size()) + value.size();
        size_t value_loc = trie->mem_alloc(value_size);
        if (value_loc == terark::MainPatricia::mem_alloc_fail) {
          return InsertResult::Fail;
        }
        memcpy(EncodeVarint32((char*)trie->mem_get(value_loc), (uint32_t)value.size()),
               value.data(), value.size());
        if (!vector->full()) {
          data[size].loc = (uint32_t)value_loc;
          data[size].tag = token.get_tag();
          vector->size = size + 1;
          return InsertResult::Success;
        }
        size_t cow_data_loc = trie->mem_alloc(sizeof(tag_vector_t::data_t) * size * 2);
        if (cow_data_loc == terark::MainPatricia::mem_alloc_fail) {
          trie->mem_free(value_loc, value_size);
          return InsertResult::Fail;
        }
        auto* cow_data = (tag_vector_t::data_t*)trie->mem_get(cow_data_loc);
        memcpy(cow_data, data, sizeof(tag_vector_t::data_t) * size);
        cow_data[size].loc = (uint32_t)value_loc;
        cow_data[size].tag = token.get_tag();
        vector->loc = (uint32_t)cow_data_loc;
        vector->size = size + 1;
        trie->mem_lazy_free(data_loc, sizeof(tag_vector_t::data_t) * size);
        return InsertResult::Success;
      } else if (token.value() != nullptr) {
        return InsertResult::Success;
      }
      return InsertResult::Fail;
    };
    for (trie_item_t* it = trie_arr_; it <= current_; ++it) {
      auto insert_result = insert_impl(current_->trie.get());
      if (insert_result != InsertResult::Fail) {
        return insert_result == InsertResult::Success;
      }
    }
    size_t accumulate_mem_size = 0;
    for (trie_item_t* it = trie_arr_; it <= current_; ++it) {
      accumulate_mem_size += it->trie->mem_size();
    }
    size_t new_trie_size =
      std::max(accumulate_mem_size - trie_arr_->trie->mem_size() * 7 / 8,
        key.size() + VarintLength(value.size()) + value.size()) + 1024;
    auto next = current_ + 1;
    assert(next != trie_arr_ + max_trie_count);
    next->accumulate_mem_size = accumulate_mem_size;
    next->trie.reset(new terark::MainPatricia(sizeof(uint32_t), new_trie_size,
      terark::Patricia::OneWriteMultiRead));
    current_ = next;
    auto insert_result = insert_impl(current_->trie.get());
    assert(insert_result == InsertResult::Success); (void)insert_result;
    ++num_entries_;
    return true;
  }

  // Returns true iff an entry that compares equal to key is in the list.
  virtual bool Contains(const Slice& internal_key) const override {
    terark::fstring find_key(internal_key.data(), internal_key.size() - 8);
    uint64_t tag = DecodeFixed64(find_key.end());
    for (trie_item_t* it = trie_arr_; it <= current_; ++it) {
      auto trie = it->trie.get();
      terark::Patricia::ReaderToken token(trie);
      if (!trie->lookup(find_key, &token)) {
        continue;
      }
      auto vector = (tag_vector_t*)trie->mem_get(*(uint32_t*)token.value());
      size_t size = vector->size;
      auto data = (tag_vector_t::data_t*)trie->mem_get(vector->loc);
      if (terark::binary_search_0(data, size, tag)) {
        return true;
      }
    }
    return false;
  }

  virtual void MarkReadOnly() override {
    for (trie_item_t* it = trie_arr_; it <= current_; ++it) {
      it->trie->set_readonly();
    }
    immutable_ = true;
  }

  virtual size_t ApproximateMemoryUsage() override {
    auto curr_item = current_;
    return curr_item->accumulate_mem_size + curr_item->trie->mem_size();
  }

  virtual uint64_t ApproximateNumEntries(const Slice& start_ikey,
    const Slice& end_ikey) override {
    return 0;
  }

  virtual void Get(const LookupKey &k, void *callback_args,
                   bool(*callback_func)(void *arg, const KeyValuePair*)) override {

    struct HeapItem {
      uint64_t tag;
      terark::MainPatricia* trie;
      uint32_t vector_loc;
      uint32_t index;
    };
    class Context : public KeyValuePair {
    public:
      virtual Slice GetKey() const override {
        return buffer;
      }
      virtual Slice GetValue() const override {
        return GetLengthPrefixedSlice(prefixed_value);
      }
      virtual std::pair<Slice, Slice> GetKeyValue() const override {
        return { Context::GetKey(), Context::GetValue() };
      }

      KeyValuePair* Update(HeapItem* heap) {
        auto vector = (tag_vector_t*)heap->trie->mem_get(heap->vector_loc);
        auto data = (tag_vector_t::data_t*)heap->trie->mem_get(vector->loc);
        build_key(find_key, heap->tag, &buffer);
        prefixed_value = (const char*)heap->trie->mem_get(data[heap->index].loc);
        return this;
      }

      terark::fstring find_key;
      std::string buffer;
      const char* prefixed_value;
    } ctx;

    Slice internal_key = k.internal_key();
    ctx.find_key = terark::fstring(internal_key.data(), internal_key.size() - 8);
    uint64_t tag = DecodeFixed64(ctx.find_key.end());

    HeapItem heap[max_trie_count];
    size_t heap_size = 0;

    for (trie_item_t* it = trie_arr_; it <= current_; ++it) {
      auto trie = it->trie.get();
      terark::Patricia::ReaderToken token(trie);
      if (!trie->lookup(ctx.find_key, &token)) {
        continue;
      }
      uint32_t vector_loc = *(uint32_t*)token.value();
      auto vector = (tag_vector_t*)trie->mem_get(vector_loc);
      size_t size = vector->size;
      auto data = (tag_vector_t::data_t*)trie->mem_get(vector->loc);
      size_t index = terark::upper_bound_0(data, size, tag) - 1;
      if (index != size_t(-1)) {
        heap[heap_size++] = HeapItem{ data[index].tag, trie, vector_loc, (uint32_t)index };
      }
    }
    auto heap_comp = [](const HeapItem& l, const HeapItem& r) {
      return l.tag < r.tag;
    };
    std::make_heap(heap, heap + heap_size, heap_comp);
    while (heap_size > 0 && callback_func(callback_args, ctx.Update(heap))) {
      if (heap->index == 0) {
        std::pop_heap(heap, heap + heap_size, heap_comp);
        --heap_size;
        continue;
      }
      --heap->index;
      auto vector = (tag_vector_t*)heap->trie->mem_get(heap->vector_loc);
      auto data = (tag_vector_t::data_t*)heap->trie->mem_get(vector->loc);
      heap->tag = data[heap->index].tag;
      terark::adjust_heap_top(heap, heap_size, heap_comp);
    }
  }

  virtual ~PTrieRep() override {}

  template<bool heap_mode>
  class Iterator : public MemTableRep::Iterator, boost::noncopyable {
    typedef terark::Patricia::ReaderToken token_t;
    friend class PTrieRep;

    struct HeapItem : boost::noncopyable {
      terark::Patricia::Iterator iter;
      uint64_t tag;
      size_t index;

      HeapItem(terark::Patricia* trie)
        : iter(trie)
        , tag(uint64_t(-1))
        , index(size_t(-1))  {
      }
      struct VectorData {
        size_t size;
        const tag_vector_t::data_t* data;
      };
      VectorData GetVector() {
        auto trie = static_cast<terark::MainPatricia*>(iter.main());
        auto vector = (tag_vector_t*)trie->mem_get(*(uint32_t*)iter.value());
        size_t size = vector->size;
        auto data = (tag_vector_t::data_t*)trie->mem_get(vector->loc);
        return { size, data };
      }
      uint32_t GetValue() const {
        auto trie = static_cast<terark::MainPatricia*>(iter.main());
        auto vector = (tag_vector_t*)trie->mem_get(*(uint32_t*)iter.value());
        auto data = (tag_vector_t::data_t*)trie->mem_get(vector->loc);
        return data[index].loc;
      }
      void Seek(terark::fstring find_key, uint64_t find_tag) {
        if (!iter.seek_lower_bound(find_key)) {
          index = size_t(-1);
          return;
        }
        auto vec = GetVector();
        if (iter.word() == find_key) {
          index = terark::upper_bound_0(vec.data, vec.size, find_tag) - 1;
          if (index != size_t(-1)) {
            tag = vec.data[index];
            return;
          }
          if (!iter.incr()) {
            assert(index == size_t(-1));
            return;
          }
          vec = GetVector();
        }
        assert(iter.word() > find_key);
        index = vec.size - 1;
        tag = vec.data[index].tag;
      }
      void SeekForPrev(terark::fstring find_key, uint64_t find_tag) {
        if (!iter.seek_rev_lower_bound(find_key)) {
          index = size_t(-1);
          return;
        }
        auto vec = GetVector();
        if (iter.word() == find_key) {
          index = terark::lower_bound_0(vec.data, vec.size, find_tag);
          if (index != vec.size) {
            tag = vec.data[index].tag;
            return;
          }
          if (!iter.decr()) {
            index = size_t(-1);
            return;
          }
          vec = GetVector();
        }
        assert(iter.word() < find_key);
        index = 0;
        tag = vec.data[index].tag;
      }
      void SeekToFirst() {
        if (!iter.seek_begin()) {
          index = size_t(-1);
          return;
        }
        auto vec = GetVector();
        index = vec.size - 1;
        tag = vec.data[index].tag;
      }
      void SeekToLast() {
        if (!iter.seek_end()) {
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
          if (!iter.incr()) {
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
          if (!iter.decr()) {
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
        multi_.count = rep_->current_ - rep_->trie_arr_ + 1;
        multi_.array = (HeapItem*)malloc(sizeof(HeapItem) * multi_.count);
        multi_.heap = (HeapItem**)malloc(sizeof(void*) * multi_.count);
        for (size_t i = 0; i < multi_.count; ++i) {
          multi_.heap[i] =
              new(multi_.array + i) HeapItem(rep_->trie_arr_[i].trie.get());
        }
        multi_.size = 0;
      } else {
        new(&single_) HeapItem(rep_->trie_arr_->trie.get());
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
      } else {
        return &single_;
      }
    }
    terark::fstring CurrentKey() {
      return Current()->iter.word();
    }
    uint64_t CurrentTag() {
      return Current()->tag;
    }

    struct ForwardComp {
      bool operator()(HeapItem* l, HeapItem* r) const {
        int c = terark::fstring_func::compare3()(l->iter.word(), r->iter.word());
        if (c == 0) {
          return l->tag < r->tag;
        } else {
          return c > 0;
        }
      }
    };
    struct BackwardComp {
      bool operator()(HeapItem* l, HeapItem* r) const {
        int c = terark::fstring_func::compare3()(l->iter.word(), r->iter.word());
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
          } else {
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
      uint32_t value_loc = item->GetValue();
      auto trie = static_cast<terark::MainPatricia*>(item->iter.main());
      return GetLengthPrefixedSlice((const char*)trie->mem_get(value_loc));
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
      build_key(CurrentKey(), CurrentTag(), &buffer_);
    }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToFirst() override {
      if (heap_mode) {
        Rebuild<1>([&](HeapItem* item) {
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
      build_key(CurrentKey(), CurrentTag(), &buffer_);
    }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToLast() override {
      if (heap_mode) {
        Rebuild<-1>([&](HeapItem* item) {
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
      build_key(CurrentKey(), CurrentTag(), &buffer_);
    }

    virtual bool IsKeyPinned() const override { return false; }

    virtual bool IsSeekForPrevSupported() const { return true; }
  };
  virtual MemTableRep::Iterator *GetIterator(Arena *arena = nullptr) override {
    if (current_ == trie_arr_) {
      typedef PTrieRep::Iterator<false> i_t;
      return arena ? new(arena->AllocateAligned(sizeof(i_t))) i_t(this) : new i_t(this);
    } else {
      typedef PTrieRep::Iterator<true> i_t;
      return arena ? new(arena->AllocateAligned(sizeof(i_t))) i_t(this) : new i_t(this);
    }
  }
};

class PTrieMemtableRepFactory : public MemTableRepFactory {
public:
  PTrieMemtableRepFactory(std::shared_ptr<class MemTableRepFactory> fallback)
    : fallback_(fallback) {}
  virtual ~PTrieMemtableRepFactory() {}

  virtual MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& key_cmp,
                                         Allocator* allocator,
                                         const SliceTransform* transform,
                                         Logger* logger) override {
    auto icomp = key_cmp.icomparator();
    auto user_comparator = icomp->user_comparator();
    if (strcmp(user_comparator->Name(), BytewiseComparator()->Name()) == 0) {
      return new PTrieRep(allocator->BlockSize() * 9 / 8, key_cmp, allocator,
                          transform);
    } else {
      return fallback_->CreateMemTableRep(key_cmp, allocator, transform, logger);
    }
  }
  virtual MemTableRep* CreateMemTableRep(
      const MemTableRep::KeyComparator& key_cmp, Allocator* allocator,
      const ImmutableCFOptions& ioptions,
      const MutableCFOptions& mutable_cf_options,
      uint32_t column_family_id) {
    auto icomp = key_cmp.icomparator();
    auto user_comparator = icomp->user_comparator();
    if (strcmp(user_comparator->Name(), BytewiseComparator()->Name()) == 0) {
      return new PTrieRep(mutable_cf_options.write_buffer_size * 9 / 8, key_cmp,
                          allocator, mutable_cf_options.prefix_extractor.get());
    } else {
      return fallback_->CreateMemTableRep(key_cmp, allocator, ioptions,
                                          mutable_cf_options, column_family_id);
    }
  }

  virtual const char *Name() const override {
    return "PatriciaTrieRepFactory";
  }

  virtual bool IsInsertConcurrentlySupported() const override {
    return false;
  }

  virtual bool CanHandleDuplicatedKey() const override { return true; }

private:
  std::shared_ptr<class MemTableRepFactory> fallback_;
};

}

MemTableRepFactory*
NewPatriciaTrieRepFactory(std::shared_ptr<class MemTableRepFactory> fallback) {
  if (!fallback) {
    fallback.reset(new SkipListFactory());
  }
  return new PTrieMemtableRepFactory(fallback);
}

} // namespace rocksdb
