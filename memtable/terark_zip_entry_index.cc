//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <terark/fsa/cspptrie.inl>

#include "table/terark_zip_internal.h"
#include "util/arena.h"
#include "util/coding.h"
#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

namespace rocksdb {

using terark::lower_bound_0;
using terark::upper_bound_0;
using terark::Patricia;
using terark::MainPatricia;

namespace WriteBatchEntryPTrieIndexDetail {

#pragma pack(push)
#pragma pack(4)
struct value_vector_t {
  uint32_t size;
  uint32_t loc;
  struct data_t {
    WriteBatchIndexEntry* value;

    operator size_t() const { return value->offset; }
  };

  bool full() { return terark::fast_popcount(size) == 1; }
};
#pragma pack(pop)
typedef typename value_vector_t::data_t value_wrap_t;

struct IteratorImplBase {
  bool is_tls;
  uint32_t index;
  terark::Patricia::IteratorPtr handle;
  WriteBatchKeyExtractor extractor;

  IteratorImplBase() : is_tls(false), extractor(nullptr) {}
};

struct IteratorImplWithoutOffset : public IteratorImplBase {
  WriteBatchIndexEntry* GetValue() {
    return *(WriteBatchIndexEntry**)handle->value();
  }
  WriteBatchIndexEntry* Seek(WriteBatchIndexEntry* entry) {
    auto key = extractor(entry);
    if (!handle->seek_lower_bound(key)) {
      return nullptr;
    }
    return GetValue();
  }
  WriteBatchIndexEntry* SeekForPrev(WriteBatchIndexEntry* entry) {
    auto key = extractor(entry);
    if (!handle->seek_rev_lower_bound(key)) {
      return nullptr;
    }
    return GetValue();
  }
  WriteBatchIndexEntry* SeekToFirst() {
    if (!handle->seek_begin()) {
      return nullptr;
    }
    return GetValue();
  }
  WriteBatchIndexEntry* SeekToLast() {
    if (!handle->seek_end()) {
      return nullptr;
    }
    return GetValue();
  }
  WriteBatchIndexEntry* Next() {
    if (!handle->incr()) {
      return nullptr;
    }
    return GetValue();
  }
  WriteBatchIndexEntry* Prev() {
    if (!handle->decr()) {
      return nullptr;
    }
    return GetValue();
  }
};
struct IteratorImplWithOffset : public IteratorImplBase {
  struct VectorData {
    size_t size;
    const value_wrap_t* data;
  };
  VectorData GetVector() {
    auto trie = static_cast<MainPatricia*>(handle->trie());
    auto vector =
        (value_vector_t*)trie->mem_get(*(uint32_t*)handle->value());
    size_t size = vector->size;
    auto data = (value_wrap_t*)trie->mem_get(vector->loc);
    return {size, data};
  }
  WriteBatchIndexEntry* Seek(WriteBatchIndexEntry* entry) {
    fstring find_key = extractor(entry);
    if (!handle->seek_lower_bound(find_key)) {
      return nullptr;
    }
    auto vec = GetVector();
    if (handle->word() == find_key) {
      index = (uint32_t)lower_bound_0(vec.data, vec.size, entry->offset);
      if (index != vec.size) {
        return vec.data[index].value;
      }
      if (!handle->incr()) {
        return nullptr;
      }
      vec = GetVector();
    }
    assert(handle->word() > find_key);
    index = 0;
    return vec.data[index].value;
  }
  WriteBatchIndexEntry* SeekForPrev(WriteBatchIndexEntry* entry) {
    fstring find_key = extractor(entry);
    if (!handle->seek_rev_lower_bound(find_key)) {
      return nullptr;
    }
    auto vec = GetVector();
    if (handle->word() == find_key) {
      index = (uint32_t)upper_bound_0(vec.data, vec.size, entry->offset) - 1;
      if (index != vec.size) {
        return vec.data[index].value;
      }
      if (!handle->decr()) {
        return nullptr;
      }
      vec = GetVector();
    }
    assert(handle->word() < find_key);
    index = (uint32_t)vec.size - 1;
    return vec.data[index].value;
  }
  WriteBatchIndexEntry* SeekToFirst() {
    if (!handle->seek_begin()) {
      return nullptr;
    }
    auto vec = GetVector();
    index = 0;
    return vec.data[index].value;
  }
  WriteBatchIndexEntry* SeekToLast() {
    if (!handle->seek_end()) {
      return nullptr;
    }
    auto vec = GetVector();
    index = (uint32_t)vec.size - 1;
    return vec.data[index].value;
  }
  WriteBatchIndexEntry* Next() {
    auto vec = GetVector();
    if (++index == vec.size) {
      if (!handle->incr()) {
        return nullptr;
      }
      vec = GetVector();
      index = 0;
    }
    return vec.data[index].value;
  }
  WriteBatchIndexEntry* Prev() {
    if (index-- == 0) {
      if (!handle->decr()) {
        return nullptr;
      }
      auto vec = GetVector();
      index = (uint32_t)vec.size - 1;
      return vec.data[index].value;
    } else {
      auto vec = GetVector();
      return vec.data[index].value;
    }
  }
};

static thread_local IteratorImplBase tls_impl;
}  // namespace WriteBatchEntryPTrieIndexDetail

template <bool OverwriteKey>
class WriteBatchEntryPTrieIndex : public WriteBatchEntryIndex {
 protected:
  terark::MainPatricia index_;
  WriteBatchKeyExtractor extractor_;

  using value_vector_t = WriteBatchEntryPTrieIndexDetail::value_vector_t;
  using value_wrap_t = WriteBatchEntryPTrieIndexDetail::value_wrap_t;
  using IteratorImplWithoutOffset =
      WriteBatchEntryPTrieIndexDetail::IteratorImplWithoutOffset;
  using IteratorImplWithOffset =
      WriteBatchEntryPTrieIndexDetail::IteratorImplWithOffset;

  typedef typename std::conditional<OverwriteKey, IteratorImplWithoutOffset,
                                    IteratorImplWithOffset>::type IteratorImpl;

  class PTrieIterator : public WriteBatchEntryIndex::Iterator {
   public:
    PTrieIterator(Patricia* index, WriteBatchKeyExtractor e, bool ephemeral)
        : impl_(nullptr), key_(nullptr) {
      if (ephemeral) {
        auto& tls_impl = WriteBatchEntryPTrieIndexDetail::tls_impl;
        impl_ = reinterpret_cast<IteratorImpl*>(&tls_impl);
        impl_->is_tls = true;
      } else {
        impl_ = new IteratorImpl();
      }
      impl_->handle.reset(index->new_iter());
      impl_->extractor = e;
    }
    ~PTrieIterator() {
      if (impl_->is_tls) {
        impl_->handle.reset(nullptr);
      } else {
        delete impl_;
      }
    }

    IteratorImpl* impl_;
    WriteBatchIndexEntry* key_;

   public:
    bool Valid() const override { return key_ != nullptr; }
    void SeekToFirst() override { key_ = impl_->SeekToFirst(); }
    void SeekToLast() override { key_ = impl_->SeekToLast(); }
    void Seek(WriteBatchIndexEntry* target) override {
      key_ = impl_->Seek(target);
    }
    void SeekForPrev(WriteBatchIndexEntry* target) override {
      key_ = impl_->SeekForPrev(target);
    }
    void Next() override { key_ = impl_->Next(); }
    void Prev() override { key_ = impl_->Prev(); }
    WriteBatchIndexEntry* key() const override { return key_; }
  };

 public:
  WriteBatchEntryPTrieIndex(WriteBatchKeyExtractor e)
      : index_(trie_value_size, 512ull << 10, Patricia::SingleThreadShared),
        extractor_(e) {}

  static constexpr size_t trie_value_size =
      OverwriteKey ? sizeof(void*) : sizeof(uint32_t);

  Iterator* NewIterator() override {
    return new PTrieIterator(&index_, extractor_, false);
  }
  void NewIterator(IteratorStorage& storage, bool ephemeral) override {
    static_assert(sizeof(PTrieIterator) <= sizeof storage.buffer,
                  "Need larger buffer for PTrieIterator");
    storage.iter =
        new (storage.buffer) PTrieIterator(&index_, extractor_, ephemeral);
  }
  bool Upsert(WriteBatchIndexEntry* key) override {
    fstring slice_key = extractor_(key);
    if (OverwriteKey) {
      auto& token = *index_.tls_writer_token_nn<Patricia::WriterToken>();
      token.acquire(&index_);
      if (index_.insert(slice_key, &key, &token)) {
        token.release();
        return true;
      }
      // insert fail , replace
      auto entry = *(WriteBatchIndexEntry**)token.value();
      std::swap(entry->offset, key->offset);
      token.release();
      return false;
    } else {
      class Token : public terark::Patricia::WriterToken {
       public:
        Token(WriteBatchIndexEntry* value) : value_(value) {}
       protected:
        bool init_value(void* valptr, size_t valsize) noexcept override {
          assert(valsize == sizeof(uint32_t));
          auto trie = static_cast<MainPatricia*>(m_trie);
          size_t data_loc = trie->mem_alloc(sizeof(value_wrap_t));
          assert(data_loc != MainPatricia::mem_alloc_fail);
          auto* data = (value_wrap_t*)trie->mem_get(data_loc);
          data->value = value_;

          size_t vector_loc = trie->mem_alloc(sizeof(value_vector_t));
          assert(vector_loc != MainPatricia::mem_alloc_fail);
          auto* vector = (value_vector_t*)trie->mem_get(vector_loc);
          vector->loc = (uint32_t)data_loc;
          vector->size = 1;

          uint32_t u32_vector_loc = (uint32_t)vector_loc;
          memcpy(valptr, &u32_vector_loc, valsize);
          return true;
        }

       private:
        WriteBatchIndexEntry* value_;
      };
      Token& token = *index_.tls_writer_token_nn([&]{return new Token(key);});
      token.acquire(&index_);
      WriteBatchIndexEntry* value_store;
      if (!index_.insert(slice_key, &value_store, &token)) {
        // insert fail , append to vector
        size_t vector_loc = *(uint32_t*)token.value();
        auto* vector = (value_vector_t*)index_.mem_get(vector_loc);
        size_t data_loc = vector->loc;
        auto* data = (value_wrap_t*)index_.mem_get(data_loc);
        uint32_t size = vector->size;
        assert(size > 0);
        assert(key->offset > data[size - 1].value->offset);
        if (!vector->full()) {
          data[size].value = key;
          vector->size = size + 1;
        } else {
          size_t cow_data_loc =
              index_.mem_alloc(sizeof(value_wrap_t) * size * 2);
          assert(cow_data_loc != MainPatricia::mem_alloc_fail);
          auto* cow_data = (value_wrap_t*)index_.mem_get(cow_data_loc);
          vector = (value_vector_t*)index_.mem_get(vector_loc);
          data = (value_wrap_t*)index_.mem_get(data_loc);
          memcpy(cow_data, data, sizeof(value_wrap_t) * size);
          cow_data[size].value = key;
          vector->loc = (uint32_t)cow_data_loc;
          vector->size = size + 1;
          index_.mem_lazy_free(data_loc, sizeof(value_wrap_t) * size);
        }
      }
      token.release();
      return true;
    }
  }
};

const WriteBatchEntryIndexFactory* patricia_WriteBatchEntryIndexFactory(
    const WriteBatchEntryIndexFactory* fallback) {
  class WriteBatchEntryPTrieIndexContext : public WriteBatchEntryIndexContext {
   public:
    WriteBatchEntryIndexContext* fallback_context;
    WriteBatchEntryPTrieIndexContext() : fallback_context(nullptr) {}

    ~WriteBatchEntryPTrieIndexContext() {
      if (fallback_context != nullptr) {
        fallback_context->~WriteBatchEntryIndexContext();
      }
    }
  };
  class PTrieIndexFactory : public WriteBatchEntryIndexFactory {
   public:
    WriteBatchEntryIndexContext* NewContext(Arena* a) const override {
      typedef WriteBatchEntryPTrieIndexContext ctx_t;
      auto ctx = new (a->AllocateAligned(sizeof(ctx_t))) ctx_t();
      ctx->fallback_context = fallback->NewContext(a);
      return ctx;
    }
    WriteBatchEntryIndex* New(WriteBatchEntryIndexContext* ctx,
                              WriteBatchKeyExtractor e, const Comparator* c,
                              Arena* a, bool overwrite_key) const override {
      auto ptrie_ctx = static_cast<WriteBatchEntryPTrieIndexContext*>(ctx);
      if (!IsForwardBytewiseComparator(c)) {
        return fallback->New(ptrie_ctx->fallback_context, e, c, a,
                             overwrite_key);
      } else if (overwrite_key) {
        typedef WriteBatchEntryPTrieIndex<true> index_t;
        return new (a->AllocateAligned(sizeof(index_t))) index_t(e);
      } else {
        typedef WriteBatchEntryPTrieIndex<false> index_t;
        return new (a->AllocateAligned(sizeof(index_t))) index_t(e);
      }
    }
    PTrieIndexFactory(const WriteBatchEntryIndexFactory* _fallback)
        : fallback(_fallback) {}
    const char* Name() const override {
      return "WriteBatchEntryIndexFactory";
    }

   private:
    const WriteBatchEntryIndexFactory* fallback;
  };
  if (fallback == nullptr) {
    fallback = skip_list_WriteBatchEntryIndexFactory();
  }
  static PTrieIndexFactory factory(fallback);
  return &factory;
}

}  // namespace rocksdb
