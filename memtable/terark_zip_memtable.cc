#include "terark_zip_memtable.h"

bool TlsWriterToken::init_value(
    void *valptr,
    size_t valsize) noexcept override
{
  assert(valsize == sizeof(uint32_t));
  size_t data_loc = terark::MainPatricia::mem_alloc_fail;
  size_t value_loc = terark::MainPatricia::mem_alloc_fail;
  size_t vector_loc = terark::MainPatricia::mem_alloc_fail;
  size_t value_size = VarintLength(value_.size()) + value_.size();
  do {
    vector_loc = trie()->mem_alloc(sizeof(tag_vector_t));
    if (vector_loc == terark::MainPatricia::mem_alloc_fail) break;
    data_loc = trie()->mem_alloc(sizeof(tag_vector_t::data_t));
    if (data_loc == terark::MainPatricia::mem_alloc_fail) break;
    value_loc = trie()->mem_alloc(value_size);
    if (value_loc == terark::MainPatricia::mem_alloc_fail) break;
    char *value_dst = EncodeVarint32((char *)trie()->mem_get(value_loc),
                                     (uint32_t)value_.size());
    memcpy(value_dst, value_.data(), value_.size());
    auto *data = (tag_vector_t::data_t *)trie()->mem_get(data_loc);
    data->loc = (uint32_t)value_loc;
    data->tag = tag_;
    auto *vector = (tag_vector_t *)trie()->mem_get(vector_loc);
    vector->loc = (uint32_t)data_loc;
    vector->size = 1;
    uint32_t u32_vector_loc = vector_loc;
    memcpy(valptr, &u32_vector_loc, valsize);
    return true;
  } while (false);
  if (value_loc != terark::MainPatricia::mem_alloc_fail)
    trie()->mem_free(value_loc, value_size);
  if (data_loc != terark::MainPatricia::mem_alloc_fail)
    trie()->mem_free(data_loc, sizeof(tag_vector_t::data_t));
  if (vector_loc != terark::MainPatricia::mem_alloc_fail)
    trie()->mem_free(vector_loc, sizeof(tag_vector_t));
  return false;
}

virtual size_t PatriciaMemtableRep::ApproximateMemoryUsage() override {
  size_t sum = 0;
  for (auto trie : trie_vec_)
    sum += trie->mem_size_inline();
  return sum;
}

virtual bool PatriciaMemtableRep::Contains(
    const Slice &internal_key) const override
{
  terark::fstring find_key(internal_key.data(), internal_key.size() - 8);
  uint64_t tag = DecodeFixed64(find_key.end());
  for (auto trie : trie_vec_) {
    TlsReaderToken *token = get_tls();
    if (trie->lookup(find_key, token->get())) {
      auto vector = (tag_vector_t *)trie->mem_get(*(uint32_t *)token.value());
      size_t size = vector->size;
      auto data = (tag_vector_t::data_t *)trie->mem_get(vector->loc);
      return terark::binary_search_0(data, size, tag);
    }
  }
  return false;
}

virtual void PatriciaMemtableRep::Get(
    const LookupKey &k,
    void *callback_args,
    bool (*callback_func)(void *arg, const KeyValuePair *)) override {
  // assistant structure define start
  struct HeapItem {
    uint32_t idx;
    uint32_t loc;
    uint64_t tag;
    MainPatricia *p_trie;
  };

  struct TlsItem {
    valvec<HeapItem> heap;
    valvec<char> buffer;
  };

  class Context : public KeyValuePair {
  public:
    virtual Slice GetKey() const override {
      return Slice(buffer->data(), buffer->size());
    }

    virtual Slice GetValue() const override {
      return GetLengthPrefixedSlice(prefixed_value);
    }
    
    virtual std::pair<Slice, Slice> GetKeyValue() const override {
      return {Context::GetKey(), Context::GetValue()};
    }

    KeyValuePair *Update(HeapItem *p_heap) {
      auto vector = (tag_vector_t *)p_heap->p_trie->mem_get(p_heap->loc);
      auto data = (tag_vector_t::data_t *)p_heap->p_trie->mem_get(vector->loc);
      build_key(find_key, p_heap->tag, buffer);
      prefixed_value =
          (const char *)p_heap->p_trie->mem_get(data[p_heap->idx].loc);
      return this;
    }

    terark::fstring find_key;
    valvec<char> *buffer;
    const char *prefixed_value;

    Context(valvec<char> *_buffer) : buffer(_buffer) {}
  } 
  // assistant structure define end

  // varible define
  static thread_local TlsItem tls_ctx;
  Context ctx(&tls_ctx.buffer);

  Slice internal_key = k.internal_key();
  ctx.find_key = terark::fstring(internal_key.data(), internal_key.size() - 8);
  uint64_t tag = DecodeFixed64(ctx.find_key.end());

  valvec<HeapItem> &heap = tls_ctx.heap;
  assert(heap.empty());
  heap.reserve(trie_vec_.size());

  // initialization
  for (auto& trie : trie_vec_) {
    auto token = trie->get_tls();
    token->update_lazy();
    if (trie->lookup(ctx.find_key, token)) {
      uint32_t loc = *(uint32_t *)token->value();
      auto vector = (tag_vector_t *)trie->mem_get(loc);
      size_t size = vector->size;
      auto data = (tag_vector_t::data_t *)trie->mem_get(vector->loc);
      size_t idx = terark::upper_bound_0(data, size, tag) - 1;
      if (idx != size_t(-1)) {
        heap.emplace_back(HeapItem{(uint32_t)idx, loc, data[idx].tag,
                                    token, trie});
      }
      break;
    }
  }

  // comparator define
  auto heap_comp = [](const HeapItem &l, const HeapItem &r) { return l.tag < r.tag; };

  // elements heap functioning
  std::make_heap(heap.begin(), heap.end(), heap_comp);
  auto item = heap.front();
  while (heap.size() > 0 && callback_func(callback_args, ctx.Update(&item))) {
    if (item.idx == 0) {
      std::pop_heap(heap.begin(), heap.end(), heap_comp);
      heap.pop_back();
      continue;
    }
    --item.idx;
    auto vector = (tag_vector_t *)item.trie->mem_get(item.loc);
    auto data = (tag_vector_t::data_t *)item.trie->mem_get(vector->loc);
    item.tag = data[item.idx].tag;
    terark::adjust_heap_top(heap.begin(), heap.size(), heap_comp);
  }
  heap.erase_all();
}

template <bool heap_mode> class Iterator;
virtual MemTableRep::Iterator *PatriciaMemtableRep::GetIterator(
    Arena *arena) override 
{
  MemTableRep::Iterator *iter;
  if (trie_vec_.size() == 1) {
    typedef terark::Patricia::Iterator iter_t
    iter = arena ? new (arena->AllocateAligned(sizeof(iter_t))) iter_t(trie_vec[0]->trie());
                 : iter_t(trie_vec[0]->trie());
  } else {
    typedef HeapIterator iter_t;
    iter = arena ? new (arena->AllocateAligned(sizeof(iter_t))) iter_t(this)
                 : new iter_t(this);
  }
  return iter;
}

virtual bool PatriciaMemtableRep::InsertKeyValue(
    const Slice &internal_key, const Slice &value) override {
  // immutable check
  if (immutable_) return false;
  // prepare key
  terark::fstring key(internal_key.data(),
                      internal_key.data() + internal_key.size() - 8); 
  // tool lambda fn start
  auto fn_insert_impl = [&](MainPatricia *trie) {
    auto& token = trie->tls_writer_token();
    if (token == nullptr)
      token.reset(new TlsWriterToken(trie, DecodeFixed64(key.end()), value)));
    uint32_t value_storage;
    if (!trie->insert(key, &value_storage, token)) {
      size_t vector_loc = *(uint32_t *)token->value();
      auto *vector = (tag_vector_t *)trie->mem_get(vector_loc);
      size_t data_loc = vector->loc;
      auto *data = (tag_vector_t::data_t *)trie->mem_get(data_loc);
      size_t size = vector->size;
      assert(size > 0);
      assert(token->get_tag() > data[size - 1].tag);
      if ((token->get_tag() >> 8) == (data[size - 1].tag >> 8)) {
        vector->size = size;
        return InsertResult::Duplicated;
      }
      size_t value_size = VarintLength(value.size()) + value.size();
      size_t value_loc = trie->mem_alloc(value_size);
      if (value_loc == terark::MainPatricia::mem_alloc_fail) {
        vector->size = size;
        return InsertResult::Fail;
      }
      memcpy(EncodeVarint32((char *)trie->mem_get(value_loc),
                            (uint32_t)value.size()),
              value.data(), value.size());
      if (!vector->full()) {
        data[size].loc = (uint32_t)value_loc;
        data[size].tag = token->get_tag();
        as_atomic(vector->size).store(size + 1, std::memory_order_seq_cst);
        return InsertResult::Success;
      }
      size_t cow_data_loc =
          trie->mem_alloc(sizeof(tag_vector_t::data_t) * size * 2);
      if (cow_data_loc == terark::MainPatricia::mem_alloc_fail) {
        as_atomic(vector->size).store(size, std::memory_order_seq_cst);
        trie->mem_free(value_loc, value_size);
        return InsertResult::Fail;
      }
      auto *cow_data = (tag_vector_t::data_t *)trie->mem_get(cow_data_loc);
      memcpy(cow_data, data, sizeof(tag_vector_t::data_t) * size);
      cow_data[size].loc = (uint32_t)value_loc;
      cow_data[size].tag = token->get_tag();
      vector->loc = (uint32_t)cow_data_loc;
      as_atomic(vector->size).store(size + 1, std::memory_order_seq_cst);
      trie->mem_lazy_free(data_loc, sizeof(tag_vector_t::data_t) * size);
      return InsertResult::Success;
    } else if (token->value() != nullptr) {
      return InsertResult::Success;
    } else
      return InsertResult::Fail;
  };

  auto fn_create_new_trie = [&](){
    if (write_buffer_size_ > 0) {
      if (write_buffer_size_ < size_limit_)
        write_buffer_size_ *= 2;
      if (write_buffer_size_ > size_limit_)
        write_buffer_size_ = size_limit_;
      size_t bound = key.size() + VarintLength(value.size()) + value.size();
      if (write_buffer_size_ < bound)
        write_buffer_size_ = std::min(bound + 16 << 20, LLONG_MAX);
    }
    return new MainPatricia(
      write_buffer_size_, this,
      concurrent_type_ == ConcurrentType::native
        ? terark::Patricia::ConcurrentLevel::MultiWriteMultiRead
        : terark::Patricia::ConcurrentLevel::OneWriteMultiRead);
  }
  // tool lambda fn end
  // function start                     
  InsertResult insert_result = fn_insert_impl(trie_vec_.back());
  while (insert_result == InsertResult::Fail) {
    if (handle_duplicate_) {
      for (auto iter = trie_vec_.rbegin(); iter != trie_vec_.rend(); iter++)
        if (InsertResult::Fail != (insert_result = fn_insert_impl(*iter)))
          break;
    } else insert_result = fn_insert_impl(trie_vec_.back());
    if (insert_result == InsertResult::Fail) {
      mutex_.WriteLock();
      trie_vec_.emplace_back(fn_create_new_trie());
      mutex_.WriteUnlock();
    }
  }
  assert(insert_result != InsertResult::Fail);
  return insert_result == InsertResult::Success;
}

virtual void PatriciaMemtableRep::MarkReadOnly() override {
  for (auto iter : trie_vec_)
    iter->set_readonly();
  immutable_ = true;
}

VectorData HeapItem::GetVector() {
  auto trie = static_cast<terark::MainPatricia *>(iter.main());
  auto vector = (tag_vector_t *)trie->mem_get(*(uint32_t *)iter.value());
  size_t size = vector->size;
  auto data = (typename tag_vector_t::data_t *)trie->mem_get(vector->loc);
  return {size, data};
}

uint32_t HeapItem::GetValue() const {
  auto trie = static_cast<terark::MainPatricia *>(iter.main());
  auto vector = (tag_vector_t *)trie->mem_get(*(uint32_t *)iter.value());
  auto data = (tag_vector_t::data_t *)trie->mem_get(vector->loc);
  return data[index].loc;
}

void HeapItem::Seek(terark::fstring find_key, uint64_t find_tag) {
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

void HeapItem::SeekForPrev(terark::fstring find_key, uint64_t find_tag) {
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

void HeapItem::SeekToFirst() {
  if (!iter.seek_begin()) {
    index = size_t(-1);
    return;
  }
  auto vec = GetVector();
  index = vec.size - 1;
  tag = vec.data[index].tag;
}

void HeapItem::SeekToLast() {
  if (!iter.seek_end()) {
    index = size_t(-1);
    return;
  }
  auto vec = GetVector();
  index = 0;
  tag = vec.data[index].tag;
}

void HeapItem::Next() {
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

void HeapItem::Prev() {
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

template <int direction, class func_t> 
void HeapIterator::Rebuild(func_t &&callback_func) {
  static_assert(direction == 1 || direction == -1,
                "direction must be 1 or -1");
  direction_ = direction;
  size = count;
  if (direction == 1) {
    for (size_t i = 0; i < size;)
      if (callback_func(heap[i]))
        ++i;
      else
        std::swap(heap[i], heap[--size]);
    std::make_heap(heap, heap + size, ForwardComp());
  } else {
    for (size_t i = 0; i < size;)
      if (callback_func(heap[i]))
        ++i;
      else
        std::swap(heap[i], heap[--size]);
    std::make_heap(heap, heap + size, BackwardComp());
  }
}

HeapIterator::HeapIterator(PatriciaMemtableRep *rep) : direction_(0), {
  assert(rep != nullptr);
  assert(rep->trie_vec_.size() > 1);
  valvec<HeapItem> hitem(rep->trie_vec_.size(), valvec_reserve());
  valvec<HeapItem *> hptrs(rep->trie_vec_.size(), valvec_reserve());
  for (auto trie : trie_vec) {
    hptrs.push_back(new (hitem.grow_no_init(1)) HeapItem(trie));
  }
  assert(hitem.size() == hptrs.size());
  count = hitem.size();
  array = hitem.risk_release_ownership();
  heap = hptrs.risk_release_ownership();
  size = 0;
}

    virtual ~HeapIterator() {
      free(heap);
      for (size_t i = 0; i < count; ++i) {
        array[i].~HeapItem();
      }
      free(array);
    }

    

    

    virtual Slice GetValue() const override {
      auto trie = static_cast<terark::MainPatricia *>(*heap->iter.main());
      return GetLengthPrefixedSlice((const char *)trie->mem_get(*heap->GetValue()));
    }

    virtual std::pair<Slice, Slice> GetKeyValue() const override {
      
    }

    virtual void Next() override {
      if (direction_ != 1) {
        terark::fstring find_key(buffer_.data(), buffer_.size() - 8);
        uint64_t tag = DecodeFixed64(find_key.end());
        Rebuild<1>([&](HeapItem *item) {
          item->Seek(find_key, tag);
          return item->index != size_t(-1);
        });
        if (size == 0) {
          direction_ = 0;
          return;
        }
      }
      heap[0]->Next();
      if (heap[0]->index == size_t(-1)) {
        std::pop_heap(heap, heap + size, ForwardComp());
        if (--size == 0) {
          direction_ = 0;
          return;
        }
      } else {
        terark::adjust_heap_top(heap, size, ForwardComp());
      }
      PatriciaMemtable::build_key(*heap->iter.word(), *heap->tag, &buffer_);
    }

    virtual void Prev() override {
      if (direction_ != -1) {
        terark::fstring find_key(buffer_.data(), buffer_.size() - 8);
        uint64_t tag = DecodeFixed64(find_key.end());
        Rebuild<-1>([&](HeapItem *item) {
          item->SeekForPrev(find_key, tag);
          return item->index != size_t(-1);
        });
        if (size == 0) {
          direction_ = 0;
          return;
        }
      }
      heap[0]->Prev();
      if (heap[0]->index == size_t(-1)) {
        std::pop_heap(heap, heap + size, BackwardComp());
        if (--size == 0) {
          direction_ = 0;
          return;
        }
      } else {
        terark::adjust_heap_top(heap, size, BackwardComp());
      }
      PatriciaMemtable::build_key(*heap->iter.word(), *heap->tag, &buffer_);
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

      Rebuild<1>([&](HeapItem *item) {
        item->Seek(find_key, tag);
        return item->index != size_t(-1);
      });
      if (size == 0) {
        direction_ = 0;
        return;
      }
      PatriciaMemtable::build_key(*heap->iter.word(), *heap->tag, &buffer_);
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

      Rebuild<-1>([&](HeapItem *item) {
        item->SeekForPrev(find_key, tag);
        return item->index != size_t(-1);
      });
      if (size == 0) {
        direction_ = 0;
        return;
      }
      PatriciaMemtable::build_key(*heap->iter.word(), *heap->tag, &buffer_);
    }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToFirst() override {
      Rebuild<1>([&](HeapItem *item) {
        item->SeekToFirst();
        return item->index != size_t(-1);
      });
      if (size == 0) {
        direction_ = 0;
        return;
      }
      PatriciaMemtable::build_key(*heap->iter.word(), *heap->tag, &buffer_);
    }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToLast() override {
      Rebuild<-1>([&](HeapItem *item) {
        item->SeekToLast();
        return item->index != size_t(-1);
      });
      if (size == 0) {
        direction_ = 0;
        return;
      }
      PatriciaMemtable::build_key(*heap->iter.word(), *heap->tag, &buffer_);
    }


  };




virtual MemTableRep *
PatriciaMemTableRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator &key_cmp,
    bool needs_dup_key_check,
    Allocator *allocator,
    const SliceTransform *transform,
    Logger *logger) override {
  if (IsForwardBytewiseComparator(key_cmp->user_comparator())) {
    return new PatriciaMemtableRep(concurrent_type_, patricia_key_type_,
                                   needs_dup_key_check, write_buffer_size_,
                                   allocator, key_cmp);
  } else {
    return fallback_->CreateMemTableRep(key_cmp, needs_dup_key_check,
                                        allocator, transform, logger);
  }
 }

virtual MemTableRep *
PatriciaMemTableRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator &key_cmp,
    bool needs_dup_key_check,
    Allocator *allocator,
    const ImmutableCFOptions &ioptions,
    const MutableCFOptions &mutable_cf_options,
    uint32_t column_family_id) override {
  if (IsForwardBytewiseComparator(key_cmp->user_comparator())) {
    return new PatriciaMemtableRep(concurrent_type_, patricia_key_type_,
                                    needs_dup_key_check, write_buffer_size_,
                                    allocator, key_cmp);
  } else {
    return fallback_->CreateMemTableRep(key_cmp, needs_dup_key_check,
                                        allocator, ioptions, 
                                        mutable_cf_options,
                                        column_family_id);
  }
}