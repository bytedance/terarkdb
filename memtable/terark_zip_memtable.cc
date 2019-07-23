#include "terark_zip_memtable.h"

namespace rocksdb {

bool MemWriterToken::init_value(void *valptr, size_t valsize) noexcept {
  assert(valsize == sizeof(uint32_t));
  size_t data_loc = MemPatricia::mem_alloc_fail;
  size_t value_loc = MemPatricia::mem_alloc_fail;
  size_t vector_loc = MemPatricia::mem_alloc_fail;
  size_t value_size = VarintLength(value_.size()) + value_.size();
  auto trie = static_cast<MemPatricia *>(m_trie);
  do {
    vector_loc = trie->mem_alloc(sizeof(detail::tag_vector_t));
    if (vector_loc == MemPatricia::mem_alloc_fail) break;
    data_loc = trie->mem_alloc(sizeof(detail::tag_vector_t::data_t));
    if (data_loc == MemPatricia::mem_alloc_fail) break;
    value_loc = trie->mem_alloc(value_size);
    if (value_loc == MemPatricia::mem_alloc_fail) break;
    char *value_dst = EncodeVarint32((char *)trie->mem_get(value_loc),
                                     (uint32_t)value_.size());
    memcpy(value_dst, value_.data(), value_.size());
    auto *data = (detail::tag_vector_t::data_t *)trie->mem_get(data_loc);
    data->loc = (uint32_t)value_loc;
    data->tag = tag_;
    auto *vector = (detail::tag_vector_t *)trie->mem_get(vector_loc);
    vector->loc = (uint32_t)data_loc;
    vector->size = 1;
    uint32_t u32_vector_loc = vector_loc;
    memcpy(valptr, &u32_vector_loc, valsize);
    return true;
  } while (false);
  if (value_loc != MemPatricia::mem_alloc_fail)
    trie->mem_free(value_loc, value_size);
  if (data_loc != MemPatricia::mem_alloc_fail)
    trie->mem_free(data_loc, sizeof(detail::tag_vector_t::data_t));
  if (vector_loc != MemPatricia::mem_alloc_fail)
    trie->mem_free(vector_loc, sizeof(detail::tag_vector_t));
  return false;
}

size_t PatriciaMemtableRep::ApproximateMemoryUsage() {
  size_t sum = 0;
  for (auto trie : trie_vec_)
    sum += trie->mem_size_inline();
  return sum;
}

bool PatriciaMemtableRep::Contains(
    const Slice &internal_key) const {
  terark::fstring find_key(internal_key.data(), internal_key.size() - 8);
  uint64_t tag = DecodeFixed64(find_key.end());
  for (auto trie : trie_vec_) {
    auto token = trie->acquire_tls_reader_token();
    if (trie->lookup(find_key, token)) {
      auto vector = (detail::tag_vector_t *)trie->mem_get(*(uint32_t *)token->value());
      size_t size = vector->size;
      auto data = (detail::tag_vector_t::data_t *)trie->mem_get(vector->loc);
      return terark::binary_search_0(data, size, tag);
    }
  }
  return false;
}

void PatriciaMemtableRep::Get(
    const LookupKey &k,
    void *callback_args,
    bool (*callback_func)(void *arg, const KeyValuePair *)){
  // assistant structure define start
  struct HeapItem {
    uint32_t idx;
    uint32_t loc;
    uint64_t tag;
    MemPatricia *trie;
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

    KeyValuePair *Update(HeapItem *heap) {
      auto vector = (detail::tag_vector_t *)heap->trie->mem_get(heap->loc);
      auto data = (detail::tag_vector_t::data_t *)heap->trie->mem_get(vector->loc);
      detail::build_key(find_key, heap->tag, buffer);
      prefixed_value =
          (const char *)heap->trie->mem_get(data[heap->idx].loc);
      return this;
    }

    terark::fstring find_key;
    valvec<char> *buffer;
    const char *prefixed_value;

    Context(valvec<char> *_buffer) : buffer(_buffer) {}
  };

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
  for (auto trie : trie_vec_) {
    auto token = trie->acquire_tls_reader_token();
    token->update_lazy();
    if (trie->lookup(ctx.find_key, token)) {
      uint32_t loc = *(uint32_t *)token->value();
      auto vector = (detail::tag_vector_t *)trie->mem_get(loc);
      size_t size = vector->size;
      auto data = (detail::tag_vector_t::data_t *)trie->mem_get(vector->loc);
      size_t idx = terark::upper_bound_0(data, size, tag) - 1;
      if (idx != size_t(-1)) {
        heap.emplace_back(HeapItem{(uint32_t)idx, loc, data[idx].tag, trie});
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
    auto vector = (detail::tag_vector_t *)(item.trie->mem_get(item.loc));
    auto data = (detail::tag_vector_t::data_t *)(item.trie->mem_get(vector->loc));
    item.tag = data[item.idx].tag;
    terark::adjust_heap_top(heap.begin(), heap.size(), heap_comp);
  }
  heap.erase_all();
}

MemTableRep::Iterator *PatriciaMemtableRep::GetIterator(Arena *arena) {
  MemTableRep::Iterator *iter;
  if (trie_vec_.size() == 1) {
    typedef PatriciaRepIterator<false> iter_t;
    iter = arena ? new (arena->AllocateAligned(sizeof(iter_t))) iter_t(trie_vec_)
                 : new iter_t(trie_vec_);
  } else {
    typedef PatriciaRepIterator<true> iter_t;
    iter = arena ? new (arena->AllocateAligned(sizeof(iter_t))) iter_t(trie_vec_)
                 : new iter_t(trie_vec_);
  }
  return iter;
}

bool PatriciaMemtableRep::InsertKeyValue(
    const Slice &internal_key, const Slice &value) {
  // immutable check
  if (immutable_) return false;
  // prepare key
  terark::fstring key(internal_key.data(),
                      internal_key.data() + internal_key.size() - 8); 
  // tool lambda fn start
  auto fn_insert_impl = [&](MemPatricia *trie) {
    if (trie->tls_writer_token() == nullptr)
      trie->tls_writer_token().reset(new MemWriterToken(trie, DecodeFixed64(key.end()), value));
    auto token = static_cast<MemWriterToken*>(trie->tls_writer_token().get());
    uint32_t value_storage;
    if (!trie->insert(key, &value_storage, token)) {
      size_t vector_loc = *(uint32_t *)token->value();
      auto *vector = (detail::tag_vector_t *)trie->mem_get(vector_loc);
      size_t data_loc = vector->loc;
      auto *data = (detail::tag_vector_t::data_t *)trie->mem_get(data_loc);
      size_t size = vector->size;
      assert(size > 0);
      assert(token->get_tag() > data[size - 1].tag);
      if ((token->get_tag() >> 8) == (data[size - 1].tag >> 8)) {
        vector->size = size;
        return detail::InsertResult::Duplicated;
      }
      size_t value_size = VarintLength(value.size()) + value.size();
      size_t value_loc = trie->mem_alloc(value_size);
      if (value_loc == MemPatricia::mem_alloc_fail) {
        vector->size = size;
        return detail::InsertResult::Fail;
      }
      memcpy(EncodeVarint32((char *)trie->mem_get(value_loc),
                            (uint32_t)value.size()),
              value.data(), value.size());
      if (!vector->full()) {
        data[size].loc = (uint32_t)value_loc;
        data[size].tag = token->get_tag();
        vector->size = size + 1;
        return detail::InsertResult::Success;
      }
      size_t cow_data_loc =
          trie->mem_alloc(sizeof(detail::tag_vector_t::data_t) * size * 2);
      if (cow_data_loc == MemPatricia::mem_alloc_fail) {
       vector->size = size;
        trie->mem_free(value_loc, value_size);
        return detail::InsertResult::Fail;
      }
      auto *cow_data = (detail::tag_vector_t::data_t *)trie->mem_get(cow_data_loc);
      memcpy(cow_data, data, sizeof(detail::tag_vector_t::data_t) * size);
      cow_data[size].loc = (uint32_t)value_loc;
      cow_data[size].tag = token->get_tag();
      vector->loc = (uint32_t)cow_data_loc;
      vector->size = size + 1;
      trie->mem_lazy_free(data_loc, sizeof(detail::tag_vector_t::data_t) * size);
      return detail::InsertResult::Success;
    } else if (token->value() != nullptr) {
      return detail::InsertResult::Success;
    } else
      return detail::InsertResult::Fail;
  };

  auto fn_create_new_trie = [&](){
    if (write_buffer_size_ > 0) {
      if (write_buffer_size_ < size_limit_)
        write_buffer_size_ *= 2;
      if (write_buffer_size_ > size_limit_)
        write_buffer_size_ = size_limit_;
      size_t bound = key.size() + VarintLength(value.size()) + value.size();
      if (size_t(write_buffer_size_) < bound)
        write_buffer_size_ = std::min(bound + (16 << 20), size_t(-1) >> 1);
    }
    return new MemPatricia(4, write_buffer_size_, concurrent_level_);
  };
  // tool lambda fn end
  // function start                     
  detail::InsertResult insert_result = fn_insert_impl(trie_vec_.back());
  while (insert_result == detail::InsertResult::Fail) {
    if (handle_duplicate_) {
      for (auto iter = trie_vec_.rbegin(); iter != trie_vec_.rend(); iter++)
        if (detail::InsertResult::Fail != (insert_result = fn_insert_impl(*iter)))
          break;
    } else insert_result = fn_insert_impl(trie_vec_.back());
    if (insert_result == detail::InsertResult::Fail) {
      //mutex_.WriteLock();
      trie_vec_.emplace_back(fn_create_new_trie());
      //mutex_.WriteUnlock();
    }
  }
  assert(insert_result != detail::InsertResult::Fail);
  return insert_result == detail::InsertResult::Success;
}

uint32_t PatriciaMemtableRep::HeapItem::GetValue() {
  auto trie = static_cast<MemPatricia *>(handle.iter()->trie());
  auto vector = (detail::tag_vector_t *)trie->mem_get(*(uint32_t *)handle.iter()->value());
  auto data = (detail::tag_vector_t::data_t *)trie->mem_get(vector->loc);
  return data[index].loc;
}

void PatriciaMemtableRep::HeapItem::Seek(terark::fstring find_key, uint64_t find_tag) {
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

void PatriciaMemtableRep::HeapItem::SeekForPrev(terark::fstring find_key, uint64_t find_tag) {
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

void PatriciaMemtableRep::HeapItem::SeekToFirst() {
  if (!handle.iter()->seek_begin()) {
    index = size_t(-1);
    return;
  }
  auto vec = GetVector();
  index = vec.size - 1;
  tag = vec.data[index].tag;
}

void PatriciaMemtableRep::HeapItem::SeekToLast() {
  if (!handle.iter()->seek_end()) {
    index = size_t(-1);
    return;
  }
  auto vec = GetVector();
  index = 0;
  tag = vec.data[index].tag;
}

void PatriciaMemtableRep::HeapItem::Next() {
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

void PatriciaMemtableRep::HeapItem::Prev() {
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

PatriciaMemtableRep::HeapIterator::HeapIterator(PatriciaMemtableRep *rep) : direction_(0) {
  assert(rep != nullptr);
  assert(rep->trie_vec_.size() > 1);
  valvec<HeapItem> hitem(rep->trie_vec_.size(), terark::valvec_reserve());
  valvec<HeapItem *> hptrs(rep->trie_vec_.size(), terark::valvec_reserve());
  for (auto trie : rep->trie_vec_) {
    hptrs.push_back(new (hitem.grow_no_init(1)) HeapItem(trie));
  }
  assert(hitem.size() == hptrs.size());
  count = hitem.size();
  array = hitem.risk_release_ownership();
  heap = hptrs.risk_release_ownership();
  size = 0;
}

PatriciaMemtableRep::HeapIterator::~HeapIterator() {
  free(heap);
  for (size_t i = 0; i < count; ++i)
    array[i].~HeapItem();
  free(array);
}

void PatriciaMemtableRep::HeapIterator::Next() {
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
  detail::build_key(heap[0]->handle.iter()->word(), heap[0]->tag, &buffer_);
}

void PatriciaMemtableRep::HeapIterator::Prev() {
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
  } else
    terark::adjust_heap_top(heap, size, BackwardComp());
  detail::build_key(heap[0]->handle.iter()->word(), heap[0]->tag, &buffer_);
}

template <int direction, class func_t> 
void PatriciaMemtableRep::HeapIterator::Rebuild(func_t &&callback_func) {
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

void PatriciaMemtableRep::HeapIterator::Seek(const Slice &user_key, const char *memtable_key) {
  terark::fstring find_key;
  if (memtable_key != nullptr) {
    Slice internal_key = GetLengthPrefixedSlice(memtable_key);
    find_key = terark::fstring(internal_key.data(), internal_key.size() - 8);
  } else
    find_key = terark::fstring(user_key.data(), user_key.size() - 8);
  uint64_t tag = DecodeFixed64(find_key.end());

  Rebuild<1>([&](HeapItem *item) {
    item->Seek(find_key, tag);
    return item->index != size_t(-1);
  });
  if (size == 0) {
    direction_ = 0;
    return;
  }
  detail::build_key(heap[0]->handle.iter()->word(), heap[0]->tag, &buffer_);
}

void PatriciaMemtableRep::HeapIterator::SeekForPrev(const Slice &user_key,
                          const char *memtable_key) {
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
  detail::build_key(heap[0]->handle.iter()->word(), heap[0]->tag, &buffer_);
}

void PatriciaMemtableRep::HeapIterator::SeekToFirst() {
  Rebuild<1>([&](HeapItem *item) {
    item->SeekToFirst();
    return item->index != size_t(-1);
  });
  if (size == 0) {
    direction_ = 0;
    return;
  }
  detail::build_key(heap[0]->handle.iter()->word(), heap[0]->tag, &buffer_);
}

void PatriciaMemtableRep::HeapIterator::SeekToLast() {
  Rebuild<-1>([&](HeapItem *item) {
    item->SeekToLast();
    return item->index != size_t(-1);
  });
  if (size == 0) {
    direction_ = 0;
    return;
  }
  detail::build_key(heap[0]->handle.iter()->word(), heap[0]->tag, &buffer_);
}

MemTableRep* PatriciaMemTableRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator &key_cmp,
    bool needs_dup_key_check,
    Allocator *allocator,
    const SliceTransform *transform,
    Logger *logger) {
  if (IsForwardBytewiseComparator(key_cmp.icomparator())) {
    return new PatriciaMemtableRep(concurrent_type_, patricia_key_type_,
                                   needs_dup_key_check, write_buffer_size_,
                                   allocator, key_cmp);
  } else {
    return fallback_->CreateMemTableRep(key_cmp, needs_dup_key_check,
                                        allocator, transform, logger);
  }
}

MemTableRep* PatriciaMemTableRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator &key_cmp,
    bool needs_dup_key_check,
    Allocator *allocator,
    const ImmutableCFOptions &ioptions,
    const MutableCFOptions &mutable_cf_options,
    uint32_t column_family_id) {
  if (IsForwardBytewiseComparator(key_cmp.icomparator())) {
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

MemTableRepFactory*
NewPatriciaTrieRepFactory(std::shared_ptr<class MemTableRepFactory> fallback) {
  if (!fallback) {
    fallback.reset(new SkipListFactory());
  }
  return new PatriciaMemTableRepFactory(fallback);
}

MemTableRepFactory*
NewPatriciaTrieRepFactory(const std::unordered_map<std::string, std::string>& options, Status* s) {
  auto f = options.find("fallback");
  std::shared_ptr<class MemTableRepFactory> fallback;
  if (f != options.end() && f->second != "patricia") {
    fallback.reset(CreateMemTableRepFactory(f->second, options, s));
    if (!s->ok()) {
      *s = Status::InvalidArgument("NewPatriciaTrieRepFactory", s->getState());
      return nullptr;
    }
  }
  return NewPatriciaTrieRepFactory(fallback);
}


} // namespace rocksdb