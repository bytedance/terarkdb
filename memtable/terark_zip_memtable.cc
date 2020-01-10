#include "terark_zip_memtable.h"

namespace {

inline const char *build_key(terark::fstring user_key, uint64_t tag,
                             terark::valvec<char> *buffer) {
  buffer->resize(0);
  buffer->reserve(user_key.size() + 8);
  buffer->append(user_key.data(), user_key.size());
  if (rocksdb::port::kLittleEndian) {
    buffer->append(const_cast<const char *>(reinterpret_cast<char *>(&tag)),
                   sizeof(tag));
  } else {
    char buf[sizeof(tag)];
    rocksdb::EncodeFixed64(buf, tag);
    buffer->append(buf, sizeof(buf));
  }
  return buffer->data();
}

inline const char *build_key(terark::fstring user_key, uint64_t tag,
                             std::string *buffer) {
  buffer->resize(0);
  buffer->reserve(user_key.size() + 8);
  buffer->append(user_key.data(), user_key.size());
  rocksdb::PutFixed64(buffer, tag);
  return buffer->data();
}

}  // namespace

namespace rocksdb {

namespace details = terark_memtable_details;

bool MemWriterToken::init_value(void *valptr, size_t valsize) noexcept {
  assert(valsize == sizeof(uint32_t));
  size_t data_loc = MemPatricia::mem_alloc_fail;
  size_t value_loc = MemPatricia::mem_alloc_fail;
  size_t vector_loc = MemPatricia::mem_alloc_fail;
  size_t value_size = VarintLength(value_.size()) + value_.size();
  auto trie = static_cast<MemPatricia *>(m_trie);
  do {
    vector_loc = trie->mem_alloc(sizeof(details::tag_vector_t));
    if (vector_loc == MemPatricia::mem_alloc_fail) break;
    data_loc = trie->mem_alloc(sizeof(details::tag_vector_t::data_t));
    if (data_loc == MemPatricia::mem_alloc_fail) break;
    value_loc = trie->mem_alloc(value_size);
    if (value_loc == MemPatricia::mem_alloc_fail) break;
    char *value_dst = EncodeVarint32((char *)trie->mem_get(value_loc),
                                     (uint32_t)value_.size());
    memcpy(value_dst, value_.data(), value_.size());
    auto *data = (details::tag_vector_t::data_t *)trie->mem_get(data_loc);
    data->loc = (uint32_t)value_loc;
    data->tag = tag_;
    auto *vector = (details::tag_vector_t *)trie->mem_get(vector_loc);
    vector->loc.store((uint32_t)data_loc, std::memory_order_release);
    vector->size.store(1, std::memory_order_release);
    uint32_t u32_vector_loc = (uint32_t)vector_loc;
    memcpy(valptr, &u32_vector_loc, valsize);
    return true;
  } while (false);
  if (value_loc != MemPatricia::mem_alloc_fail)
    trie->mem_free(value_loc, value_size);
  if (data_loc != MemPatricia::mem_alloc_fail)
    trie->mem_free(data_loc, sizeof(details::tag_vector_t::data_t));
  if (vector_loc != MemPatricia::mem_alloc_fail)
    trie->mem_free(vector_loc, sizeof(details::tag_vector_t));
  return false;
}

PatriciaTrieRep::PatriciaTrieRep(details::ConcurrentType concurrent_type,
                                 details::PatriciaKeyType patricia_key_type,
                                 bool handle_duplicate,
                                 intptr_t write_buffer_size,
                                 Allocator *allocator,
                                 const MemTableRep::KeyComparator & /*compare*/)
    : MemTableRep(allocator) {
  immutable_ = false;
  patricia_key_type_ = patricia_key_type;
  handle_duplicate_ = handle_duplicate;
  write_buffer_size_ = write_buffer_size;
  if (concurrent_type == details::ConcurrentType::Native)
    concurrent_level_ = terark::Patricia::ConcurrentLevel::MultiWriteMultiRead;
  else
    concurrent_level_ = terark::Patricia::ConcurrentLevel::OneWriteMultiRead;
  trie_vec_[0] =
      new MemPatricia(sizeof(uint32_t), write_buffer_size_, concurrent_level_);
  trie_vec_size_ = 1;
  overhead_ = trie_vec_[0]->mem_size_inline();
}

PatriciaTrieRep::~PatriciaTrieRep() {
  for (size_t i = 0; i < trie_vec_size_; ++i) {
    delete trie_vec_[i];
  }
}

size_t PatriciaTrieRep::ApproximateMemoryUsage() {
  size_t sum = 0;
  for (size_t i = 0; i < trie_vec_size_; ++i) {
    sum += trie_vec_[i]->mem_size_inline();
  }
  assert(sum >= overhead_);
  return sum - overhead_;
}

bool PatriciaTrieRep::Contains(const Slice &internal_key) const {
  terark::fstring find_key(internal_key.data(), internal_key.size() - 8);
  uint64_t tag = ExtractInternalKeyFooter(internal_key);
  for (size_t i = 0; i < trie_vec_size_; ++i) {
    auto token = trie_vec_[i]->acquire_tls_reader_token();
    if ((trie_vec_[i])->lookup(find_key, token)) {
      auto vector = (details::tag_vector_t *)(trie_vec_[i])
                        ->mem_get(*(uint32_t *)token->value());
      size_t size = vector->size.load(std::memory_order_relaxed) & ~(1u << 31);
      auto data = (details::tag_vector_t::data_t *)trie_vec_[i]->mem_get(
          vector->loc.load(std::memory_order_relaxed));
      return terark::binary_search_0(data, size, tag);
    }
  }
  return false;
}

void PatriciaTrieRep::Get(const LookupKey &k, void *callback_args,
                          bool (*callback_func)(void *arg, const Slice &key,
                                                LazyBuffer &&value)) {
  // assistant structures
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

  class LazyBufferStateImpl : public LazyBufferState {
   public:
    virtual void destroy(LazyBuffer * /*buffer*/) const override {}

    virtual void pin_buffer(LazyBuffer * /*buffer*/) const override {}

    Status fetch_buffer(LazyBuffer *buffer) const override {
      auto context = get_context(buffer);
      auto trie = reinterpret_cast<MemPatricia *>(context->data[0]);
      auto loc = static_cast<uint32_t>(context->data[1]);
      auto idx = static_cast<uint32_t>(context->data[2]);
      auto vector = (details::tag_vector_t *)trie->mem_get(loc);
      auto data = (details::tag_vector_t::data_t *)trie->mem_get(
          vector->loc.load(std::memory_order_relaxed));
      set_slice(buffer, GetLengthPrefixedSlice(
                            (const char *)trie->mem_get(data[idx].loc)));
      return Status::OK();
    }
  };
  static LazyBufferStateImpl static_state;

  // variable definition
  static thread_local TlsItem tls_ctx;
  auto buffer = &tls_ctx.buffer;

  Slice internal_key = k.internal_key();
  auto find_key = terark::fstring(internal_key.data(), internal_key.size() - 8);
  uint64_t tag = DecodeFixed64(find_key.end());

  auto do_callback = [&](HeapItem *heap) {
    build_key(find_key, heap->tag, buffer);
    return callback_func(
        callback_args, Slice(buffer->data(), buffer->size()),
        LazyBuffer(&static_state, {reinterpret_cast<uint64_t>(heap->trie),
                                   static_cast<uint64_t>(heap->loc),
                                   static_cast<uint64_t>(heap->idx)}));
  };

  valvec<HeapItem> &heap = tls_ctx.heap;
  assert(heap.empty());
  heap.reserve(trie_vec_size_);

  // initialization
  for (size_t i = 0; i < trie_vec_size_; ++i) {
    auto token = trie_vec_[i]->acquire_tls_reader_token();
    token->update_lazy();
    if (trie_vec_[i]->lookup(find_key, token)) {
      uint32_t loc = *(uint32_t *)token->value();
      auto vector = (details::tag_vector_t *)trie_vec_[i]->mem_get(loc);
      size_t size = vector->size.load(std::memory_order_relaxed) & ~(1u << 31);
      auto data = (details::tag_vector_t::data_t *)trie_vec_[i]->mem_get(
          vector->loc.load(std::memory_order_relaxed));
      size_t idx = terark::upper_bound_0(data, size, tag) - 1;
      if (idx != size_t(-1)) {
        heap.emplace_back(
            HeapItem{(uint32_t)idx, loc, data[idx].tag, trie_vec_[i]});
      }
      break;
    }
  }

  // make heap for multi-merge
  auto heap_comp = [](const HeapItem &l, const HeapItem &r) {
    return l.tag < r.tag;
  };

  std::make_heap(heap.begin(), heap.end(), heap_comp);
  while (heap.size() > 0 && do_callback(&heap.front())) {
    auto item = heap.front();
    if (item.idx == 0) {
      std::pop_heap(heap.begin(), heap.end(), heap_comp);
      heap.pop_back();
      continue;
    }
    --item.idx;
    auto vector = (details::tag_vector_t *)(item.trie->mem_get(item.loc));
    auto data = (details::tag_vector_t::data_t *)(item.trie->mem_get(
        vector->loc.load(std::memory_order_relaxed)));
    item.tag = data[item.idx].tag;
    terark::adjust_heap_top(heap.begin(), heap.size(), heap_comp);
  }
  heap.erase_all();
}

MemTableRep::Iterator *PatriciaTrieRep::GetIterator(Arena *arena) {
  MemTableRep::Iterator *iter;
  if (trie_vec_size_ == 1) {
    typedef PatriciaRepIterator<false> iter_t;
    iter = arena ? new (arena->AllocateAligned(sizeof(iter_t)))
                       iter_t(trie_vec_, 1)
                 : new iter_t(trie_vec_, 1);
  } else {
    typedef PatriciaRepIterator<true> iter_t;
    iter = arena ? new (arena->AllocateAligned(sizeof(iter_t)))
                       iter_t(trie_vec_, trie_vec_size_)
                 : new iter_t(trie_vec_, trie_vec_size_);
  }
  return iter;
}

bool PatriciaTrieRep::InsertKeyValue(const Slice &internal_key,
                                     const Slice &value) {
  // immutable check
  if (immutable_) return false;
  // prepare key
  terark::fstring key(internal_key.data(),
                      internal_key.data() + internal_key.size() - 8);
  auto tag = ExtractInternalKeyFooter(internal_key);
  // lambda impl fn for insert
  auto fn_insert_impl = [&](MemPatricia *trie) {
    MemWriterToken *token;
    if (trie->tls_writer_token() == nullptr) {
      trie->tls_writer_token().reset(
          token = new MemWriterToken(trie, DecodeFixed64(key.end()), value));
    } else {
      assert(dynamic_cast<MemWriterToken *>(trie->tls_writer_token().get()) !=
             nullptr);
      token = static_cast<MemWriterToken *>(trie->tls_writer_token().get());
      token->reset_tag_value(DecodeFixed64(key.end()), value);
    }
    uint32_t tmp_loc;
    if (!token->insert(key, &tmp_loc)) {
      size_t vector_loc = *(uint32_t *)token->value();
      auto *vector = (details::tag_vector_t *)trie->mem_get(vector_loc);
      size_t value_size = VarintLength(value.size()) + value.size();
      size_t value_loc = trie->mem_alloc(value_size);
      if (value_loc == MemPatricia::mem_alloc_fail) {
        return details::InsertResult::Fail;
      }
      memcpy(EncodeVarint32((char *)trie->mem_get(value_loc),
                            (uint32_t)value.size()),
             value.data(), value.size());
      uint32_t size;
      do {
        do {
          size = vector->size.load(std::memory_order_relaxed);
        } while ((size >> 31) != 0);
      } while (((size = vector->size.fetch_or(1u << 31,
                                              std::memory_order_acq_rel)) >>
                31) != 0);
      uint32_t data_loc = vector->loc.load(std::memory_order_relaxed);
      auto *data = (details::tag_vector_t::data_t *)trie->mem_get(data_loc);
      assert(size > 0);
      if ((tag >> 8) == (data[size - 1].tag >> 8)) {
        vector->size.store(size, std::memory_order_release);
        trie->mem_free(value_loc, value_size);
        return details::InsertResult::Duplicated;
      }
      assert(tag > data[size - 1].tag);
      if (!details::tag_vector_t::full(size)) {
        data[size].loc = (uint32_t)value_loc;
        data[size].tag = tag;
        vector->size.store(size + 1, std::memory_order_release);
        return details::InsertResult::Success;
      }
      size_t cow_data_loc =
          trie->mem_alloc(sizeof(details::tag_vector_t::data_t) * size * 2);
      if (cow_data_loc == MemPatricia::mem_alloc_fail) {
        vector->size.store(size, std::memory_order_release);
        trie->mem_free(value_loc, value_size);
        return details::InsertResult::Fail;
      }
      auto *cow_data =
          (details::tag_vector_t::data_t *)trie->mem_get(cow_data_loc);
      memcpy(cow_data, data, sizeof(details::tag_vector_t::data_t) * size);
      cow_data[size].loc = (uint32_t)value_loc;
      cow_data[size].tag = tag;
      vector->loc.store((uint32_t)cow_data_loc, std::memory_order_release);
      vector->size.store(size + 1, std::memory_order_release);
      trie->mem_lazy_free(data_loc,
                          sizeof(details::tag_vector_t::data_t) * size);
      return details::InsertResult::Success;
    } else if (token->value() != nullptr) {
      return details::InsertResult::Success;
    } else
      return details::InsertResult::Fail;
  };

  auto fn_create_new_trie = [&]() {
    if (write_buffer_size_ > 0) {
      if (write_buffer_size_ < size_limit_) write_buffer_size_ *= 2;
      if (write_buffer_size_ > size_limit_) write_buffer_size_ = size_limit_;
      size_t bound = key.size() + VarintLength(value.size()) + value.size();
      if (size_t(write_buffer_size_) < bound)
        write_buffer_size_ = std::min(bound + (16 << 20), size_t(-1) >> 1);
    }
    trie_vec_[trie_vec_size_] = new MemPatricia(
        sizeof(uint32_t), write_buffer_size_, concurrent_level_);
    trie_vec_size_++;
  };
  // tool lambda fn end
  // function start
  if (handle_duplicate_) {
    uint64_t tag = DecodeFixed64(key.end());
    for (size_t i = 0; i < trie_vec_size_; ++i) {
      auto token = trie_vec_[i]->acquire_tls_reader_token();
      if (trie_vec_[i]->lookup(key, token)) {
        auto vector = (details::tag_vector_t *)trie_vec_[i]->mem_get(
            *(uint32_t *)token->value());
        size_t size =
            vector->size.load(std::memory_order_relaxed) & ~(1u << 31);
        auto data = (details::tag_vector_t::data_t *)trie_vec_[i]->mem_get(
            vector->loc.load(std::memory_order_relaxed));
        if (terark::binary_search_0(data, size, tag)) {
          return false;
        }
      }
    }
  }
  details::InsertResult insert_result = details::InsertResult::Fail;
  for (;;) {
    insert_result = fn_insert_impl(trie_vec_[trie_vec_size_ - 1]);
    if (insert_result == details::InsertResult::Duplicated) {
      return !handle_duplicate_;
    }
    if (insert_result == details::InsertResult::Success) {
      break;
    } else {
      assert(insert_result == details::InsertResult::Fail);
      if (mutex_.try_lock()) {
        fn_create_new_trie();
      } else {
        mutex_.lock();
      }
      mutex_.unlock();
    }
  }
  assert(insert_result == details::InsertResult::Success);
  return true;
}

template <bool heap_mode>
typename PatriciaRepIterator<heap_mode>::HeapItem::VectorData
PatriciaRepIterator<heap_mode>::HeapItem::GetVector() {
  auto trie = static_cast<terark::MainPatricia *>(handle.iter()->trie());
  auto vector = (details::tag_vector_t *)trie->mem_get(
      *(uint32_t *)handle.iter()->value());
  auto size = vector->size.load(std::memory_order_relaxed) & ~(1u << 31);
  auto data = (typename details::tag_vector_t::data_t *)trie->mem_get(
      vector->loc.load(std::memory_order_relaxed));
  return {size, data};
}

template <bool heap_mode>
uint32_t PatriciaRepIterator<heap_mode>::HeapItem::GetValue() const {
  auto trie = static_cast<terark::MainPatricia *>(handle.iter()->trie());
  auto vector = (details::tag_vector_t *)trie->mem_get(
      *(uint32_t *)handle.iter()->value());
  auto data = (details::tag_vector_t::data_t *)trie->mem_get(
      vector->loc.load(std::memory_order_relaxed));
  return data[index].loc;
}

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::HeapItem::Seek(terark::fstring find_key,
                                                    uint64_t find_tag) {
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

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::HeapItem::SeekForPrev(
    terark::fstring find_key, uint64_t find_tag) {
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

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::HeapItem::SeekToFirst() {
  if (!handle.iter()->seek_begin()) {
    index = size_t(-1);
    return;
  }
  auto vec = GetVector();
  index = vec.size - 1;
  tag = vec.data[index].tag;
}

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::HeapItem::SeekToLast() {
  if (!handle.iter()->seek_end()) {
    index = size_t(-1);
    return;
  }
  auto vec = GetVector();
  index = 0;
  tag = vec.data[index].tag;
}

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::HeapItem::Next() {
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

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::HeapItem::Prev() {
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

template <bool heap_mode>
template <int direction, class func_t>
void PatriciaRepIterator<heap_mode>::Rebuild(func_t &&callback_func) {
  static_assert(direction == 1 || direction == -1, "direction must be 1 or -1");
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

template <bool heap_mode>
PatriciaRepIterator<heap_mode>::PatriciaRepIterator(details::tries_t &tries,
                                                    size_t tries_size)
    : direction_(0) {
  assert(tries.size() > 0);
  if (heap_mode) {
    valvec<HeapItem> hitem(tries.size(), terark::valvec_reserve());
    valvec<HeapItem *> hptrs(tries.size(), terark::valvec_reserve());
    for (size_t i = 0; i < tries_size; ++i) {
      hptrs.push_back(new (hitem.grow_no_init(1)) HeapItem(tries[i]));
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
template <bool heap_mode>
PatriciaRepIterator<heap_mode>::~PatriciaRepIterator() {
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

template <bool heap_mode>
Slice PatriciaRepIterator<heap_mode>::GetValue() const {
  const HeapItem *item = Current();
  uint32_t value_loc = item->GetValue();
  auto trie = static_cast<terark::MainPatricia *>(item->handle.iter()->trie());
  return GetLengthPrefixedSlice((const char *)trie->mem_get(value_loc));
}

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::Next() {
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
  build_key(CurrentKey(), CurrentTag(), &buffer_);
}

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::Prev() {
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
  build_key(CurrentKey(), CurrentTag(), &buffer_);
}

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::Seek(const Slice &user_key,
                                          const char *memtable_key) {
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
  build_key(CurrentKey(), CurrentTag(), &buffer_);
}

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::SeekForPrev(const Slice &user_key,
                                                 const char *memtable_key) {
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
  build_key(CurrentKey(), CurrentTag(), &buffer_);
}

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::SeekToFirst() {
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
  build_key(CurrentKey(), CurrentTag(), &buffer_);
}

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::SeekToLast() {
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
  build_key(CurrentKey(), CurrentTag(), &buffer_);
}

MemTableRep *PatriciaTrieRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator &key_cmp, bool needs_dup_key_check,
    Allocator *allocator, const SliceTransform *transform, Logger *logger) {
  if (IsForwardBytewiseComparator(key_cmp.icomparator()->user_comparator())) {
    return new PatriciaTrieRep(concurrent_type_, patricia_key_type_,
                               needs_dup_key_check, write_buffer_size_,
                               allocator, key_cmp);
  } else {
    return fallback_->CreateMemTableRep(key_cmp, needs_dup_key_check, allocator,
                                        transform, logger);
  }
}

MemTableRep *PatriciaTrieRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator &key_cmp, bool needs_dup_key_check,
    Allocator *allocator, const ImmutableCFOptions &ioptions,
    const MutableCFOptions &mutable_cf_options, uint32_t column_family_id) {
  if (IsForwardBytewiseComparator(key_cmp.icomparator()->user_comparator())) {
    return new PatriciaTrieRep(concurrent_type_, patricia_key_type_,
                               needs_dup_key_check, write_buffer_size_,
                               allocator, key_cmp);
  } else {
    return fallback_->CreateMemTableRep(key_cmp, needs_dup_key_check, allocator,
                                        ioptions, mutable_cf_options,
                                        column_family_id);
  }
}

static MemTableRepFactory *CreatePatriciaTrieRepFactory(
    std::shared_ptr<class MemTableRepFactory> &fallback,
    details::ConcurrentType concurrent_type,
    details::PatriciaKeyType patricia_key_type, int64_t write_buffer_size) {
  if (!fallback) fallback.reset(new SkipListFactory());
  return new PatriciaTrieRepFactory(fallback, concurrent_type,
                                    patricia_key_type, write_buffer_size);
}

MemTableRepFactory *NewPatriciaTrieRepFactory(
    std::shared_ptr<class MemTableRepFactory> fallback) {
  return CreatePatriciaTrieRepFactory(fallback, details::ConcurrentType::Native,
                                      details::PatriciaKeyType::FullKey,
                                      64ull << 20);
}

MemTableRepFactory *NewPatriciaTrieRepFactory(
    const std::unordered_map<std::string, std::string> &options, Status *s) {
  details::ConcurrentType concurrent_type = details::ConcurrentType::Native;
  int64_t write_buffer_size = 64 * 1024 * 1024;
  std::shared_ptr<class MemTableRepFactory> fallback;
  details::PatriciaKeyType patricia_key_type =
      details::PatriciaKeyType::UserKey;

  auto c = options.find("concurrent_type");
  if (c != options.end() && c->second == "none")
    concurrent_type = details::ConcurrentType::None;

  auto u = options.find("use_virtual_mem");
  if (u != options.end() && u->second == "enable") {
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
      *s = Status::InvalidArgument("NewPatriciaTrieRepFactory", s->getState());
      return nullptr;
    }
  }

  auto p = options.find("key_catagory");
  if (p != options.end() && p->second == "full")
    patricia_key_type = details::PatriciaKeyType::FullKey;

  return CreatePatriciaTrieRepFactory(fallback, concurrent_type,
                                      patricia_key_type, write_buffer_size);
}

}  // namespace rocksdb
