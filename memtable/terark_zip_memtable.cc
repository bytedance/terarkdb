#include "terark_zip_memtable.h"

#if defined(_MSC_VER)
//#include <windows.h>
#else
#include <sys/mman.h>
#endif

using terark::MainPatricia;

namespace {

inline const char* build_key(terark::fstring user_key, uint64_t tag,
                             terark::valvec<char>* buffer) {
  buffer->resize(0);
  buffer->reserve(user_key.size() + 8);
  buffer->append(user_key.data(), user_key.size());
  if (rocksdb::port::kLittleEndian) {
    buffer->append(const_cast<const char*>(reinterpret_cast<char*>(&tag)),
                   sizeof(tag));
  } else {
    char buf[sizeof(tag)];
    rocksdb::EncodeFixed64(buf, tag);
    buffer->append(buf, sizeof(buf));
  }
  return buffer->data();
}

inline const char* build_key(terark::fstring user_key, uint64_t tag,
                             std::string* buffer) {
  buffer->resize(0);
  buffer->reserve(user_key.size() + 8);
  buffer->append(user_key.data(), user_key.size());
  rocksdb::PutFixed64(buffer, tag);
  return buffer->data();
}

}  // namespace

namespace rocksdb {

namespace details = terark_memtable_details;

static const uint64_t LOCK_FLAG = 1ULL << 63;
static const uint32_t SIZE_MASK = INT32_MAX;

bool MemWriterToken::init_value(void* valptr, size_t valsize) noexcept {
  assert(valsize == sizeof(uint32_t));
  assert(size_t(valptr) % sizeof(uint32_t) == 0);  // must be aligned
  (void)valsize;
  size_t data_loc = MainPatricia::mem_alloc_fail;
  size_t value_loc = MainPatricia::mem_alloc_fail;
  size_t vector_loc = MainPatricia::mem_alloc_fail;
  size_t value_size = VarintLength(value_.size()) + value_.size();
  auto trie = static_cast<MainPatricia*>(m_trie);
  do {
    vector_loc = trie->mem_alloc(sizeof(details::tag_vector_t));
    if (vector_loc == MainPatricia::mem_alloc_fail) break;
    data_loc = trie->mem_alloc(sizeof(details::tag_vector_t::data_t));
    if (data_loc == MainPatricia::mem_alloc_fail) break;
    value_loc = trie->mem_alloc(value_size);
    if (value_loc == MainPatricia::mem_alloc_fail) break;
    auto value_enc = (char*)trie->mem_get(value_loc);
    auto value_dst = EncodeVarint32(value_enc, (uint32_t)value_.size());
    memcpy(value_dst, value_.data(), value_.size());
    auto* data = (details::tag_vector_t::data_t*)trie->mem_get(data_loc);
    data->loc = (uint32_t)value_loc;
    data->tag = tag_;
    auto* vector = (details::tag_vector_t*)trie->mem_get(vector_loc);
    vector->size_loc.store((1ULL << 32) + data_loc, std::memory_order_release);
    *(uint32_t*)valptr = (uint32_t)vector_loc;
    return true;
  } while (false);
  if (value_loc != MainPatricia::mem_alloc_fail)
    trie->mem_free(value_loc, value_size);
  if (data_loc != MainPatricia::mem_alloc_fail)
    trie->mem_free(data_loc, sizeof(details::tag_vector_t::data_t));
  if (vector_loc != MainPatricia::mem_alloc_fail)
    trie->mem_free(vector_loc, sizeof(details::tag_vector_t));
  return false;
}

PatriciaTrieRep::PatriciaTrieRep(details::ConcurrentType concurrent_type,
                                 details::PatriciaKeyType patricia_key_type,
                                 bool handle_duplicate,
                                 intptr_t write_buffer_size,
                                 Allocator* allocator)
    : MemTableRep(allocator) {
  immutable_ = false;
  patricia_key_type_ = patricia_key_type;
  handle_duplicate_ = handle_duplicate;
  write_buffer_size_ = write_buffer_size;
  if (concurrent_type == details::ConcurrentType::Native)
    concurrent_level_ = terark::Patricia::MultiWriteMultiRead;
  else
    concurrent_level_ = terark::Patricia::OneWriteMultiRead;
  trie_vec_[0] =
      new MainPatricia(sizeof(uint32_t), write_buffer_size_, concurrent_level_);
  trie_vec_size_ = 1;
  overhead_ = trie_vec_[0]->mem_size_inline();
}

PatriciaTrieRep::~PatriciaTrieRep() {
  for (size_t i = 0; i < trie_vec_size_; ++i) {
    auto trie = trie_vec_[i];
    void* base = trie->mem_get(0);
    size_t size = terark::align_up(trie->mem_size(), 4096);
    if (mprotect(base, size, PROT_READ | PROT_WRITE) < 0) {
      fprintf(stderr, "%s:%d: %s: FATAL: mprotect(%p, %zd, READ|WRITE) = %s\n",
              __FILE__, __LINE__, BOOST_CURRENT_FUNCTION, base, size,
              strerror(errno));
    }
    delete trie;
  }
}

void PatriciaTrieRep::MarkReadOnly() {
#if 0  // set_readonly not released
  for (size_t i = 0; i < trie_vec_size_; ++i) {
    trie_vec_[i]->set_readonly();
  }
#endif
  static std::atomic<int> file_seq(0);

  if (terark::getEnvBool("TerarkDB_csppMemTabDump")) {
    int curr_seq = file_seq++;
    for (size_t i = 0; i < trie_vec_size_; ++i) {
      char fname[64];
      snprintf(fname, sizeof(fname) - 1, "cspp-memtab-%06d-%03zd.mmap",
               curr_seq, i);
      trie_vec_[i]->save_mmap(fname);
    }
  }
  for (size_t i = 0; i < trie_vec_size_; ++i) {
    auto trie = trie_vec_[i];
    void* base = trie->mem_get(0);
    size_t size = terark::align_up(trie->mem_size(), 4096);
    if (mprotect(base, size, PROT_READ) < 0) {
      fprintf(stderr, "%s:%d: %s: FATAL: mprotect(%p, %zd, READ) = %s\n",
              __FILE__, __LINE__, BOOST_CURRENT_FUNCTION, base, size,
              strerror(errno));
    }
  }
  immutable_ = true;
}

size_t PatriciaTrieRep::ApproximateMemoryUsage() {
  size_t sum = 0;
  for (size_t i = 0; i < trie_vec_size_; ++i) {
    sum += trie_vec_[i]->mem_size_inline();
  }
  assert(sum >= overhead_);
  return sum - overhead_;
}

bool PatriciaTrieRep::Contains(const Slice& internal_key) const {
  terark::fstring find_key(internal_key.data(), internal_key.size() - 8);
  uint64_t tag = ExtractInternalKeyFooter(internal_key);
  for (size_t i = 0; i < trie_vec_size_; ++i) {
    auto* trie = trie_vec_[i];
    auto token = trie->tls_reader_token();
    token->acquire(trie);
    if (trie->lookup(find_key, token)) {
      auto vector =
          (details::tag_vector_t*)trie->mem_get(*(uint32_t*)token->value());
      uint64_t size_loc = vector->size_loc.load(std::memory_order_relaxed);
      auto data =
          (details::tag_vector_t::data_t*)trie->mem_get((uint32_t)size_loc);
      bool ret =
          terark::binary_search_0(data, (size_loc >> 32) & SIZE_MASK, tag);
      token->idle();
      return ret;
    }
    token->idle();
  }
  return false;
}

void PatriciaTrieRep::Get(const LookupKey& k, void* callback_args,
                          bool (*callback_func)(void* arg, const Slice& key,
                                                const char* value)) {
  // assistant structures
  struct HeapItem {
    uint32_t idx;
    uint32_t loc;
    uint64_t tag;
    MainPatricia::ReaderToken* token;

    MainPatricia* trie() { return static_cast<MainPatricia*>(token->trie()); }
  };

  struct TlsItem {
    valvec<HeapItem> heap;
    valvec<char> buffer;
  };

  // variable definition
  static thread_local TlsItem tls_ctx;
  auto buffer = &tls_ctx.buffer;

  Slice internal_key = k.internal_key();
  auto find_key = terark::fstring(internal_key.data(), internal_key.size() - 8);
  auto tag = ExtractInternalKeyFooter(internal_key);

  auto do_callback = [&](HeapItem* heap) -> bool {
    build_key(find_key, heap->tag, buffer);
    auto trie = heap->trie();
    auto vector = (details::tag_vector_t*)trie->mem_get(heap->loc);
    auto data = (details::tag_vector_t::data_t*)trie->mem_get(
        (uint32_t)vector->size_loc.load(std::memory_order_relaxed));
    auto value = (const char*)trie->mem_get(data[heap->idx].loc);
    return callback_func(callback_args, Slice(buffer->data(), buffer->size()),
                         value);
  };

  valvec<HeapItem>& heap = tls_ctx.heap;
  assert(heap.empty());
  heap.reserve(trie_vec_size_);

  // initialization
  for (size_t i = 0; i < trie_vec_size_; ++i) {
    auto* trie = trie_vec_[i];
    auto token = trie->tls_reader_token();
    token->acquire(trie);
    if (trie_vec_[i]->lookup(find_key, token)) {
      uint32_t loc = token->value_of<uint32_t>();
      auto vector = (details::tag_vector_t*)trie->mem_get(loc);
      uint64_t size_loc = vector->size_loc.load(std::memory_order_relaxed);
      auto data =
          (details::tag_vector_t::data_t*)trie->mem_get((uint32_t)size_loc);
      size_t idx =
          terark::upper_bound_0(data, (size_loc >> 32) & SIZE_MASK, tag) - 1;
      if (idx != size_t(-1)) {
        heap.emplace_back(HeapItem{(uint32_t)idx, loc, data[idx].tag, token});
        continue;
      }
    }
    token->idle();
  }

  // make heap for multi-merge
  auto heap_comp = [](const HeapItem& l, const HeapItem& r) {
    return l.tag < r.tag;
  };
  if (heap.empty()) {
    return;
  }
  std::make_heap(heap.begin(), heap.end(), heap_comp);
  while (do_callback(&heap.front())) {
    auto& item = heap.front();
    if (item.idx == 0) {
      if (heap.size() > 1) {
        item.token->idle();
        std::pop_heap(heap.begin(), heap.end(), heap_comp);
        heap.pop_back();
      } else {
        assert(heap.size() == 1);
        break;
      }
    } else {
      auto idx = --item.idx;
      auto trie = item.trie();
      auto vector = (details::tag_vector_t*)(trie->mem_get(item.loc));
      auto dataloc = (uint32_t)vector->size_loc.load(std::memory_order_relaxed);
      auto data = (details::tag_vector_t::data_t*)(trie->mem_get(dataloc));
      item.tag = data[idx].tag;
      terark::adjust_heap_top(heap.begin(), heap.size(), heap_comp);
    }
  }
  for (auto& item : heap) {
    item.token->idle();
  }
  heap.erase_all();
}

MemTableRep::Iterator* PatriciaTrieRep::GetIterator(Arena* arena) {
  MemTableRep::Iterator* iter;
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

bool PatriciaTrieRep::InsertKeyValue(const Slice& internal_key,
                                     const Slice& value) {
  TERARK_VERIFY(!immutable_);
  // immutable check
  // if (immutable_) return false;
  // prepare key
  terark::fstring key(internal_key.data(), internal_key.size() - 8);
  auto tag = ExtractInternalKeyFooter(internal_key);
  // lambda impl fn for insert
  auto fn_insert_impl = [&](MainPatricia* trie) {
    auto token = trie->tls_writer_token_nn<MemWriterToken>();
    assert(dynamic_cast<MemWriterToken*>(token) != nullptr);
    token->reset_tag_value(tag, value);
    token->acquire(trie);
    TERARK_SCOPE_EXIT(token->idle());
    uint32_t tmp_loc = UINT32_MAX;
    if (!token->insert(key, &tmp_loc)) {
      size_t vector_loc = token->value_of<uint32_t>();
      auto* vector = (details::tag_vector_t*)trie->mem_get(vector_loc);
      size_t value_size = VarintLength(value.size()) + value.size();
      size_t value_loc = trie->mem_alloc(value_size);
      if (value_loc == MainPatricia::mem_alloc_fail) {
        return details::InsertResult::Fail;
      }
      auto valptr = (char*)trie->mem_get(value_loc);
      valptr = EncodeVarint32(valptr, (uint32_t)value.size());
      memcpy(valptr, value.data(), value.size());
      uint64_t size_loc;
      // row lock: infinite spin on LOCK_FLAG
      do {
        do {
          size_loc = vector->size_loc.load(std::memory_order_relaxed);
        } while (size_loc & LOCK_FLAG);
        size_loc =
            vector->size_loc.fetch_or(LOCK_FLAG, std::memory_order_acq_rel);
      } while (size_loc & LOCK_FLAG);
      auto* data =
          (details::tag_vector_t::data_t*)trie->mem_get((uint32_t)size_loc);
      uint32_t size = (size_loc >> 32);
      assert(size > 0);
      size_t insert_pos = terark::lower_bound_ex_n(
          data, 0, size, tag >> 8,
          [](details::tag_vector_t::data_t& item) { return item.tag >> 8; });
      if (insert_pos < size && (tag >> 8) == (data[insert_pos].tag >> 8)) {
        vector->size_loc.store(size_loc, std::memory_order_release);
        trie->mem_free(value_loc, value_size);
        return details::InsertResult::Duplicated;
      }
      if (!details::tag_vector_t::full(size) && insert_pos == size) {
        data[size].loc = (uint32_t)value_loc;
        data[size].tag = tag;

        // update 'size' and unlock
        vector->size_loc.store(size_loc + (1ULL << 32),
                               std::memory_order_release);
        return details::InsertResult::Success;
      }
      size_t old_data_cap =
          sizeof(details::tag_vector_t::data_t) *
          (1u << (32 - details::tag_vector_t::full(size) - fast_clz32(size)));
      size_t cow_data_loc = trie->mem_alloc(
          old_data_cap * (1 + details::tag_vector_t::full(size)));
      if (cow_data_loc == MainPatricia::mem_alloc_fail) {
        vector->size_loc.store(size_loc, std::memory_order_release);
        trie->mem_free(value_loc, value_size);
        return details::InsertResult::Fail;
      }
      auto* cow_data =
          (details::tag_vector_t::data_t*)trie->mem_get(cow_data_loc);
      memcpy(cow_data, data,
             sizeof(details::tag_vector_t::data_t) * insert_pos);
      cow_data[insert_pos].loc = (uint32_t)value_loc;
      cow_data[insert_pos].tag = tag;
      memcpy(cow_data + insert_pos + 1, data + insert_pos,
             sizeof(details::tag_vector_t::data_t) * (size - insert_pos));
      vector->size_loc.store((uint64_t(size + 1) << 32) + cow_data_loc,
                             std::memory_order_release);
      trie->mem_lazy_free((uint32_t)size_loc, old_data_cap);
      return details::InsertResult::Success;
    } else if (token->value() != nullptr) {
      const auto token_value_loc = token->value_of<uint32_t>();
      TERARK_VERIFY(token_value_loc == tmp_loc);
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
    trie_vec_[trie_vec_size_] = new MainPatricia(
        sizeof(uint32_t), write_buffer_size_, concurrent_level_);
    trie_vec_size_++;
  };
  // tool lambda fn end
  // function start
  if (handle_duplicate_) {
    for (size_t i = 0; i < trie_vec_size_; ++i) {
      auto* trie = trie_vec_[i];
      auto token = trie->tls_reader_token();
      token->acquire(trie);
      TERARK_SCOPE_EXIT(token->idle());
      if (trie->lookup(key, token)) {
        auto vector =
            (details::tag_vector_t*)trie->mem_get(token->value_of<uint32_t>());
        uint64_t size_loc = vector->size_loc.load(std::memory_order_relaxed);
        auto data =
            (details::tag_vector_t::data_t*)trie->mem_get((uint32_t)size_loc);
        if (terark::binary_search_0(data, (size_loc >> 32) & SIZE_MASK, tag)) {
          return false;
        }
      }
    }
  }
  details::InsertResult insert_result = details::InsertResult::Fail;
  for (;;) {
    size_t curr_trie_vec_size = trie_vec_size_;
    insert_result = fn_insert_impl(trie_vec_[curr_trie_vec_size - 1]);
    if (insert_result == details::InsertResult::Duplicated) {
      return !handle_duplicate_;
    }
    if (insert_result == details::InsertResult::Success) {
      break;
    } else {
      assert(insert_result == details::InsertResult::Fail);
      std::unique_lock<std::mutex> lock(mutex_);
      if (curr_trie_vec_size == trie_vec_size_) {
        fn_create_new_trie();
      }
    }
  }
  assert(insert_result == details::InsertResult::Success);
  return true;
}

template <bool heap_mode>
typename PatriciaRepIterator<heap_mode>::HeapItem::VectorData
PatriciaRepIterator<heap_mode>::HeapItem::GetVector() {
  auto trie = static_cast<terark::MainPatricia*>(handle->trie());
  auto vectorloc = handle->value_of<uint32_t>();
  auto vector = (details::tag_vector_t*)trie->mem_get(vectorloc);
  uint64_t size_loc = vector->size_loc.load(std::memory_order_relaxed);
  auto dataloc = (uint32_t)size_loc;
  auto data = (details::tag_vector_t::data_t*)trie->mem_get(dataloc);
  return {(size_loc >> 32) & SIZE_MASK, data};
}

template <bool heap_mode>
uint32_t PatriciaRepIterator<heap_mode>::HeapItem::GetValue() const {
  auto trie = static_cast<terark::MainPatricia*>(handle->trie());
  auto vectorloc = handle->value_of<uint32_t>();
  auto vector = (details::tag_vector_t*)trie->mem_get(vectorloc);
  uint64_t size_loc = vector->size_loc.load(std::memory_order_relaxed);
  auto dataloc = (uint32_t)size_loc;
  auto data = (details::tag_vector_t::data_t*)trie->mem_get(dataloc);
  return data[index].loc;
}

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::HeapItem::Seek(terark::fstring find_key,
                                                    uint64_t find_tag) {
  if (!handle->seek_lower_bound(find_key)) {
    index = size_t(-1);
    return;
  }
  auto vec = GetVector();
  if (handle->word() == find_key) {
    index = terark::upper_bound_0(vec.data, vec.size, find_tag) - 1;
    if (index != size_t(-1)) {
      tag = vec.data[index];
      return;
    }
    if (!handle->incr()) {
      assert(index == size_t(-1));
      return;
    }
    vec = GetVector();
  }
  assert(handle->word() > find_key);
  index = vec.size - 1;
  tag = vec.data[index].tag;
}

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::HeapItem::SeekForPrev(
    terark::fstring find_key, uint64_t find_tag) {
  if (!handle->seek_rev_lower_bound(find_key)) {
    index = size_t(-1);
    return;
  }
  auto vec = GetVector();
  if (handle->word() == find_key) {
    index = terark::lower_bound_0(vec.data, vec.size, find_tag);
    if (index != vec.size) {
      tag = vec.data[index].tag;
      return;
    }
    if (!handle->decr()) {
      index = size_t(-1);
      return;
    }
    vec = GetVector();
  }
  assert(handle->word() < find_key);
  index = 0;
  tag = vec.data[index].tag;
}

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::HeapItem::SeekToFirst() {
  if (!handle->seek_begin()) {
    index = size_t(-1);
    return;
  }
  auto vec = GetVector();
  index = vec.size - 1;
  tag = vec.data[index].tag;
}

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::HeapItem::SeekToLast() {
  if (!handle->seek_end()) {
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
    if (!handle->incr()) {
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
    if (!handle->decr()) {
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
void PatriciaRepIterator<heap_mode>::Rebuild(func_t&& callback_func) {
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
PatriciaRepIterator<heap_mode>::PatriciaRepIterator(details::tries_t& tries,
                                                    size_t tries_size)
    : direction_(0) {
  assert(tries.size() > 0);
  if (heap_mode) {
    valvec<HeapItem> hitem(tries.size(), terark::valvec_reserve());
    valvec<HeapItem*> hptrs(tries.size(), terark::valvec_reserve());
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
const char* PatriciaRepIterator<heap_mode>::value() const {
  assert(direction_ != 0);
  const HeapItem* item = Current();
  uint32_t value_loc = item->GetValue();
  auto trie = static_cast<terark::MainPatricia*>(item->handle->trie());
  return (const char*)trie->mem_get(value_loc);
}

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::Next() {
  if (heap_mode) {
    if (direction_ != 1) {
      terark::fstring find_key(buffer_.data(), buffer_.size() - 8);
      auto tag = ExtractInternalKeyFooter(buffer_);
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

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::Prev() {
  if (heap_mode) {
    if (direction_ != -1) {
      terark::fstring find_key(buffer_.data(), buffer_.size() - 8);
      auto tag = ExtractInternalKeyFooter(buffer_);
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

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::Seek(const Slice& user_key,
                                          const char* memtable_key) {
  terark::fstring find_key;
  uint64_t tag;
  if (memtable_key != nullptr) {
    Slice internal_key = GetLengthPrefixedSlice(memtable_key);
    find_key = terark::fstring(internal_key.data(), internal_key.size() - 8);
    tag = ExtractInternalKeyFooter(internal_key);
  } else {
    find_key = terark::fstring(user_key.data(), user_key.size() - 8);
    tag = ExtractInternalKeyFooter(user_key);
  }

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

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::SeekForPrev(const Slice& user_key,
                                                 const char* memtable_key) {
  terark::fstring find_key;
  uint64_t tag;
  if (memtable_key != nullptr) {
    Slice internal_key = GetLengthPrefixedSlice(memtable_key);
    find_key = terark::fstring(internal_key.data(), internal_key.size() - 8);
    tag = ExtractInternalKeyFooter(internal_key);
  } else {
    find_key = terark::fstring(user_key.data(), user_key.size() - 8);
    tag = ExtractInternalKeyFooter(user_key);
  }

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

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::SeekToFirst() {
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

template <bool heap_mode>
void PatriciaRepIterator<heap_mode>::SeekToLast() {
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

MemTableRep* PatriciaTrieRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& key_cmp, bool needs_dup_key_check,
    Allocator* allocator, const SliceTransform* transform, Logger* logger) {
  if (IsForwardBytewiseComparator(key_cmp.icomparator()->user_comparator())) {
    return new PatriciaTrieRep(concurrent_type_, patricia_key_type_,
                               needs_dup_key_check, write_buffer_size_,
                               allocator);
  } else {
    return fallback_->CreateMemTableRep(key_cmp, needs_dup_key_check, allocator,
                                        transform, logger);
  }
}

MemTableRep* PatriciaTrieRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& key_cmp, bool needs_dup_key_check,
    Allocator* allocator, const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, uint32_t column_family_id) {
  if (IsForwardBytewiseComparator(key_cmp.icomparator()->user_comparator())) {
    return new PatriciaTrieRep(concurrent_type_, patricia_key_type_,
                               needs_dup_key_check, write_buffer_size_,
                               allocator);
  } else {
    return fallback_->CreateMemTableRep(key_cmp, needs_dup_key_check, allocator,
                                        ioptions, mutable_cf_options,
                                        column_family_id);
  }
}

static MemTableRepFactory* CreatePatriciaTrieRepFactory(
    std::shared_ptr<class MemTableRepFactory>& fallback,
    details::ConcurrentType concurrent_type,
    details::PatriciaKeyType patricia_key_type, int64_t write_buffer_size) {
  if (!fallback) fallback.reset(new SkipListFactory());
  return new PatriciaTrieRepFactory(fallback, concurrent_type,
                                    patricia_key_type, write_buffer_size);
}

MemTableRepFactory* NewPatriciaTrieRepFactory(
    std::shared_ptr<class MemTableRepFactory> fallback) {
  return CreatePatriciaTrieRepFactory(fallback, details::ConcurrentType::Native,
                                      details::PatriciaKeyType::UserKey,
                                      64ull << 20);
}

MemTableRepFactory* NewPatriciaTrieRepFactory(
    const std::unordered_map<std::string, std::string>& options, Status* s) {
  details::ConcurrentType concurrent_type = details::ConcurrentType::Native;
  int64_t write_buffer_size = 64 * 1024 * 1024;
  std::shared_ptr<class MemTableRepFactory> fallback;
  details::PatriciaKeyType patricia_key_type =
      details::PatriciaKeyType::UserKey;

  auto c = options.find("concurrent_type");
  if (c != options.end() && c->second == "none") {
    concurrent_type = details::ConcurrentType::None;
  }

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
  if (p != options.end() && p->second == "full") {
    patricia_key_type = details::PatriciaKeyType::FullKey;
  }

  return CreatePatriciaTrieRepFactory(fallback, concurrent_type,
                                      patricia_key_type, write_buffer_size);
}

}  // namespace rocksdb
