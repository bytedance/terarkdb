#include <db/memtable.h>
#include <rocksdb/memtablerep.h>

#include "util/string_util.h"
#include "db/log_writer.h"

namespace rocksdb {

void MemTableRep::InsertConcurrently(KeyHandle /*handle*/) {
#ifndef ROCKSDB_LITE
  throw std::runtime_error("concurrent insert not supported");
#else
  abort();
#endif
}

const InternalKeyComparator* MemTable::KeyComparator::icomparator() const {
  return &comparator;
}

Slice MemTableRep::UserKey(const char* key) const {
  Slice slice = GetLengthPrefixedSlice(key);
  return Slice(slice.data(), slice.size() - 8);
}

size_t MemTableRep::EncodeKeyValueSize(const Slice& key,
                                       const SliceParts& value) {
  size_t buf_size = 0;
  buf_size += VarintLength(key.size()) + key.size();
  buf_size += VarintLength(value.size()) + value.size();
  return buf_size;
}

void MemTableRep::EncodeKeyValue(const Slice& key, const SliceParts& value,
                                 char* buf) {
  // size of this buf already allocate according to EncodeKeyValueSize
  assert(value.num_parts <= 2);

  char* p = EncodeVarint32(buf, (uint32_t)key.size());
  memcpy(p, key.data(), key.size());
  p = EncodeVarint32(p + key.size(), (uint32_t)value.size());
  for (int i = 0; i < value.num_parts; ++i) {
    memcpy(p, value.parts[i].data(), value.parts[i].size());
    p += value.parts[i].size();
  }
}

LazyBuffer MemTableRep::DecodeToLazyBuffer(const char* key) {
  struct LazyBufferStateImpl : public LazyBufferState {
    void destroy(LazyBuffer* /*buffer*/) const override {}

    Status pin_buffer(LazyBuffer* /*buffer*/) const override {
      return Status::OK();
    }

    Status fetch_buffer(LazyBuffer* buffer) const override {
      auto context = get_context(buffer);
      const char* k = reinterpret_cast<const char*>(context->data[0]);
      Slice key_slice = GetLengthPrefixedSlice(k);
      set_slice(buffer,
                GetLengthPrefixedSlice(key_slice.data() + key_slice.size()));
      return Status::OK();
    }
  };

  static LazyBufferStateImpl static_state;

  return LazyBuffer(&static_state, {reinterpret_cast<uint64_t>(key)});
}

const char* MemTableRep::LengthPrefixedValue(const char* key) {
  Slice key_slice = GetLengthPrefixedSlice(key);
  return key_slice.data() + key_slice.size();
}

bool MemTableRep::InsertKeyValue(const Slice& internal_key,
                                 const SliceParts& value) {
  size_t buf_size = EncodeKeyValueSize(internal_key, value);
  char* buf;
  KeyHandle handle = Allocate(buf_size, &buf);
  EncodeKeyValue(internal_key, value, buf);
  Insert(handle);
  return true;
}

bool MemTableRep::InsertKeyValueWithHint(const Slice& internal_key,
                                         const SliceParts& value, void** hint) {
  size_t buf_size = EncodeKeyValueSize(internal_key, value);
  char* buf;
  KeyHandle handle = Allocate(buf_size, &buf);
  EncodeKeyValue(internal_key, value, buf);
  InsertWithHint(handle, hint);
  return true;
}

bool MemTableRep::InsertKeyValueConcurrently(const Slice& internal_key,
                                             const SliceParts& value) {
  size_t buf_size = EncodeKeyValueSize(internal_key, value);
  char* buf;
  KeyHandle handle = Allocate(buf_size, &buf);
  EncodeKeyValue(internal_key, value, buf);
  InsertConcurrently(handle);
  return true;
}

KeyHandle MemTableRep::Allocate(const size_t len, char** buf) {
  *buf = allocator_->Allocate(len);
  return static_cast<KeyHandle>(*buf);
}

void MemTableRep::Get(const LookupKey& k, void* callback_args,
                      bool (*callback_func)(void* arg, const Slice& key,
                                            const char* value)) {
  auto iter = GetDynamicPrefixIterator();
  for (iter->Seek(k.internal_key(), k.memtable_key().data());
       iter->Valid() &&
       callback_func(callback_args, iter->key(), iter->value());
       iter->Next()) {
  }
  delete iter;
}

static std::unordered_map<std::string, MemTableRegister::FactoryCreator>&
GetMemtableFactoryMap() {
  static std::unordered_map<std::string, MemTableRegister::FactoryCreator>
      memtable_factory_map;
  return memtable_factory_map;
}

MemTableRegister::MemTableRegister(const char* name, FactoryCreator fc) {
  auto ib = GetMemtableFactoryMap().emplace(name, fc);
  assert(ib.second);
  if (!ib.second) {
    fprintf(
        stderr,
        "ERROR: duplicate MemTable name: %s, DLL may be loaded multi times\n",
        name);
    abort();
  }
}

MemTableRepFactory* CreateMemTableRepFactory(
    const std::string& name,
    const std::unordered_map<std::string, std::string>& options, Status* s) {
  auto& memtable_factory_map = GetMemtableFactoryMap();
  auto f = memtable_factory_map.find(name);
  if (memtable_factory_map.end() != f) {
    return f->second(options, s);
  }
  *s = Status::NotFound("CreateMemTableRepFactory", name);
  return NULL;
}

}  // namespace rocksdb
