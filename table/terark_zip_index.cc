
#ifndef INDEX_UT
#include "db/builder.h" // for cf_options.h
#endif

#include "terark_zip_index.h"
#include <typeindex>
#include "terark_zip_table.h"
#include "terark_zip_common.h"
#include <terark/bitmap.hpp>
#include <terark/entropy/huffman_encoding.hpp>
#include <terark/hash_strmap.hpp>
#include <terark/fsa/dfa_mmap_header.hpp>
#include <terark/fsa/fsa_cache.hpp>
#include <terark/fsa/nest_louds_trie_inline.hpp>
#include <terark/fsa/nest_trie_dawg.hpp>
#include <terark/util/crc.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/zbs/blob_store_file_header.hpp>
#include <terark/zbs/zip_reorder_map.hpp>
#include <terark/zbs/zip_offset_blob_store.hpp>
#include <terark/zbs/entropy_zip_blob_store.hpp>
#include <terark/zbs/dict_zip_blob_store.hpp>
#include <terark/zbs/xxhash_helper.hpp>
#include <terark/num_to_str.hpp>
#include <terark/io/MemStream.hpp>

namespace rocksdb {

using namespace terark;

static const uint64_t g_terark_index_prefix_seed = 0x505f6b7261726554ull; // echo Terark_P | od -t x8
static const uint64_t g_terark_index_suffix_seed = 0x535f6b7261726554ull; // echo Terark_S | od -t x8

using terark::getEnvBool;
static bool g_indexEnableCompositeIndex = getEnvBool("TerarkZipTable_enableCompositeIndex", true);
static bool g_indexEnableUintIndex = getEnvBool("TerarkZipTable_enableUintIndex", true);
static bool g_indexEnableFewZero = getEnvBool("TerarkZipTable_enableFewZero", true);
static bool g_indexEnableNonDescUint = getEnvBool("TerarkZipTable_enableNonDescUint", true);
static bool g_indexEnableDynamicSuffix = getEnvBool("TerarkZipTable_enableDynamicSuffix", true);
static bool g_indexEnableEntropySuffix = getEnvBool("TerarkZipTable_enableEntropySuffix", true);
static bool g_indexEnableDictZipSuffix = getEnvBool("TerarkZipTable_enableDictZipSuffix", true);

template<class RankSelect> struct RankSelectNeedHint : public std::false_type {};
template<size_t P, size_t W> struct RankSelectNeedHint<rank_select_few<P, W>> : public std::true_type {};

static hash_strmap<TerarkIndex::FactoryPtr> g_TerarkIndexFactroy;
struct TerarkIndexTypeFactroyHash {
  size_t operator()(const std::pair<std::type_index, std::type_index>& key) const {
    return terark::FaboHashCombine(
            std::hash<std::type_index>()(key.first),
            std::hash<std::type_index>()(key.second));
  }
};
static std::unordered_map<
    std::pair<std::type_index, std::type_index>,
    TerarkIndex::FactoryPtr,
    TerarkIndexTypeFactroyHash
> g_TerarkIndexTypeFactroy;

template<size_t Align, class Writer>
void Padzero(const Writer& write, size_t offset) {
  static const char zeros[Align] = {0};
  if (offset % Align) {
    write(zeros, Align - offset % Align);
  }
}

struct TerarkIndexFooter {
  uint32_t common_size;
  uint32_t common_crc32;
  uint64_t prefix_size;
  uint64_t prefix_xxhash;
  uint64_t suffix_size;
  uint64_t suffix_xxhash;

  char class_name[32];
  uint16_t bfs_suffix : 1;
  uint16_t rev_suffix : 1;
  uint16_t format_version : 14;
  uint16_t footer_size;
  uint32_t footer_crc32;
};

struct IndexUintPrefixHeader {
  uint8_t key_length;
  uint8_t padding_1;
  uint16_t format_version;
  uint32_t padding_4;
  uint64_t min_value;
  uint64_t max_value;
  uint64_t rank_select_size;
};

TerarkIndex::~TerarkIndex() {}
TerarkIndex::Factory::~Factory() {}
TerarkIndex::Iterator::~Iterator() {}

using UintPrefixBuildInfo = TerarkIndex::UintPrefixBuildInfo;

namespace index_detail {

struct StatusFlags {
  StatusFlags() : is_user_mem(0), is_bfs_suffix(0), is_rev_suffix(0) {}

  bool is_user_mem;
  bool is_bfs_suffix;
  bool is_rev_suffix;
};

struct Common {
  Common() { flags.is_user_mem = false; }
  Common(Common&& o) : common(o.common) {
    flags.is_user_mem = o.flags.is_user_mem;
    o.flags.is_user_mem = true;
  }
  Common(fstring c, bool copy) {
    reset(c, copy);
  }
  void reset(fstring c, bool copy) {
    if (!flags.is_user_mem) {
      free((void*)common.p);
    }
    if (copy && !c.empty()) {
      flags.is_user_mem = false;
      auto p = (char*)malloc(c.size());
      if (p == nullptr) {
        throw std::bad_alloc();
      }
      memcpy(p, c.p, c.size());
      common.p = p;
      common.n = c.size();
    } else {
      flags.is_user_mem = true;
      common = c;
    }
  }
  ~Common() {
    if (!flags.is_user_mem && common.n > 0) {
      free((void*)common.p);
    }
  }

  Common& operator = (const Common&) = delete;

  operator fstring() const {
    return common;
  }
  size_t size() const {
    return common.size();
  }
  const char* data() const {
    return common.data();
  }
  byte_t operator[](ptrdiff_t i) const {
    return common[i];
  }

  fstring common;
  StatusFlags flags;
};

struct LowerBoundResult {
  size_t id;
  fstring key;
  ContextBuffer buffer;
};

struct PrefixBase {
  StatusFlags flags;
  virtual ~PrefixBase() = default;

  virtual bool Load(fstring mem) = 0;
  virtual void Save(std::function<void(const void*, size_t)> append) const = 0;
};

struct SuffixBase {
  StatusFlags flags;

  virtual ~SuffixBase() = default;

  virtual LowerBoundResult
  LowerBound(fstring target, size_t suffix_id, size_t suffix_count, TerarkContext* ctx) const = 0;
  virtual void AppendKey(size_t suffix_id, valvec<byte_t>* buffer, TerarkContext* ctx) const = 0;

  virtual bool Load(fstring mem) = 0;
  virtual void Save(std::function<void(const void*, size_t)> append) const = 0;
  virtual void
  Reorder(ZReorderMap& newToOld, std::function<void(const void*, size_t)> append, fstring tmpFile) const = 0;
};

template<class T>
struct ComponentIteratorStorageImpl {
  size_t IteratorStorageSize() const { return sizeof(T); }
  void IteratorStorageConstruct(TerarkContext* ctx, void* ptr) const { ::new(ptr) T(); }
  void IteratorStorageDestruct(TerarkContext* ctx, void* ptr) const { static_cast<T*>(ptr)->~T(); }
};

template<>
struct ComponentIteratorStorageImpl<void> {
  size_t IteratorStorageSize() const { return 0; }
  void IteratorStorageConstruct(TerarkContext* ctx, void* ptr) const {}
  void IteratorStorageDestruct(TerarkContext* ctx, void* ptr) const {}
};

struct VirtualPrefixBase {
  virtual ~VirtualPrefixBase() {}

  virtual size_t IteratorStorageSize() const = 0;
  virtual void IteratorStorageConstruct(TerarkContext* ctx, void* ptr) const = 0;
  virtual void IteratorStorageDestruct(TerarkContext* ctx, void* ptr) const = 0;

  virtual size_t KeyCount() const = 0;
  virtual size_t TotalKeySize() const = 0;
  virtual size_t Find(fstring key, const SuffixBase* suffix, TerarkContext* ctx) const = 0;
  virtual size_t DictRank(fstring key, const SuffixBase* suffix, TerarkContext* ctx) const = 0;
  virtual size_t AppendMinKey(valvec<byte_t>* buffer, TerarkContext* ctx) const = 0;
  virtual size_t AppendMaxKey(valvec<byte_t>* buffer, TerarkContext* ctx) const = 0;

  virtual bool NeedsReorder() const = 0;
  virtual void GetOrderMap(UintVecMin0& newToOld) const = 0;
  virtual void BuildCache(double cacheRatio) = 0;

  virtual bool IterSeekToFirst(size_t& id, size_t& count, void* iter) const = 0;
  virtual bool IterSeekToLast(size_t& id, size_t* count, void* iter) const = 0;
  virtual bool IterSeek(size_t& id, size_t& count, fstring target, void* iter) const = 0;
  virtual bool IterNext(size_t& id, size_t count, void* iter) const = 0;
  virtual bool IterPrev(size_t& id, size_t* count, void* iter) const = 0;
  virtual fstring IterGetKey(size_t id, const void* iter) const = 0;
  virtual size_t IterDictRank(size_t id, const void* iter) const = 0;

  virtual bool Load(fstring mem) = 0;
  virtual void Save(std::function<void(const void*, size_t)> append) const = 0;
};
template<class Prefix>
struct VirtualPrefixWrapper : public VirtualPrefixBase, public Prefix {
  using IteratorStorage = typename Prefix::IteratorStorage;

  VirtualPrefixWrapper(Prefix&& prefix) : Prefix(std::move(prefix)) {}

  size_t IteratorStorageSize() const final {
    return Prefix::IteratorStorageSize();
  }
  void IteratorStorageConstruct(TerarkContext* ctx, void* ptr) const final {
    Prefix::IteratorStorageConstruct(ctx, ptr);
  }
  void IteratorStorageDestruct(TerarkContext* ctx, void* ptr) const final {
    Prefix::IteratorStorageDestruct(ctx, ptr);
  }

  size_t KeyCount() const final {
    return Prefix::KeyCount();
  }
  size_t TotalKeySize() const final {
    return Prefix::TotalKeySize();
  }
  size_t Find(fstring key, const SuffixBase* suffix, TerarkContext* ctx) const final {
    return Prefix::Find(key, suffix, ctx);
  }
  size_t DictRank(fstring key, const SuffixBase* suffix, TerarkContext* ctx) const final {
    return Prefix::DictRank(key, suffix, ctx);
  }
  size_t AppendMinKey(valvec<byte_t>* buffer, TerarkContext* ctx) const final {
    return Prefix::AppendMinKey(buffer, ctx);
  }
  size_t AppendMaxKey(valvec<byte_t>* buffer, TerarkContext* ctx) const final {
    return Prefix::AppendMaxKey(buffer, ctx);
  }

  bool NeedsReorder() const final {
    return Prefix::NeedsReorder();
  }
  void GetOrderMap(UintVecMin0& newToOld) const final {
    Prefix::GetOrderMap(newToOld);
  }
  void BuildCache(double cacheRatio) final {
    Prefix::BuildCache(cacheRatio);
  }

  bool IterSeekToFirst(size_t& id, size_t& count, void* iter) const final {
    return Prefix::IterSeekToFirst(id, count, (IteratorStorage*)iter);
  }
  bool IterSeekToLast(size_t& id, size_t* count, void* iter) const final {
    return Prefix::IterSeekToLast(id, count, (IteratorStorage*)iter);
  }
  bool IterSeek(size_t& id, size_t& count, fstring target, void* iter) const final {
    return Prefix::IterSeek(id, count, target, (IteratorStorage*)iter);
  }
  bool IterNext(size_t& id, size_t count, void* iter) const final {
    return Prefix::IterNext(id, count, (IteratorStorage*)iter);
  }
  bool IterPrev(size_t& id, size_t* count, void* iter) const final {
    return Prefix::IterPrev(id, count, (IteratorStorage*)iter);
  }
  fstring IterGetKey(size_t id, const void* iter) const final {
    return Prefix::IterGetKey(id, (const IteratorStorage*)iter);
  }
  size_t IterDictRank(size_t id, const void* iter) const final {
    return Prefix::IterDictRank(id, (const IteratorStorage*)iter);
  }

  bool Load(fstring mem) final {
    return Prefix::Load(mem);
  }
  void Save(std::function<void(const void*, size_t)> append) const final {
    Prefix::Save(append);
  }
};
struct VirtualPrefix : public PrefixBase {
  typedef void* IteratorStorage;

  template<class Prefix>
  VirtualPrefix(Prefix&& p) {
    flags = p.flags;
    prefix = new VirtualPrefixWrapper<Prefix>(std::move(p));
  }

  VirtualPrefix(VirtualPrefix&& o) {
    prefix = o.prefix;
    flags = o.flags;
    o.prefix = nullptr;
  }

  ~VirtualPrefix() {
    if (prefix != nullptr) {
      delete prefix;
    }
  }

  VirtualPrefixBase* prefix;

  size_t IteratorStorageSize() const {
    return prefix->IteratorStorageSize();
  }
  void IteratorStorageConstruct(TerarkContext* ctx, void* ptr) const {
    prefix->IteratorStorageConstruct(ctx, ptr);
  }
  void IteratorStorageDestruct(TerarkContext* ctx, void* ptr) const {
    prefix->IteratorStorageDestruct(ctx, ptr);
  }

  size_t KeyCount() const {
    return prefix->KeyCount();
  }
  size_t TotalKeySize() const {
    return prefix->TotalKeySize();
  }
  size_t Find(fstring key, const SuffixBase* suffix, TerarkContext* ctx) const {
    return prefix->Find(key, suffix, ctx);
  }
  size_t DictRank(fstring key, const SuffixBase* suffix, TerarkContext* ctx) const {
    return prefix->DictRank(key, suffix, ctx);
  }
  size_t AppendMinKey(valvec<byte_t>* buffer, TerarkContext* ctx) const {
    return prefix->AppendMinKey(buffer, ctx);
  }
  size_t AppendMaxKey(valvec<byte_t>* buffer, TerarkContext* ctx) const {
    return prefix->AppendMaxKey(buffer, ctx);
  }

  bool NeedsReorder() const {
    return prefix->NeedsReorder();
  }
  void GetOrderMap(UintVecMin0& newToOld) const {
    prefix->GetOrderMap(newToOld);
  }
  void BuildCache(double cacheRatio) {
    prefix->BuildCache(cacheRatio);
  }

  bool IterSeekToFirst(size_t& id, size_t& count, void* iter) const {
    return prefix->IterSeekToFirst(id, count, iter);
  }
  bool IterSeekToLast(size_t& id, size_t* count, void* iter) const {
    return prefix->IterSeekToLast(id, count, iter);
  }
  bool IterSeek(size_t& id, size_t& count, fstring target, void* iter) const {
    return prefix->IterSeek(id, count, target, iter);
  }
  bool IterNext(size_t& id, size_t count, void* iter) const {
    return prefix->IterNext(id, count, iter);
  }
  bool IterPrev(size_t& id, size_t* count, void* iter) const {
    return prefix->IterPrev(id, count, iter);
  }
  fstring IterGetKey(size_t id, const void* iter) const {
    return prefix->IterGetKey(id, iter);
  }
  size_t IterDictRank(size_t id, const void* iter) const {
    return prefix->IterDictRank(id, iter);
  }

  bool Load(fstring mem) final {
    return prefix->Load(mem);
  }
  void Save(std::function<void(const void*, size_t)> append) const final {
    prefix->Save(append);
  }
};

struct VirtualSuffixBase {
  virtual ~VirtualSuffixBase() {}

  virtual size_t IteratorStorageSize() const = 0;
  virtual void IteratorStorageConstruct(TerarkContext* ctx, void* ptr) const = 0;
  virtual void IteratorStorageDestruct(TerarkContext* ctx, void* ptr) const = 0;

  virtual size_t TotalKeySize() const = 0;
  virtual LowerBoundResult
  LowerBound(fstring target, size_t suffix_id, size_t suffix_count, TerarkContext* ctx) const = 0;
  virtual void AppendKey(size_t suffix_id, valvec<byte_t>* buffer, TerarkContext* ctx) const = 0;
  virtual void GetMetaData(valvec<fstring>* blocks) const = 0;
  virtual void DetachMetaData(const valvec<fstring>& blocks) = 0;

  virtual void IterSet(size_t suffix_id, void* iter) const = 0;
  virtual bool IterSeek(fstring target, size_t& suffix_id, size_t suffix_count, void* iter) const = 0;
  virtual fstring IterGetKey(size_t id, const void* iter) const = 0;

  virtual bool Load(fstring mem) = 0;
  virtual void Save(std::function<void(const void*, size_t)> append) const = 0;
  virtual void
  Reorder(ZReorderMap& newToOld, std::function<void(const void*, size_t)> append, fstring tmpFile) const = 0;
};
template<class Suffix>
struct VirtualSuffixWrapper : public VirtualSuffixBase, public Suffix {
  using IteratorStorage = typename Suffix::IteratorStorage;

  VirtualSuffixWrapper(Suffix&& suffix) : Suffix(std::move(suffix)) {}

  size_t IteratorStorageSize() const final {
    return Suffix::IteratorStorageSize();
  }
  void IteratorStorageConstruct(TerarkContext* ctx, void* ptr) const final {
    Suffix::IteratorStorageConstruct(ctx, ptr);
  }
  void IteratorStorageDestruct(TerarkContext* ctx, void* ptr) const final {
    Suffix::IteratorStorageDestruct(ctx, ptr);
  }

  size_t TotalKeySize() const final {
    return Suffix::TotalKeySize();
  }
  virtual LowerBoundResult
  LowerBound(fstring target, size_t suffix_id, size_t suffix_count, TerarkContext* ctx) const final {
    return Suffix::LowerBound(target, suffix_id, suffix_count, ctx);
  }
  void AppendKey(size_t suffix_id, valvec<byte_t>* buffer, TerarkContext* ctx) const final {
    return Suffix::AppendKey(suffix_id, buffer, ctx);
  }
  void GetMetaData(valvec<fstring>* blocks) const final {
    return Suffix::GetMetaData(blocks);
  }
  void DetachMetaData(const valvec<fstring>& blocks) final {
    Suffix::DetachMetaData(blocks);
  }

  void IterSet(size_t suffix_id, void* iter) const final {
    Suffix::IterSet(suffix_id, (IteratorStorage*)iter);
  }
  bool IterSeek(fstring target, size_t& suffix_id, size_t suffix_count, void* iter) const final {
    return Suffix::IterSeek(target, suffix_id, suffix_count, (IteratorStorage*)iter);
  }
  fstring IterGetKey(size_t id, const void* iter) const final {
    return Suffix::IterGetKey(id, (const IteratorStorage*)iter);
  }

  bool Load(fstring mem) final {
    return Suffix::Load(mem);
  }
  void Save(std::function<void(const void*, size_t)> append) const final {
    Suffix::Save(append);
  }
  void Reorder(ZReorderMap& newToOld, std::function<void(const void*, size_t)> append,
               fstring tmpFile) const final {
    Suffix::Reorder(newToOld, append, tmpFile);
  }
};
struct VirtualSuffix : public SuffixBase {
  typedef void* IteratorStorage;

  template<class Suffix>
  VirtualSuffix(Suffix&& s) {
    flags = s.flags;
    suffix = new VirtualSuffixWrapper<Suffix>(std::move(s));
  }

  VirtualSuffix(VirtualSuffix&& o) {
    suffix = o.suffix;
    flags = o.flags;
    o.suffix = nullptr;
  }

  ~VirtualSuffix() {
    if (suffix != nullptr) {
      delete suffix;
    }
  }

  VirtualSuffixBase* suffix;

  size_t IteratorStorageSize() const {
    return suffix->IteratorStorageSize();
  }
  void IteratorStorageConstruct(TerarkContext* ctx, void* ptr) const {
    suffix->IteratorStorageConstruct(ctx, ptr);
  }
  void IteratorStorageDestruct(TerarkContext* ctx, void* ptr) const {
    suffix->IteratorStorageDestruct(ctx, ptr);
  }

  size_t TotalKeySize() const {
    return suffix->TotalKeySize();
  }
  LowerBoundResult LowerBound(fstring target, size_t suffix_id, size_t suffix_count, TerarkContext* ctx) const final {
    return suffix->LowerBound(target, suffix_id, suffix_count, ctx);
  }
  void AppendKey(size_t suffix_id, valvec<byte_t>* buffer, TerarkContext* ctx) const final {
    return suffix->AppendKey(suffix_id, buffer, ctx);
  }
  void GetMetaData(valvec<fstring>* blocks) const {
    return suffix->GetMetaData(blocks);
  }
  void DetachMetaData(const valvec<fstring>& blocks) {
    suffix->DetachMetaData(blocks);
  }

  void IterSet(size_t suffix_id, void* iter) const {
    suffix->IterSet(suffix_id, iter);
  }
  bool IterSeek(fstring target, size_t& suffix_id, size_t suffix_count, void* iter) const {
    return suffix->IterSeek(target, suffix_id, suffix_count, iter);
  }
  fstring IterGetKey(size_t id, const void* iter) const {
    return suffix->IterGetKey(id, iter);
  }

  bool Load(fstring mem) final {
    return suffix->Load(mem);
  }
  void Save(std::function<void(const void*, size_t)> append) const final {
    suffix->Save(append);
  }
  void Reorder(ZReorderMap& newToOld, std::function<void(const void*, size_t)> append,
               fstring tmpFile) const final {
    suffix->Reorder(newToOld, append, tmpFile);
  }
};

template<class Prefix, class Suffix>
struct IndexParts {
  IndexParts() {}
  IndexParts(Common&& common, Prefix&& prefix, Suffix&& suffix)
      : common_(std::move(common)), prefix_(std::move(prefix)), suffix_(std::move(suffix)) {
  }

  Common common_;
  Prefix prefix_;
  Suffix suffix_;
};

struct IteratorStorage {
  const fstring common_;
  const PrefixBase& prefix_;
  const SuffixBase& suffix_;
  void* prefix_storage_;
  void* suffix_storage_;

  template<class Prefix, class Suffix>
  static size_t GetIteratorStorageSize(const IndexParts<Prefix, Suffix>* index) {
    return 0
           + align_up(index->prefix_.IteratorStorageSize(), 8)
           + align_up(index->suffix_.IteratorStorageSize(), 8);
  }

  template<class Prefix, class Suffix>
  IteratorStorage(const IndexParts<Prefix, Suffix>* index, TerarkContext* ctx, void* storage)
      : common_(index->common_), prefix_(index->prefix_), suffix_(index->suffix_) {
    prefix_storage_ = storage;
    suffix_storage_ = (uint8_t*)storage + align_up(index->prefix_.IteratorStorageSize(), 8);
    if (index->prefix_.IteratorStorageSize() > 0) {
      index->prefix_.IteratorStorageConstruct(ctx, prefix_storage_);
    }
    if (index->suffix_.IteratorStorageSize() > 0) {
      index->suffix_.IteratorStorageConstruct(ctx, suffix_storage_);
    }
  }

  template<class Prefix, class Suffix>
  void Destroy(const IndexParts<Prefix, Suffix>* index, TerarkContext* ctx) {
    if (index->prefix_.IteratorStorageSize() > 0) {
      index->prefix_.IteratorStorageDestruct(ctx, prefix_storage_);
    }
    if (index->suffix_.IteratorStorageSize() > 0) {
      index->suffix_.IteratorStorageDestruct(ctx, suffix_storage_);
    }
  }

};

class IndexFactoryBase : public TerarkIndex::Factory {
public:
  virtual fstring Name() const = 0;

  unique_ptr<TerarkIndex> LoadMemory(fstring mem) const {
    auto& footer = ((const TerarkIndexFooter*)(mem.data() + mem.size()))[-1];
    fstring suffix = mem.substr(mem.size() - footer.footer_size - footer.suffix_size, footer.suffix_size);
    fstring prefix = fstring(suffix.data() - footer.prefix_size, footer.prefix_size);
    fstring common = fstring(prefix.data() - align_up(footer.common_size, 8), footer.common_size);
    if (isChecksumVerifyEnabled()) {
      uint64_t computed = Crc32c_update(0, common.data(), common.size());
      uint64_t saved = footer.common_crc32;
      if (saved != computed) {
        throw terark::BadCrc32cException("TerarkIndex::LoadMemory Common",
                                         (uint32_t)saved, (uint32_t)computed);
      }
      computed = XXHash64(g_terark_index_prefix_seed)(prefix);
      saved = footer.prefix_xxhash;
      if (saved != computed) {
        throw BadChecksumException("TerarkIndex::LoadMemory Prefix",
                                   saved, computed);
      }
      computed = XXHash64(g_terark_index_suffix_seed)(suffix);
      saved = footer.suffix_xxhash;
      if (saved != computed) {
        throw BadChecksumException("TerarkIndex::LoadMemory Suffix",
                                   saved, computed);
      }
    }
    unique_ptr<PrefixBase> p(CreatePrefix());
    p->flags.is_bfs_suffix = footer.bfs_suffix;
    if (!p->Load(prefix)) {
      throw std::invalid_argument("TerarkIndex::LoadMemory Prefix Fail, bad mem");
    }
    unique_ptr<SuffixBase> s(CreateSuffix());
    s->flags.is_rev_suffix = footer.rev_suffix;
    if (!s->Load(suffix)) {
      throw std::invalid_argument("TerarkIndex::LoadMemory Suffix Fail, bad mem");
    }
    return unique_ptr<TerarkIndex>(CreateIndex(&footer, Common(common, false), p.release(), s.release()));
  }

  template<class Prefix, class Suffix>
  void SaveMmap(const IndexParts<Prefix, Suffix>* index,
                std::function<void(const void*, size_t)> write) const {
    SaveMmap(index->common_, index->prefix_, index->suffix_, write);
  }

  template<class Prefix, class Suffix>
  void Reorder(const IndexParts<Prefix, Suffix>* index,
               ZReorderMap& newToOld, std::function<void(const void*, size_t)> write, fstring tmpFile) const {
    Reorder(index->common_, index->prefix_, index->suffix_, newToOld, write, tmpFile);
  }

  virtual void SaveMmap(const Common& common, const PrefixBase& prefix, const SuffixBase& suffix,
                        std::function<void(const void*, size_t)> write) const {
    TerarkIndexFooter footer;
    footer.format_version = 0;
    footer.bfs_suffix = prefix.flags.is_bfs_suffix;
    footer.rev_suffix = suffix.flags.is_rev_suffix;
    footer.footer_size = sizeof footer;
    footer.common_size = common.size();
    footer.common_crc32 = Crc32c_update(0, common.data(), common.size());
    write(common.data(), common.size());
    Padzero<8>(write, common.size());
    XXHash64 dist(g_terark_index_prefix_seed);
    footer.prefix_size = 0;
    prefix.Save([&](const void* data, size_t size) {
      dist.update(data, size);
      write(data, size);
      footer.prefix_size += size;
    });
    assert(footer.prefix_size % 8 == 0);
    footer.prefix_xxhash = dist.digest();
    dist.reset(g_terark_index_suffix_seed);
    footer.suffix_size = 0;
    suffix.Save([&](const void* data, size_t size) {
      dist.update(data, size);
      write(data, size);
      footer.suffix_size += size;
    });
    assert(footer.suffix_size % 8 == 0);
    footer.suffix_xxhash = dist.digest();
    auto name = Name();
    assert(name.size() == sizeof footer.class_name);
    memcpy(footer.class_name, name.data(), sizeof footer.class_name);
    footer.footer_crc32 = Crc32c_update(0, &footer, sizeof footer - sizeof(uint32_t));
    write(&footer, sizeof footer);
  }

  virtual void Reorder(const Common& common, const PrefixBase& prefix, const SuffixBase& suffix, ZReorderMap& newToOld,
                       std::function<void(const void*, size_t)> write, fstring tmpFile) const {
    TerarkIndexFooter footer;
    footer.format_version = 0;
    footer.bfs_suffix = true;
    footer.rev_suffix = false;
    footer.footer_size = sizeof footer;
    footer.common_size = common.size();
    footer.common_crc32 = Crc32c_update(0, common.data(), common.size());
    write(common.data(), common.size());
    Padzero<8>(write, common.size());
    XXHash64 dist(g_terark_index_prefix_seed);
    footer.prefix_size = 0;
    prefix.Save([&](const void* data, size_t size) {
      dist.update(data, size);
      write(data, size);
      footer.prefix_size += size;
    });
    footer.prefix_xxhash = dist.digest();
    dist.reset(g_terark_index_suffix_seed);
    footer.suffix_size = 0;
    suffix.Reorder(newToOld, [&](const void* data, size_t size) {
      dist.update(data, size);
      write(data, size);
      footer.suffix_size += size;
    }, tmpFile);
    footer.suffix_xxhash = dist.digest();
    auto name = Name();
    assert(name.size() == sizeof footer.class_name);
    memcpy(footer.class_name, name.data(), sizeof footer.class_name);
    footer.footer_crc32 = Crc32c_update(0, &footer, sizeof footer - sizeof(uint32_t));
    write(&footer, sizeof footer);
  }

  static IndexFactoryBase* GetFactoryByType(std::type_index prefix, std::type_index suffix) {
    auto find = g_TerarkIndexTypeFactroy.find(std::make_pair(prefix, suffix));
    if (find == g_TerarkIndexTypeFactroy.end()) {
      return nullptr;
    }
    return static_cast<IndexFactoryBase*>(find->second.get());
  }

  virtual ~IndexFactoryBase() {}

  virtual TerarkIndex*
  CreateIndex(const TerarkIndexFooter* footer, Common&& common, PrefixBase* prefix, SuffixBase* suffix) const {
    TERARK_RT_assert(0, std::logic_error);
    return nullptr;
  }
  virtual PrefixBase* CreatePrefix() const {
    TERARK_RT_assert(0, std::logic_error);
    return nullptr;
  }
  virtual SuffixBase* CreateSuffix() const {
    TERARK_RT_assert(0, std::logic_error);
    return nullptr;
  }
};

template<class Prefix, class Suffix>
class IndexIterator
    : public TerarkIndex::Iterator, public IteratorStorage {
public:
  using IteratorStorage = IteratorStorage;

  using TerarkIndex::Iterator::m_id;
  using IteratorStorage::common_;
  using IteratorStorage::prefix_;
  using IteratorStorage::suffix_;
  using IteratorStorage::prefix_storage_;
  using IteratorStorage::suffix_storage_;
  const IndexParts<Prefix, Suffix>* index_;
  TerarkContext* ctx_;
  valvec<byte_t> storage_;
  valvec<byte_t> key_;

  fstring common() const {
    return common_;
  }
  const Prefix& prefix() const {
    return static_cast<const Prefix&>(prefix_);
  }
  const Suffix& suffix() const {
    return static_cast<const Suffix&>(suffix_);
  }
  typename Prefix::IteratorStorage* prefix_storage() {
    return (typename Prefix::IteratorStorage*)prefix_storage_;
  }
  const typename Prefix::IteratorStorage* prefix_storage() const {
    return (typename Prefix::IteratorStorage*)prefix_storage_;
  }

  typename Suffix::IteratorStorage* suffix_storage() {
    return (typename Suffix::IteratorStorage*)suffix_storage_;
  }
  const typename Suffix::IteratorStorage* suffix_storage() const {
    return (typename Suffix::IteratorStorage*)suffix_storage_;
  }

private:
  ContextBuffer AllocIteratorStorage_(const IndexParts<Prefix, Suffix>* index, TerarkContext* ctx, void* storage) {
    size_t storage_size = IteratorStorage::GetIteratorStorageSize(index);
    ContextBuffer buffer;
    if (storage != nullptr) {
      buffer.get().risk_set_data(reinterpret_cast<byte_t*>(storage));
      buffer.get().risk_set_size(storage_size); // mark as user memory
      buffer.get().risk_set_capacity(storage_size);
      return buffer;
    } else if (ctx == nullptr || storage_size == 0) {
      return ContextBuffer(valvec<byte_t>(storage_size, valvec_reserve()), nullptr);
    } else {
      return ctx->alloc(storage_size);
    }
  }

  IndexIterator(const IndexParts<Prefix, Suffix>* index, TerarkContext* ctx, valvec<byte_t>& buffer)
      : IteratorStorage(index, ctx, buffer.data()), index_(index), ctx_(ctx),
        storage_(std::move(buffer)) {
    if (ctx_ != nullptr) {
      key_.swap(ctx_->alloc());
    }
  }

  bool UpdateKey() {
    key_.assign(common_);
    key_.append(prefix().IterGetKey(m_id, prefix_storage()));
    key_.append(suffix().IterGetKey(m_id, suffix_storage()));
    return true;
  }

public:
  IndexIterator(const IndexParts<Prefix, Suffix>* index, TerarkContext* ctx, void* storage)
      : IndexIterator(index, ctx, AllocIteratorStorage_(index, ctx, storage)) {}


  ~IndexIterator() {
    IteratorStorage::Destroy(index_, ctx_);
    if (storage_.size() != 0) {
      storage_.risk_release_ownership();
    } else if (ctx_ != nullptr) {
      ContextBuffer(std::move(storage_), ctx_);
    }
    if (ctx_ != nullptr) {
      ContextBuffer(std::move(key_), ctx_);
    }
  }

  bool SeekToFirst() final {
    size_t suffix_count;
    if (!prefix().IterSeekToFirst(m_id, suffix_count, prefix_storage())) {
      assert(m_id == size_t(-1));
      return false;
    }
    suffix().IterSet(m_id, suffix_storage());
    return UpdateKey();
  }

  bool SeekToLast() final {
    if (!prefix().IterSeekToLast(m_id, nullptr, prefix_storage())) {
      assert(m_id == size_t(-1));
      return false;
    }
    suffix().IterSet(m_id, suffix_storage());
    return UpdateKey();
  }

  bool Seek(fstring target) final {
    size_t cplen = target.commonPrefixLen(common());
    if (cplen != common().size()) {
      assert(target.size() >= cplen);
      assert(target.size() == cplen || byte_t(target[cplen]) != byte_t(common()[cplen]));
      if (target.size() == cplen || byte_t(target[cplen]) < byte_t(common()[cplen])) {
        return SeekToFirst();
      } else {
        m_id = size_t(-1);
        return false;
      }
    }
    target = target.substr(cplen);
    size_t suffix_count;
    if (suffix().TotalKeySize() == 0) {
      if (prefix().IterSeek(m_id, suffix_count, target, prefix_storage())) {
        assert(suffix_count == 1);
        suffix().IterSet(m_id, suffix_storage());
        return UpdateKey();
      } else {
        m_id = size_t(-1);
        return false;
      }
    }
    fstring prefix_key;
    if (prefix().IterSeek(m_id, suffix_count, target, prefix_storage())) {
      prefix_key = prefix().IterGetKey(m_id, prefix_storage());
      assert(prefix_key >= target);
      if (prefix_key != target) {
        if (prefix().IterPrev(m_id, &suffix_count, prefix_storage())) {
          prefix_key = prefix().IterGetKey(m_id, prefix_storage());
        } else {
          return SeekToFirst();
        }
      }
    } else {
      if (!prefix().IterSeekToLast(m_id, &suffix_count, prefix_storage())) {
        assert(m_id == size_t(-1));
        return false;
      }
      prefix_key = prefix().IterGetKey(m_id, prefix_storage());
    }
    if (target.startsWith(prefix_key)) {
      target = target.substr(prefix_key.size());
      size_t suffix_id = m_id;
      if (suffix().IterSeek(target, suffix_id, suffix_count, suffix_storage())) {
        assert(suffix_id >= m_id);
        assert(suffix_id < m_id + suffix_count);
        if (suffix_id > m_id && !prefix().IterNext(m_id, suffix_id - m_id, prefix_storage())) {
          assert(m_id == size_t(-1));
          return false;
        }
        return UpdateKey();
      }
    }
    if (!prefix().IterNext(m_id, suffix_count, prefix_storage())) {
      assert(m_id == size_t(-1));
      return false;
    }
    suffix().IterSet(m_id, suffix_storage());
    return UpdateKey();
  }

  bool Next() final {
    if (prefix().IterNext(m_id, 1, prefix_storage())) {
      suffix().IterSet(m_id, suffix_storage());
      return UpdateKey();
    } else {
      m_id = size_t(-1);
      return false;
    }
  }

  bool Prev() final {
    if (prefix().IterPrev(m_id, nullptr, prefix_storage())) {
      suffix().IterSet(m_id, suffix_storage());
      return UpdateKey();
    } else {
      m_id = size_t(-1);
      return false;
    }
  }

  size_t DictRank() const final {
    return prefix().IterDictRank(m_id, prefix_storage());
  }

  fstring key() const final {
    return key_;
  }

  fstring pkey() const {
    return prefix().IterGetKey(m_id, prefix_storage());
  }

  fstring skey() const {
    return suffix().IterGetKey(m_id, suffix_storage());
  }
};

////////////////////////////////////////////////////////////////////////////////
//  Prefix :
//    VirtualImpl :
//      NestLoudsTriePrefix<>
//        NestLoudsTrieDAWG_IL_256
//        NestLoudsTrieDAWG_IL_256_32_FL
//        NestLoudsTrieDAWG_Mixed_SE_512
//        NestLoudsTrieDAWG_Mixed_SE_512_32_FL
//        NestLoudsTrieDAWG_Mixed_IL_256
//        NestLoudsTrieDAWG_Mixed_IL_256_32_FL
//        NestLoudsTrieDAWG_Mixed_XL_256
//        NestLoudsTrieDAWG_Mixed_XL_256_32_FL
//        NestLoudsTrieDAWG_SE_512_64
//        NestLoudsTrieDAWG_SE_512_64_FL
//      AscendingUintPrefix<>
//        rank_select_fewzero<2>
//        rank_select_fewzero<3>
//        rank_select_fewzero<4>
//        rank_select_fewzero<5>
//        rank_select_fewzero<6>
//        rank_select_fewzero<7>
//        rank_select_fewzero<8>
//        rank_select_fewone<2>
//        rank_select_fewone<3>
//        rank_select_fewone<4>
//        rank_select_fewone<5>
//        rank_select_fewone<6>
//        rank_select_fewone<7>
//        rank_select_fewone<8>
//      NonDescendingUintPrefix<>
//        rank_select_fewone<2>
//        rank_select_fewone<3>
//        rank_select_fewone<4>
//        rank_select_fewone<5>
//        rank_select_fewone<6>
//        rank_select_fewone<7>
//        rank_select_fewone<8>
//    AscendingUintPrefix<>
//      rank_select_allone
//      rank_select_il_256_32
//      rank_select_se_512_64
//    NonDescendingUintPrefix<>
//      rank_select_il_256_32
//      rank_select_se_512_64
//  Suffix :
//    VirtualImpl :
//      BlobStoreSuffix<>
//        ZipOffsetBlobStore
//        DictZipBlobStore
//    EmptySuffix
//    FixedStringSuffix
////////////////////////////////////////////////////////////////////////////////

template<class Prefix, class Suffix>
class Index : public TerarkIndex, public IndexParts<Prefix, Suffix> {
public:
  typedef IndexParts<Prefix, Suffix> base_t;
  using base_t::common_;
  using base_t::prefix_;
  using base_t::suffix_;

  Index(const IndexFactoryBase* factory, const TerarkIndexFooter* footer) : factory_(factory), footer_(footer) {}

  Index(const IndexFactoryBase* factory, const TerarkIndexFooter* footer, Common&& common, Prefix&& prefix,
        Suffix&& suffix)
      : base_t(std::move(common), std::move(prefix), std::move(suffix)), factory_(factory), footer_(footer) {
  }

  const IndexFactoryBase* factory_;
  const TerarkIndexFooter* footer_;

  fstring Name() const final {
    return factory_->Name();
  }

  void SaveMmap(std::function<void(const void*, size_t)> write) const final {
    factory_->SaveMmap(this, write);
  }

  void Reorder(ZReorderMap& newToOld, std::function<void(const void*, size_t)> write,
               fstring tmpFile) const final {
    factory_->Reorder(this, newToOld, write, tmpFile);
  }

  size_t Find(fstring key, TerarkContext* ctx) const final {
    if (!key.startsWith(common_)) {
      return size_t(-1);
    }
    key = key.substr(common_.size());
    return prefix_.Find(key, suffix_.TotalKeySize() != 0 ? &suffix_ : nullptr, ctx);
  }

  size_t DictRank(fstring key, TerarkContext* ctx) const final {
    size_t cplen = key.commonPrefixLen(common_);
    if (cplen != common_.size()) {
      assert(key.size() >= cplen);
      assert(key.size() == cplen || byte_t(key[cplen]) != byte_t(common_[cplen]));
      if (key.size() == cplen || byte_t(key[cplen]) < byte_t(common_[cplen])) {
        return 0;
      } else {
        return NumKeys();
      }
    }
    key = key.substr(common_.size());
    return prefix_.DictRank(key, suffix_.TotalKeySize() != 0 ? &suffix_ : nullptr, ctx);
  }

  void MinKey(valvec<byte_t>* key, TerarkContext* ctx) const final {
    key->assign(common_.data(), common_.size());
    size_t id = prefix_.AppendMinKey(key, ctx);
    suffix_.AppendKey(id, key, ctx);
  }

  void MaxKey(valvec<byte_t>* key, TerarkContext* ctx) const final {
    key->assign(common_.data(), common_.size());
    size_t id = prefix_.AppendMaxKey(key, ctx);
    suffix_.AppendKey(id, key, ctx);
  }

  size_t NumKeys() const final {
    return prefix_.KeyCount();
  }

  size_t TotalKeySize() const final {
    size_t size = NumKeys() * common_.size();
    size += prefix_.TotalKeySize();
    size += suffix_.TotalKeySize();
    return size;
  }

  fstring Memory() const final {
    auto f = footer_;
    size_t index_size = f ? align_up(f->common_size, 8) + f->prefix_size + f->suffix_size : 0;
    return index_size == 0 ? fstring() : fstring((byte_t*)footer_ - index_size, index_size + f->footer_size);
  }

  valvec<fstring> GetMetaData() const final {
    assert(footer_ != nullptr);
    auto f = footer_;
    fstring prefix = fstring((byte_t*)f - f->suffix_size - f->prefix_size, f->prefix_size);
    valvec<fstring> meta_data;
    suffix_.GetMetaData(&meta_data);
    meta_data.append(prefix);
    return meta_data;
  }

  void DetachMetaData(const valvec<fstring>& blocks) final {
    assert(footer_ != nullptr);
    bool ok = prefix_.Load(blocks.back());
    assert(ok); (void)ok;
    valvec<fstring> suffix_blocks;
    suffix_blocks.risk_set_data((fstring*)blocks.data(), blocks.size() - 1);
    suffix_.DetachMetaData(suffix_blocks);
    suffix_blocks.risk_release_ownership();
  }

  const char* Info(char* buffer, size_t size) const final {
    auto f = footer_;
    double c = prefix_.KeyCount();
    double r = 1e9;
    size_t t = prefix_.TotalKeySize() + suffix_.TotalKeySize() + prefix_.KeyCount() * common_.size();
    size_t index_size = f ? f->footer_size + align_up(f->common_size, 8) + f->prefix_size + f->suffix_size : 0;
    snprintf(
        buffer, size,
        "    total_key_len = %zd  common_size = %zd  entry_cnt = %zd\n"
        "    prefix: raw-key =%9.4f GB  zip-key =%9.4f GB  avg-key =%7.2f  avg-zkey =%7.2f\n"
        "    suffix: raw-key =%9.4f GB  zip-key =%9.4f GB  avg-key =%7.2f  avg-zkey =%7.2f\n"
        "    index : raw-key =%9.4f GB  zip-key =%9.4f GB  avg-key =%7.2f  avg-zkey =%7.2f\n"
        , prefix_.KeyCount() * common_.size() + prefix_.TotalKeySize() + suffix_.TotalKeySize()
        , common_.size(), prefix_.KeyCount()
        , prefix_.TotalKeySize() / r, (f ? f->prefix_size : 0) / r
        , prefix_.TotalKeySize() / c, (f ? f->prefix_size : 0) / c
        , suffix_.TotalKeySize() / r, (f ? f->suffix_size : 0) / r
        , suffix_.TotalKeySize() / c, (f ? f->suffix_size : 0) / c
        , t / r, index_size / r , t / c, index_size / c
    );
    return buffer;
  }

  Iterator* NewIterator(valvec<byte_t>* buffer, TerarkContext* ctx) const final {
    if (buffer == nullptr) {
      return new IndexIterator<Prefix, Suffix>(this, ctx, nullptr);
    } else {
      buffer->ensure_capacity(IteratorSize());
      auto storage = buffer->data() + sizeof(IndexIterator<Prefix, Suffix>);
      return ::new(buffer->data()) IndexIterator<Prefix, Suffix>(this, ctx, storage);
    }
  }

  size_t IteratorSize() const final {
    return sizeof(IndexIterator<Prefix, Suffix>) +
           IteratorStorage::GetIteratorStorageSize(this);
  }

  bool NeedsReorder() const final {
    return prefix_.NeedsReorder();
  }

  void GetOrderMap(terark::UintVecMin0& newToOld) const final {
    prefix_.GetOrderMap(newToOld);
  }

  void BuildCache(double cacheRatio) final {
    prefix_.BuildCache(cacheRatio);
  }

  void DumpKeys(std::function<void(fstring, fstring, fstring)> callback) const final {
    auto g_ctx = terark::GetTlsTerarkContext();
    auto buffer = g_ctx->alloc(IteratorSize());
    auto storage = buffer.data() + sizeof(IndexIterator<Prefix, Suffix>);
    auto iter = ::new(buffer.data()) IndexIterator<Prefix, Suffix>(this, g_ctx, storage);
    for (bool ok = iter->SeekToFirst(); ok; ok = iter->Next()) {
      callback(common_.common, iter->pkey(), iter->skey());
    }
    iter->~IndexIterator<Prefix, Suffix>();
  }
};

template<class Prefix, size_t PV, class Suffix, size_t SV>
struct IndexDeclare {
  typedef Index<
      typename std::conditional<PV, VirtualPrefix, Prefix>::type,
      typename std::conditional<SV, VirtualSuffix, Suffix>::type
  > index_type;
};


template<class PrefixInfo, class Prefix, class SuffixInfo, class Suffix>
class IndexFactory : public IndexFactoryBase {
public:
  typedef typename IndexDeclare<Prefix, PrefixInfo::use_virtual, Suffix, SuffixInfo::use_virtual>::index_type index_type;

  IndexFactory() {
    auto prefix_name = PrefixInfo::Name();
    auto suffix_name = SuffixInfo::Name();
    valvec<char> name(prefix_name.size() + suffix_name.size(), valvec_reserve());
    name.assign(prefix_name);
    name.append(suffix_name);
    map_id = g_TerarkIndexFactroy.insert_i(name, this).first;
    g_TerarkIndexTypeFactroy[std::make_pair(std::type_index(typeid(Prefix)), std::type_index(typeid(Suffix)))] = this;
  }

  fstring Name() const final {
    return g_TerarkIndexFactroy.key(map_id);
  }

protected:
  TerarkIndex* CreateIndex(const TerarkIndexFooter* footer, Common&& common, PrefixBase* prefix,
                           SuffixBase* suffix) const final {
    return new index_type(this, footer, std::move(common), Prefix(prefix), Suffix(suffix));
  }
  PrefixBase* CreatePrefix() const final {
    return new Prefix();
  }
  SuffixBase* CreateSuffix() const final {
    return new Suffix();
  }

  size_t map_id;
};

////////////////////////////////////////////////////////////////////////////////
// Impls
////////////////////////////////////////////////////////////////////////////////

template<class WithHint>
struct IndexUintPrefixIteratorStorage {
  byte_t buffer[8];
  size_t pos;
  mutable size_t hint = 0;

  size_t* get_hint() const {
    return &hint;
  }
};

template<>
struct IndexUintPrefixIteratorStorage<std::false_type> {
  byte_t buffer[8];
  size_t pos;

  size_t* get_hint() const {
    return nullptr;
  }
};

template<class RankSelect>
struct IndexAscendingUintPrefix
    : public PrefixBase,
      public ComponentIteratorStorageImpl<IndexUintPrefixIteratorStorage<typename RankSelectNeedHint<RankSelect>::type>> {
  using IteratorStorage = IndexUintPrefixIteratorStorage<typename RankSelectNeedHint<RankSelect>::type>;

  IndexAscendingUintPrefix() = default;
  IndexAscendingUintPrefix(const IndexAscendingUintPrefix&) = delete;
  IndexAscendingUintPrefix(IndexAscendingUintPrefix&& other) {
    *this = std::move(other);
  }
  IndexAscendingUintPrefix(PrefixBase* base) {
    assert(dynamic_cast<IndexAscendingUintPrefix<RankSelect>*>(base) != nullptr);
    auto other = static_cast<IndexAscendingUintPrefix<RankSelect>*>(base);
    *this = std::move(*other);
    delete other;
  }
  IndexAscendingUintPrefix& operator = (const IndexAscendingUintPrefix&) = delete;
  IndexAscendingUintPrefix& operator = (IndexAscendingUintPrefix&& other) {
    rank_select.swap(other.rank_select);
    key_length = other.key_length;
    min_value = other.min_value;
    max_value = other.max_value;
    std::swap(flags, other.flags);
    return *this;
  }

  ~IndexAscendingUintPrefix() {
    if (flags.is_user_mem) {
      rank_select.risk_release_ownership();
    }
  }

  RankSelect rank_select;
  size_t key_length;
  uint64_t min_value;
  uint64_t max_value;

  size_t KeyCount() const {
    return rank_select.max_rank1();
  }

  size_t TotalKeySize() const {
    return key_length * rank_select.max_rank1();
  }
  size_t Find(fstring key, const SuffixBase* suffix, TerarkContext* ctx) const {
    if (key.size() < key_length) {
      return size_t(-1);
    }
    byte_t buffer[8] = {};
    memcpy(buffer + (8 - key_length), key.data(), std::min<size_t>(key_length, key.size()));
    uint64_t value = ReadBigEndianUint64Aligned(buffer, 8);
    if (value < min_value || value > max_value) {
      return size_t(-1);
    }
    size_t hint = 0;
    auto rs = make_rank_select_hint_wrapper(rank_select, &hint);
    uint64_t pos = value - min_value;
    if (!rs[pos]) {
      return size_t(-1);
    }
    size_t id = rs.rank1(pos);
    if (suffix == nullptr) {
      return key.size() == key_length ? id : size_t(-1);
    }
    key = key.substr(key_length);
    ContextBuffer suffix_key = ctx->alloc();
    suffix->AppendKey(id, &suffix_key.get(), ctx);
    return key == suffix_key ? id : size_t(-1);
  }
  size_t DictRank(fstring key, const SuffixBase* suffix, TerarkContext* ctx) const {
    size_t id, pos, hint = 0;
    bool seek_result, is_find;
    std::tie(seek_result, is_find) =
        SeekImpl(key.size() > key_length ? key.substr(0, key_length) : key, id, pos, &hint);
    if (!seek_result) {
      return rank_select.max_rank1();
    } else if (key.size() < key_length || !is_find) {
      return id;
    } else if (suffix == nullptr) {
      return id + (key.size() > key_length);
    } else {
      ContextBuffer suffix_key = ctx->alloc();
      suffix->AppendKey(id, &suffix_key.get(), ctx);
      return id + (key.substr(key_length) > suffix_key);
    }
  }
  size_t AppendMinKey(valvec<byte_t>* buffer, TerarkContext* ctx) const {
    size_t pos = buffer->size();
    buffer->resize_no_init(pos + key_length);
    SaveAsBigEndianUint64(buffer->data() + pos, key_length, min_value);
    return 0;
  }
  size_t AppendMaxKey(valvec<byte_t>* buffer, TerarkContext* ctx) const {
    size_t pos = buffer->size();
    buffer->resize_no_init(pos + key_length);
    SaveAsBigEndianUint64(buffer->data() + pos, key_length, max_value);
    return rank_select.max_rank1() - 1;
  }

  std::integral_constant<bool, false> NeedsReorder() const {
    return std::integral_constant<bool, false>();
  }
  void GetOrderMap(terark::UintVecMin0& newToOld) const {
    assert(false);
  }
  void BuildCache(double cacheRatio) {
  }

  bool IterSeekToFirst(size_t& id, size_t& count, IteratorStorage* iter) const {
    id = 0;
    iter->pos = 0;
    count = 1;
    UpdateBuffer(iter);
    return true;
  }
  bool IterSeekToLast(size_t& id, size_t* count, IteratorStorage* iter) const {
    id = rank_select.max_rank1() - 1;
    iter->pos = rank_select.size() - 1;
    if (count != nullptr) {
      *count = 1;
    }
    UpdateBuffer(iter);
    return true;
  }
  bool IterSeek(size_t& id, size_t& count, fstring target, IteratorStorage* iter) const {
    if (!SeekImpl(target, id, iter->pos, iter->get_hint()).first) {
      return false;
    }
    count = 1;
    UpdateBuffer(iter);
    return true;
  }
  bool IterNext(size_t& id, size_t count, IteratorStorage* iter) const {
    auto rs = make_rank_select_hint_wrapper(rank_select, iter->get_hint());
    assert(id != size_t(-1));
    assert(count > 0);
    assert(rs[iter->pos]);
    assert(rs.rank1(iter->pos) == id);
    do {
      if (id == rs.max_rank1() - 1) {
        id = size_t(-1);
        return false;
      } else {
        ++id;
        iter->pos = iter->pos + rs.zero_seq_len(iter->pos + 1) + 1;
      }
    } while (--count > 0);
    UpdateBuffer(iter);
    return true;
  }
  bool IterPrev(size_t& id, size_t* count, IteratorStorage* iter) const {
    auto rs = make_rank_select_hint_wrapper(rank_select, iter->get_hint());
    assert(id != size_t(-1));
    assert(rs[iter->pos]);
    assert(rs.rank1(iter->pos) == id);
    if (id == 0) {
      id = size_t(-1);
      return false;
    } else {
      --id;
      iter->pos = iter->pos - rs.zero_seq_revlen(iter->pos) - 1;
      if (count != nullptr) {
        *count = 1;
      }
      UpdateBuffer(iter);
      return true;
    }
  }
  size_t IterDictRank(size_t id, const IteratorStorage* iter) const {
    if (id == size_t(-1)) {
      return rank_select.max_rank1();
    }
    return id;
  }
  fstring IterGetKey(size_t id, const IteratorStorage* iter) const {
    return fstring(iter->buffer, key_length);
  }

  bool Load(fstring mem) override {
    if (mem.size() < sizeof(IndexUintPrefixHeader)) {
      return false;
    }
    auto header = reinterpret_cast<const IndexUintPrefixHeader*>(mem.data());
    if (mem.size() != sizeof(IndexUintPrefixHeader) + header->rank_select_size) {
      return false;
    }
    key_length = header->key_length;
    min_value = header->min_value;
    max_value = header->max_value;
    rank_select.risk_release_ownership();
    rank_select.risk_mmap_from((byte_t*)mem.data() + sizeof(IndexUintPrefixHeader), header->rank_select_size);
    flags.is_user_mem = true;
    return true;
  }
  void Save(std::function<void(const void*, size_t)> append) const override {
    IndexUintPrefixHeader header;
    memset(&header, 0, sizeof header);
    header.format_version = 0;
    header.key_length = key_length;
    header.min_value = min_value;
    header.max_value = max_value;
    header.rank_select_size = rank_select.mem_size();
    append(&header, sizeof header);
    append(rank_select.data(), rank_select.mem_size());
  }

  std::pair<bool, bool> SeekImpl(fstring target, size_t& id, size_t& pos, size_t* hint) const {
    //
    //  key.size() == 4;
    //  key_length == 6;
    //  | - - - - - - - - |  <- buffer
    //      | - - - - - - |  <- index
    //      | - - - - |      <- key
    //
    byte_t buffer[8] = {};
    memcpy(buffer + (8 - key_length), target.data(), std::min<size_t>(key_length, target.size()));
    uint64_t value = ReadBigEndianUint64Aligned(buffer, 8);
    if (value > max_value) {
      id = size_t(-1);
      return {false, false};
    }
    if (value < min_value) {
      id = 0;
      pos = 0;
      return {true, false};
    }
    auto rs = make_rank_select_hint_wrapper(rank_select, hint);
    pos = value - min_value;
    id = rs.rank1(pos);
    if (!rs[pos]) {
      pos += rs.zero_seq_len(pos);
      return {true, false};
    } else if (target.size() > key_length) {
      if (pos == rs.size() - 1) {
        id = size_t(-1);
        return {false, false};
      }
      ++id;
      pos += rs.zero_seq_len(pos + 1) + 1;
      return {true, false};
    }
    return {true, true};
  }

  void UpdateBuffer(IteratorStorage* iter) const {
    assert(make_rank_select_hint_wrapper(rank_select, iter->get_hint())[iter->pos]);
    SaveAsBigEndianUint64(iter->buffer, key_length, iter->pos + min_value);
  }
};


template<class RankSelect>
struct IndexNonDescendingUintPrefix
    : public PrefixBase,
      public ComponentIteratorStorageImpl<IndexUintPrefixIteratorStorage<typename RankSelectNeedHint<RankSelect>::type>> {
  using IteratorStorage = IndexUintPrefixIteratorStorage<typename RankSelectNeedHint<RankSelect>::type>;

  IndexNonDescendingUintPrefix() = default;
  IndexNonDescendingUintPrefix(const IndexNonDescendingUintPrefix&) = delete;
  IndexNonDescendingUintPrefix(IndexNonDescendingUintPrefix&& other) {
    *this = std::move(other);
  }
  IndexNonDescendingUintPrefix(PrefixBase* base) {
    assert(dynamic_cast<IndexNonDescendingUintPrefix<RankSelect>*>(base) != nullptr);
    auto other = static_cast<IndexNonDescendingUintPrefix<RankSelect>*>(base);
    *this = std::move(*other);
    delete other;
  }
  IndexNonDescendingUintPrefix& operator = (const IndexNonDescendingUintPrefix&) = delete;
  IndexNonDescendingUintPrefix& operator = (IndexNonDescendingUintPrefix&& other) {
    rank_select.swap(other.rank_select);
    key_length = other.key_length;
    min_value = other.min_value;
    max_value = other.max_value;
    std::swap(flags, other.flags);
    return *this;
  }

  ~IndexNonDescendingUintPrefix() {
    if (flags.is_user_mem) {
      rank_select.risk_release_ownership();
    }
  }

  RankSelect rank_select;
  size_t key_length;
  uint64_t min_value;
  uint64_t max_value;

  size_t KeyCount() const {
    return rank_select.max_rank1();
  }
  size_t TotalKeySize() const {
    return key_length * rank_select.max_rank1();
  }
  size_t Find(fstring key, const SuffixBase* suffix, TerarkContext* ctx) const {
    assert(suffix != nullptr);
    if (key.size() < key_length) {
      return size_t(-1);
    }
    byte_t buffer[8] = {};
    memcpy(buffer + (8 - key_length), key.data(), std::min<size_t>(key_length, key.size()));
    uint64_t value = ReadBigEndianUint64Aligned(buffer, 8);
    if (value < min_value || value > max_value) {
      return size_t(-1);
    }
    size_t hint = 0;
    auto rs = make_rank_select_hint_wrapper(rank_select, &hint);
    uint64_t pos = rs.select0(value - min_value) + 1;
    assert(pos > 0);
    size_t count = rs.one_seq_len(pos);
    if (count == 0) {
      return size_t(-1);
    }
    size_t id = rs.rank1(pos);
    key = key.substr(key_length);
    if (count == 1) {
      ContextBuffer suffix_key = ctx->alloc();
      suffix->AppendKey(id, &suffix_key.get(), ctx);
      return suffix_key == key ? id : size_t(-1);
    } else {
      auto suffix_result = suffix->LowerBound(key, id, count, ctx);
      if (suffix_result.id == id + count || suffix_result.key != key) {
        return size_t(-1);
      }
      return suffix_result.id;
    }
  }
  size_t DictRank(fstring key, const SuffixBase* suffix, TerarkContext* ctx) const {
    assert(suffix != nullptr);
    size_t id, count, pos, hint = 0;
    bool seek_result, is_find;
    std::tie(seek_result, is_find) =
        SeekImpl(key.size() > key_length ? key.substr(0, key_length) : key, id, count, pos, &hint);
    if (!seek_result) {
      return rank_select.max_rank1();
    } else if (key.size() < key_length || !is_find) {
      return id;
    } else if (suffix == nullptr) {
      return id + (key.size() > key_length);
    } else if (count == 1) {
      ContextBuffer suffix_key = ctx->alloc();
      suffix->AppendKey(id, &suffix_key.get(), ctx);
      return id + (key.substr(key_length) > suffix_key);
    } else {
      return suffix->LowerBound(key.substr(key_length), id, count, ctx).id;
    }
  }
  size_t AppendMinKey(valvec<byte_t>* buffer, TerarkContext* ctx) const {
    size_t pos = buffer->size();
    buffer->resize_no_init(pos + key_length);
    SaveAsBigEndianUint64(buffer->data() + pos, key_length, min_value);
    return 0;
  }
  size_t AppendMaxKey(valvec<byte_t>* buffer, TerarkContext* ctx) const {
    size_t pos = buffer->size();
    buffer->resize_no_init(pos + key_length);
    SaveAsBigEndianUint64(buffer->data() + pos, key_length, max_value);
    return rank_select.max_rank1() - 1;
  }

  std::integral_constant<bool, false> NeedsReorder() const {
    return std::integral_constant<bool, false>();
  }
  void GetOrderMap(terark::UintVecMin0& newToOld) const {
    assert(false);
  }
  void BuildCache(double cacheRatio) {
  }

  bool IterSeekToFirst(size_t& id, size_t& count, IteratorStorage* iter) const {
    auto rs = make_rank_select_hint_wrapper(rank_select, iter->get_hint());
    id = 0;
    iter->pos = 1;
    count = rs.one_seq_len(1);
    UpdateBuffer(iter);
    return true;
  }
  bool IterSeekToLast(size_t& id, size_t* count, IteratorStorage* iter) const {
    auto rs = make_rank_select_hint_wrapper(rank_select, iter->get_hint());
    if (count == nullptr) {
      id = rs.max_rank1() - 1;
      iter->pos = rs.size() - 1;
    } else {
      size_t one_seq_revlen = rs.one_seq_revlen(rs.size());
      assert(one_seq_revlen > 0);
      id = rs.max_rank1() - one_seq_revlen;
      iter->pos = rs.size() - one_seq_revlen;
      *count = one_seq_revlen;
    }
    UpdateBuffer(iter);
    return true;
  }
  bool IterSeek(size_t& id, size_t& count, fstring target, IteratorStorage* iter) const {
    if (!SeekImpl(target, id, count, iter->pos, iter->get_hint()).first) {
      return false;
    }
    UpdateBuffer(iter);
    return true;
  }
  bool IterNext(size_t& id, size_t count, IteratorStorage* iter) const {
    auto rs = make_rank_select_hint_wrapper(rank_select, iter->get_hint());
    assert(id != size_t(-1));
    assert(count > 0);
    assert(rs[iter->pos]);
    assert(rs.rank1(iter->pos) == id);
    if (id + count >= rs.max_rank1()) {
      id = size_t(-1);
      return false;
    }
    id += count;
    if (count == 1) {
      size_t zero_seq_len = rs.zero_seq_len(iter->pos + 1);
      iter->pos += zero_seq_len + 1;
      if (zero_seq_len > 0) {
        UpdateBuffer(iter);
      }
    } else {
      iter->pos = rs.select1(id);
      UpdateBuffer(iter);
    }
    return true;
  }
  bool IterPrev(size_t& id, size_t* count, IteratorStorage* iter) const {
    auto rs = make_rank_select_hint_wrapper(rank_select, iter->get_hint());
    assert(id != size_t(-1));
    assert(rs[iter->pos]);
    assert(rs.rank1(iter->pos) == id);
    if (id == 0) {
      id = size_t(-1);
      return false;
    } else if (count == nullptr) {
      size_t zero_seq_revlen = rs.zero_seq_revlen(iter->pos);
      --id;
      iter->pos -= zero_seq_revlen + 1;
      if (zero_seq_revlen > 0) {
        UpdateBuffer(iter);
      }
      return true;
    } else {
      assert(iter->pos >= 1);
      assert(!rs[iter->pos - 1]);
      if (iter->pos == 1) {
        id = size_t(-1);
        return false;
      }
      iter->pos -= rs.zero_seq_revlen(iter->pos) + 1;
      size_t one_seq_revlen = rs.one_seq_revlen(iter->pos);
      id -= one_seq_revlen + 1;
      iter->pos -= one_seq_revlen;
      *count = one_seq_revlen + 1;
      UpdateBuffer(iter);
      return true;
    }
  }
  size_t IterDictRank(size_t id, const IteratorStorage* iter) const {
    if (id == size_t(-1)) {
      return rank_select.max_rank1();
    }
    return id;
  }
  fstring IterGetKey(size_t id, const IteratorStorage* iter) const {
    return fstring(iter->buffer, key_length);
  }

  bool Load(fstring mem) override {
    if (mem.size() < sizeof(IndexUintPrefixHeader)) {
      return false;
    }
    auto header = reinterpret_cast<const IndexUintPrefixHeader*>(mem.data());
    if (mem.size() != sizeof(IndexUintPrefixHeader) + header->rank_select_size) {
      return false;
    }
    key_length = header->key_length;
    min_value = header->min_value;
    max_value = header->max_value;
    rank_select.risk_release_ownership();
    rank_select.risk_mmap_from((byte_t*)mem.data() + sizeof(IndexUintPrefixHeader), header->rank_select_size);
    flags.is_user_mem = true;
    return true;
  }
  void Save(std::function<void(const void*, size_t)> append) const override {
    IndexUintPrefixHeader header;
    memset(&header, 0, sizeof header);
    header.format_version = 0;
    header.key_length = key_length;
    header.min_value = min_value;
    header.max_value = max_value;
    header.rank_select_size = rank_select.mem_size();
    append(&header, sizeof header);
    append(rank_select.data(), rank_select.mem_size());
  }

  std::pair<bool, bool> SeekImpl(fstring target, size_t& id, size_t& count, size_t& pos, size_t* hint) const {
    //
    //  key.size() == 4;
    //  key_length == 6;
    //  | - - - - - - - - |  <- buffer
    //      | - - - - - - |  <- index
    //      | - - - - |      <- key
    //
    byte_t buffer[8] = {};
    memcpy(buffer + (8 - key_length), target.data(), std::min<size_t>(key_length, target.size()));
    uint64_t value = ReadBigEndianUint64Aligned(buffer, 8);
    auto rs = make_rank_select_hint_wrapper(rank_select, hint);
    if (value > max_value) {
      id = size_t(-1);
      return {false, false};
    }
    if (value < min_value) {
      id = 0;
      pos = 1;
      count = rs.one_seq_len(1);
      return {true, false};
    }
    pos = rs.select0(value - min_value) + 1;
    assert(pos < rs.size());
    if (target.size() <= key_length && rs[pos]) {
      id = rs.rank1(pos);
      count = rs.one_seq_len(pos);
      return {true, target.size() == key_length};
    } else {
      pos += rs.one_seq_len(pos);
      if (pos == rs.size()) {
        id = size_t(-1);
        return {false, false};
      }
      pos += rs.zero_seq_len(pos + 1) + 1;
      id = rs.rank1(pos);
      count = rs.one_seq_len(pos);
      return {true, false};
    }
  }

  void UpdateBuffer(IteratorStorage* iter) const {
    auto rs = make_rank_select_hint_wrapper(rank_select, iter->get_hint());
    assert(rs[iter->pos]);
    SaveAsBigEndianUint64(iter->buffer, key_length, rs.rank0(iter->pos) - 1 + min_value);
  }
};

template<class NestLoudsTrieDAWG>
class IndexNestLoudsTriePrefixIterator {
protected:
  valvec<byte_t> buffer_;
  typename NestLoudsTrieDAWG::UserMemIterator iter_;
  bool Done(size_t& id, bool ok) {
    auto dawg = static_cast<const NestLoudsTrieDAWG*>(iter_.get_dfa());
    id = ok ? dawg->state_to_word_id(iter_.word_state()) : size_t(-1);
    return ok;
  }
  bool Trans(size_t& id, bool ok) {
    auto dawg = static_cast<const NestLoudsTrieDAWG*>(iter_.get_dfa());
    id = ok ? dawg->state_to_dict_rank(iter_.word_state()) : size_t(-1);
    return ok;
  }
public:
  IndexNestLoudsTriePrefixIterator(valvec<byte_t>&& buffer, const NestLoudsTrieDAWG* trie)
      : buffer_(std::move(buffer)), iter_(trie, buffer_.data()) {}

  void ReclaimContextBuffer(TerarkContext* ctx) {
    ContextBuffer(std::move(buffer_), ctx);
  }

  fstring GetKey() const { return iter_.word(); }
  bool SeekToFirst(size_t& id, bool bfs_sufflx) {
    if (bfs_sufflx)
      return Done(id, iter_.seek_begin());
    else
      return Trans(id, iter_.seek_begin());
  }
  bool SeekToLast(size_t& id, bool bfs_sufflx) {
    if (bfs_sufflx)
      return Done(id, iter_.seek_end());
    else
      return Trans(id, iter_.seek_end());
  }
  bool Seek(size_t& id, bool bfs_sufflx, fstring key) {
    if (bfs_sufflx)
      return Done(id, iter_.seek_lower_bound(key));
    else
      return Trans(id, iter_.seek_lower_bound(key));
  }
  bool Next(size_t& id, bool bfs_sufflx) {
    if (bfs_sufflx)
      return Done(id, iter_.incr());
    else
      return iter_.incr() ? (id = id + 1, true) : (id = size_t(-1), false);
  }
  bool Prev(size_t& id, bool bfs_sufflx) {
    if (bfs_sufflx)
      return Done(id, iter_.decr());
    else
      return iter_.decr() ? (id = id - 1, true) : (id = size_t(-1), false);
  }
  size_t DictRank(size_t id, bool bfs_sufflx) const {
    auto dawg = static_cast<const NestLoudsTrieDAWG*>(iter_.get_dfa());
    assert(id != size_t(-1));
    if (bfs_sufflx)
      return dawg->state_to_dict_rank(iter_.word_state());
    else
      return id;
  }
};

template<class NestLoudsTrieDAWG>
struct IndexNestLoudsTriePrefix
    : public PrefixBase {
  using IteratorStorage = IndexNestLoudsTriePrefixIterator<NestLoudsTrieDAWG>;

  IndexNestLoudsTriePrefix() = default;
  IndexNestLoudsTriePrefix(const IndexNestLoudsTriePrefix&) = delete;
  IndexNestLoudsTriePrefix(IndexNestLoudsTriePrefix&&) = default;
  IndexNestLoudsTriePrefix(PrefixBase* base) {
    assert(dynamic_cast<IndexNestLoudsTriePrefix<NestLoudsTrieDAWG>*>(base) != nullptr);
    auto other = static_cast<IndexNestLoudsTriePrefix<NestLoudsTrieDAWG>*>(base);
    *this = std::move(*other);
    delete other;
  }
  IndexNestLoudsTriePrefix(NestLoudsTrieDAWG* trie, bool bfs_sufflx)
      : trie_(trie) {
    flags.is_bfs_suffix = bfs_sufflx;
  }
  IndexNestLoudsTriePrefix& operator = (const IndexNestLoudsTriePrefix&) = delete;
  IndexNestLoudsTriePrefix& operator = (IndexNestLoudsTriePrefix&&) = default;

  unique_ptr<NestLoudsTrieDAWG> trie_;

  size_t IteratorStorageSize() const {
    return sizeof(IteratorStorage);
  }
  void IteratorStorageConstruct(TerarkContext* ctx, void* ptr) const {
    ContextBuffer buffer;
    size_t mem_size = trie_->iterator_max_mem_size();
    if (ctx != nullptr) {
      buffer = ctx->alloc(mem_size);
    } else {
      buffer.ensure_capacity(mem_size);
    }
    ::new(ptr) IteratorStorage(std::move(buffer.get()), trie_.get());
  }
  void IteratorStorageDestruct(TerarkContext* ctx, void* ptr) const {
    auto iter = static_cast<IteratorStorage*>(ptr);
    if (ctx != nullptr) {
      iter->ReclaimContextBuffer(ctx);
    }
    iter->~IteratorStorage();
  }

  size_t KeyCount() const {
    return trie_->num_words();
  }
  size_t TotalKeySize() const {
    return trie_->adfa_total_words_len();
  }
  size_t Find(fstring key, const SuffixBase* suffix, TerarkContext* ctx) const {
    if (suffix == nullptr && flags.is_bfs_suffix) {
      return trie_->index(key);
    }
    auto buffer = ctx->alloc(trie_->iterator_max_mem_size());
    typename NestLoudsTrieDAWG::UserMemIterator iter(trie_.get(), buffer.data());
    if (iter.seek_lower_bound(key)) {
      if (iter.word() != key) {
        if (!iter.decr()) {
          return size_t(-1);
        }
      }
    } else {
      iter.seek_end();
    }
    if (!key.startsWith(iter.word())) {
      return size_t(-1);
    }
    key = key.substr(iter.word().size());
    size_t suffix_id;
    if (flags.is_bfs_suffix) {
      suffix_id = trie_->state_to_word_id(iter.word_state());
    } else {
      suffix_id = trie_->state_to_dict_rank(iter.word_state());
    }
    ContextBuffer suffix_key = ctx->alloc();
    suffix->AppendKey(suffix_id, &suffix_key.get(), ctx);
    return key == suffix_key ? suffix_id : size_t(-1);
  }
  size_t DictRank(fstring key, const SuffixBase* suffix, TerarkContext* ctx) const {
    size_t rank;
    if (suffix == nullptr) {
      trie_->lower_bound(key, nullptr, &rank);
      return rank;
    }
    auto buffer = ctx->alloc(trie_->iterator_max_mem_size());
    typename NestLoudsTrieDAWG::UserMemIterator iter(trie_.get(), buffer.data());
    if (iter.seek_lower_bound(key)) {
      if (iter.word() != key) {
        if (!iter.decr()) {
          return 0;
        }
      }
    } else {
      iter.seek_end();
    }
    rank = trie_->state_to_dict_rank(iter.word_state());
    if (!key.startsWith(iter.word())) {
      return rank + 1;
    }
    key = key.substr(iter.word().size());
    size_t suffix_id;
    if (flags.is_bfs_suffix) {
      suffix_id = trie_->state_to_word_id(iter.word_state());
    } else {
      suffix_id = rank;
    }
    ContextBuffer suffix_key = ctx->alloc();
    suffix->AppendKey(suffix_id, &suffix_key.get(), ctx);
    return rank + (key > suffix_key);
  }
  size_t AppendMinKey(valvec<byte_t>* buffer, TerarkContext* ctx) const {
    size_t id = trie_->index_begin();
    auto key_buffer = ctx->alloc();
    trie_->nth_word(id, &key_buffer.get());
    buffer->append(key_buffer.get());
    return flags.is_bfs_suffix ? id : 0;
  }
  size_t AppendMaxKey(valvec<byte_t>* buffer, TerarkContext* ctx) const {
    size_t id = trie_->index_end();
    auto key_buffer = ctx->alloc();
    trie_->nth_word(id, &key_buffer.get());
    buffer->append(key_buffer.get());
    return flags.is_bfs_suffix ? id : trie_->num_words() - 1;
  }

  std::integral_constant<bool, true> NeedsReorder() const {
    return std::integral_constant<bool, true>();
  }
  void GetOrderMap(terark::UintVecMin0& newToOld) const {
    NonRecursiveDictionaryOrderToStateMapGenerator gen;
    gen(*trie_, [&](size_t dictOrderOldId, size_t state) {
      size_t newId = trie_->state_to_word_id(state);
      //assert(trie->state_to_dict_index(state) == dictOrderOldId);
      //assert(trie->dict_index_to_state(dictOrderOldId) == state);
      newToOld.set_wire(newId, dictOrderOldId);
    });
  }
  void BuildCache(double cacheRatio) {
    if (cacheRatio > 1e-8) {
      trie_->build_fsa_cache(cacheRatio, NULL);
    }
  }

  bool IterSeekToFirst(size_t& id, size_t& count, IteratorStorage* iter) const {
    count = 1;
    return iter->SeekToFirst(id, flags.is_bfs_suffix);
  }
  bool IterSeekToLast(size_t& id, size_t* count, IteratorStorage* iter) const {
    if (count != nullptr) {
      *count = 1;
    }
    return iter->SeekToLast(id, flags.is_bfs_suffix);
  }
  bool IterSeek(size_t& id, size_t& count, fstring target, IteratorStorage* iter) const {
    count = 1;
    return iter->Seek(id, flags.is_bfs_suffix, target);
  }
  bool IterNext(size_t& id, size_t count, IteratorStorage* iter) const {
    assert(count > 0);
    do {
      if (!iter->Next(id, flags.is_bfs_suffix)) {
        return false;
      }
    } while (--count > 0);
    return true;
  }
  bool IterPrev(size_t& id, size_t* count, IteratorStorage* iter) const {
    if (count != nullptr) {
      *count = 1;
    }
    return iter->Prev(id, flags.is_bfs_suffix);
  }
  size_t IterDictRank(size_t id, const IteratorStorage* iter) const {
    return iter->DictRank(id, flags.is_bfs_suffix);
  }
  fstring IterGetKey(size_t id, const IteratorStorage* iter) const {
    return iter->GetKey();
  }

  bool Load(fstring mem) override {
    std::unique_ptr<BaseDFA> dfa(BaseDFA::load_mmap_user_mem(mem.data(), mem.size()));
    auto dawg = dynamic_cast<NestLoudsTrieDAWG*>(dfa.get());
    if (dawg == nullptr) {
      return false;
    }
    trie_.reset(dawg);
    dfa.release();
    return true;
  }
  void Save(std::function<void(const void*, size_t)> append) const override {
    trie_->save_mmap(append);
  }
};

struct IndexEmptySuffix
    : public SuffixBase, public ComponentIteratorStorageImpl<void> {
  typedef void IteratorStorage;

  IndexEmptySuffix() = default;
  IndexEmptySuffix(const IndexEmptySuffix&) = delete;
  IndexEmptySuffix(IndexEmptySuffix&&) = default;
  IndexEmptySuffix(SuffixBase* base) {
    delete base;
  }
  IndexEmptySuffix& operator = (const IndexEmptySuffix&) = delete;
  IndexEmptySuffix& operator = (IndexEmptySuffix&&) = default;

  std::integral_constant<size_t, 0> TotalKeySize() const {
    return {};
  }
  LowerBoundResult
  LowerBound(fstring target, size_t suffix_id, size_t suffix_count, TerarkContext* ctx) const override {
    return {suffix_id, {}, {}};
  }
  void AppendKey(size_t suffix_id, valvec<byte_t>* buffer, TerarkContext* ctx) const override {
  }
  void GetMetaData(valvec<fstring>* blocks) const {
  }
  void DetachMetaData(const valvec<fstring>& blocks) {
  }

  void IterSet(size_t suffix_id, void*) const {
  }
  bool IterSeek(fstring target, size_t& suffix_id, size_t suffix_count, void*) const {
    return true;
  }
  fstring IterGetKey(size_t suffix_id, const void*) const {
    return fstring();
  }

  bool Load(fstring mem) override {
    return mem.empty();
  }
  void Save(std::function<void(const void*, size_t)> append) const override {
  }
  void
  Reorder(ZReorderMap& newToOld, std::function<void(const void*, size_t)> append, fstring tmpFile) const override {
  }
};

struct IndexFixedStringSuffix
    : public SuffixBase, public ComponentIteratorStorageImpl<void> {
  typedef void IteratorStorage;

  IndexFixedStringSuffix() = default;
  IndexFixedStringSuffix(const IndexFixedStringSuffix&) = delete;
  IndexFixedStringSuffix(IndexFixedStringSuffix&& other) {
    *this = std::move(other);
  }
  IndexFixedStringSuffix(SuffixBase* base) {
    assert(dynamic_cast<IndexFixedStringSuffix*>(base) != nullptr);
    auto other = static_cast<IndexFixedStringSuffix*>(base);
    *this = std::move(*other);
    delete other;
  }
  IndexFixedStringSuffix& operator = (const IndexFixedStringSuffix&) = delete;
  IndexFixedStringSuffix& operator = (IndexFixedStringSuffix&& other) {
    str_pool_.swap(other.str_pool_);
    std::swap(flags, other.flags);
    return *this;
  }

  ~IndexFixedStringSuffix() {
    if (flags.is_user_mem) {
      str_pool_.risk_release_ownership();
    }
  }

  struct Header {
    uint64_t fixlen;
    uint64_t size;
  };
  FixedLenStrVec str_pool_;

  size_t TotalKeySize() const {
    return str_pool_.mem_size();
  }
  LowerBoundResult
  LowerBound(fstring target, size_t suffix_id, size_t suffix_count, TerarkContext* ctx) const override {
    if (flags.is_rev_suffix) {
      size_t num_records = str_pool_.m_size;
      suffix_id = num_records - suffix_id - suffix_count;
      size_t end = suffix_id + suffix_count;
      suffix_id = str_pool_.lower_bound(suffix_id, end, target);
      if (suffix_id == end) {
        return {num_records - suffix_id - 1, {}, {}};
      }
      return {num_records - suffix_id - 1, str_pool_[suffix_id], {}};
    } else {
      size_t end = suffix_id + suffix_count;
      suffix_id = str_pool_.lower_bound(suffix_id, end, target);
      if (suffix_id == end) {
        return {suffix_id, {}, {}};
      }
      return {suffix_id, str_pool_[suffix_id], {}};
    }
  }
  void AppendKey(size_t suffix_id, valvec<byte_t>* buffer, TerarkContext* ctx) const override {
    fstring key;
    if (flags.is_rev_suffix) {
      key = str_pool_[str_pool_.m_size - suffix_id - 1];
    } else {
      key = str_pool_[suffix_id];
    }
    buffer->append(key.data(), key.size());
  }
  void GetMetaData(valvec<fstring>* blocks) const {
  }
  void DetachMetaData(const valvec<fstring>& blocks) {
  }

  void IterSet(size_t /*suffix_id*/, void*) const {
  }
  bool IterSeek(fstring target, size_t& suffix_id, size_t suffix_count, void*) const {
    if (flags.is_rev_suffix) {
      size_t num_records = str_pool_.m_size;
      suffix_id = num_records - suffix_id - suffix_count;
      size_t end = suffix_id + suffix_count;
      suffix_id = str_pool_.lower_bound(suffix_id, end, target);
      bool success = suffix_id != end;
      suffix_id = num_records - suffix_id - suffix_count;
      return success;
    } else {
      size_t end = suffix_id + suffix_count;
      suffix_id = str_pool_.lower_bound(suffix_id, end, target);
      return suffix_id != end;
    }
  }
  fstring IterGetKey(size_t suffix_id, const void*) const {
    if (flags.is_rev_suffix) {
      return str_pool_[str_pool_.m_size - suffix_id - 1];
    } else {
      return str_pool_[suffix_id];
    }
  }

  bool Load(fstring mem) override {
    Header header;
    if (mem.size() < sizeof header) {
      return false;
    }
    memcpy(&header, mem.data(), sizeof header);
    if (mem.size() < sizeof header + header.fixlen * header.size) {
      return false;
    }
    str_pool_.m_fixlen = header.fixlen;
    str_pool_.m_size = header.size;
    assert(mem.size() - sizeof header >= header.fixlen * header.size);
    str_pool_.m_strpool.risk_set_data((byte_t*)mem.data() + sizeof header, header.fixlen * header.size);
    flags.is_user_mem = true;
    return true;
  }
  void Save(std::function<void(const void*, size_t)> append) const override {
    Header header = {
        str_pool_.m_fixlen, str_pool_.m_size
    };
    append(&header, sizeof header);
    append(str_pool_.data(), str_pool_.mem_size());
    Padzero<8>(append, sizeof header + header.fixlen * header.size);
  }
  void
  Reorder(ZReorderMap& newToOld, std::function<void(const void*, size_t)> append, fstring tmpFile) const override {
    FunctionAdaptBuffer adaptBuffer(append);
    OutputBuffer buffer(&adaptBuffer);
    Header header = {
        str_pool_.m_fixlen, str_pool_.m_size
    };
    buffer.ensureWrite(&header, sizeof header);
    for (assert(newToOld.size() == str_pool_.size()); !newToOld.eof(); ++newToOld) {
      size_t oldId = *newToOld;
      assert(oldId < str_pool_.size());
      auto rec = str_pool_[oldId];
      buffer.ensureWrite(rec.data(), rec.size());
    }
    PadzeroForAlign<8>(buffer, sizeof header + header.fixlen * header.size);
  }
};

template<class BlobStoreType>
struct IndexBlobStoreSuffix
    : public SuffixBase {
  typedef BlobStore::CacheOffsets IteratorStorage;

  IndexBlobStoreSuffix() = default;
  IndexBlobStoreSuffix(const IndexBlobStoreSuffix&) = delete;
  IndexBlobStoreSuffix(IndexBlobStoreSuffix&& other) {
    *this = std::move(other);
  }
  IndexBlobStoreSuffix(SuffixBase* base) {
    assert(dynamic_cast<IndexBlobStoreSuffix<BlobStoreType>*>(base) != nullptr);
    auto other = static_cast<IndexBlobStoreSuffix<BlobStoreType>*>(base);
    *this = std::move(*other);
    delete other;
  }
  IndexBlobStoreSuffix(BlobStoreType* store, FileMemIO& mem, bool rev_suffix) {
    store_.swap(*store);
    memory_.swap(mem);
    flags.is_rev_suffix = rev_suffix;
    delete store;
  }
  IndexBlobStoreSuffix& operator = (const IndexBlobStoreSuffix&) = delete;

  IndexBlobStoreSuffix& operator = (IndexBlobStoreSuffix&& other) {
    store_.swap(other.store_);
    memory_.swap(other.memory_);
    std::swap(flags, other.flags);
    return *this;
  }

  size_t IteratorStorageSize() const { return sizeof(IteratorStorage); }
  void IteratorStorageConstruct(TerarkContext* ctx, void* ptr) const {
    auto iter = ::new(ptr) IteratorStorage();
    if (ctx != nullptr) {
      iter->recData.swap(ctx->alloc());
    }
  }
  void IteratorStorageDestruct(TerarkContext* ctx, void* ptr) const {
    auto iter = static_cast<IteratorStorage*>(ptr);
    if (ctx != nullptr) {
      ContextBuffer(std::move(iter->recData), ctx);
    }
    iter->~IteratorStorage();
  }

  BlobStoreType store_;
  FileMemIO memory_;

  size_t TotalKeySize() const {
    return store_.total_data_size();
  }
  LowerBoundResult
  LowerBound(fstring target, size_t suffix_id, size_t suffix_count, TerarkContext* ctx) const override {
    ContextBuffer buffer = ctx->alloc();
    if (flags.is_rev_suffix) {
      size_t num_records = store_.num_records();
      suffix_id = num_records - suffix_id - suffix_count;
      size_t end = suffix_id + suffix_count;
      suffix_id = store_.lower_bound(suffix_id, end, target, &buffer.get());
      if (suffix_id == end) {
        return {num_records - suffix_id - 1, {}, {}};
      }
      fstring suffix_key = buffer;
      return {num_records - suffix_id - 1, suffix_key, std::move(buffer)};
    } else {
      size_t end = suffix_id + suffix_count;
      suffix_id = store_.lower_bound(suffix_id, end, target, &buffer.get());
      if (suffix_id == end) {
        return {suffix_id, {}, {}};
      }
      fstring suffix_key = buffer;
      return {suffix_id, suffix_key, std::move(buffer)};
    }
  }
  void AppendKey(size_t suffix_id, valvec<byte_t>* buffer, TerarkContext* ctx) const override {
    if (flags.is_rev_suffix) {
      store_.get_record_append(store_.num_records() - suffix_id - 1, buffer);
    } else {
      store_.get_record_append(suffix_id, buffer);
    }
  }
  void GetMetaData(valvec<fstring>* blocks) const {
    store_.get_meta_blocks(blocks);
  }
  void DetachMetaData(const valvec<fstring>& blocks) {
    store_.detach_meta_blocks(blocks);
  }

  void IterSet(size_t suffix_id, IteratorStorage* iter) const {
    iter->recData.risk_set_size(0);
    if (flags.is_rev_suffix) {
      store_.get_record_append(store_.num_records() - suffix_id - 1, iter);
    } else {
      store_.get_record_append(suffix_id, iter);
    }
  }
  bool IterSeek(fstring target, size_t& suffix_id, size_t suffix_count, IteratorStorage* iter) const {
    if (flags.is_rev_suffix) {
      size_t num_records = store_.num_records();
      suffix_id = num_records - suffix_id - suffix_count;
      size_t end = suffix_id + suffix_count;
      suffix_id = store_.lower_bound(suffix_id, end, target, iter);
      bool success = suffix_id != end;
      suffix_id = num_records - suffix_id - suffix_count;
      return success;
    } else {
      size_t end = suffix_id + suffix_count;
      suffix_id = store_.lower_bound(suffix_id, end, target, iter);
      return suffix_id != end;
    }
  }
  fstring IterGetKey(size_t /*suffix_id*/, const IteratorStorage* iter) const {
    return iter->recData;
  }

  bool Load(fstring mem) override {
    std::unique_ptr<BlobStore> base_store(
        AbstractBlobStore::load_from_user_memory(mem, AbstractBlobStore::Dictionary()));
    auto store = dynamic_cast<BlobStoreType*>(base_store.get());
    if (store == nullptr) {
      return false;
    }
    store_.swap(*store);
    return true;
  }
  void Save(std::function<void(const void*, size_t)> append) const override {
    store_.save_mmap(append);
  }
  void
  Reorder(ZReorderMap& newToOld, std::function<void(const void*, size_t)> append, fstring tmpFile) const override {
    store_.reorder_zip_data(newToOld, append, tmpFile);
  }
};

struct IndexEntropySuffix : public IndexBlobStoreSuffix<EntropyZipBlobStore> {
  using BaseType = IndexBlobStoreSuffix<EntropyZipBlobStore>;

  IndexEntropySuffix() = default;
  IndexEntropySuffix(const IndexEntropySuffix&) = delete;
  IndexEntropySuffix(IndexEntropySuffix&& other) = default;
  IndexEntropySuffix(SuffixBase* base) : BaseType(base) {}
  IndexEntropySuffix(EntropyZipBlobStore* store, FileMemIO& mem, bool rev_suffix)
      : BaseType(store, mem, rev_suffix) {}
  IndexEntropySuffix& operator = (const IndexEntropySuffix&) = delete;
  IndexEntropySuffix& operator = (IndexEntropySuffix&& other) = default;

  bool Load(fstring mem) override {
    try {
      if (BaseType::Load(mem)) {
        return true;
      }
    } catch(...) {}
    struct Footer {
      uint64_t offset_size;
      uint64_t table_size;
      uint64_t data_size;
      uint64_t raw_size;

      size_t size() const {
        return align_up(data_size + table_size, 8) + offset_size + sizeof(Footer);
      }
    } footer;
    if (mem.size() < sizeof footer) {
      return false;
    }
    memcpy(&footer, mem.data() + mem.size() - sizeof footer, sizeof footer);
    if (mem.size() < footer.size()) {
      return false;
    }
    SortedUintVec offsets;
    valvec<byte_t> content;
    valvec<byte_t> table;
    byte_t* ptr = (byte_t*)mem.data() + mem.size() - sizeof footer;
    ptr -= footer.offset_size;
    offsets.risk_set_data(ptr, footer.offset_size);
    ptr -= (8 - (footer.data_size + footer.table_size) % 8) % 8;
    table.risk_set_data(ptr -= footer.table_size, footer.table_size);
    content.risk_set_data(ptr -= footer.data_size, footer.data_size);
    assert(ptr == (byte_t*)mem.data());
    flags.is_user_mem = true;
    store_.init_from_components(std::move(offsets), std::move(content), std::move(table), 1, footer.raw_size,
                                3 /* check data area */, 0 /* ignored */);
    return true;
  }
};

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

template<class RankSelect, class InputBufferType>
void AscendingUintPrefixFillRankSelect(
    const UintPrefixBuildInfo& info,
    const TerarkIndex::KeyStat& ks,
    RankSelect& rs, InputBufferType& input) {
  assert(info.max_value - info.min_value < std::numeric_limits<uint64_t>::max());
  rs.resize(info.max_value - info.min_value + 1);
  for (size_t seq_id = 0; seq_id < info.key_count; ++seq_id) {
    auto key = input.next();
    assert(key.size() == info.key_length);
    auto cur = ReadBigEndianUint64(key);
    rs.set1(cur - info.min_value);
  }
  rs.build_cache(false, false);
}

template<class InputBufferType>
void AscendingUintPrefixFillRankSelect(
    const UintPrefixBuildInfo& info,
    const TerarkIndex::KeyStat& ks,
    rank_select_allone& rs, InputBufferType& input) {
  assert(info.max_value - info.min_value < std::numeric_limits<uint64_t>::max());
  rs.resize(info.max_value - info.min_value + 1);
}

template<size_t P, size_t W, class InputBufferType>
void AscendingUintPrefixFillRankSelect(
    const UintPrefixBuildInfo& info,
    const TerarkIndex::KeyStat& ks,
    rank_select_few<P, W>& rs, InputBufferType& input) {
  assert(g_indexEnableFewZero);
  rank_select_few_builder <P, W> builder(info.bit_count0, info.bit_count1, ks.minKey > ks.maxKey);
  for (size_t seq_id = 0; seq_id < info.key_count; ++seq_id) {
    auto key = input.next();
    assert(key.size() == info.key_length);
    auto cur = ReadBigEndianUint64(key);
    builder.insert(cur - info.min_value);
  }
  builder.finish(&rs);
}

template<class RankSelect, class InputBufferType>
PrefixBase*
BuildAscendingUintPrefix(
    InputBufferType& input,
    const TerarkIndex::KeyStat& ks,
    const UintPrefixBuildInfo& info) {
  RankSelect rank_select;
  assert(info.min_value <= info.max_value);
  AscendingUintPrefixFillRankSelect(info, ks, rank_select, input);
  auto prefix = new IndexAscendingUintPrefix<RankSelect>();
  prefix->rank_select.swap(rank_select);
  prefix->key_length = info.key_length;
  prefix->min_value = info.min_value;
  prefix->max_value = info.max_value;
  return prefix;
}

template<class RankSelect, class InputBufferType>
void NonDescendingUintPrefixFillRankSelect(
    const UintPrefixBuildInfo& info,
    const TerarkIndex::KeyStat& ks,
    RankSelect& rs, InputBufferType& input) {
  size_t bit_count = info.bit_count0 + info.bit_count1;
  assert(info.bit_count0 + info.bit_count1 < std::numeric_limits<uint64_t>::max());
  rs.resize(bit_count);
  if (ks.minKey <= ks.maxKey) {
    size_t pos = 0;
    uint64_t last = info.min_value;
    for (size_t seq_id = 0; seq_id < info.key_count; ++seq_id) {
      auto key = input.next();
      assert(key.size() == info.key_length);
      auto cur = ReadBigEndianUint64(key);
      pos += cur - last;
      last = cur;
      rs.set1(++pos);
    }
    assert(last == info.max_value);
    assert(pos + 1 == bit_count);
  } else {
    size_t pos = bit_count - 1;
    uint64_t last = info.max_value;
    for (size_t seq_id = 0; seq_id < info.key_count; ++seq_id) {
      auto key = input.next();
      assert(key.size() == info.key_length);
      auto cur = ReadBigEndianUint64(key);
      pos -= last - cur;
      last = cur;
      rs.set1(pos--);
    }
    assert(last == info.min_value);
    assert(pos == 0);
  }
  rs.build_cache(true, true);
}

template<size_t P, size_t W, class InputBufferType>
void NonDescendingUintPrefixFillRankSelect(
    const UintPrefixBuildInfo& info,
    const TerarkIndex::KeyStat& ks,
    rank_select_few<P, W>& rs, InputBufferType& input) {
  assert(g_indexEnableFewZero);
  bool isReverse = ks.minKey > ks.maxKey;
  rank_select_few_builder<P, W> builder(info.bit_count0, info.bit_count1, isReverse);
  size_t bit_count = info.bit_count0 + info.bit_count1;
  if (!isReverse) {
    size_t pos = 0;
    uint64_t last = info.min_value;
    for (size_t seq_id = 0; seq_id < info.key_count; ++seq_id) {
      auto key = input.next();
      assert(key.size() == info.key_length);
      auto cur = ReadBigEndianUint64(key);
      pos += cur - last;
      last = cur;
      builder.insert(++pos);
    }
    assert(last == info.max_value);
    assert(pos + 1 == bit_count);
  } else {
    size_t pos = bit_count - 1;
    uint64_t last = info.max_value;
    for (size_t seq_id = 0; seq_id < info.key_count; ++seq_id) {
      auto key = input.next();
      assert(key.size() == info.key_length);
      auto cur = ReadBigEndianUint64(key);
      pos -= last - cur;
      last = cur;
      builder.insert(pos--);
    }
    assert(last == info.min_value);
    assert(pos == 0);
  }
  builder.finish(&rs);
}

template<class RankSelect, class InputBufferType>
PrefixBase*
BuildNonDescendingUintPrefix(
    InputBufferType& input,
    const TerarkIndex::KeyStat& ks,
    const UintPrefixBuildInfo& info) {
  assert(g_indexEnableNonDescUint);
  RankSelect rank_select;
  assert(info.min_value <= info.max_value);
  NonDescendingUintPrefixFillRankSelect(info, ks, rank_select, input);
  auto prefix = new IndexNonDescendingUintPrefix<RankSelect>();
  prefix->rank_select.swap(rank_select);
  prefix->key_length = info.key_length;
  prefix->min_value = info.min_value;
  prefix->max_value = info.max_value;
  return prefix;
}

template<class InputBufferType>
PrefixBase*
BuildUintPrefix(
    InputBufferType& input,
    const TerarkIndex::KeyStat& ks,
    const UintPrefixBuildInfo& info) {
  assert(g_indexEnableUintIndex);
  input.rewind();
  switch (info.type) {
  case UintPrefixBuildInfo::asc_few_zero_3:
    return BuildAscendingUintPrefix<rank_select_fewzero<3>>(input, ks, info);
  case UintPrefixBuildInfo::asc_few_zero_4:
    return BuildAscendingUintPrefix<rank_select_fewzero<4>>(input, ks, info);
  case UintPrefixBuildInfo::asc_few_zero_5:
    return BuildAscendingUintPrefix<rank_select_fewzero<5>>(input, ks, info);
  case UintPrefixBuildInfo::asc_few_zero_6:
    return BuildAscendingUintPrefix<rank_select_fewzero<6>>(input, ks, info);
  case UintPrefixBuildInfo::asc_few_zero_7:
    return BuildAscendingUintPrefix<rank_select_fewzero<7>>(input, ks, info);
  case UintPrefixBuildInfo::asc_few_zero_8:
    return BuildAscendingUintPrefix<rank_select_fewzero<8>>(input, ks, info);
  case UintPrefixBuildInfo::asc_allone:
    return BuildAscendingUintPrefix<rank_select_allone>(input, ks, info);
  case UintPrefixBuildInfo::asc_il_256:
    return BuildAscendingUintPrefix<rank_select_il_256_32>(input, ks, info);
  case UintPrefixBuildInfo::asc_se_512:
    return BuildAscendingUintPrefix<rank_select_se_512_64>(input, ks, info);
  case UintPrefixBuildInfo::asc_few_one_3:
    return BuildAscendingUintPrefix<rank_select_fewone<3>>(input, ks, info);
  case UintPrefixBuildInfo::asc_few_one_4:
    return BuildAscendingUintPrefix<rank_select_fewone<4>>(input, ks, info);
  case UintPrefixBuildInfo::asc_few_one_5:
    return BuildAscendingUintPrefix<rank_select_fewone<5>>(input, ks, info);
  case UintPrefixBuildInfo::asc_few_one_6:
    return BuildAscendingUintPrefix<rank_select_fewone<6>>(input, ks, info);
  case UintPrefixBuildInfo::asc_few_one_7:
    return BuildAscendingUintPrefix<rank_select_fewone<7>>(input, ks, info);
  case UintPrefixBuildInfo::asc_few_one_8:
    return BuildAscendingUintPrefix<rank_select_fewone<8>>(input, ks, info);
  case UintPrefixBuildInfo::non_desc_il_256:
    assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
    return BuildNonDescendingUintPrefix<rank_select_il_256_32>(input, ks, info);
  case UintPrefixBuildInfo::non_desc_se_512:
    assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
    return BuildNonDescendingUintPrefix<rank_select_se_512_64>(input, ks, info);
  case UintPrefixBuildInfo::non_desc_few_one_3:
    assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
    return BuildNonDescendingUintPrefix<rank_select_fewone<3>>(input, ks, info);
  case UintPrefixBuildInfo::non_desc_few_one_4:
    assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
    return BuildNonDescendingUintPrefix<rank_select_fewone<4>>(input, ks, info);
  case UintPrefixBuildInfo::non_desc_few_one_5:
    assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
    return BuildNonDescendingUintPrefix<rank_select_fewone<5>>(input, ks, info);
  case UintPrefixBuildInfo::non_desc_few_one_6:
    assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
    return BuildNonDescendingUintPrefix<rank_select_fewone<6>>(input, ks, info);
  case UintPrefixBuildInfo::non_desc_few_one_7:
    assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
    return BuildNonDescendingUintPrefix<rank_select_fewone<7>>(input, ks, info);
  case UintPrefixBuildInfo::non_desc_few_one_8:
    assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
    return BuildNonDescendingUintPrefix<rank_select_fewone<8>>(input, ks, info);
  case UintPrefixBuildInfo::fail:
  default:
    assert(false);
    return nullptr;
  }
}

void NestLoudsTriePrefixSetConfig(
    NestLoudsTrieConfig& conf,
    size_t memSize, double avgSize,
    const TerarkZipTableOptions& tzopt) {
  conf.nestLevel = tzopt.indexNestLevel;
  conf.nestScale = tzopt.indexNestScale;
  if (tzopt.indexTempLevel >= 0 && tzopt.indexTempLevel < 5) {
    if (memSize > tzopt.smallTaskMemory) {
      // use tmp files during index building
      conf.tmpDir = tzopt.localTempDir;
      if (0 == tzopt.indexTempLevel) {
        // adjust tmpLevel for linkVec, wihch is proportional to num of keys
        if (memSize > tzopt.smallTaskMemory * 2 && avgSize <= 50) {
          // not need any mem in BFS, instead 8G file of 4G mem (linkVec)
          // this reduce 10% peak mem when avg keylen is 24 bytes
          if (avgSize <= 30) {
            // write str data(each len+data) of nestStrVec to tmpfile
            conf.tmpLevel = 4;
          } else {
            // write offset+len of nestStrVec to tmpfile
            // which offset is ref to outer StrVec's data
            conf.tmpLevel = 3;
          }
        } else if (memSize > tzopt.smallTaskMemory * 3 / 2) {
          // for example:
          // 1G mem in BFS, swap to 1G file after BFS and before build nextStrVec
          conf.tmpLevel = 2;
        }
      } else {
        conf.tmpLevel = tzopt.indexTempLevel;
      }
    }
  }
  if (tzopt.indexTempLevel >= 5) {
    // always use max tmpLevel 4
    conf.tmpDir = tzopt.localTempDir;
    conf.tmpLevel = 4;
  }
  conf.isInputSorted = true;
  conf.debugLevel = tzopt.debugLevel > 0 ? 1 : 0;
}

template<class NestLoudsTrieDAWG, class StrVec>
PrefixBase*
NestLoudsTriePrefixProcess(const NestLoudsTrieConfig& cfg, StrVec& keyVec) {
  std::unique_ptr<NestLoudsTrieDAWG> trie(new NestLoudsTrieDAWG());
  trie->build_from(keyVec, cfg);
  return new IndexNestLoudsTriePrefix<NestLoudsTrieDAWG>(trie.release(), false);
}

template<class StrVec>
PrefixBase*
NestLoudsTriePrefixSelect(fstring type, NestLoudsTrieConfig& cfg, StrVec& keyVec) {
#if !defined(NDEBUG)
  for (size_t i = 1; i < keyVec.size(); ++i) {
    fstring prev = keyVec[i - 1];
    fstring curr = keyVec[i];
    assert(prev < curr);
  }
#endif
  if (keyVec.mem_size() < 0x1E0000000) {
    if (type.endsWith("IL_256_32") || type.endsWith("IL_256")) {
      return NestLoudsTriePrefixProcess<NestLoudsTrieDAWG_IL_256>(cfg, keyVec);
    }
    if (type.endsWith("IL_256_32_FL") || type.endsWith("IL_256_FL")) {
      return NestLoudsTriePrefixProcess<NestLoudsTrieDAWG_IL_256_32_FL>(cfg, keyVec);
    }
    if (type.endsWith("Mixed_SE_512")) {
      return NestLoudsTriePrefixProcess<NestLoudsTrieDAWG_Mixed_SE_512>(cfg, keyVec);
    }
    if (type.endsWith("Mixed_SE_512_32_FL") || type.endsWith("Mixed_SE_512_FL")) {
      return NestLoudsTriePrefixProcess<NestLoudsTrieDAWG_Mixed_SE_512_32_FL>(cfg, keyVec);
    }
    if (type.endsWith("Mixed_IL_256")) {
      return NestLoudsTriePrefixProcess<NestLoudsTrieDAWG_Mixed_IL_256>(cfg, keyVec);
    }
    if (type.endsWith("Mixed_IL_256_32_FL") || type.endsWith("Mixed_IL_256_FL")) {
      return NestLoudsTriePrefixProcess<NestLoudsTrieDAWG_Mixed_IL_256_32_FL>(cfg, keyVec);
    }
    if (type.endsWith("Mixed_XL_256")) {
      return NestLoudsTriePrefixProcess<NestLoudsTrieDAWG_Mixed_XL_256>(cfg, keyVec);
    }
    if (type.endsWith("Mixed_XL_256_32_FL") || type.endsWith("Mixed_XL_256_FL")) {
      return NestLoudsTriePrefixProcess<NestLoudsTrieDAWG_Mixed_XL_256_32_FL>(cfg, keyVec);
    }
  }
  if (type.endsWith("SE_512_64") || type.endsWith("SE_512")) {
    return NestLoudsTriePrefixProcess<NestLoudsTrieDAWG_SE_512_64>(cfg, keyVec);
  }
  if (type.endsWith("SE_512_64_FL") || type.endsWith("SE_512_FL")) {
    return NestLoudsTriePrefixProcess<NestLoudsTrieDAWG_SE_512_64_FL>(cfg, keyVec);
  }
  if (keyVec.mem_size() < 0x1E0000000) {
    return NestLoudsTriePrefixProcess<NestLoudsTrieDAWG_Mixed_XL_256_32_FL>(cfg, keyVec);
  } else {
    return NestLoudsTriePrefixProcess<NestLoudsTrieDAWG_SE_512_64_FL>(cfg, keyVec);
  }
}

template<class InputBufferType>
void
IndexFillKeyVector(InputBufferType& input, FixedLenStrVec& keyVec, size_t numKeys, size_t sumKeyLen, size_t fixedLen,
                   bool isReverse) {
  if (isReverse) {
    keyVec.m_size = numKeys;
    keyVec.m_strpool.resize(sumKeyLen);
    for (size_t i = numKeys; i > 0;) {
      --i;
      auto str = input.next();
      assert(str.size() == fixedLen);
      memcpy(keyVec.m_strpool.data() + fixedLen * i, str.data(), fixedLen);
    }
  } else {
    keyVec.reserve(numKeys, sumKeyLen);
    for (size_t i = 0; i < numKeys; ++i) {
      keyVec.push_back(input.next());
    }
  }
}

template<class InputBufferType>
void
IndexFillKeyVector(InputBufferType& input, SortedStrVec& keyVec, size_t numKeys, size_t sumKeyLen, bool isReverse) {
  if (isReverse) {
    keyVec.m_offsets.resize_with_wire_max_val(numKeys + 1, sumKeyLen);
    keyVec.m_offsets.set_wire(numKeys, sumKeyLen);
    keyVec.m_strpool.resize(sumKeyLen);
    size_t offset = sumKeyLen;
    for (size_t i = numKeys; i > 0;) {
      --i;
      auto str = input.next();
      offset -= str.size();
      memcpy(keyVec.m_strpool.data() + offset, str.data(), str.size());
      keyVec.m_offsets.set_wire(i, offset);
    }
    assert(offset == 0);
  } else {
    keyVec.reserve(numKeys, sumKeyLen);
    for (size_t i = 0; i < numKeys; ++i) {
      keyVec.push_back(input.next());
    }
  }
}

template<class InputBufferType>
PrefixBase*
BuildNestLoudsTriePrefix(
    InputBufferType& input,
    const TerarkZipTableOptions& tzopt,
    size_t numKeys, size_t sumKeyLen,
    bool isReverse, bool isFixedLen) {
  input.rewind();
  NestLoudsTrieConfig cfg;
  if (isFixedLen) {
    FixedLenStrVec keyVec(sumKeyLen / numKeys);
    assert(sumKeyLen % numKeys == 0);
    IndexFillKeyVector(input, keyVec, numKeys, sumKeyLen, keyVec.m_fixlen, isReverse);
    NestLoudsTriePrefixSetConfig(cfg, keyVec.mem_size(), keyVec.avg_size(), tzopt);
    return NestLoudsTriePrefixSelect(tzopt.indexType, cfg, keyVec);
  } else {
    SortedStrVec keyVec;
    IndexFillKeyVector(input, keyVec, numKeys, sumKeyLen, isReverse);
    NestLoudsTriePrefixSetConfig(cfg, keyVec.mem_size(), keyVec.avg_size(), tzopt);
    return NestLoudsTriePrefixSelect(tzopt.indexType, cfg, keyVec);
  }
}

SuffixBase*
BuildEmptySuffix() {
  return new IndexEmptySuffix();
}

bool UseEntropySuffix(size_t numKeys, size_t sumKeyLen, double zipRatio) {
  return g_indexEnableEntropySuffix && numKeys > 4096 && sumKeyLen * zipRatio + 1024 < sumKeyLen;
}

template<class InputBufferType>
SuffixBase*
BuildFixedStringSuffix(
    InputBufferType& input,
    size_t numKeys, size_t sumKeyLen, size_t fixedLen, bool isReverse) {
  assert(g_indexEnableCompositeIndex);
  input.rewind();
  FixedLenStrVec keyVec(fixedLen);
  IndexFillKeyVector(input, keyVec, numKeys, sumKeyLen, fixedLen, false);
  auto suffix = new IndexFixedStringSuffix();
  suffix->str_pool_.swap(keyVec);
  suffix->flags.is_rev_suffix = isReverse;
  return suffix;
}

bool UseDictZipSuffix(size_t numKeys, size_t sumKeyLen, double zipRatio) {
  return g_indexEnableDictZipSuffix && numKeys > 8192 && sumKeyLen > numKeys * 16 &&
         (sumKeyLen + numKeys * 4) * zipRatio + 1024 < sumKeyLen + numKeys;
}

template<class InputBufferType>
SuffixBase*
BuildVerLenSuffix(
    InputBufferType& input,
    size_t numKeys, size_t sumKeyLen, bool isReverse) {
  assert(g_indexEnableCompositeIndex);
  assert(g_indexEnableDynamicSuffix);
  input.rewind();
  FileMemIO memory;
  terark::ZipOffsetBlobStore::MyBuilder builder(128, memory);
  for (size_t i = 0; i < numKeys; ++i) {
    auto str = input.next();
    builder.addRecord(str);
  }
  builder.finish();
  memory.shrink_to_fit();
  auto store = AbstractBlobStore::load_from_user_memory(
      fstring(memory.begin(), memory.size()), AbstractBlobStore::Dictionary());
  assert(dynamic_cast<ZipOffsetBlobStore*>(store) != nullptr);
#ifndef NDEBUG
  input.rewind();
  for (size_t i = 0; i < numKeys; ++i) {
    auto str = input.next();
    assert(str == store->get_record(i));
  }
#endif
  return new IndexBlobStoreSuffix<ZipOffsetBlobStore>(static_cast<ZipOffsetBlobStore*>(store), memory, isReverse);
}


template<class InputBufferType>
SuffixBase*
BuildEntropySuffix(
    InputBufferType& input,
    size_t numKeys, size_t sumKeyLen, bool isReverse) {
  assert(g_indexEnableCompositeIndex);
  assert(g_indexEnableEntropySuffix);
  input.rewind();
  std::unique_ptr<freq_hist_o1> freq(new freq_hist_o1);
  for (size_t i = 0; i < numKeys; ++i) {
    freq->add_record(input.next());
  }
  freq->finish();
  FileMemIO memory;
  EntropyZipBlobStore::MyBuilder builder(*freq, 128, memory);
  input.rewind();
  for (size_t i = 0; i < numKeys; ++i) {
    builder.addRecord(input.next());
  }
  builder.finish();
  memory.shrink_to_fit();
  auto store = AbstractBlobStore::load_from_user_memory(
      fstring(memory.begin(), memory.size()), AbstractBlobStore::Dictionary());
  assert(dynamic_cast<EntropyZipBlobStore*>(store) != nullptr);
#ifndef NDEBUG
  input.rewind();
  for (size_t i = 0; i < numKeys; ++i) {
    auto str = input.next();
    assert(str == store->get_record(i));
  }
#endif
  return new IndexEntropySuffix(static_cast<EntropyZipBlobStore*>(store), memory, isReverse);
}

template<class InputBufferType>
SuffixBase*
BuildDictZipSuffix(
    InputBufferType& input,
    size_t numKeys, size_t sumKeyLen, bool isReverse) {
  assert(g_indexEnableCompositeIndex);
  assert(g_indexEnableDynamicSuffix);
  input.rewind();
  FileMemIO memory;
  std::unique_ptr<DictZipBlobStore::ZipBuilder> zbuilder;
  DictZipBlobStore::Options dzopt;
  size_t avgLen = sumKeyLen / numKeys;
  dzopt.checksumLevel = 1;
  dzopt.entropyAlgo = DictZipBlobStore::Options::kHuffmanO1;
  dzopt.useSuffixArrayLocalMatch = false;
  dzopt.compressGlobalDict = true;
  dzopt.entropyInterleaved = avgLen > 256 ? 8 : avgLen > 128 ? 4 : avgLen > 64 ? 2 : 1;
  dzopt.offsetArrayBlockUnits = 128;
  dzopt.entropyZipRatioRequire = 0.95f;
  dzopt.embeddedDict = true;
  dzopt.enableLake = true;
  zbuilder.reset(DictZipBlobStore::createZipBuilder(dzopt));

  std::mt19937_64 randomGenerator;
  uint64_t upperBoundSample = uint64_t(randomGenerator.max() * 0.1);
  uint64_t sampleLenSum = 0;
  for (size_t i = 0; i < numKeys; ++i) {
    auto str = input.next();
    if (randomGenerator() < upperBoundSample) {
      zbuilder->addSample(str);
      sampleLenSum += str.size();
    }
  }
  if (sampleLenSum == 0) {
    zbuilder->addSample("Hello World!");
  }
  zbuilder->finishSample();
  zbuilder->prepareDict();
  zbuilder->prepare(numKeys, memory);
  input.rewind();
  for (size_t i = 0; i < numKeys; ++i) {
    auto str = input.next();
    zbuilder->addRecord(str);
  }
  zbuilder->finish(DictZipBlobStore::ZipBuilder::FinishFreeDict);
  zbuilder.reset();
  memory.shrink_to_fit();
  auto store = AbstractBlobStore::load_from_user_memory(
      fstring(memory.begin(), memory.size()), AbstractBlobStore::Dictionary());
  assert(dynamic_cast<DictZipBlobStore*>(store) != nullptr);
#ifndef NDEBUG
  input.rewind();
  for (size_t i = 0; i < numKeys; ++i) {
    auto str = input.next();
    assert(str == store->get_record(i));
  }
#endif
  return new IndexBlobStoreSuffix<DictZipBlobStore>(static_cast<DictZipBlobStore*>(store), memory, isReverse);
}

bool UseRawSuffix(size_t numKeys, size_t sumKeyLen, double zipRatio) {
  return !UseEntropySuffix(numKeys, sumKeyLen, zipRatio) && !UseEntropySuffix(numKeys, sumKeyLen, zipRatio);
}

template<class InputBufferType>
SuffixBase*
BuildSuffixAutoSelect(
    InputBufferType& input, size_t numKeys, size_t sumKeyLen, bool isFixedLen, bool isReverse, double zipRatio) {
  if (sumKeyLen == 0) {
    return BuildEmptySuffix();
  } else if (UseDictZipSuffix(numKeys, sumKeyLen, zipRatio)) {
    return BuildDictZipSuffix(input, numKeys, sumKeyLen, isReverse);
  } else if (UseEntropySuffix(numKeys, sumKeyLen, zipRatio)) {
    return BuildEntropySuffix(input, numKeys, sumKeyLen, isReverse);
  } else if (isFixedLen) {
    assert(sumKeyLen % numKeys == 0);
    return BuildFixedStringSuffix(input, numKeys, sumKeyLen, sumKeyLen / numKeys, isReverse);
  } else {
    return BuildVerLenSuffix(input, numKeys, sumKeyLen, isReverse);
  }
}

}

UintPrefixBuildInfo TerarkIndex::GetUintPrefixBuildInfo(const TerarkIndex::KeyStat& ks) {
  size_t cplen = ks.keyCount > 1 ? commonPrefixLen(ks.minKey, ks.maxKey) : 0;
  auto GetZipRatio = [&](size_t p) {
    if (ks.sumKeyLen < 1024) {
      return 1.0;
    }
    return std::pow(double(ks.entropyLen) / ks.sumKeyLen, double(ks.sumKeyLen - p * ks.keyCount) / ks.sumKeyLen);
  };
  UintPrefixBuildInfo result = {
      cplen, 0, 0, 0, 0, 0, 0, 0, GetZipRatio(0), 0, UintPrefixBuildInfo::fail
  };
  if (!g_indexEnableUintIndex || (!g_indexEnableDynamicSuffix && ks.maxKeyLen != ks.minKeyLen)) {
    return result;
  }
  size_t keyCount = ks.keyCount;
  size_t maxPrefixLen = std::min<size_t>(8, ks.minKeyLen - cplen);
  size_t totalKeySize = ks.sumKeyLen - keyCount * cplen;
  size_t best_estimate_size = totalKeySize;
  if (ks.minKeyLen != ks.maxKeyLen) {
    best_estimate_size += keyCount;
  }
  size_t target_estimate_size = best_estimate_size * 4 / 5;
  size_t entryCnt[8] = {};
  for (size_t i = cplen, e = cplen + 8; i < e; ++i) {
    entryCnt[i - cplen] = keyCount - (i < ks.diff.size() ? ks.diff[i].cnt : 0);
  }
  for (size_t i = 2; i <= maxPrefixLen; ++i) {
    if (!g_indexEnableCompositeIndex && (ks.maxKeyLen != ks.minKeyLen || cplen + i != ks.maxKeyLen)) {
      continue;
    }
    if (cplen + i < ks.diff.size() && (ks.diff[cplen + i].max > 32 || entryCnt[i - 1] * 4 < keyCount)) {
      continue;
    }
    UintPrefixBuildInfo info;
    info.common_prefix = cplen;
    info.key_length = i;
    info.key_count = keyCount;
    info.min_value = ReadBigEndianUint64(ks.minKey.begin() + cplen, i);
    info.max_value = ReadBigEndianUint64(ks.maxKey.begin() + cplen, i);
    info.zip_ratio = GetZipRatio(cplen + i);
    if (info.min_value > info.max_value) std::swap(info.min_value, info.max_value);
    uint64_t diff = info.max_value - info.min_value;
    info.entry_count = entryCnt[i - 1];
    assert(info.entry_count > 0);
    assert(diff >= info.entry_count - 1);
    size_t bit_count;
    bool useFewOne = g_indexEnableFewZero;
    bool useFewZero = g_indexEnableFewZero;
    bool useNormal = true;
    if (info.entry_count == keyCount) {
      // ascending
      info.bit_count0 = diff - keyCount + 1;
      info.bit_count1 = keyCount;
      if (diff == std::numeric_limits<uint64_t>::max()) {
        bit_count = 0;
        useFewZero = false;
        useNormal = false;
      } else {
        bit_count = info.bit_count0 + info.bit_count1;
        if (info.bit_count0 > bit_count / (i * 9)) {
          useFewZero = false;
        }
        if (info.bit_count1 > bit_count / (i * 9)) {
          useFewOne = false;
        }
      }
    } else {
      // non descending
      info.bit_count1 = keyCount;
      if (!g_indexEnableNonDescUint || keyCount + 1 > std::numeric_limits<uint64_t>::max() - diff) {
        info.bit_count0 = 0;
        bit_count = 0;
        useFewOne = false;
        useNormal = false;
      } else {
        info.bit_count0 = diff + 1;
        bit_count = diff + keyCount + 1;
        if (info.bit_count1 > bit_count / (i * 9)) {
          useFewOne = false;
        }
      }
      useFewZero = false;
    }
    size_t prefixCost;
    using UintType = UintPrefixBuildInfo::UintType;
    if (info.entry_count == keyCount && info.entry_count == diff + 1) {
      info.type = UintType::asc_allone;
      prefixCost = 0;
    } else if (useFewOne) {
      assert(bit_count > 0);
      /*****/if (bit_count < (1ULL << 24)) {
        info.type = info.entry_count == keyCount ? UintType::asc_few_one_3 : UintType::non_desc_few_one_3;
      } else if (bit_count < (1ULL << 32)) {
        info.type = info.entry_count == keyCount ? UintType::asc_few_one_4 : UintType::non_desc_few_one_4;
      } else if (bit_count < (1ULL << 40)) {
        info.type = info.entry_count == keyCount ? UintType::asc_few_one_5 : UintType::non_desc_few_one_5;
      } else if (bit_count < (1ULL << 48)) {
        info.type = info.entry_count == keyCount ? UintType::asc_few_one_6 : UintType::non_desc_few_one_6;
      } else if (bit_count < (1ULL << 56)) {
        info.type = info.entry_count == keyCount ? UintType::asc_few_one_7 : UintType::non_desc_few_one_7;
      } else {
        info.type = info.entry_count == keyCount ? UintType::asc_few_one_8 : UintType::non_desc_few_one_8;
      }
      prefixCost = info.bit_count1 * i * 256 / 255;
    } else if (useFewZero) {
      assert(bit_count > 0);
      assert(info.entry_count == keyCount);
      /*****/if (bit_count < (1ULL << 24)) {
        info.type = UintType::asc_few_zero_3;
      } else if (bit_count < (1ULL << 32)) {
        info.type = UintType::asc_few_zero_4;
      } else if (bit_count < (1ULL << 40)) {
        info.type = UintType::asc_few_zero_5;
      } else if (bit_count < (1ULL << 48)) {
        info.type = UintType::asc_few_zero_6;
      } else if (bit_count < (1ULL << 56)) {
        info.type = UintType::asc_few_zero_7;
      } else {
        info.type = UintType::asc_few_zero_8;
      }
      prefixCost = info.bit_count0 * i * 256 / 255;
    } else if (useNormal) {
      assert(bit_count > 0);
      if (bit_count <= std::numeric_limits<uint32_t>::max()) {
        info.type = info.entry_count == keyCount ? UintType::asc_il_256 : UintType::non_desc_il_256;
      } else {
        info.type = info.entry_count == keyCount ? UintType::asc_se_512 : UintType::non_desc_se_512;
      }
      prefixCost = bit_count * 21 / 128;
    } else {
      continue;
    }
    size_t suffixCost = totalKeySize - i * keyCount;
    if (index_detail::UseDictZipSuffix(keyCount, suffixCost, info.zip_ratio)) {
      suffixCost = (suffixCost + 8 * keyCount) * info.zip_ratio + 1024;
    } else if (index_detail::UseEntropySuffix(keyCount, suffixCost, info.zip_ratio)) {
      suffixCost = suffixCost * info.zip_ratio + keyCount + 1024;
    } else if (ks.minKeyLen != ks.maxKeyLen) {
      suffixCost += keyCount;
    }
    info.estimate_size = prefixCost + suffixCost;
    if (info.estimate_size <= best_estimate_size && info.estimate_size <= target_estimate_size) {
      result = info;
      best_estimate_size = info.estimate_size;
    }
  }
  return result;
};


void TerarkIndex::TerarkIndexDebugBuilder::Init(size_t count) {
  freq.clear();
  stat.~KeyStat();
  ::new(&stat) KeyStat;
  data.erase_all();
  stat.keyCount = count;
  keyCount = 0;
  prevSamePrefix = 0;
}

void TerarkIndex::TerarkIndexDebugBuilder::Add(fstring key) {
  freq.add_record(key);
  auto processKey = [&](fstring key, size_t samePrefix) {
    size_t prefixSize = std::min(key.size(), std::max(samePrefix, prevSamePrefix) + 1);
    size_t suffixSize = key.size() - prefixSize;
    stat.minKeyLen = std::min(key.size(), stat.minKeyLen);
    stat.maxKeyLen = std::max(key.size(), stat.maxKeyLen);
    stat.sumKeyLen += key.size();
    stat.sumPrefixLen += prefixSize;
    stat.minPrefixLen = std::min(stat.minPrefixLen, prefixSize);
    stat.maxPrefixLen = std::max(stat.maxPrefixLen, prefixSize);
    stat.minSuffixLen = std::min(stat.minSuffixLen, suffixSize);
    stat.maxSuffixLen = std::max(stat.maxSuffixLen, suffixSize);
    auto& diff = stat.diff;
    if (diff.size() < samePrefix) {
      diff.resize(samePrefix);
    }
    for (size_t i = 0; i < samePrefix; ++i) {
      ++diff[i].cur;
      ++diff[i].cnt;
    }
    for (size_t i = samePrefix; i < diff.size(); ++i) {
      diff[i].max = std::max(diff[i].cur, diff[i].max);
      diff[i].cur = 0;
    }
    prevSamePrefix = samePrefix;
  };
  if (keyCount++ == 0) {
    data.push_back(key);
    stat.minKey.assign(key);
  } else {
    auto last = data.back();
    processKey(last, key.commonPrefixLen(last));
    data.push_back(key);
    if (keyCount == stat.keyCount) {
      processKey(key, 0);
      stat.maxKey.assign(key);
    }
  }
}

TerarkKeyReader* TerarkIndex::TerarkIndexDebugBuilder::Finish(KeyStat* output) {
  freq.finish();
  stat.entropyLen = freq_hist_o1::estimate_size(freq.histogram());
  *output = std::move(stat);
  class TerarkKeyDebugReader : public TerarkKeyReader {
  public:
    fstrvec data;
    size_t i;

    fstring next() final {
      return data[i++];
    }
    void rewind() final {
      i = 0;
    }
  };
  auto reader = new TerarkKeyDebugReader;
  reader->data.swap(data);
  reader->i = 0;
  return reader;
}

TerarkIndex* TerarkIndex::Factory::Build(TerarkKeyReader* reader, const TerarkZipTableOptions& tzopt,
                                         const TerarkIndex::KeyStat& ks, const UintPrefixBuildInfo* info_ptr) {
  using namespace index_detail;
  struct DefaultInputBuffer {
    TerarkKeyReader* reader;
    size_t cplen;

    fstring next() {
      auto buffer = reader->next();
      return {buffer.data() + cplen, ptrdiff_t(buffer.size() - cplen)};
    }

    void rewind() {
      reader->rewind();
    }

    DefaultInputBuffer(TerarkKeyReader* _reader, size_t _cplen)
        : reader(_reader), cplen(_cplen) {
    }
  };
  struct MinimizePrefixInputBuffer {
    TerarkKeyReader* reader;
    size_t cplen;
    size_t count;
    size_t pos;
    valvec<byte_t> last;
    valvec<byte_t> buffer;
    size_t lastSamePrefix;

    fstring next() {
      size_t maxSamePrefix;
      if (++pos == count) {
        maxSamePrefix = lastSamePrefix + 1;
        last.swap(buffer);
      } else {
        buffer.assign(reader->next());
        size_t samePrefix = commonPrefixLen(buffer, last);
        last.swap(buffer);
        maxSamePrefix = std::max(samePrefix, lastSamePrefix) + 1;
        lastSamePrefix = samePrefix;
      }
      return {buffer.data() + cplen, ptrdiff_t(std::min(maxSamePrefix, buffer.size()) - cplen)};
    }

    void rewind() {
      reader->rewind();
      assert(count > 0);
      last.assign(reader->next());
      lastSamePrefix = 0;
      pos = 0;
    }

    MinimizePrefixInputBuffer(TerarkKeyReader* _reader, size_t _cplen, size_t _keyCount,
                              size_t _maxKeyLen)
        : reader(_reader), cplen(_cplen), count(_keyCount), last(_maxKeyLen, valvec_reserve()),
          buffer(_maxKeyLen, valvec_reserve()) {
    }
  };
  struct MinimizePrefixRemainingInputBuffer {
    TerarkKeyReader* reader;
    size_t cplen;
    size_t count;
    size_t pos;
    valvec<byte_t> last;
    valvec<byte_t> buffer;
    size_t lastSamePrefix;

    fstring next() {
      size_t maxSamePrefix;
      if (++pos == count) {
        maxSamePrefix = lastSamePrefix + 1;
        last.swap(buffer);
      } else {
        buffer.assign(reader->next());
        size_t samePrefix = commonPrefixLen(buffer, last);
        last.swap(buffer);
        maxSamePrefix = std::max(samePrefix, lastSamePrefix) + 1;
        lastSamePrefix = samePrefix;
      }
      return fstring(buffer).substr(std::min(maxSamePrefix, buffer.size()));
    }

    void rewind() {
      reader->rewind();
      assert(count > 0);
      last.assign(reader->next());
      lastSamePrefix = 0;
      pos = 0;
    }

    MinimizePrefixRemainingInputBuffer(TerarkKeyReader* _reader, size_t _cplen, size_t _keyCount,
                                       size_t _maxKeyLen)
        : reader(_reader), cplen(_cplen), count(_keyCount), last(_maxKeyLen, valvec_reserve()),
          buffer(_maxKeyLen, valvec_reserve()) {
    }
  };
  struct FixPrefixInputBuffer {
    TerarkKeyReader* reader;
    size_t cplen;
    size_t cplenPrefixSize;

    fstring next() {
      auto buffer = reader->next();
      assert(buffer.size() >= cplenPrefixSize);
      return {buffer.data() + cplen, buffer.data() + cplenPrefixSize};
    }

    void rewind() {
      reader->rewind();
    }

    FixPrefixInputBuffer(TerarkKeyReader* _reader, size_t _cplen, size_t _prefixSize, size_t _maxKeyLen)
        : reader(_reader), cplen(_cplen), cplenPrefixSize(_cplen + _prefixSize) {
    }
  };
  struct FixPrefixRemainingInputBuffer {
    TerarkKeyReader* reader;
    size_t cplenPrefixSize;

    fstring next() {
      auto buffer = reader->next();
      assert(buffer.size() >= cplenPrefixSize);
      return {buffer.data() + cplenPrefixSize, ptrdiff_t(buffer.size() - cplenPrefixSize)};
    }

    void rewind() {
      reader->rewind();
    }

    FixPrefixRemainingInputBuffer(TerarkKeyReader* _reader, size_t _cplen, size_t _prefixSize,
                                  size_t _maxKeyLen)
        : reader(_reader), cplenPrefixSize(_cplen + _prefixSize) {
    }
  };

  assert(ks.keyCount > 0);
  bool isReverse = ks.minKey > ks.maxKey;
  UintPrefixBuildInfo uint_prefix_info = info_ptr != nullptr ? *info_ptr : GetUintPrefixBuildInfo(ks);
  size_t cplen = uint_prefix_info.common_prefix;
  PrefixBase* prefix;
  SuffixBase* suffix;
  if (uint_prefix_info.key_length > 0) {
    if (ks.minKeyLen == ks.maxKeyLen && ks.maxKeyLen == cplen + uint_prefix_info.key_length) {
      DefaultInputBuffer input_reader{reader, cplen};
      prefix = BuildUintPrefix(input_reader, ks, uint_prefix_info);
      suffix = BuildEmptySuffix();
    } else {
      FixPrefixInputBuffer prefix_input_reader{reader, cplen, uint_prefix_info.key_length, ks.maxKeyLen};
      prefix = BuildUintPrefix(prefix_input_reader, ks, uint_prefix_info);
      FixPrefixRemainingInputBuffer suffix_input_reader{reader, cplen, uint_prefix_info.key_length, ks.maxKeyLen};
      suffix = BuildSuffixAutoSelect(suffix_input_reader, ks.keyCount,
                                     ks.sumKeyLen - ks.keyCount * prefix_input_reader.cplenPrefixSize,
                                     ks.minKeyLen == ks.maxKeyLen, isReverse, uint_prefix_info.zip_ratio);
    }
  } else if ((!g_indexEnableDynamicSuffix && ks.minSuffixLen != ks.maxSuffixLen) ||
             !g_indexEnableCompositeIndex || ks.sumPrefixLen >= ks.sumKeyLen * 31 / 32) {
    DefaultInputBuffer input_reader{reader, cplen};
    prefix = BuildNestLoudsTriePrefix(
        input_reader, tzopt, ks.keyCount, ks.sumKeyLen - ks.keyCount * cplen,
        isReverse, ks.minKeyLen == ks.maxKeyLen);
    suffix = BuildEmptySuffix();
  } else {
    MinimizePrefixInputBuffer prefix_input_reader{reader, cplen, ks.keyCount, ks.maxKeyLen};
    prefix = BuildNestLoudsTriePrefix(
        prefix_input_reader, tzopt, ks.keyCount, ks.sumPrefixLen - ks.keyCount * cplen,
        isReverse, ks.minPrefixLen == ks.maxPrefixLen);
    MinimizePrefixRemainingInputBuffer suffix_input_reader{reader, cplen, ks.keyCount, ks.maxKeyLen};
    suffix = BuildSuffixAutoSelect(suffix_input_reader, ks.keyCount, ks.sumKeyLen - ks.sumPrefixLen,
                                   ks.minSuffixLen == ks.maxSuffixLen, isReverse, uint_prefix_info.zip_ratio);
  }
  valvec<char> common(cplen, valvec_reserve());
  common.append(ks.minKey.data(), cplen);
  auto factory = IndexFactoryBase::GetFactoryByType(std::type_index(typeid(*prefix)), std::type_index(typeid(*suffix)));
  assert(factory != nullptr);
  return factory->CreateIndex(nullptr, Common(common, true), prefix, suffix);
}

size_t TerarkIndex::Factory::MemSizeForBuild(const TerarkIndex::KeyStat& ks) {
  size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
  size_t indexSize = UintVecMin0::compute_mem_size_by_max_val(ks.sumKeyLen - cplen, ks.keyCount);
  return ks.sumKeyLen - ks.keyCount * commonPrefixLen(ks.minKey, ks.maxKey) + indexSize;
}

////////////////////////////////////////////////////////////////////////////////

unique_ptr<TerarkIndex> TerarkIndex::LoadMemory(fstring mem) {
  valvec<unique_ptr<TerarkIndex>> index_vec;
  size_t offset = 0;
  do {
    if (mem.size() - offset < sizeof(TerarkIndexFooter)) {
      throw std::invalid_argument("TerarkIndex::LoadMemory(): Bad mem size");
    }
    auto& footer = ((const TerarkIndexFooter*)(mem.data() + mem.size() - offset))[-1];
    auto crc = terark::Crc32c_update(0, &footer, sizeof footer - sizeof(uint32_t));
    if (crc != footer.footer_crc32) {
      throw terark::BadCrc32cException("TerarkIndex::LoadMemory(): Bad footer crc",
                                       footer.footer_crc32, crc);
    }
    size_t idx = g_TerarkIndexFactroy.find_i(fstring(footer.class_name, sizeof footer.class_name));
    if (idx >= g_TerarkIndexFactroy.end_i()) {
      std::string class_name(footer.class_name, sizeof footer.class_name);
      throw std::invalid_argument(
          std::string("TerarkIndex::LoadMemory(): Unknown class: ")
          + class_name.c_str());
    }
    TerarkIndex::Factory* factory = g_TerarkIndexFactroy.val(idx).get();
    size_t index_size = footer.footer_size + align_up(footer.common_size, 8) + footer.prefix_size + footer.suffix_size;
    index_vec.emplace_back(factory->LoadMemory(mem.substr(mem.size() - index_size)));
    offset += index_size;
  } while (offset < mem.size());
  if (index_vec.size() == 1) {
    return std::move(index_vec.front());
  } else {
    std::reverse(index_vec.begin(), index_vec.end());
    // TODO TerarkUnionIndex
    return nullptr;
  }
}

template<char... chars_t>
struct StringHolder {
  static fstring Name() {
    static char str[] = {chars_t ...};
    return fstring{str, sizeof(str)};
  }
};
#define _G(name,i) ((i+1)<sizeof(#name)?#name[i]:' ')
#define NAME(s) StringHolder<                                              \
  _G(s, 0),_G(s, 1),_G(s, 2),_G(s, 3),_G(s, 4),_G(s, 5),_G(s, 6),_G(s, 7), \
  _G(s, 8),_G(s, 9),_G(s,10),_G(s,11),_G(s,12),_G(s,13),_G(s,14),_G(s,15)>

template<class N, size_t V>
struct ComponentInfo {
  static fstring Name() {
    return N::Name();
  }
  static constexpr size_t use_virtual = V;
};

template<class I, class T>
struct Component {
  using info = I;
  using type = T;
};

template<class ...args_t>
struct ComponentList;

template<class T, class ...next_t>
struct ComponentList<T, next_t...> {
  using type = T;
  using next = ComponentList<next_t...>;

  template<class N> struct push_back { using type = ComponentList<T, next_t..., N>; };
};
template<>
struct ComponentList<> {
  template<class N> struct push_back { using type = ComponentList<N>; };
};


template<class list_t = ComponentList<>>
struct ComponentRegister {
  using list = list_t;
  template<class N, size_t V, class T>
  using reg = ComponentRegister<typename list::template push_back<Component<ComponentInfo<N, V>, T>>::type>;
};

using namespace index_detail;

using PrefixComponentList_0 = ComponentRegister<>
::reg<NAME(IL_256      ), 1, IndexNestLoudsTriePrefix<NestLoudsTrieDAWG_IL_256            >>
::reg<NAME(IL_256_FL   ), 1, IndexNestLoudsTriePrefix<NestLoudsTrieDAWG_IL_256_32_FL      >>
::reg<NAME(M_SE_512    ), 1, IndexNestLoudsTriePrefix<NestLoudsTrieDAWG_Mixed_SE_512      >>
::reg<NAME(M_SE_512_FL ), 1, IndexNestLoudsTriePrefix<NestLoudsTrieDAWG_Mixed_SE_512_32_FL>>
::reg<NAME(M_IL_256    ), 1, IndexNestLoudsTriePrefix<NestLoudsTrieDAWG_Mixed_IL_256      >>
::reg<NAME(M_IL_256_FL ), 1, IndexNestLoudsTriePrefix<NestLoudsTrieDAWG_Mixed_IL_256_32_FL>>
::reg<NAME(M_XL_256    ), 1, IndexNestLoudsTriePrefix<NestLoudsTrieDAWG_Mixed_XL_256      >>
::reg<NAME(M_XL_256_FL ), 1, IndexNestLoudsTriePrefix<NestLoudsTrieDAWG_Mixed_XL_256_32_FL>>
::reg<NAME(SE_512_64   ), 1, IndexNestLoudsTriePrefix<NestLoudsTrieDAWG_SE_512_64         >>
::reg<NAME(SE_512_64_FL), 1, IndexNestLoudsTriePrefix<NestLoudsTrieDAWG_SE_512_64_FL      >>
::reg<NAME(A_AllOne    ), 0, IndexAscendingUintPrefix<rank_select_allone    >>
::reg<NAME(A_FewZero_3 ), 1, IndexAscendingUintPrefix<rank_select_fewzero<3>>>
::reg<NAME(A_FewZero_4 ), 1, IndexAscendingUintPrefix<rank_select_fewzero<4>>>
::reg<NAME(A_FewZero_5 ), 1, IndexAscendingUintPrefix<rank_select_fewzero<5>>>
::reg<NAME(A_FewZero_6 ), 1, IndexAscendingUintPrefix<rank_select_fewzero<6>>>
::reg<NAME(A_FewZero_7 ), 1, IndexAscendingUintPrefix<rank_select_fewzero<7>>>
::reg<NAME(A_FewZero_8 ), 1, IndexAscendingUintPrefix<rank_select_fewzero<8>>>
::reg<NAME(A_IL_256_32 ), 0, IndexAscendingUintPrefix<rank_select_il_256_32>>
::reg<NAME(A_SE_512_64 ), 0, IndexAscendingUintPrefix<rank_select_se_512_64>>
::reg<NAME(A_FewOne_3  ), 1, IndexAscendingUintPrefix<rank_select_fewone<3>>>
::reg<NAME(A_FewOne_4  ), 1, IndexAscendingUintPrefix<rank_select_fewone<4>>>
::reg<NAME(A_FewOne_5  ), 1, IndexAscendingUintPrefix<rank_select_fewone<5>>>
::reg<NAME(A_FewOne_6  ), 1, IndexAscendingUintPrefix<rank_select_fewone<6>>>
::reg<NAME(A_FewOne_7  ), 1, IndexAscendingUintPrefix<rank_select_fewone<7>>>
::reg<NAME(A_FewOne_8  ), 1, IndexAscendingUintPrefix<rank_select_fewone<8>>>
::list;

using SuffixComponentList_0 = ComponentRegister<>
::reg<NAME(Empty  ), 0, IndexEmptySuffix                        >
::reg<NAME(FixLen ), 0, IndexFixedStringSuffix                  >
::reg<NAME(VarLen ), 1, IndexBlobStoreSuffix<ZipOffsetBlobStore>>
::reg<NAME(Entropy), 1, IndexEntropySuffix                      >
::reg<NAME(DictZip), 1, IndexBlobStoreSuffix<DictZipBlobStore  >>
::list;

using PrefixComponentList_1 = ComponentRegister<>
::reg<NAME(ND_IL_256_32), 0, IndexNonDescendingUintPrefix<rank_select_il_256_32>>
::reg<NAME(ND_SE_512_64), 0, IndexNonDescendingUintPrefix<rank_select_se_512_64>>
::reg<NAME(ND_FewOne_3 ), 1, IndexNonDescendingUintPrefix<rank_select_fewone<3>>>
::reg<NAME(ND_FewOne_4 ), 1, IndexNonDescendingUintPrefix<rank_select_fewone<4>>>
::reg<NAME(ND_FewOne_5 ), 1, IndexNonDescendingUintPrefix<rank_select_fewone<5>>>
::reg<NAME(ND_FewOne_6 ), 1, IndexNonDescendingUintPrefix<rank_select_fewone<6>>>
::reg<NAME(ND_FewOne_7 ), 1, IndexNonDescendingUintPrefix<rank_select_fewone<7>>>
::reg<NAME(ND_FewOne_8 ), 1, IndexNonDescendingUintPrefix<rank_select_fewone<8>>>
::list;

using SuffixComponentList_1 = ComponentRegister<>
::reg<NAME(FixLen ), 0, IndexFixedStringSuffix                  >
::reg<NAME(VarLen ), 1, IndexBlobStoreSuffix<ZipOffsetBlobStore>>
::reg<NAME(Entropy), 1, IndexEntropySuffix                      >
::reg<NAME(DictZip), 1, IndexBlobStoreSuffix<DictZipBlobStore  >>
::list;


#if __clang__
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wunused-value"
#endif

template<class PrefixComponentList, class SuffixComponentList>
struct FactoryExpander {

  template<class ...args_t>
  struct FactorySet {
    FactorySet() {
      std::initializer_list<TerarkIndex::FactoryPtr>{TerarkIndex::FactoryPtr(new args_t())...};
    }
  };

  template<class L, class E, class V, class F>
  struct Iter {
    using result = typename Iter<typename L::next, E, typename F::template invoke<V, typename L::type>::type, F>::result;
  };
  template<class E, class V, class F>
  struct Iter<E, E, V, F> {
    using result = V;
  };

  template<class PreifxComponent>
  struct AddFactory {
    template<class ...args_t>
    struct invoke;

    template<class SuffixComponent, class ...args_t>
    struct invoke<FactorySet<args_t...>, SuffixComponent> {
      using factory = IndexFactory<
          typename PreifxComponent::info, typename PreifxComponent::type,
          typename SuffixComponent::info, typename SuffixComponent::type>;
      using type = FactorySet<args_t..., factory>;
    };
  };
  struct ExpandSuffix {
    template<class ...args_t>
    struct invoke;

    template<class PreifxComponent, class ...args_t>
    struct invoke<FactorySet<args_t...>, PreifxComponent> {
      using type = typename Iter<SuffixComponentList, ComponentList<>, FactorySet<args_t...>, AddFactory<PreifxComponent>>::result;
    };
  };

  using ExpandedFactorySet = typename Iter<PrefixComponentList, ComponentList<>, FactorySet<>, ExpandSuffix>::result;
};

FactoryExpander<PrefixComponentList_0, SuffixComponentList_0>::ExpandedFactorySet g_factory_init_0;
FactoryExpander<PrefixComponentList_1, SuffixComponentList_1>::ExpandedFactorySet g_factory_init_1;

#if __clang__
# pragma clang diagnostic pop
#endif

} // namespace rocksdb
