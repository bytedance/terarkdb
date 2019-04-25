
#ifndef INDEX_UT
#include "db/builder.h" // for cf_options.h
#endif

#include "terark_zip_index.h"
#include <typeindex>
#include "terark_zip_table.h"
#include "terark_zip_common.h"
#include <terark/bitmap.hpp>
#include <terark/hash_strmap.hpp>
#include <terark/fsa/dfa_mmap_header.hpp>
#include <terark/fsa/fsa_cache.hpp>
#include <terark/fsa/nest_louds_trie_inline.hpp>
#include <terark/fsa/nest_trie_dawg.hpp>
#include <terark/util/crc.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/zbs/zip_offset_blob_store.hpp>
#include <terark/zbs/dict_zip_blob_store.hpp>
#include <terark/zbs/zip_reorder_map.hpp>
#include <terark/zbs/blob_store_file_header.hpp>
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
static bool g_indexEnableFewZero = getEnvBool("TerarkZipTable_enableFewZero", false);
static bool g_indexEnableNonDescUint = getEnvBool("TerarkZipTable_enableNonDescUint", true);
static bool g_indexEnableDynamicSuffix = getEnvBool("TerarkZipTable_enableDynamicSuffix", true);
static bool g_indexEnableDictZipSuffix = getEnvBool("TerarkZipTable_enableDictZipSuffix", true);

template<class RankSelect> struct RankSelectNeedHint : public std::false_type {};
template<size_t P, size_t W> struct RankSelectNeedHint<rank_select_few<P, W>> : public std::true_type {};

//// -- fast zero-seq-len
//template<class RankSelect>
//size_t rs_zero_seq_len(const RankSelect& rs, size_t pos, size_t* hint) {
//  return rs.zero_seq_len(pos);
//}
//template<class Uint>
//size_t rs_zero_seq_len(const rank_select_fewzero<Uint>& rs, size_t pos, size_t* hint) {
//  return rs.zero_seq_len(pos, hint);
//}
//template<class Uint>
//size_t rs_zero_seq_len(const rank_select_fewone<Uint>& rs, size_t pos, size_t* hint) {
//  return rs.zero_seq_len(pos, hint);
//}
//// -- fast zero-seq-revlen
//template<class RankSelect>
//size_t rs_zero_seq_revlen(const RankSelect& rs, size_t pos, size_t* hint) {
//  return rs.zero_seq_revlen(pos);
//}
//template<class Uint>
//size_t rs_zero_seq_revlen(const rank_select_fewzero<Uint>& rs, size_t pos, size_t* hint) {
//  return rs.zero_seq_revlen(pos, hint);
//}
//template<class Uint>
//size_t rs_zero_seq_revlen(const rank_select_fewone<Uint>& rs, size_t pos, size_t* hint) {
//  return rs.zero_seq_revlen(pos, hint);
//}

static hash_strmap<TerarkIndex::FactoryPtr> g_TerarkIndexFactroy;
struct TerarkIndexTypeFactroyHash {
  size_t operator()(const std::pair<std::type_index, std::type_index>& key) const {
    return std::hash<std::type_index>()(key.first) ^ std::hash<std::type_index>()(key.second);
  }
};
static std::unordered_map<
  std::pair<std::type_index, std::type_index>,
  TerarkIndex::FactoryPtr,
  TerarkIndexTypeFactroyHash
> g_TerarkIndexTypeFactroy;

template<size_t Align, class Writer>
void Padzero(const Writer& write, size_t offset) {
  static const char zeros[Align] = { 0 };
  if (offset % Align) {
    write(zeros, Align - offset % Align);
  }
}

struct TerarkIndexFooter {
  uint32_t  common_size;
  uint32_t  common_crc32;
  uint64_t  prefix_size;
  uint64_t  prefix_xxhash;
  uint64_t  suffix_size;
  uint64_t  suffix_xxhash;

  char      class_name[40];
  uint16_t  bfs_suffix : 1;
  uint16_t  format_version : 15;
  uint16_t  footer_size;
  uint32_t  footer_crc32;
};

struct IndexUintPrefixHeader {
  uint8_t   key_length;
  uint8_t   padding_1;
  uint16_t  format_version;
  uint32_t  padding_4;
  uint64_t  min_value;
  uint64_t  max_value;
  uint64_t  rank_select_size;
};

TerarkIndex::~TerarkIndex() {}
TerarkIndex::Factory::~Factory() {}
TerarkIndex::Iterator::~Iterator() {}

namespace index_detail {

  struct StatusFlags {
    StatusFlags() : is_user_mem(0), is_bfs_suffix(0) {}

    uint64_t is_user_mem : 1;
    uint64_t is_bfs_suffix : 1;
    uint64_t : 0;
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
      }
      else {
        flags.is_user_mem = true;
        common = c;
      }
    }
    ~Common() {
      if (!flags.is_user_mem && common.n > 0) {
        free((void*)common.p);
      }
    }
    Common& operator = (const Common &) = delete;

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

  struct PrefixBase {
    StatusFlags flags;
    virtual ~PrefixBase() {}

    virtual bool Load(fstring mem) = 0;
    virtual void Save(std::function<void(const void*, size_t)> append) const = 0;
  };

  struct SuffixBase {
    StatusFlags flags;
    virtual ~SuffixBase() {}

    virtual std::pair<size_t, fstring> LowerBound(fstring target, size_t suffix_id, size_t suffix_count, valvec<byte_t>* ctx) const = 0;

    virtual bool Load(fstring mem) = 0;
    virtual void Save(std::function<void(const void*, size_t)> append) const = 0;
    virtual void Reorder(ZReorderMap& newToOld, std::function<void(const void*, size_t)> append, fstring tmpFile) const = 0;
  };

  template<class T>
  struct ComponentIteratorStorageImpl {
    size_t IteratorStorageSize() const { return sizeof(T); }
    void IteratorStorageConstruct(void* ptr) const { ::new(ptr) T(); }
    void IteratorStorageDestruct(void* ptr) const { static_cast<T*>(ptr)->~T(); }
  };
  template<>
  struct ComponentIteratorStorageImpl<void> {
    size_t IteratorStorageSize() const { return 0; }
    void IteratorStorageConstruct(void* ptr) const { }
    void IteratorStorageDestruct(void* ptr) const { }
  };

  struct VirtualPrefixBase {
    virtual ~VirtualPrefixBase() {}

    virtual size_t IteratorStorageSize() const = 0;
    virtual void IteratorStorageConstruct(void* ptr) const = 0;
    virtual void IteratorStorageDestruct(void* ptr) const = 0;

    virtual size_t KeyCount() const = 0;
    virtual size_t TotalKeySize() const = 0;
    virtual size_t Find(fstring key, const SuffixBase* suffix, valvec<byte_t>* ctx) const = 0;
    virtual size_t DictRank(fstring key, const SuffixBase* suffix, valvec<byte_t>* ctx) const = 0;
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

    size_t IteratorStorageSize() const override {
      return Prefix::IteratorStorageSize();
    }
    void IteratorStorageConstruct(void* ptr) const override {
      Prefix::IteratorStorageConstruct(ptr);
    }
    void IteratorStorageDestruct(void* ptr) const override {
      Prefix::IteratorStorageDestruct(ptr);
    }

    size_t KeyCount() const override {
      return Prefix::KeyCount();
    }
    size_t TotalKeySize() const override {
      return Prefix::TotalKeySize();
    }
    size_t Find(fstring key, const SuffixBase* suffix, valvec<byte_t>* ctx) const override {
      return Prefix::Find(key, suffix, ctx);
    }
    size_t DictRank(fstring key, const SuffixBase* suffix, valvec<byte_t>* ctx) const override {
      return Prefix::DictRank(key, suffix, ctx);
    }
    bool NeedsReorder() const override {
      return Prefix::NeedsReorder();
    }
    void GetOrderMap(UintVecMin0& newToOld) const override {
      Prefix::GetOrderMap(newToOld);
    }
    void BuildCache(double cacheRatio) override {
      Prefix::BuildCache(cacheRatio);
    }

    bool IterSeekToFirst(size_t& id, size_t& count, void* iter) const override {
      return Prefix::IterSeekToFirst(id, count, (IteratorStorage*)iter);
    }
    bool IterSeekToLast(size_t& id, size_t* count, void* iter) const override {
      return Prefix::IterSeekToLast(id, count, (IteratorStorage*)iter);
    }
    bool IterSeek(size_t& id, size_t& count, fstring target, void* iter) const override {
      return Prefix::IterSeek(id, count, target, (IteratorStorage*)iter);
    }
    bool IterNext(size_t& id, size_t count, void* iter) const override {
      return Prefix::IterNext(id, count, (IteratorStorage*)iter);
    }
    bool IterPrev(size_t& id, size_t* count, void* iter) const override {
      return Prefix::IterPrev(id, count, (IteratorStorage*)iter);
    }
    fstring IterGetKey(size_t id, const void* iter) const override {
      return Prefix::IterGetKey(id, (const IteratorStorage*)iter);
    }
    size_t IterDictRank(size_t id, const void* iter) const override {
      return Prefix::IterDictRank(id, (const IteratorStorage*)iter);
    }

    bool Load(fstring mem) override {
      return Prefix::Load(mem);
    }
    void Save(std::function<void(const void*, size_t)> append) const override {
      Prefix::Save(append);
    }
  };
  struct VirtualPrefix : public PrefixBase {
    typedef void* IteratorStorage;
    template<class Prefix>
    VirtualPrefix(Prefix&& p) {
      prefix = new VirtualPrefixWrapper<Prefix>(std::move(p));
    }
    VirtualPrefix(VirtualPrefix&& o) {
      prefix = o.prefix;
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
    void IteratorStorageConstruct(void* ptr) const {
      prefix->IteratorStorageConstruct(ptr);
    }
    void IteratorStorageDestruct(void* ptr) const {
      prefix->IteratorStorageDestruct(ptr);
    }

    size_t KeyCount() const {
      return prefix->KeyCount();
    }
    size_t TotalKeySize() const {
      return prefix->TotalKeySize();
    }
    size_t Find(fstring key, const SuffixBase* suffix, valvec<byte_t>* ctx) const {
      return prefix->Find(key, suffix, ctx);
    }
    size_t DictRank(fstring key, const SuffixBase* suffix, valvec<byte_t>* ctx) const {
      return prefix->DictRank(key, suffix, ctx);
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

    bool Load(fstring mem) override {
      return prefix->Load(mem);
    }
    void Save(std::function<void(const void*, size_t)> append) const override {
      prefix->Save(append);
    }
  };

  struct VirtualSuffixBase {
    virtual ~VirtualSuffixBase() {}

    virtual size_t IteratorStorageSize() const = 0;
    virtual void IteratorStorageConstruct(void* ptr) const = 0;
    virtual void IteratorStorageDestruct(void* ptr) const = 0;

    virtual size_t TotalKeySize() const = 0;
    virtual std::pair<size_t, fstring> LowerBound(fstring target, size_t suffix_id, size_t suffix_count, valvec<byte_t>* ctx) const = 0;

    virtual void IterSet(size_t suffix_id, void* iter) const = 0;
    virtual bool IterSeek(fstring target, size_t& suffix_id, size_t suffix_count, void* iter) const = 0;
    virtual fstring IterGetKey(size_t id, const void* iter) const = 0;

    virtual bool Load(fstring mem) = 0;
    virtual void Save(std::function<void(const void*, size_t)> append) const = 0;
    virtual void Reorder(ZReorderMap& newToOld, std::function<void(const void*, size_t)> append, fstring tmpFile) const = 0;
  };
  template<class Suffix>
  struct VirtualSuffixWrapper : public VirtualSuffixBase, public Suffix {
    using IteratorStorage = typename Suffix::IteratorStorage;
    VirtualSuffixWrapper(Suffix&& suffix) : Suffix(std::move(suffix)) {}

    size_t IteratorStorageSize() const override {
      return Suffix::IteratorStorageSize();
    }
    void IteratorStorageConstruct(void* ptr) const override {
      Suffix::IteratorStorageConstruct(ptr);
    }
    void IteratorStorageDestruct(void* ptr) const override {
      Suffix::IteratorStorageDestruct(ptr);
    }

    size_t TotalKeySize() const override {
      return Suffix::TotalKeySize();
    }
    std::pair<size_t, fstring> LowerBound(fstring target, size_t suffix_id, size_t suffix_count, valvec<byte_t>* ctx) const override {
      return Suffix::LowerBound(target, suffix_id, suffix_count, ctx);
    }

    void IterSet(size_t suffix_id, void* iter) const override {
      Suffix::IterSet(suffix_id, (IteratorStorage*)iter);
    }
    bool IterSeek(fstring target, size_t& suffix_id, size_t suffix_count, void* iter) const override {
      return Suffix::IterSeek(target, suffix_id, suffix_count, (IteratorStorage*)iter);
    }
    fstring IterGetKey(size_t id, const void* iter) const override {
      return Suffix::IterGetKey(id, (const IteratorStorage*)iter);
    }

    bool Load(fstring mem) override {
      return Suffix::Load(mem);
    }
    void Save(std::function<void(const void*, size_t)> append) const override {
      Suffix::Save(append);
    }
    void Reorder(ZReorderMap& newToOld, std::function<void(const void*, size_t)> append, fstring tmpFile) const override {
      Suffix::Reorder(newToOld, append, tmpFile);
    }
  };
  struct VirtualSuffix : public SuffixBase {
    typedef void* IteratorStorage;
    template<class Suffix>
    VirtualSuffix(Suffix&& s) {
      suffix = new VirtualSuffixWrapper<Suffix>(std::move(s));
    }
    VirtualSuffix(VirtualSuffix&& o) {
      suffix = o.suffix;
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
    void IteratorStorageConstruct(void* ptr) const {
      suffix->IteratorStorageConstruct(ptr);
    }
    void IteratorStorageDestruct(void* ptr) const {
      suffix->IteratorStorageDestruct(ptr);
    }

    size_t TotalKeySize() const {
      return suffix->TotalKeySize();
    }
    std::pair<size_t, fstring> LowerBound(fstring target, size_t suffix_id, size_t suffix_count, valvec<byte_t>* ctx) const override {
      return suffix->LowerBound(target, suffix_id, suffix_count, ctx);
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

    bool Load(fstring mem) override {
      return suffix->Load(mem);
    }
    void Save(std::function<void(const void*, size_t)> append) const override {
      suffix->Save(append);
    }
    void Reorder(ZReorderMap& newToOld, std::function<void(const void*, size_t)> append, fstring tmpFile) const override {
      suffix->Reorder(newToOld, append, tmpFile);
    }
  };

  template<class Prefix, class Suffix>
  struct IndexParts {
    IndexParts() {}
    IndexParts(Common&& common, Prefix&& prefix, Suffix&& suffix)
      : common_(std::move(common))
      , prefix_(std::move(prefix))
      , suffix_(std::move(suffix)) {
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
    std::function<void(void*, void*)> destructor_;

    template<class Prefix, class Suffix>
    static size_t GetIteratorStorageSize(const IndexParts<Prefix, Suffix>* index) {
      return 0
        + align_up(index->prefix_.IteratorStorageSize(), 8)
        + align_up(index->suffix_.IteratorStorageSize(), 8)
        ;
    }

    template<class Prefix, class Suffix>
    IteratorStorage(const IndexParts<Prefix, Suffix>* index, void* iterator_storage, size_t iterator_storage_size)
      : common_(index->common_)
      , prefix_(index->prefix_)
      , suffix_(index->suffix_) {
      assert(iterator_storage_size >= GetIteratorStorageSize(index));
      prefix_storage_ = iterator_storage;
      suffix_storage_ = (uint8_t*)prefix_storage_ + align_up(index->prefix_.IteratorStorageSize(), 8);
      if (index->prefix_.IteratorStorageSize() > 0) {
        index->prefix_.IteratorStorageConstruct(prefix_storage_);
      }
      if (index->suffix_.IteratorStorageSize() > 0) {
        index->suffix_.IteratorStorageConstruct(suffix_storage_);
      }
      destructor_ = [index](void* prefix_storage, void* suffix_storage) {
        if (index->prefix_.IteratorStorageSize() > 0) {
          index->prefix_.IteratorStorageDestruct(prefix_storage);
        }
        if (index->suffix_.IteratorStorageSize() > 0) {
          index->suffix_.IteratorStorageDestruct(suffix_storage);
        }
      };
    }
    ~IteratorStorage() {
      destructor_(prefix_storage_, suffix_storage_);
    }

  };

  struct UintPrefixBuildInfo {
    size_t key_length;
    size_t key_count;
    size_t entry_count;
    size_t bit_count0;
    size_t bit_count1;
    uint64_t min_value;
    uint64_t max_value;
    enum {
      fail = 0,
      asc_allone,
      asc_few_zero_1,
      asc_few_zero_2,
      asc_few_zero_3,
      asc_few_zero_4,
      asc_few_zero_5,
      asc_few_zero_6,
      asc_few_zero_7,
      asc_few_zero_8,
      asc_il_256,
      asc_se_512,
      asc_few_one_1,
      asc_few_one_2,
      asc_few_one_3,
      asc_few_one_4,
      asc_few_one_5,
      asc_few_one_6,
      asc_few_one_7,
      asc_few_one_8,
      non_desc_il_256,
      non_desc_se_512,
      non_desc_few_one_1,
      non_desc_few_one_2,
      non_desc_few_one_3,
      non_desc_few_one_4,
      non_desc_few_one_5,
      non_desc_few_one_6,
      non_desc_few_one_7,
      non_desc_few_one_8,
    } type;
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
      if (!s->Load(suffix)) {
        throw std::invalid_argument("TerarkIndex::LoadMemory Suffix Fail, bad mem");
      }
      return unique_ptr<TerarkIndex>(CreateIndex(Common(common, false), p.release(), s.release()));
    }
    template<class Prefix, class Suffix>
    void SaveMmap(const IndexParts<Prefix, Suffix>* index,
      std::function<void(const void *, size_t)> write) const {
      SaveMmap(index->common_, index->prefix_, index->suffix_, write);
    }
    template<class Prefix, class Suffix>
    void Reorder(const IndexParts<Prefix, Suffix>* index,
      ZReorderMap& newToOld, std::function<void(const void *, size_t)> write, fstring tmpFile) const {
      Reorder(index->common_, index->prefix_, index->suffix_, newToOld, write, tmpFile);
    }

    virtual void SaveMmap(const Common& common, const PrefixBase& prefix, const SuffixBase& suffix, std::function<void(const void *, size_t)> write) const {
      TerarkIndexFooter footer;
      footer.format_version = 0;
      footer.bfs_suffix = prefix.flags.is_bfs_suffix;
      footer.footer_size = sizeof footer;
      footer.common_size = common.size();
      footer.common_crc32 = Crc32c_update(0, common.data(), common.size());
      write(common.data(), common.size());
      Padzero<8>(write, common.size());
      XXHash64 dist(g_terark_index_prefix_seed);
      footer.prefix_size = 0;
      prefix.Save([&](const void *data, size_t size) {
        dist.update(data, size);
        write(data, size);
        footer.prefix_size += size;
      });
      assert(footer.prefix_size % 8 == 0);
      footer.prefix_xxhash = dist.digest();
      dist.reset(g_terark_index_suffix_seed);
      footer.suffix_size = 0;
      suffix.Save([&](const void *data, size_t size) {
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
    virtual void Reorder(const Common& common, const PrefixBase& prefix, const SuffixBase& suffix, ZReorderMap& newToOld, std::function<void(const void *, size_t)> write, fstring tmpFile) const {
      TerarkIndexFooter footer;
      footer.format_version = 0;
      footer.bfs_suffix = true;
      footer.footer_size = sizeof footer;
      footer.common_size = common.size();
      footer.common_crc32 = Crc32c_update(0, common.data(), common.size());
      write(common.data(), common.size());
      Padzero<8>(write, common.size());
      XXHash64 dist(g_terark_index_prefix_seed);
      footer.prefix_size = 0;
      prefix.Save([&](const void *data, size_t size) {
        dist.update(data, size);
        write(data, size);
        footer.prefix_size += size;
      });
      footer.prefix_xxhash = dist.digest();
      dist.reset(g_terark_index_suffix_seed);
      footer.suffix_size = 0;
      suffix.Reorder(newToOld, [&](const void *data, size_t size) {
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

    virtual TerarkIndex* CreateIndex(
      Common&& common,
      PrefixBase* prefix,
      SuffixBase* suffix) const {
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
    : public TerarkIndex::Iterator
    , public IteratorStorage {
  public:
    using IteratorStorage = IteratorStorage;

    using TerarkIndex::Iterator::m_id;
    using IteratorStorage::common_;
    using IteratorStorage::prefix_;
    using IteratorStorage::suffix_;
    using IteratorStorage::prefix_storage_;
    using IteratorStorage::suffix_storage_;
    std::unique_ptr<byte_t> iterator_storage_;
    mutable valvec<byte_t> iterator_key_;

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
    std::pair<void*, size_t> AllocIteratorStorage_(const IndexParts<Prefix, Suffix>* index) {
      size_t iterator_storage_size = index == nullptr ? 0 : IteratorStorage::GetIteratorStorageSize(index);
      void* ptr = iterator_storage_size > 0 ? ::new byte_t[iterator_storage_size] : nullptr;
      return { ptr, iterator_storage_size };
    }
    IndexIterator(const IndexParts<Prefix, Suffix>* index, bool user_mem, std::pair<void*, size_t> iterator_storage)
      : IteratorStorage(index, iterator_storage.first, iterator_storage.second) {
      if (user_mem) {
        iterator_storage_.reset((byte_t*)iterator_storage.first);
      }
    }

  public:
    IndexIterator(const IndexParts<Prefix, Suffix>* index, void* iterator_storage, size_t iterator_storage_size)
      : IndexIterator(index, true, { iterator_storage, iterator_storage_size }) {}

    IndexIterator(const IndexParts<Prefix, Suffix>* index)
      : IndexIterator(index, false, AllocIteratorStorage_(index)) {}

    bool SeekToFirst() override {
      size_t suffix_count;
      if (!prefix().IterSeekToFirst(m_id, suffix_count, prefix_storage())) {
        assert(m_id == size_t(-1));
        return false;
      }
      suffix().IterSet(m_id, suffix_storage());
      return true;

    }
    bool SeekToLast() override {
      if (!prefix().IterSeekToLast(m_id, nullptr, prefix_storage())) {
        assert(m_id == size_t(-1));
        return false;
      }
      suffix().IterSet(m_id, suffix_storage());
      return true;
    }
    bool Seek(fstring target) override {
      size_t cplen = target.commonPrefixLen(common());
      if (cplen != common().size()) {
        assert(target.size() >= cplen);
        assert(target.size() == cplen || byte_t(target[cplen]) != byte_t(common()[cplen]));
        if (target.size() == cplen || byte_t(target[cplen]) < byte_t(common()[cplen])) {
          return SeekToFirst();
        }
        else {
          m_id = size_t(-1);
          return false;
        }
      }
      target = target.substr(cplen);
      size_t suffix_count;
      if (suffix().TotalKeySize() == 0) {
        if (prefix().IterSeek(m_id, suffix_count, target, prefix_storage())) {
          suffix().IterSet(m_id, suffix_storage());
          return true;
        }
        else {
          m_id = size_t(-1);
          return false;
        }
      }
      fstring prefix_key;
      if (prefix().IterSeek(m_id, suffix_count, target, prefix_storage())) {
        prefix_key = prefix().IterGetKey(m_id, prefix_storage());
        if (prefix_key != target) {
          if (prefix().IterPrev(m_id, &suffix_count, prefix_storage())) {
            prefix_key = prefix().IterGetKey(m_id, prefix_storage());
          }
          else {
            return SeekToFirst();
          }
        }
      }
      else {
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
          return true;
        }
      }
      if (!prefix().IterNext(m_id, suffix_count, prefix_storage())) {
        assert(m_id == size_t(-1));
        return false;
      }
      suffix().IterSet(m_id, suffix_storage());
      return true;
    }
    bool Next() override {
      if (prefix().IterNext(m_id, 1, prefix_storage())) {
        suffix().IterSet(m_id, suffix_storage());
        return true;
      }
      else {
        m_id = size_t(-1);
        return false;
      }
    }
    bool Prev() override {
      if (prefix().IterPrev(m_id, nullptr, prefix_storage())) {
        suffix().IterSet(m_id, suffix_storage());
        return true;
      }
      else {
        m_id = size_t(-1);
        return false;
      }
    }
    size_t DictRank() const override {
      return prefix().IterDictRank(m_id, prefix_storage());
    }
    fstring key() const override {
      iterator_key_.assign(common_);
      iterator_key_.append(prefix().IterGetKey(m_id, prefix_storage()));
      iterator_key_.append(suffix().IterGetKey(m_id, suffix_storage()));
      return iterator_key_;
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
  //        rank_select_fewzero<1>
  //        rank_select_fewzero<2>
  //        rank_select_fewzero<3>
  //        rank_select_fewzero<4>
  //        rank_select_fewzero<5>
  //        rank_select_fewzero<6>
  //        rank_select_fewzero<7>
  //        rank_select_fewzero<8>
  //        rank_select_fewone<1>
  //        rank_select_fewone<2>
  //        rank_select_fewone<3>
  //        rank_select_fewone<4>
  //        rank_select_fewone<5>
  //        rank_select_fewone<6>
  //        rank_select_fewone<7>
  //        rank_select_fewone<8>
  //      NonDescendingUintPrefix<>
  //        rank_select_fewone<1>
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

    Index(const IndexFactoryBase* factory) : factory_(factory) {}
    Index(const IndexFactoryBase* factory, Common&& common, Prefix&& prefix, Suffix&& suffix)
      : base_t(std::move(common), std::move(prefix), std::move(suffix))
      , factory_(factory) {
    }

    const IndexFactoryBase* factory_;

    fstring Name() const override {
      return factory_->Name();
    }
    void SaveMmap(std::function<void(const void *, size_t)> write) const override {
      factory_->SaveMmap(this, write);
    }
    void Reorder(ZReorderMap& newToOld, std::function<void(const void *, size_t)> write, fstring tmpFile) const override {
      factory_->Reorder(this, newToOld, write, tmpFile);
    }
    size_t Find(fstring key, valvec<byte_t>* ctx) const override {
      if (!key.startsWith(common_)) {
        return size_t(-1);
      }
      key = key.substr(common_.size());
      return prefix_.Find(key, suffix_.TotalKeySize() != 0 ? &suffix_ : nullptr, ctx);
    }
    size_t DictRank(fstring key, valvec<byte_t>* ctx) const override {
      size_t cplen = key.commonPrefixLen(common_);
      if (cplen != common_.size()) {
        assert(key.size() >= cplen);
        assert(key.size() == cplen || byte_t(key[cplen]) != byte_t(common_[cplen]));
        if (key.size() == cplen || byte_t(key[cplen]) < byte_t(common_[cplen])) {
          return 0;
        }
        else {
          return NumKeys();
        }
      }
      key = key.substr(common_.size());
      return prefix_.DictRank(key, suffix_.TotalKeySize() != 0 ? &suffix_ : nullptr, ctx);
    }
    size_t NumKeys() const override {
      return prefix_.KeyCount();
    }
    size_t TotalKeySize() const override {
      size_t size = NumKeys() * common_.size();
      size += prefix_.TotalKeySize();
      size += suffix_.TotalKeySize();
      return size;
    }
    fstring Memory() const override {
      return fstring();
    }
    Iterator* NewIterator(void* ptr) const override {
      if (ptr == nullptr) {
        return new IndexIterator<Prefix, Suffix>(this);
      }
      else {
        auto storage = (uint8_t*)ptr + sizeof(IndexIterator<Prefix, Suffix>);
        size_t storage_size = IteratorStorage::GetIteratorStorageSize(this);
        return ::new(ptr) IndexIterator<Prefix, Suffix>(this, storage, storage_size);
      }
    }
    size_t IteratorSize() const override {
      return sizeof(IndexIterator<Prefix, Suffix>) +
        IteratorStorage::GetIteratorStorageSize(this);
    }
    bool NeedsReorder() const override {
      return prefix_.NeedsReorder();
    }
    void GetOrderMap(terark::UintVecMin0& newToOld) const  override {
      prefix_.GetOrderMap(newToOld);
    }
    void BuildCache(double cacheRatio) override {
      prefix_.BuildCache(cacheRatio);
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

    fstring Name() const override {
      return g_TerarkIndexFactroy.key(map_id);
    }

  protected:
    TerarkIndex* CreateIndex(
      Common&& common,
      PrefixBase* prefix,
      SuffixBase* suffix) const override {
      return new index_type(this, std::move(common), Prefix(prefix), Suffix(suffix));
    }
    PrefixBase* CreatePrefix() const override {
      return new Prefix();
    }
    SuffixBase* CreateSuffix() const override {
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
    size_t* get_hint() {
      return nullptr;
    }
    const size_t* get_hint() const {
      return nullptr;
    }
  };
  template<>
  struct IndexUintPrefixIteratorStorage<std::false_type> {
    byte_t buffer[8];
    size_t pos;
    size_t hint;
    size_t* get_hint() {
      return &hint;
    }
    const size_t* get_hint() const {
      return &hint;
    }
  };

  template<class RankSelect>
  struct IndexAscendingUintPrefix
    : public PrefixBase
    , public ComponentIteratorStorageImpl<IndexUintPrefixIteratorStorage<typename RankSelectNeedHint<RankSelect>::type>> {
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
    size_t Find(fstring key, const SuffixBase* suffix, valvec<byte_t>* ctx) const {
      if (key.size() < key_length) {
        return size_t(-1);
      }
      byte_t buffer[8] = {};
      memcpy(buffer + (8 - key_length), key.data(), std::min<size_t>(key_length, key.size()));
      uint64_t value = ReadBigEndianUint64Aligned(buffer, 8);
      if (value < min_value || value > max_value) {
        return size_t(-1);
      }
      uint64_t pos = value - min_value;
      if (!rank_select[pos]) {
        return size_t(-1);
      }
      size_t id = rank_select.rank1(pos);
      if (suffix == nullptr) {
        return key.size() == key_length ? id : size_t(-1);
      }
      size_t suffix_id;
      fstring suffix_key;
      key = key.substr(key_length);
      std::tie(suffix_id, suffix_key) = suffix->LowerBound(key, id, 1, ctx);
      if (suffix_id != id || suffix_key != key) {
        return size_t(-1);
      }
      return suffix_id;
    }
    size_t DictRank(fstring key, const SuffixBase* suffix, valvec<byte_t>* ctx) const {
      size_t id, pos, hint;
      bool seek_result, is_find;
      std::tie(seek_result, is_find) = SeekImpl(key, id, pos, &hint);
      if (!seek_result) {
        return rank_select.max_rank1();
      }
      if (key.size() != key_length || !is_find) {
        return id + 1;
      }
      if (suffix == nullptr) {
        return id;
      }
      return suffix->LowerBound(key.substr(key_length), id, 1, ctx).first;
    }
    bool NeedsReorder() const {
      return false;
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
      UpdateBuffer(id, iter);
      return true;
    }
    bool IterSeekToLast(size_t& id, size_t* count, IteratorStorage* iter) const {
      id = rank_select.max_rank1() - 1;
      iter->pos = rank_select.size() - 1;
      if (count != nullptr) {
        *count = 1;
      }
      UpdateBuffer(id, iter);
      return true;
    }
    bool IterSeek(size_t& id, size_t& count, fstring target, IteratorStorage* iter) const {
      if (!SeekImpl(target, id, iter->pos, iter->get_hint()).first) {
        return false;
      }
      count = 1;
      UpdateBuffer(id, iter);
      return true;
    }
    bool IterNext(size_t& id, size_t count, IteratorStorage* iter) const {
      assert(id != size_t(-1));
      assert(count > 0);
      assert(rank_select[iter->pos]);
      assert(rank_select.rank1(iter->pos) == id);
      do {
        if (id == rank_select.max_rank1() - 1) {
          id = size_t(-1);
          return false;
        }
        else {
          ++id;
          iter->pos = iter->pos + rank_select.zero_seq_len(iter->pos + 1) + 1;
        }
      } while (--count > 0);
      UpdateBuffer(id, iter);
      return true;
    }
    bool IterPrev(size_t& id, size_t* count, IteratorStorage* iter) const {
      assert(id != size_t(-1));
      assert(rank_select[iter->pos]);
      assert(rank_select.rank1(iter->pos) == id);
      if (id == 0) {
        id = size_t(-1);
        return false;
      }
      else {
        --id;
        iter->pos = iter->pos - rank_select.zero_seq_revlen(iter->pos) - 1;
        if (count != nullptr) {
          *count = 1;
        }
        UpdateBuffer(id, iter);
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

  private:
    std::pair<bool, bool> SeekImpl(fstring target, size_t& id, size_t& pos, size_t* hint) const {
      /*
        *    key.size() == 4;
        *    key_length == 6;
        *    | - - - - - - - - |  <- buffer
        *        | - - - - - - |  <- index
        *        | - - - - |      <- key
        */
      byte_t buffer[8] = {};
      memcpy(buffer + (8 - key_length), target.data(), std::min<size_t>(key_length, target.size()));
      uint64_t value = ReadBigEndianUint64Aligned(buffer, 8);
      if (value > max_value) {
        id = size_t(-1);
        return { false, false };
      }
      if (value < min_value) {
        id = 0;
        pos = 0;
        return { true, false };
      }
      pos = value - min_value;
      id = rank_select.rank1(pos);
      if (!rank_select[pos]) {
        pos += rank_select.zero_seq_len(pos);
        return { true, false };
      }
      else if (target.size() > key_length) {
        if (pos == rank_select.size() - 1) {
          id = size_t(-1);
          return { false, false };
        }
        ++id;
        pos += rank_select.zero_seq_len(pos + 1) + 1;
        return { true, false };
      }
      return { true, true };
    }
    void UpdateBuffer(size_t id, IteratorStorage* iter) const {
      SaveAsBigEndianUint64(iter->buffer, key_length, iter->pos + min_value);
    }
  };


  template<class RankSelect>
  struct IndexNonDescendingUintPrefix
    : public PrefixBase
    , public ComponentIteratorStorageImpl<IndexUintPrefixIteratorStorage<typename RankSelectNeedHint<RankSelect>::type>> {
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
    size_t Find(fstring key, const SuffixBase* suffix, valvec<byte_t>* ctx) const {
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
      uint64_t pos = rank_select.select0(value - min_value);
      assert(pos > 0);
      size_t count = rank_select.one_seq_revlen(pos);
      if (count == 0) {
        return size_t(-1);
      }
      size_t id = rank_select.rank1(pos - count);
      size_t suffix_id;
      fstring suffix_key;
      key = key.substr(key_length);
      std::tie(suffix_id, suffix_key) = suffix->LowerBound(key, id, count, ctx);
      if (suffix_id == id + count || suffix_key != key) {
        return size_t(-1);
      }
      return suffix_id;
    }
    size_t DictRank(fstring key, const SuffixBase* suffix, valvec<byte_t>* ctx) const {
      assert(suffix != nullptr);
      size_t id, count, pos, hint;
      bool seek_result, is_find;
      std::tie(seek_result, is_find) = SeekImpl(key, id, count, pos, &hint);
      if (!seek_result) {
        return rank_select.max_rank1();
      }
      if (key.size() != key_length || !is_find) {
        return id + 1;
      }
      return suffix->LowerBound(key.substr(key_length), id, count, ctx).first;
    }
    bool NeedsReorder() const {
      return false;
    }
    void GetOrderMap(terark::UintVecMin0& newToOld) const {
      assert(false);
    }
    void BuildCache(double cacheRatio) {
    }

    bool IterSeekToFirst(size_t& id, size_t& count, IteratorStorage* iter) const {
      id = 0;
      iter->pos = 0;
      count = rank_select.one_seq_len(0);
      UpdateBuffer(id, iter);
      assert(rank_select[iter->pos]);
      return true;
    }
    bool IterSeekToLast(size_t& id, size_t* count, IteratorStorage* iter) const {
      if (count == nullptr) {
        id = rank_select.max_rank1() - 1;
        iter->pos = rank_select.size() - 2;
        assert(rank_select[iter->pos]);
      }
      else {
        size_t one_seq_revlen = rank_select.one_seq_revlen(rank_select.size() - 1);
        assert(one_seq_revlen > 0);
        id = rank_select.max_rank1() - one_seq_revlen;
        iter->pos = rank_select.size() - 1 - one_seq_revlen;
        *count = one_seq_revlen;
        assert(rank_select[iter->pos]);
      }
      UpdateBuffer(id, iter);
      return true;
    }
    bool IterSeek(size_t& id, size_t& count, fstring target, IteratorStorage* iter) const {
      if (!SeekImpl(target, id, count, iter->pos, iter->get_hint()).first) {
        return false;
      }
      UpdateBuffer(id, iter);
      return true;
    }
    bool IterNext(size_t& id, size_t count, IteratorStorage* iter) const {
      assert(id != size_t(-1));
      assert(count > 0);
      assert(rank_select[iter->pos]);
      assert(rank_select.rank1(iter->pos) == id);
      if (id + count >= rank_select.max_rank1()) {
        id = size_t(-1);
        return false;
      }
      id += count;
      if (count == 1) {
        size_t zero_seq_len = rank_select.zero_seq_len(iter->pos + 1);
        iter->pos += zero_seq_len + 1;
        if (zero_seq_len > 0) {
          UpdateBuffer(id, iter);
        }
      }
      else {
        size_t one_seq_len = rank_select.one_seq_len(iter->pos + 1);
        if (count <= one_seq_len) {
          iter->pos += count;
        }
        else {
          iter->pos = rank_select.select1(id);
          UpdateBuffer(id, iter);
        }
      }
      return true;
    }
    bool IterPrev(size_t& id, size_t* count, IteratorStorage* iter) const {
      assert(id != size_t(-1));
      assert(rank_select[iter->pos]);
      assert(rank_select.rank1(iter->pos) == id);
      if (id == 0) {
        id = size_t(-1);
        return false;
      }
      else if (count == nullptr) {
        size_t zero_seq_revlen = rank_select.zero_seq_revlen(iter->pos);
        --id;
        iter->pos -= zero_seq_revlen + 1;
        if (zero_seq_revlen > 0) {
          UpdateBuffer(id, iter);
        }
        return true;
      }
      else {
        size_t one_seq_revlen = rank_select.one_seq_revlen(iter->pos);
        id -= one_seq_revlen;
        if (id == 0) {
          id = size_t(-1);
          return false;
        }
        iter->pos = rank_select.select1(--id);
        one_seq_revlen = rank_select.one_seq_revlen(iter->pos);
        id -= one_seq_revlen;
        iter->pos -= one_seq_revlen;
        *count = one_seq_revlen + 1;
        UpdateBuffer(id, iter);
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

  private:
    std::pair<bool, bool> SeekImpl(fstring target, size_t& id, size_t& count, size_t& pos, size_t* hint) const {
      /*
        *    key.size() == 4;
        *    key_length == 6;
        *    | - - - - - - - - |  <- buffer
        *        | - - - - - - |  <- index
        *        | - - - - |      <- key
        */
      byte_t buffer[8] = {};
      memcpy(buffer + (8 - key_length), target.data(), std::min<size_t>(key_length, target.size()));
      uint64_t value = ReadBigEndianUint64Aligned(buffer, 8);
      if (value > max_value) {
        id = size_t(-1);
        return { false, false };
      }
      if (value < min_value) {
        id = 0;
        pos = 0;
        return { true, false };
      }
      pos = rank_select.select0(value - min_value);
      assert(pos > 0);
      if (target.size() == key_length && rank_select[pos - 1]) {
        count = rank_select.one_seq_revlen(pos);
        pos -= count;
        id = rank_select.rank1(pos);
        return { true, true };
      }
      else {
        if (pos == rank_select.size() - 1) {
          id = size_t(-1);
          return { false, false };
        }
        pos += rank_select.zero_seq_len(pos);
        id = rank_select.rank1(pos);
        count = rank_select.one_seq_len(pos);
        return { true, false };
      }
    }
    void UpdateBuffer(size_t id, IteratorStorage* iter) const {
      SaveAsBigEndianUint64(iter->buffer, key_length, rank_select.rank0(iter->pos) + min_value);
    }
  };

  template<class NestLoudsTrieDAWG>
  class IndexNestLoudsTriePrefixIterator {
  protected:
    typename NestLoudsTrieDAWG::Iterator iter_;
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
    IndexNestLoudsTriePrefixIterator(const NestLoudsTrieDAWG* trie) : iter_(trie) {}

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
        : trie_(trie), bfs_sufflx_(bfs_sufflx) {
      flags.is_bfs_suffix = bfs_sufflx;
    }
    IndexNestLoudsTriePrefix& operator = (const IndexNestLoudsTriePrefix&) = delete;
    IndexNestLoudsTriePrefix& operator = (IndexNestLoudsTriePrefix&&) = default;

    unique_ptr<NestLoudsTrieDAWG> trie_;
    bool bfs_sufflx_;

    size_t IteratorStorageSize() const {
      return sizeof(IteratorStorage);
    }
    void IteratorStorageConstruct(void* ptr) const {
      ::new(ptr) IteratorStorage(trie_.get());
    }
    void IteratorStorageDestruct(void* ptr) const {
      static_cast<IteratorStorage*>(ptr)->~IteratorStorage();
    }

    size_t KeyCount() const {
      return trie_->num_words();
    }
    size_t TotalKeySize() const {
      return trie_->adfa_total_words_len();
    }
    size_t Find(fstring key, const SuffixBase* suffix, valvec<byte_t>* ctx) const {
      if (suffix == nullptr && bfs_sufflx_) {
        return trie_->index(key);
      }
      size_t id, rank;
      fstring prefix_key;
      trie_->lower_bound(key, &id, &rank);
      if (id != size_t(-1)) {
        trie_->nth_word(id, ctx);
        prefix_key = fstring(*ctx);
        if (prefix_key != key) {
          id = trie_->index_prev(id);
          if (id != size_t(-1)) {
            trie_->nth_word(id, ctx);
            prefix_key = fstring(*ctx);
            --rank;
          }
          else {
            assert(rank == 0);
            return size_t(-1);
          }
        }
      }
      else {
        id = trie_->index_end();
        trie_->nth_word(id, ctx);
        prefix_key = fstring(*ctx);
      }
      if (!key.startsWith(prefix_key)) {
        return size_t(-1);
      }
      size_t suffix_id;
      fstring suffix_key;
      key = key.substr(prefix_key.size());
      size_t seek_id = bfs_sufflx_ ? id : rank;
      std::tie(suffix_id, suffix_key) = suffix->LowerBound(key, seek_id, 1, ctx);
      if (suffix_id != seek_id || suffix_key != key) {
        return size_t(-1);
      }
      return suffix_id;
    }
    size_t DictRank(fstring key, const SuffixBase* suffix, valvec<byte_t>* ctx) const {
      size_t id, rank;
      if (suffix == nullptr) {
        trie_->lower_bound(key, nullptr, &rank);
        return rank;
      }
      fstring prefix_key;
      trie_->lower_bound(key, &id, &rank);
      if (id != size_t(-1)) {
        trie_->nth_word(id, ctx);
        prefix_key = fstring(*ctx);
        if (prefix_key != key) {
          id = trie_->index_prev(id);
          if (id != size_t(-1)) {
            trie_->nth_word(id, ctx);
            prefix_key = fstring(*ctx);
            --rank;
          }
          else {
            assert(rank == 0);
            return 0;
          }
        }
      }
      else {
        id = trie_->index_end();
        trie_->nth_word(id, ctx);
        prefix_key = fstring(*ctx);
      }
      if (key.startsWith(prefix_key)) {
        key = key.substr(prefix_key.size());
        size_t suffix_id;
        fstring suffix_key;
        size_t seek_id = bfs_sufflx_ ? id : rank;
        std::tie(suffix_id, suffix_key) = suffix->LowerBound(key, seek_id, 1, ctx);
        if (suffix_id == seek_id && suffix_key == key) {
          return rank;
        }
      }
      return rank + 1;
    }
    bool NeedsReorder() const {
      return true;
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
      return iter->SeekToFirst(id, bfs_sufflx_);
    }
    bool IterSeekToLast(size_t& id, size_t* count, IteratorStorage* iter) const {
      if (count != nullptr) {
        *count = 1;
      }
      return iter->SeekToLast(id, bfs_sufflx_);
    }
    bool IterSeek(size_t& id, size_t& count, fstring target, IteratorStorage* iter) const {
      count = 1;
      return iter->Seek(id, bfs_sufflx_, target);
    }
    bool IterNext(size_t& id, size_t count, IteratorStorage* iter) const {
      assert(count > 0);
      do {
        if (!iter->Next(id, bfs_sufflx_)) {
          return false;
        }
      } while (--count > 0);
      return true;
    }
    bool IterPrev(size_t& id, size_t* count, IteratorStorage* iter) const {
      if (count != nullptr) {
        *count = 1;
      }
      return iter->Prev(id, bfs_sufflx_);
    }
    size_t IterDictRank(size_t id, const IteratorStorage* iter) const {
      return iter->DictRank(id, bfs_sufflx_);
    }
    fstring IterGetKey(size_t id, const IteratorStorage* iter) const {
      return iter->GetKey();
    }

    bool Load(fstring mem) override {
      std::unique_ptr<BaseDFA> dfa(BaseDFA::load_mmap_user_mem(mem.data(), mem.size()));
      trie_.reset(dynamic_cast<NestLoudsTrieDAWG*>(dfa.get()));
      if (!trie_) {
        throw std::invalid_argument("Bad trie class: " + ClassName(*dfa)
          + ", should be " + ClassName<NestLoudsTrieDAWG>());
      }
      dfa.release();
      bfs_sufflx_ = flags.is_bfs_suffix;
      return true;
    }
    void Save(std::function<void(const void*, size_t)> append) const override {
      trie_->save_mmap(append);
    }
  };

  struct IndexEmptySuffix
    : public SuffixBase
    , public ComponentIteratorStorageImpl<void> {
    typedef void IteratorStorage;

    IndexEmptySuffix() = default;
    IndexEmptySuffix(const IndexEmptySuffix&) = delete;
    IndexEmptySuffix(IndexEmptySuffix&&) = default;
    IndexEmptySuffix(SuffixBase* base) {
      delete base;
    }
    IndexEmptySuffix& operator = (const IndexEmptySuffix&) = delete;
    IndexEmptySuffix& operator = (IndexEmptySuffix&&) = default;

    size_t TotalKeySize() const {
      return 0;
    }
    std::pair<size_t, fstring> LowerBound(fstring target, size_t suffix_id, size_t suffix_count, valvec<byte_t>* ctx) const override {
      return { suffix_id, {} };
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
    void Reorder(ZReorderMap& newToOld, std::function<void(const void*, size_t)> append, fstring tmpFile) const override {
    }
  };

  struct IndexFixedStringSuffix
    : public SuffixBase
    , public ComponentIteratorStorageImpl<void> {
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
      size_t fixlen;
      size_t size;
    };
    FixedLenStrVec str_pool_;

    size_t TotalKeySize() const {
      return str_pool_.mem_size();
    }
    std::pair<size_t, fstring> LowerBound(fstring target, size_t suffix_id, size_t suffix_count, valvec<byte_t>* ctx) const override {
      size_t end = suffix_id + suffix_count;
      suffix_id = str_pool_.lower_bound(suffix_id, end, target);
      if (suffix_id == end) {
        return { suffix_id, {} };
      }
      return { suffix_id, str_pool_[suffix_id] };
    }

    void IterSet(size_t suffix_id, void*) const {
    }
    bool IterSeek(fstring target, size_t& suffix_id, size_t suffix_count, void*) const {
      size_t end = suffix_id + suffix_count;
      suffix_id = str_pool_.lower_bound(suffix_id, end, target);
      return suffix_id != end;
    }
    fstring IterGetKey(size_t suffix_id, const void*) const {
      return str_pool_[suffix_id];
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
      Padzero<16>(append, sizeof header + header.fixlen * header.size);
    }
    void Reorder(ZReorderMap& newToOld, std::function<void(const void*, size_t)> append, fstring tmpFile) const override {
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
      PadzeroForAlign<16>(buffer, sizeof header + header.fixlen * header.size);
    }
  };

  template<class BlobStoreType>
  struct IndexBlobStoreSuffix
    : public SuffixBase
    , public ComponentIteratorStorageImpl<BlobStore::CacheOffsets> {
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
    IndexBlobStoreSuffix(BlobStoreType* store, FileMemIO& mem) {
      store_.swap(*store);
      memory_.swap(mem);
      delete store;
    }
    IndexBlobStoreSuffix& operator = (const IndexBlobStoreSuffix&) = delete;
    IndexBlobStoreSuffix& operator = (IndexBlobStoreSuffix&& other) {
      store_.swap(other.store_);
      memory_.swap(other.memory_);
      return *this;
    }

    BlobStoreType store_;
    FileMemIO memory_;

    size_t TotalKeySize() const {
      return store_.total_data_size();
    }
    std::pair<size_t, fstring> LowerBound(fstring target, size_t suffix_id, size_t suffix_count, valvec<byte_t>* ctx) const override {
      BlobStore::CacheOffsets co;
      ctx->swap(co.recData);
      size_t end = suffix_id + suffix_count;
      suffix_id = store_.lower_bound(suffix_id, end, target, &co);
      if (suffix_id == end) {
        return { suffix_id, {} };
      }
      ctx->swap(co.recData);
      return { suffix_id, *ctx };
    }

    void IterSet(size_t suffix_id, IteratorStorage* iter) const {
      iter->recData.risk_set_size(0);
      store_.get_record_append(suffix_id, iter);
    }
    bool IterSeek(fstring target, size_t& suffix_id, size_t suffix_count, IteratorStorage* iter) const {
      size_t end = suffix_id + suffix_count;
      suffix_id = store_.lower_bound(suffix_id, end, target, iter);
      return suffix_id != end;
    }
    fstring IterGetKey(size_t suffix_id, const IteratorStorage* iter) const {
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
    void Reorder(ZReorderMap& newToOld, std::function<void(const void*, size_t)> append, fstring tmpFile) const override {
      store_.reorder_zip_data(newToOld, append, tmpFile);
    }
  };

  ////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////

  template<class RankSelect, class InputBufferType>
  void AscendingUintPrefixFillRankSelect(
    const UintPrefixBuildInfo& info,
    const TerarkIndex::KeyStat& ks,
    RankSelect &rs, InputBufferType& input) {
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
    rank_select_allone &rs, InputBufferType& input) {
    assert(info.max_value - info.min_value < std::numeric_limits<uint64_t>::max());
    rs.resize(info.max_value - info.min_value + 1);
  }

  template<size_t P, size_t W, class InputBufferType>
  void AscendingUintPrefixFillRankSelect(
    const UintPrefixBuildInfo& info,
    const TerarkIndex::KeyStat& ks,
    rank_select_few<P, W> &rs, InputBufferType& input) {
    assert(g_indexEnableFewZero);
    rank_select_few_builder<P, W> builder(info.bit_count0, info.bit_count1, ks.minKey > ks.maxKey);
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
      const TerarkZipTableOptions& tzopt,
      const TerarkIndex::KeyStat& ks,
      const UintPrefixBuildInfo& info,
      const ImmutableCFOptions* ioption) {
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
    RankSelect &rs, InputBufferType& input) {
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
        rs.set1(pos++);
      }
      assert(last == info.max_value);
      assert(pos + 1 == bit_count);
    }
    else {
      size_t pos = bit_count - 1;
      uint64_t last = info.max_value;
      for (size_t seq_id = 0; seq_id < info.key_count; ++seq_id) {
        auto key = input.next();
        assert(key.size() == info.key_length);
        auto cur = ReadBigEndianUint64(key);
        pos -= last - cur;
        last = cur;
        rs.set1(--pos);
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
    rank_select_few<P, W> &rs, InputBufferType& input) {
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
        builder.insert(pos++);
      }
      assert(last == info.max_value);
      assert(pos + 1 == bit_count);
    }
    else {
      size_t pos = bit_count - 1;
      uint64_t last = info.max_value;
      for (size_t seq_id = 0; seq_id < info.key_count; ++seq_id) {
        auto key = input.next();
        assert(key.size() == info.key_length);
        auto cur = ReadBigEndianUint64(key);
        pos -= last - cur;
        last = cur;
        builder.insert(--pos);
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
      const TerarkZipTableOptions& tzopt,
      const TerarkIndex::KeyStat& ks,
      const UintPrefixBuildInfo& info,
      const ImmutableCFOptions* ioption) {
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
      const TerarkZipTableOptions& tzopt,
      const TerarkIndex::KeyStat& ks,
      const UintPrefixBuildInfo& info,
      const ImmutableCFOptions* ioption) {
    assert(g_indexEnableUintIndex);
    input.rewind();
    switch (info.type) {
    case UintPrefixBuildInfo::asc_few_zero_1:
      return BuildAscendingUintPrefix<rank_select_fewzero<1>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_few_zero_2:
      return BuildAscendingUintPrefix<rank_select_fewzero<2>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_few_zero_3:
      return BuildAscendingUintPrefix<rank_select_fewzero<3>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_few_zero_4:
      return BuildAscendingUintPrefix<rank_select_fewzero<4>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_few_zero_5:
      return BuildAscendingUintPrefix<rank_select_fewzero<5>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_few_zero_6:
      return BuildAscendingUintPrefix<rank_select_fewzero<6>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_few_zero_7:
      return BuildAscendingUintPrefix<rank_select_fewzero<7>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_few_zero_8:
      return BuildAscendingUintPrefix<rank_select_fewzero<8>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_allone:
      return BuildAscendingUintPrefix<rank_select_allone>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_il_256:
      return BuildAscendingUintPrefix<rank_select_il_256_32>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_se_512:
      return BuildAscendingUintPrefix<rank_select_se_512_64>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_few_one_1:
      return BuildAscendingUintPrefix<rank_select_fewone<1>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_few_one_2:
      return BuildAscendingUintPrefix<rank_select_fewone<2>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_few_one_3:
      return BuildAscendingUintPrefix<rank_select_fewone<3>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_few_one_4:
      return BuildAscendingUintPrefix<rank_select_fewone<4>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_few_one_5:
      return BuildAscendingUintPrefix<rank_select_fewone<5>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_few_one_6:
      return BuildAscendingUintPrefix<rank_select_fewone<6>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_few_one_7:
      return BuildAscendingUintPrefix<rank_select_fewone<7>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::asc_few_one_8:
      return BuildAscendingUintPrefix<rank_select_fewone<8>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::non_desc_il_256:
      assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
      return BuildNonDescendingUintPrefix<rank_select_il_256_32>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::non_desc_se_512:
      assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
      return BuildNonDescendingUintPrefix<rank_select_se_512_64>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::non_desc_few_one_1:
      assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
      return BuildNonDescendingUintPrefix<rank_select_fewone<1>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::non_desc_few_one_2:
      assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
      return BuildNonDescendingUintPrefix<rank_select_fewone<2>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::non_desc_few_one_3:
      assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
      return BuildNonDescendingUintPrefix<rank_select_fewone<3>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::non_desc_few_one_4:
      assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
      return BuildNonDescendingUintPrefix<rank_select_fewone<4>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::non_desc_few_one_5:
      assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
      return BuildNonDescendingUintPrefix<rank_select_fewone<6>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::non_desc_few_one_6:
      assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
      return BuildNonDescendingUintPrefix<rank_select_fewone<6>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::non_desc_few_one_7:
      assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
      return BuildNonDescendingUintPrefix<rank_select_fewone<7>>(
        input, tzopt, ks, info, ioption);
    case UintPrefixBuildInfo::non_desc_few_one_8:
      assert(ks.maxKeyLen > commonPrefixLen(ks.minKey, ks.maxKey) + info.key_length);
      return BuildNonDescendingUintPrefix<rank_select_fewone<8>>(
        input, tzopt, ks, info, ioption);
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
            }
            else {
              // write offset+len of nestStrVec to tmpfile
              // which offset is ref to outer StrVec's data
              conf.tmpLevel = 3;
            }
          }
          else if (memSize > tzopt.smallTaskMemory * 3 / 2) {
            // for example:
            // 1G mem in BFS, swap to 1G file after BFS and before build nextStrVec
            conf.tmpLevel = 2;
          }
        }
        else {
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
    }
    else {
      return NestLoudsTriePrefixProcess<NestLoudsTrieDAWG_SE_512_64_FL>(cfg, keyVec);
    }
  }

  template<class InputBufferType>
  void IndexFillKeyVector(InputBufferType& input, FixedLenStrVec& keyVec, size_t numKeys, size_t sumKeyLen, size_t fixedLen, bool isReverse) {
    if (isReverse) {
      keyVec.m_size = numKeys;
      keyVec.m_strpool.resize(sumKeyLen);
      for (size_t i = numKeys; i > 0; ) {
        --i;
        auto str = input.next();
        assert(str.size() == fixedLen);
        memcpy(keyVec.m_strpool.data() + fixedLen * i
          , str.data(), fixedLen);
      }
    }
    else {
      keyVec.reserve(numKeys, sumKeyLen);
      for (size_t i = 0; i < numKeys; ++i) {
        keyVec.push_back(input.next());
      }
    }
  }

  template<class InputBufferType>
  void IndexFillKeyVector(InputBufferType& input, SortedStrVec& keyVec, size_t numKeys, size_t sumKeyLen, bool isReverse) {
    if (isReverse) {
      keyVec.m_offsets.resize_with_wire_max_val(numKeys + 1, sumKeyLen);
      keyVec.m_offsets.set_wire(numKeys, sumKeyLen);
      keyVec.m_strpool.resize(sumKeyLen);
      size_t offset = sumKeyLen;
      for (size_t i = numKeys; i > 0; ) {
        --i;
        auto str = input.next();
        offset -= str.size();
        memcpy(keyVec.m_strpool.data() + offset, str.data(), str.size());
        keyVec.m_offsets.set_wire(i, offset);
      }
      assert(offset == 0);
    }
    else {
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
      bool isReverse, bool isFixedLen,
      const ImmutableCFOptions* ioption) {
    input.rewind();
    NestLoudsTrieConfig cfg;
    if (isFixedLen) {
      FixedLenStrVec keyVec(sumKeyLen / numKeys);
      assert(sumKeyLen % numKeys == 0);
      IndexFillKeyVector(input, keyVec, numKeys, sumKeyLen, keyVec.m_fixlen, isReverse);
      NestLoudsTriePrefixSetConfig(cfg, keyVec.mem_size(), keyVec.avg_size(), tzopt);
      return NestLoudsTriePrefixSelect(tzopt.indexType, cfg, keyVec);
    }
    else {
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

  template<class InputBufferType>
  SuffixBase*
    BuildFixedStringSuffix(
      InputBufferType& input,
      size_t numKeys, size_t sumKeyLen, size_t fixedLen) {
    assert(g_indexEnableCompositeIndex);
    input.rewind();
    FixedLenStrVec keyVec(fixedLen);
    IndexFillKeyVector(input, keyVec, numKeys, sumKeyLen, fixedLen, false);
    auto suffix = new IndexFixedStringSuffix();
    suffix->str_pool_.swap(keyVec);
    return suffix;
  }

  bool UseDictZipSuffix(size_t numKeys, size_t sumKeyLen, double zipRatio) {
    return g_indexEnableDictZipSuffix && (sumKeyLen + numKeys * 8) * zipRatio < sumKeyLen + numKeys;
  }

  template<class InputBufferType>
  SuffixBase*
    BuildBlobStoreSuffix(
      InputBufferType& input,
      size_t numKeys, size_t sumKeyLen, double zipRatio) {
    assert(g_indexEnableCompositeIndex);
    assert(g_indexEnableDynamicSuffix);
    input.rewind();
    FileMemIO memory;
    if (!UseDictZipSuffix(numKeys, sumKeyLen, zipRatio)) {
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
      return new IndexBlobStoreSuffix<ZipOffsetBlobStore>(static_cast<ZipOffsetBlobStore*>(store), memory);
    }
    else {
      std::unique_ptr<DictZipBlobStore::ZipBuilder> zbuilder;
      DictZipBlobStore::Options dzopt;
      dzopt.checksumLevel = 1;
      dzopt.entropyAlgo = DictZipBlobStore::Options::kHuffmanO1;
      dzopt.useSuffixArrayLocalMatch = false;
      dzopt.compressGlobalDict = true;
      dzopt.entropyInterleaved = 1;
      dzopt.offsetArrayBlockUnits = 128;
      dzopt.entropyZipRatioRequire = 0.95f;
      dzopt.embeddedDict = true;
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
      return new IndexBlobStoreSuffix<DictZipBlobStore>(static_cast<DictZipBlobStore*>(store), memory);
    }
  }

  UintPrefixBuildInfo GetUintPrefixBuildInfo(const TerarkIndex::KeyStat& ks, size_t cplen, double zipRatio) {
    UintPrefixBuildInfo result = {
      0, 0, 0, 0, 0, 0, 0, UintPrefixBuildInfo::fail
    };
    if (!g_indexEnableUintIndex || (!g_indexEnableDynamicSuffix && ks.maxKeyLen != ks.minKeyLen)) {
      return result;
    }
    size_t keyCount = ks.keyCount;
    size_t maxPrefixLen = std::min<size_t>(8, ks.minKeyLen - cplen);
    size_t totalKeySize = ks.sumKeyLen - keyCount * cplen;
    size_t bestCost = totalKeySize;
    if (ks.minKeyLen != ks.maxKeyLen) {
      bestCost += keyCount;
    }
    size_t targetCost = bestCost * 4 / 5;
    size_t entryCnt[8] = {};
    for (size_t i = cplen, e = cplen + 8; i < e; ++i) {
      entryCnt[i - cplen] = keyCount - (i < ks.diff.size() ? ks.diff[i].cnt : 0);
    }
    for (size_t i = 1; i <= maxPrefixLen; ++i) {
      if (cplen + i < ks.diff.size() && ks.diff[cplen + i].max > 8) {
        continue;
      }
      if (!g_indexEnableCompositeIndex && ks.maxKeyLen != ks.minKeyLen && cplen + i != ks.maxKeyLen) {
        continue;
      }
      UintPrefixBuildInfo info;
      info.key_length = i;
      info.key_count = keyCount;
      info.min_value = ReadBigEndianUint64(ks.minKey.begin() + cplen, i);
      info.max_value = ReadBigEndianUint64(ks.maxKey.begin() + cplen, i);
      if (info.min_value > info.max_value) std::swap(info.min_value, info.max_value);
      uint64_t diff = info.max_value - info.min_value;
      info.entry_count = entryCnt[i - 1];
      assert(info.entry_count > 0);
      assert(diff >= info.entry_count - 1);
      size_t bit_count;
      if (info.entry_count == keyCount) {
        // ascending
        info.bit_count0 = diff - keyCount + 1;
        info.bit_count1 = keyCount;
        bit_count = info.bit_count0 + info.bit_count1;
      }
      else {
        // non descending
        if (diff == std::numeric_limits<uint64_t>::max()) {
          info.bit_count0 = size_t(-1);
        }
        else {
          info.bit_count0 = diff + 1;
        }
        info.bit_count1 = keyCount;
        if (keyCount + 1 > std::numeric_limits<uint64_t>::max() - diff) {
          bit_count = size_t(-1);
        }
        else {
          bit_count = diff + keyCount + 1;
        }
      }
      size_t fewCount = info.bit_count0 / 100 + info.bit_count1 / 100;
      size_t prefixCost;
      if (info.entry_count == keyCount && info.entry_count == diff + 1) {
        info.type = UintPrefixBuildInfo::asc_allone;
        prefixCost = 0;
      }
      else if ((g_indexEnableNonDescUint || info.entry_count == keyCount) &&
        g_indexEnableFewZero && bit_count != size_t(-1) && info.bit_count1 < fewCount && info.bit_count1 < (1ULL << 48)) {
        if (bit_count < (1ULL << 8)) {
          info.type = info.entry_count == keyCount ? UintPrefixBuildInfo::asc_few_one_1 : UintPrefixBuildInfo::non_desc_few_one_1;
        }
        else if (bit_count < (1ULL << 16)) {
          info.type = info.entry_count == keyCount ? UintPrefixBuildInfo::asc_few_one_2 : UintPrefixBuildInfo::non_desc_few_one_2;
        }
        else if (bit_count < (1ULL << 24)) {
          info.type = info.entry_count == keyCount ? UintPrefixBuildInfo::asc_few_one_3 : UintPrefixBuildInfo::non_desc_few_one_3;
        }
        else if (bit_count < (1ULL << 32)) {
          info.type = info.entry_count == keyCount ? UintPrefixBuildInfo::asc_few_one_4 : UintPrefixBuildInfo::non_desc_few_one_4;
        }
        else if (bit_count < (1ULL << 40)) {
          info.type = info.entry_count == keyCount ? UintPrefixBuildInfo::asc_few_one_5 : UintPrefixBuildInfo::non_desc_few_one_5;
        }
        else if (bit_count < (1ULL << 48)) {
          info.type = info.entry_count == keyCount ? UintPrefixBuildInfo::asc_few_one_6 : UintPrefixBuildInfo::non_desc_few_one_6;
        }
        else if (bit_count < (1ULL << 56)) {
          info.type = info.entry_count == keyCount ? UintPrefixBuildInfo::asc_few_one_7 : UintPrefixBuildInfo::non_desc_few_one_7;
        }
        else {
          info.type = info.entry_count == keyCount ? UintPrefixBuildInfo::asc_few_one_8 : UintPrefixBuildInfo::non_desc_few_one_8;
        }
        prefixCost = info.bit_count1 * sizeof(uint32_t) * 33 / 32;
      }
      else if (g_indexEnableFewZero && bit_count != size_t(-1) && info.bit_count0 < fewCount && info.bit_count0 < (1ULL << 48)) {
        assert(info.entry_count == keyCount);
        if (bit_count < (1ULL << 8)) {
          info.type = UintPrefixBuildInfo::asc_few_zero_1;
        }
        else if (bit_count < (1ULL << 16)) {
          info.type = UintPrefixBuildInfo::asc_few_zero_2;
        }
        else if (bit_count < (1ULL << 24)) {
          info.type = UintPrefixBuildInfo::asc_few_zero_3;
        }
        else if (bit_count < (1ULL << 32)) {
          info.type = UintPrefixBuildInfo::asc_few_zero_4;
        }
        else if (bit_count < (1ULL << 40)) {
          info.type = UintPrefixBuildInfo::asc_few_zero_5;
        }
        else if (bit_count < (1ULL << 48)) {
          info.type = UintPrefixBuildInfo::asc_few_zero_6;
        }
        else if (bit_count < (1ULL << 56)) {
          info.type = UintPrefixBuildInfo::asc_few_zero_7;
        }
        else {
          info.type = UintPrefixBuildInfo::asc_few_zero_8;
        }
        prefixCost = info.bit_count0 * sizeof(uint32_t) * 33 / 32;
      }
      else {
        if (!g_indexEnableNonDescUint && info.entry_count != keyCount) {
          continue;
        }
        if (info.bit_count0 >= (1ULL << 56) || info.bit_count1 >= (1ULL << 56)) {
          // too large
          continue;
        }
        size_t bit_count = info.bit_count0 + info.bit_count1;
        if (bit_count <= std::numeric_limits<uint32_t>::max()) {
          info.type = info.entry_count == keyCount ? UintPrefixBuildInfo::asc_il_256 : UintPrefixBuildInfo::non_desc_il_256;
        }
        else {
          info.type = info.entry_count == keyCount ? UintPrefixBuildInfo::asc_se_512 : UintPrefixBuildInfo::non_desc_se_512;
        }
        prefixCost = bit_count * 21 / 16;
      }
      size_t suffixCost = (totalKeySize - i * keyCount) * zipRatio;
      if (ks.minSuffixLen != ks.maxSuffixLen) {
        suffixCost += keyCount;
      }
      size_t currCost = prefixCost + suffixCost;
      if (currCost < bestCost && currCost < targetCost) {
        result = info;
        bestCost = currCost;
      }
    }
    return result;
  };
}

TerarkIndex*
  TerarkIndex::Factory::Build(
    NativeDataInput<InputBuffer>& reader,
    const TerarkZipTableOptions& tzopt,
    const TerarkIndex::KeyStat& ks,
    const ImmutableCFOptions* ioption) {
  using namespace index_detail;
  struct DefaultInputBuffer {
    NativeDataInput<InputBuffer> &reader;
    size_t cplen;
    valvec<byte_t> buffer;
    fstring next() {
      reader >> buffer;
      return { buffer.data() + cplen, ptrdiff_t(buffer.size() - cplen) };
    }
    void rewind() {
      reader.resetbuf();
      static_cast<FileStream*>(reader.getInputStream())->rewind();
    }
    DefaultInputBuffer(NativeDataInput<InputBuffer> &_reader, size_t _cplen, size_t _maxKeyLen)
      : reader(_reader), cplen(_cplen)
      , buffer(_maxKeyLen, valvec_reserve()) {
    }
  };
  struct MinimizePrefixInputBuffer {
    NativeDataInput<InputBuffer> &reader;
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
      }
      else {
        reader >> buffer;
        size_t samePrefix = commonPrefixLen(buffer, last);
        last.swap(buffer);
        maxSamePrefix = std::max(samePrefix, lastSamePrefix) + 1;
        lastSamePrefix = samePrefix;
      }
      return { buffer.data() + cplen, ptrdiff_t(std::min(maxSamePrefix, buffer.size()) - cplen) };
    }
    void rewind() {
      reader.resetbuf();
      static_cast<FileStream*>(reader.getInputStream())->rewind();
      assert(count > 0);
      reader >> last;
      lastSamePrefix = 0;
      pos = 0;
    }
    MinimizePrefixInputBuffer(NativeDataInput<InputBuffer> &_reader, size_t _cplen, size_t _keyCount, size_t _maxKeyLen)
      : reader(_reader), cplen(_cplen), count(_keyCount)
      , last(_maxKeyLen, valvec_reserve())
      , buffer(_maxKeyLen, valvec_reserve()) {
    }
  };
  struct MinimizePrefixRemainingInputBuffer {
    NativeDataInput<InputBuffer> &reader;
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
      }
      else {
        reader >> buffer;
        size_t samePrefix = commonPrefixLen(buffer, last);
        last.swap(buffer);
        maxSamePrefix = std::max(samePrefix, lastSamePrefix) + 1;
        lastSamePrefix = samePrefix;
      }
      return fstring(buffer).substr(std::min(maxSamePrefix, buffer.size()));
    }
    void rewind() {
      reader.resetbuf();
      static_cast<FileStream*>(reader.getInputStream())->rewind();
      assert(count > 0);
      reader >> last;
      lastSamePrefix = 0;
      pos = 0;
    }
    MinimizePrefixRemainingInputBuffer(NativeDataInput<InputBuffer> &_reader, size_t _cplen, size_t _keyCount, size_t _maxKeyLen)
      : reader(_reader), cplen(_cplen), count(_keyCount)
      , last(_maxKeyLen, valvec_reserve())
      , buffer(_maxKeyLen, valvec_reserve()) {
    }
  };
  struct FixPrefixInputBuffer {
    NativeDataInput<InputBuffer> &reader;
    size_t cplen;
    size_t cplenPrefixSize;
    valvec<byte_t> buffer;
    fstring next() {
      reader >> buffer;
      assert(buffer.size() >= cplenPrefixSize);
      return { buffer.data() + cplen, buffer.data() + cplenPrefixSize };
    }
    void rewind() {
      reader.resetbuf();
      static_cast<FileStream*>(reader.getInputStream())->rewind();
    }
    FixPrefixInputBuffer(NativeDataInput<InputBuffer> &_reader, size_t _cplen, size_t _prefixSize, size_t _maxKeyLen)
      : reader(_reader), cplen(_cplen), cplenPrefixSize(_cplen + _prefixSize)
      , buffer(_maxKeyLen, valvec_reserve()) {
    }
  };
  struct FixPrefixRemainingInputBuffer {
    NativeDataInput<InputBuffer> &reader;
    size_t cplenPrefixSize;
    valvec<byte_t> buffer;
    fstring next() {
      reader >> buffer;
      assert(buffer.size() >= cplenPrefixSize);
      return { buffer.data() + cplenPrefixSize, ptrdiff_t(buffer.size() - cplenPrefixSize) };
    }
    void rewind() {
      reader.resetbuf();
      static_cast<FileStream*>(reader.getInputStream())->rewind();
    }
    FixPrefixRemainingInputBuffer(NativeDataInput<InputBuffer> &_reader, size_t _cplen, size_t _prefixSize, size_t _maxKeyLen)
      : reader(_reader), cplenPrefixSize(_cplen + _prefixSize)
      , buffer(_maxKeyLen, valvec_reserve()) {
    }
  };
  struct FixSuffixPrefixInputBuffer {
    NativeDataInput<InputBuffer> &reader;
    size_t cplen;
    size_t suffixSize;
    valvec<byte_t> buffer;
    fstring next() {
      reader >> buffer;
      assert(buffer.size() >= cplen + suffixSize);
      return { buffer.data() + cplen, ptrdiff_t(buffer.size() - cplen - suffixSize) };
    }
    void rewind() {
      reader.resetbuf();
      static_cast<FileStream*>(reader.getInputStream())->rewind();
    }
    FixSuffixPrefixInputBuffer(NativeDataInput<InputBuffer> &_reader, size_t _cplen, size_t _suffixSize, size_t _maxKeyLen)
      : reader(_reader), cplen(_cplen), suffixSize(_suffixSize)
      , buffer(_maxKeyLen, valvec_reserve()) {
    }
  };
  struct FixSuffixInputBuffer {
    NativeDataInput<InputBuffer> &reader;
    size_t suffixSize;
    valvec<byte_t> buffer;
    fstring next() {
      reader >> buffer;
      assert(buffer.size() >= suffixSize);
      return { buffer.data() + suffixSize, ptrdiff_t(suffixSize) };
    }
    void rewind() {
      reader.resetbuf();
      static_cast<FileStream*>(reader.getInputStream())->rewind();
    }
    FixSuffixInputBuffer(NativeDataInput<InputBuffer> &_reader, size_t _suffixSize, size_t _maxKeyLen)
      : reader(_reader), suffixSize(_suffixSize)
      , buffer(_maxKeyLen, valvec_reserve()) {
    }
  };

  assert(ks.keyCount > 0);
  size_t cplen = ks.keyCount > 1 ? commonPrefixLen(ks.minKey, ks.maxKey) : 0;
  double zipRatio = double(ks.entropyLen) / ks.sumKeyLen;
  UintPrefixBuildInfo uint_prefix_info = GetUintPrefixBuildInfo(ks, cplen, zipRatio);
  PrefixBase* prefix;
  SuffixBase* suffix;
  if (uint_prefix_info.key_length > 0) {
    if (ks.minKeyLen == ks.maxKeyLen && ks.maxKeyLen == cplen + uint_prefix_info.key_length) {
      DefaultInputBuffer input_reader{ reader, cplen, ks.maxKeyLen };
      prefix = BuildUintPrefix(input_reader, tzopt, ks, uint_prefix_info, ioption);
      suffix = BuildEmptySuffix();
    }
    else {
      FixPrefixInputBuffer prefix_input_reader{ reader, cplen, uint_prefix_info.key_length, ks.maxKeyLen };
      prefix = BuildUintPrefix(prefix_input_reader, tzopt, ks, uint_prefix_info, ioption);
      FixPrefixRemainingInputBuffer suffix_input_reader{ reader, cplen, uint_prefix_info.key_length, ks.maxKeyLen };
      if (ks.minKeyLen == ks.maxKeyLen) {
        suffix = BuildFixedStringSuffix(
          suffix_input_reader, uint_prefix_info.key_count,
          ks.sumKeyLen - ks.keyCount * prefix_input_reader.cplenPrefixSize, ks.maxKeyLen - prefix_input_reader.cplenPrefixSize);
      }
      else {
        suffix = BuildBlobStoreSuffix(
          suffix_input_reader, uint_prefix_info.key_count,
          ks.sumKeyLen - ks.keyCount * prefix_input_reader.cplenPrefixSize, zipRatio);
      }
    }
  }
  else if (g_indexEnableCompositeIndex && ks.minSuffixLen > 0 &&
    !UseDictZipSuffix(ks.keyCount, ks.sumKeyLen - ks.sumPrefixLen, zipRatio) &&
    ks.sumKeyLen - ks.sumPrefixLen - ks.minSuffixLen * ks.keyCount < (ks.sumKeyLen - ks.sumPrefixLen) / 8) {
    size_t suffixLen = ks.minSuffixLen;
    FixSuffixPrefixInputBuffer prefix_input_reader{ reader, cplen, suffixLen, ks.maxKeyLen };
    prefix = BuildNestLoudsTriePrefix(
      prefix_input_reader, tzopt, ks.keyCount, ks.sumKeyLen - ks.keyCount * (cplen + suffixLen),
      ks.minKey > ks.maxKey, ks.minKeyLen == ks.maxKeyLen, ioption);
    FixSuffixInputBuffer suffix_input_reader{ reader, suffixLen, ks.maxKeyLen };
    suffix = BuildFixedStringSuffix(
      suffix_input_reader, ks.keyCount,
      ks.sumKeyLen - ks.keyCount * suffixLen, suffixLen);
  }
  else if ((g_indexEnableDynamicSuffix || ks.minSuffixLen == ks.maxSuffixLen) &&
    g_indexEnableCompositeIndex && ks.sumPrefixLen < ks.sumKeyLen * 31 / 32) {
    MinimizePrefixInputBuffer prefix_input_reader{ reader, cplen, ks.keyCount, ks.maxKeyLen };
    prefix = BuildNestLoudsTriePrefix(
      prefix_input_reader, tzopt, ks.keyCount, ks.sumPrefixLen - ks.keyCount * cplen,
      ks.minKey > ks.maxKey, ks.minPrefixLen == ks.maxPrefixLen, ioption);
    MinimizePrefixRemainingInputBuffer suffix_input_reader{ reader, cplen, ks.keyCount, ks.maxKeyLen };
    if (!UseDictZipSuffix(ks.keyCount, ks.sumKeyLen - ks.sumPrefixLen, zipRatio) && ks.minSuffixLen == ks.maxSuffixLen) {
      suffix = BuildFixedStringSuffix(
        suffix_input_reader, ks.keyCount,
        ks.sumKeyLen - ks.sumPrefixLen, ks.maxSuffixLen);
    }
    else {
      suffix = BuildBlobStoreSuffix(
        suffix_input_reader, ks.keyCount,
        ks.sumKeyLen - ks.sumPrefixLen, zipRatio);
    }
  }
  else {
    DefaultInputBuffer input_reader{ reader, cplen, ks.maxKeyLen };
    prefix = BuildNestLoudsTriePrefix(
      input_reader, tzopt, ks.keyCount, ks.sumKeyLen - ks.keyCount * cplen,
      ks.minKey > ks.maxKey, ks.minKeyLen == ks.maxKeyLen, ioption);
    suffix = BuildEmptySuffix();
  }
  valvec<char> common(ks.prefix.size() + cplen, valvec_reserve());
  common.append(ks.prefix);
  common.append(ks.minKey.data(), cplen);
  auto factory = IndexFactoryBase::GetFactoryByType(std::type_index(typeid(*prefix)), std::type_index(typeid(*suffix)));
  assert(factory != nullptr);
  return factory->CreateIndex(Common(common, true), prefix, suffix);
}

size_t TerarkIndex::Factory::MemSizeForBuild(const TerarkIndex::KeyStat& ks) {
  size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
  size_t indexSize = UintVecMin0::compute_mem_size_by_max_val(ks.sumKeyLen - cplen, ks.keyCount);
  return ks.sumKeyLen - ks.keyCount * commonPrefixLen(ks.minKey, ks.maxKey) + indexSize;
}

////////////////////////////////////////////////////////////////////////////////


class TerarkUnionIndex : public TerarkIndex {
  struct Item {
    unique_ptr<TerarkIndex> index;
    valvec<byte> upper_bound;
    size_t num_keys_acc;
  };
  size_t total_key_size_;
  fstring memory_;
  size_t iter_size_;
  std::vector<Item> index_vec_;

  class UnionIterator : public TerarkIndex::Iterator {

  public:
    bool SeekToFirst() override {
      return false;
    }
    bool SeekToLast() override {
      return false;
    }
    bool Seek(fstring target) override {
      return false;
    }
    bool Next() override {
      return false;
    }
    bool Prev() override {
      return false;
    }
    size_t DictRank() const override {
      return 0;
    }
    fstring key() const override {
      return "";
    }
  };
public:
  TerarkUnionIndex(valvec<unique_ptr<TerarkIndex>> index_vec) {

  }

  fstring Name() const override {
    return "TerarkUnionIndex";
  }
  void SaveMmap(std::function<void(const void *, size_t)> write) const override {
    assert(false);
  }
  void Reorder(ZReorderMap& newToOld, std::function<void(const void *, size_t)> write, fstring tmpFile) const override {
    assert(false);
  }
  size_t Find(fstring key, valvec<byte_t>* ctx) const override {
    // TODO
    return 0;
  }
  size_t DictRank(fstring key, valvec<byte_t>* ctx) const override {
    // TODO
    return 0;
  }
  size_t NumKeys() const override {
    return index_vec_.back().num_keys_acc;
  }
  size_t TotalKeySize() const override {
    return total_key_size_;
  }
  fstring Memory() const override {
    return memory_;
  }
  Iterator* NewIterator(void* ptr) const override {
    // TODO
    return nullptr;
  }
  size_t IteratorSize() const override {
    return sizeof(UnionIterator) + iter_size_;
  }
  bool NeedsReorder() const override {
    assert(false);
    return false;
  }
  void GetOrderMap(terark::UintVecMin0& newToOld) const override {
    assert(false);
  }
  void BuildCache(double cacheRatio) override {
    for (auto& i : index_vec_) {
      i.index->BuildCache(cacheRatio);
    }
  }
};

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
  }
  else {
    std::reverse(index_vec.begin(), index_vec.end());
    // TODO TerarkUnionIndex
    return nullptr;
  }
}




template<char... chars_t>
struct StringHolder {
  static fstring Name() {
    static char str[] = { chars_t ... };
    return fstring{ str, sizeof(str) };
  }
};
#define _G(name,i) ((i+1)<sizeof(#name)?#name[i]:' ')
#define NAME(s) StringHolder<                   \
  _G(s, 0),_G(s, 1),_G(s, 2),_G(s, 3),_G(s, 4), \
  _G(s, 5),_G(s, 6),_G(s, 7),_G(s, 8),_G(s, 9), \
  _G(s,10),_G(s,11),_G(s,12),_G(s,13),_G(s,14), \
  _G(s,15),_G(s,16),_G(s,17),_G(s,18),_G(s,19)>

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

using PrefixComponentList = ComponentRegister<>
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
::reg<NAME(A_allone    ), 0, IndexAscendingUintPrefix<rank_select_allone    >>
::reg<NAME(A_fewzero_1 ), 1, IndexAscendingUintPrefix<rank_select_fewzero<1>>>
::reg<NAME(A_fewzero_2 ), 1, IndexAscendingUintPrefix<rank_select_fewzero<2>>>
::reg<NAME(A_fewzero_3 ), 1, IndexAscendingUintPrefix<rank_select_fewzero<3>>>
::reg<NAME(A_fewzero_4 ), 1, IndexAscendingUintPrefix<rank_select_fewzero<4>>>
::reg<NAME(A_fewzero_5 ), 1, IndexAscendingUintPrefix<rank_select_fewzero<5>>>
::reg<NAME(A_fewzero_6 ), 1, IndexAscendingUintPrefix<rank_select_fewzero<6>>>
::reg<NAME(A_fewzero_7 ), 1, IndexAscendingUintPrefix<rank_select_fewzero<7>>>
::reg<NAME(A_fewzero_8 ), 1, IndexAscendingUintPrefix<rank_select_fewzero<8>>>
::reg<NAME(A_il_256_32 ), 0, IndexAscendingUintPrefix<rank_select_il_256_32>>
::reg<NAME(A_se_512_64 ), 0, IndexAscendingUintPrefix<rank_select_se_512_64>>
::reg<NAME(A_fewone_1  ), 1, IndexAscendingUintPrefix<rank_select_fewone<1>>>
::reg<NAME(A_fewone_2  ), 1, IndexAscendingUintPrefix<rank_select_fewone<2>>>
::reg<NAME(A_fewone_3  ), 1, IndexAscendingUintPrefix<rank_select_fewone<3>>>
::reg<NAME(A_fewone_4  ), 1, IndexAscendingUintPrefix<rank_select_fewone<4>>>
::reg<NAME(A_fewone_5  ), 1, IndexAscendingUintPrefix<rank_select_fewone<5>>>
::reg<NAME(A_fewone_6  ), 1, IndexAscendingUintPrefix<rank_select_fewone<6>>>
::reg<NAME(A_fewone_7  ), 1, IndexAscendingUintPrefix<rank_select_fewone<7>>>
::reg<NAME(A_fewone_8  ), 1, IndexAscendingUintPrefix<rank_select_fewone<8>>>
::reg<NAME(ND_il_256_32), 0, IndexNonDescendingUintPrefix<rank_select_il_256_32>>
::reg<NAME(ND_se_512_64), 0, IndexNonDescendingUintPrefix<rank_select_se_512_64>>
::reg<NAME(ND_fewone_1 ), 1, IndexNonDescendingUintPrefix<rank_select_fewone<1>>>
::reg<NAME(ND_fewone_2 ), 1, IndexNonDescendingUintPrefix<rank_select_fewone<2>>>
::reg<NAME(ND_fewone_3 ), 1, IndexNonDescendingUintPrefix<rank_select_fewone<3>>>
::reg<NAME(ND_fewone_4 ), 1, IndexNonDescendingUintPrefix<rank_select_fewone<4>>>
::reg<NAME(ND_fewone_5 ), 1, IndexNonDescendingUintPrefix<rank_select_fewone<5>>>
::reg<NAME(ND_fewone_6 ), 1, IndexNonDescendingUintPrefix<rank_select_fewone<6>>>
::reg<NAME(ND_fewone_7 ), 1, IndexNonDescendingUintPrefix<rank_select_fewone<7>>>
::reg<NAME(ND_fewone_8 ), 1, IndexNonDescendingUintPrefix<rank_select_fewone<8>>>
::list;

using SuffixComponentList = ComponentRegister<>
::reg<NAME(Empty  ), 0, IndexEmptySuffix                        >
::reg<NAME(Fixed  ), 0, IndexFixedStringSuffix                  >
::reg<NAME(Dynamic), 1, IndexBlobStoreSuffix<ZipOffsetBlobStore>>
::reg<NAME(DictZip), 1, IndexBlobStoreSuffix<DictZipBlobStore  >>
::list;

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

FactoryExpander<PrefixComponentList, SuffixComponentList>::ExpandedFactorySet g_factory_init;

} // namespace rocksdb
