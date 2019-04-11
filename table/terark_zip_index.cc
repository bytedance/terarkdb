
#ifndef INDEX_UT
#include "db/builder.h" // for cf_options.h
#endif

#include "terark_zip_index.h"
#include "terark_zip_table.h"
#include "terark_zip_common.h"
#include <terark/bitmap.hpp>
#include <terark/hash_strmap.hpp>
#include <terark/fsa/dfa_mmap_header.hpp>
#include <terark/fsa/fsa_cache.hpp>
#include <terark/fsa/nest_louds_trie_inline.hpp>
#include <terark/fsa/nest_trie_dawg.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/num_to_str.hpp>
#if defined(TerocksPrivateCode)
#include <terark/fsa/fsa_for_union_dfa.hpp>
#endif // TerocksPrivateCode

namespace rocksdb {

using namespace terark;

typedef rank_select_fewzero<uint32_t> rs_fewzero_32;
typedef rank_select_fewzero<uint64_t> rs_fewzero_64;
typedef rank_select_fewone<uint32_t> rs_fewone_32;
typedef rank_select_fewone<uint64_t> rs_fewone_64;

template<class RankSelect> struct RankSelectNeedHint : public std::false_type {};
template<> struct RankSelectNeedHint<rs_fewzero_32> : public std::true_type {};
template<> struct RankSelectNeedHint<rs_fewzero_64> : public std::true_type {};
template<> struct RankSelectNeedHint<rs_fewone_32> : public std::true_type {};
template<> struct RankSelectNeedHint<rs_fewone_64> : public std::true_type {};

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

enum class WorkingState : uint32_t {
  Building = 1,
  UserMemory = 2,
  MmapFile = 3,
};
using terark::getEnvBool;
static bool g_indexEnableFewZero = getEnvBool("TerarkZipTable_enableFewZero", false);
static bool g_indexEnableUintIndex = getEnvBool("TerarkZipTable_enableUintIndex", true);
static bool g_indexEnableCompositeUintIndex = getEnvBool("TerarkZipTable_enableCompositeUintIndex", true);
static bool g_indexEnableSortedUint = getEnvBool("TerarkZipTable_enableSortedUint", true);
static bool g_indexEnableBigUint0 = getEnvBool("TerarkZipTable_enableBigUint0", true);

static hash_strmap<TerarkIndex::FactoryPtr> g_TerarkIndexFactroy;
static hash_strmap<std::string,
  fstring_func::hash_align,
  fstring_func::equal_align,
  ValueInline,
  SafeCopy ///< some std::string is not memmovable
> g_TerarkIndexName;
static hash_strmap<TerarkIndex::FactoryPtr> g_TerarkIndexCombin;


template<class IndexClass>
bool VerifyClassName(fstring class_name) {
  size_t name_i = g_TerarkIndexName.find_i(typeid(IndexClass).name());
  size_t self_i = g_TerarkIndexFactroy.find_i(g_TerarkIndexName.val(name_i));
  assert(self_i < g_TerarkIndexFactroy.end_i());
  size_t head_i = g_TerarkIndexFactroy.find_i(class_name);
  return head_i < g_TerarkIndexFactroy.end_i() &&
    g_TerarkIndexFactroy.val(head_i) == g_TerarkIndexFactroy.val(self_i);
}


template<size_t Align, class Writer>
void Padzero(const Writer& write, size_t offset) {
  static const char zeros[Align] = { 0 };
  if (offset % Align) {
    write(zeros, Align - offset % Align);
  }
}

struct TerarkIndexHeader {
  uint8_t   magic_len;
  char      magic[19];
  char      class_name[60];

  uint32_t  reserved_80_4;
  uint32_t  header_size;
  uint32_t  version;
  uint32_t  reserved_92_4;

  uint64_t  file_size;
  uint64_t  reserved_102_24;
};

TerarkIndex::AutoRegisterFactory::AutoRegisterFactory(
  std::initializer_list<const char*> names,
  const char* rtti_name,
  Factory* factory) {
  assert(names.size() > 0);
  auto combinName = factory->CombinName();
  if (combinName != nullptr) {
    g_TerarkIndexCombin.insert_i(combinName, factory);
  }
  fstring wireName = *names.begin();
  TERARK_RT_assert(!g_TerarkIndexFactroy.exists(wireName), std::logic_error);
  factory->mapIndex = g_TerarkIndexFactroy.end_i();
  for (const char* name : names) {
    g_TerarkIndexFactroy.insert_i(name, FactoryPtr(factory));
  }
  g_TerarkIndexName.insert_i(rtti_name, wireName.c_str());
}

const TerarkIndex::Factory* GetFactoryByCombinName(fstring name) {
  size_t idx = g_TerarkIndexCombin.find_i(name);
  if (idx < g_TerarkIndexCombin.end_i()) {
    auto factory = g_TerarkIndexCombin.val(idx).get();
    return factory;
  }
  return NULL;
}

const TerarkIndex::Factory* TerarkIndex::GetFactory(fstring name) {
  size_t idx = g_TerarkIndexFactroy.find_i(name);
  if (idx < g_TerarkIndexFactroy.end_i()) {
    auto factory = g_TerarkIndexFactroy.val(idx).get();
    return factory;
  }
  return NULL;
}

const char* TerarkIndex::Factory::WireName() const {
  TERARK_RT_assert(mapIndex < g_TerarkIndexFactroy.end_i(), std::logic_error);
  return g_TerarkIndexFactroy.key_c_str(mapIndex);
}

TerarkIndex::~TerarkIndex() {}
TerarkIndex::Factory::~Factory() {}
TerarkIndex::Iterator::~Iterator() {}

template<class NLTrie>
void NestLoudsTrieBuildCache(NLTrie* trie, double cacheRatio) {
  trie->build_fsa_cache(cacheRatio, NULL);
}
void NestLoudsTrieBuildCache(MatchingDFA* dfa, double cacheRatio) {}


template<class NLTrie>
void NestLoudsTrieGetOrderMap(const NLTrie* trie, UintVecMin0& newToOld) {
  NonRecursiveDictionaryOrderToStateMapGenerator gen;
  gen(*trie, [&](size_t dictOrderOldId, size_t state) {
    size_t newId = trie->state_to_word_id(state);
    //assert(trie->state_to_dict_index(state) == dictOrderOldId);
    //assert(trie->dict_index_to_state(dictOrderOldId) == state);
    newToOld.set_wire(newId, dictOrderOldId);
  });
}
void NestLoudsTrieGetOrderMap(const MatchingDFA* dfa, UintVecMin0& newToOld) {
  assert(0);
}

//struct NestLoudsTrieIndexBase : public TerarkIndex {
//  const BaseDAWG* m_dawg;
//  unique_ptr<BaseDFA> m_trie;
//
//  struct MyBaseFactory : public Factory {
//
//    template<class StrVec>
//    static void AssertSorted(const StrVec& keyVec) {
//#if !defined(NDEBUG)
//      for (size_t i = 1; i < keyVec.size(); ++i) {
//        fstring prev = keyVec[i - 1];
//        fstring curr = keyVec[i];
//        assert(prev < curr);
//      }
//#endif
//    }
//
//    size_t MemSizeForBuild(const KeyStat& ks) const override {
//      size_t sumRealKeyLen = ks.sumKeyLen - ks.commonPrefixLen * ks.prefix.m_cnt_sum;
//      if (ks.minKeyLen == ks.maxKeyLen) {
//        return sumRealKeyLen;
//      }
//      size_t indexSize = UintVecMin0::compute_mem_size_by_max_val(ks.prefix.m_cnt_sum + 1, sumRealKeyLen);
//      return indexSize + sumRealKeyLen;
//    }
//  };
//
//  NestLoudsTrieIndexBase(BaseDFA* trie) : m_trie(trie) {
//    m_dawg = trie->get_dawg();
//  }
//
//  const char* Name() const override {
//    if (m_trie->is_mmap()) {
//      auto header = (const TerarkIndexHeader*)m_trie->get_mmap().data();
//      return header->class_name;
//    }
//    else {
//      size_t name_i = g_TerarkIndexName.find_i(typeid(*this).name());
//      TERARK_RT_assert(name_i < g_TerarkIndexName.end_i(), std::logic_error);
//      return g_TerarkIndexName.val(name_i).c_str();
//    }
//  }
//  void SaveMmap(std::function<void(const void *, size_t)> write) const override {
//    m_trie->save_mmap(write);
//  }
//  size_t Find(fstring key) const override final {
//    return m_dawg->index(key);
//  }
//  virtual size_t DictRank(fstring key) const {
//    return m_dawg->dict_rank(key);
//  }
//  size_t NumKeys() const override final {
//    return m_dawg->num_words();
//  }
//  size_t TotalKeySize() const override final {
//    return m_trie->adfa_total_words_len();
//  }
//  fstring Memory() const override final {
//    return m_trie->get_mmap();
//  }
//  bool NeedsReorder() const override final { return true; }
//};
//
//template<class NLTrie>
//class NestLoudsTrieIndex : public NestLoudsTrieIndexBase {
//  typedef NestLoudsTrieIter<NLTrie> MyIterator;
//public:
//  NestLoudsTrieIndex(NLTrie* trie) : NestLoudsTrieIndexBase(trie) {}
//  Iterator* NewIterator() const override final {
//    auto trie = static_cast<const NLTrie*>(m_trie.get());
//    return new MyIterator(trie);
//  }
//  void GetOrderMap(UintVecMin0& newToOld)
//    const override final {
//    auto trie = static_cast<const NLTrie*>(m_trie.get());
//    NestLoudsTrieGetOrderMap(trie, newToOld);
//  }
//  void BuildCache(double cacheRatio) {
//    if (cacheRatio > 1e-8) {
//      auto trie = static_cast<NLTrie*>(m_trie.get());
//      NestLoudsTrieBuildCache(trie, cacheRatio);
//    }
//  }
//  class MyFactory : public MyBaseFactory {
//  public:
//    TerarkIndex* Build(NativeDataInput<InputBuffer>& reader,
//      const TerarkZipTableOptions& tzopt,
//      const KeyStat& ks,
//      const ImmutableCFOptions* ioptions) const override {
//      valvec<byte_t> keyBuf;
//      if (ks.minKeyLen != ks.maxKeyLen) {
//        SortedStrVec keyVec;
//        Read_SortedStrVec(keyVec, reader, ks);
//        return BuildImpl(tzopt, keyVec);
//      }
//      else {
//        FixedLenStrVec keyVec;
//        Read_FixedLenStrVec(keyVec, reader, ks);
//        return BuildImpl(tzopt, keyVec);
//      }
//    }
//  private:
//    template<class StrVec>
//    TerarkIndex*
//      BuildImpl(const TerarkZipTableOptions& tzopt, StrVec& keyVec) const {
//      AssertSorted(keyVec);
//      NestLoudsTrieConfig conf;
//      Set_NestLoudsTrieConfig(conf, keyVec, tzopt);
//      std::unique_ptr<NLTrie> trie(new NLTrie());
//      trie->build_from(keyVec, conf);
//      return new NestLoudsTrieIndex(trie.release());
//    }
//  public:
//    unique_ptr<TerarkIndex> LoadMemory(fstring mem) const override {
//      unique_ptr<BaseDFA>
//        dfa(BaseDFA::load_mmap_user_mem(mem.data(), mem.size()));
//      auto trie = dynamic_cast<NLTrie*>(dfa.get());
//      if (NULL == trie) {
//        throw std::invalid_argument("Bad trie class: " + ClassName(*dfa)
//          + ", should be " + ClassName<NLTrie>());
//      }
//      auto index = new NestLoudsTrieIndex(trie);
//      dfa.release();
//      return unique_ptr<TerarkIndex>(index);
//    }
//    unique_ptr<TerarkIndex> LoadFile(fstring fpath) const override {
//      unique_ptr<BaseDFA> dfa(BaseDFA::load_mmap(fpath));
//      auto trie = dynamic_cast<NLTrie*>(dfa.get());
//      if (NULL == trie) {
//        throw std::invalid_argument(
//          "File: " + fpath + ", Bad trie class: " + ClassName(*dfa)
//          + ", should be " + ClassName<NLTrie>());
//      }
//      auto index = new NestLoudsTrieIndex(trie);
//      dfa.release();
//      return unique_ptr<TerarkIndex>(index);
//    }
//  };
//};

namespace composite_index_detail {

  struct Common {
    Common() : working_state(WorkingState::UserMemory) {}
    Common(Common&& o) : common(o.common), working_state(o.working_state) {
      o.working_state = WorkingState::UserMemory;
    }
    Common(fstring c, bool ownership) : working_state(WorkingState::UserMemory) {
      reset(c, ownership);
    }
    void reset(fstring c, bool ownership) {
      if (working_state == WorkingState::Building) {
        free((void*)common.p);
        working_state = WorkingState::UserMemory;
      }
      if (ownership && !c.empty()) {
        working_state = WorkingState::Building;
        auto p = (char*)malloc(c.size());
        if (p == nullptr) {
          throw std::bad_alloc();
        }
        memcpy(p, c.p, c.size());
        common.p = p;
        common.n = c.size();
      }
      else {
        common = c;
      }
    }
    ~Common() {
      if (working_state == WorkingState::Building) {
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
    WorkingState working_state;
  };

  struct ComponentBase {
    WorkingState working_state;

    virtual bool Load(fstring mem) = 0;
    virtual void Save(std::function<void(void*, size_t)> append) const = 0;
    virtual ~ComponentBase() {}
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

  struct SuffixLowerBound {
    virtual std::pair<size_t, size_t> LowerBound(fstring target, size_t suffix_id, size_t suffix_count) const = 0;
  };

  struct VirtualPrefixBase {
    virtual ~VirtualPrefixBase() {}

    virtual size_t IteratorStorageSize() const = 0;
    virtual void IteratorStorageConstruct(void* ptr) const = 0;
    virtual void IteratorStorageDestruct(void* ptr) const = 0;

    virtual size_t KeyCount() const = 0;
    virtual size_t TotalKeySize() const = 0;
    virtual size_t Find(fstring key, SuffixLowerBound const* slb) const = 0;
    virtual size_t DictRank(fstring key, SuffixLowerBound const* slb) const = 0;
    virtual bool NeedsReorder() const = 0;

    virtual bool IterSeekToFirst(size_t& id, void* iter) const = 0;
    virtual bool IterSeekToLast(size_t& id, void* iter) const = 0;
    virtual bool IterSeek(size_t& id, size_t& count, fstring target, void* iter) const = 0;
    virtual bool IterNext(size_t& id, size_t count, void* iter) const = 0;
    virtual bool IterPrev(size_t& id, void* iter) const = 0;
    virtual fstring IterGetKey(size_t id, const void* iter) const = 0;
    virtual size_t IterDictRank(size_t id, const void* iter) const = 0;

    virtual bool Load(fstring mem) = 0;
    virtual void Save(std::function<void(void*, size_t)> append) const = 0;
  };
  template<class Prefix>
  struct VirtualPrefixWrapper : public VirtualPrefixBase, public Prefix {
    using IteratorStorage = typename Prefix::IteratorStorage;
    VirtualPrefixWrapper(Prefix *prefix) : Prefix(prefix) {}

    size_t IteratorStorageSize() const override {
      return Prefix::IteratorStorageSize();
    }
    void IteratorStorageConstruct(void* ptr) const {
      Prefix::IteratorStorageConstruct(ptr);
    }
    void IteratorStorageDestruct(void* ptr) const {
      Prefix::IteratorStorageDestruct(ptr);
    }

    size_t KeyCount() const override {
      return Prefix::KeyCount();
    }
    size_t TotalKeySize() const override {
      return Prefix::TotalKeySize();
    }
    size_t Find(fstring key, SuffixLowerBound const *slb) const override {
      return Prefix::Find(key, slb);
    }
    size_t DictRank(fstring key, SuffixLowerBound const *slb) const override {
      return Prefix::DictRank(key, slb);
    }
    bool NeedsReorder() const override {
      return Prefix::NeedsReorder();
    }

    bool IterSeekToFirst(size_t& id, void* iter) const override {
      return Prefix::IterSeekToFirst(id, (IteratorStorage*)iter);
    }
    bool IterSeekToLast(size_t& id, void* iter) const override {
      return Prefix::IterSeekToLast(id, (IteratorStorage*)iter);
    }
    bool IterSeek(size_t& id, size_t& count, fstring target, void* iter) const override {
      return Prefix::IterSeek(id, count, target, (IteratorStorage*)iter);
    }
    bool IterNext(size_t& id, size_t count, void* iter) const override {
      return Prefix::IterNext(id, count, (IteratorStorage*)iter);
    }
    bool IterPrev(size_t& id, void* iter) const override {
      return Prefix::IterPrev(id, (IteratorStorage*)iter);
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
    void Save(std::function<void(void*, size_t)> append) const override {
      Prefix::Save(append);
    }
  };
  struct VirtualPrefix : public ComponentBase {
    typedef void* IteratorStorage;
    template<class Prefix>
    VirtualPrefix(Prefix* p) {
      prefix = new VirtualPrefixWrapper<Prefix>(p);
    }
    template<class Prefix>
    VirtualPrefix(Prefix&& p) : VirtualPrefix(&p) {}
    ~VirtualPrefix() {
      delete prefix;
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
    size_t Find(fstring key, SuffixLowerBound const *slb) const {
      return prefix->Find(key, slb);
    }
    size_t DictRank(fstring key, SuffixLowerBound const *slb) const {
      return prefix->DictRank(key, slb);
    }
    bool NeedsReorder() const {
      return prefix->NeedsReorder();
    }

    bool IterSeekToFirst(size_t& id, void* iter) const {
      return prefix->IterSeekToFirst(id, iter);
    }
    bool IterSeekToLast(size_t& id, void* iter) const {
      return prefix->IterSeekToLast(id, iter);
    }
    bool IterSeek(size_t& id, size_t& count, fstring target, void* iter) const {
      return prefix->IterSeek(id, count, target, iter);
    }
    bool IterNext(size_t& id, size_t count, void* iter) const {
      return prefix->IterNext(id, count, iter);
    }
    bool IterPrev(size_t& id, void* iter) const {
      return prefix->IterPrev(id, iter);
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
    void Save(std::function<void(void*, size_t)> append) const override {
      prefix->Save(append);
    }
  };

  struct VirtualSuffixBase : public SuffixLowerBound {
    virtual ~VirtualSuffixBase() {}

    virtual size_t IteratorStorageSize() const = 0;
    virtual void IteratorStorageConstruct(void* ptr) const = 0;
    virtual void IteratorStorageDestruct(void* ptr) const = 0;

    virtual size_t TotalKeySize() const = 0;
    using SuffixLowerBound::LowerBound;

    virtual void IterSet(size_t suffix_id, void* iter) const = 0;
    virtual bool IterSeek(fstring target, size_t& suffix_id, size_t suffix_count, void* iter) const = 0;
    virtual fstring IterGetKey(size_t id, const void* iter) const = 0;

    virtual bool Load(fstring mem) = 0;
    virtual void Save(std::function<void(void*, size_t)> append) const = 0;
  };
  template<class Suffix>
  struct VirtualSuffixWrapper : public VirtualSuffixBase, public Suffix {
    using IteratorStorage = typename Suffix::IteratorStorage;
    VirtualSuffixWrapper(Suffix *suffix) : Suffix(suffix) {}

    size_t IteratorStorageSize() const override {
      return Suffix::IteratorStorageSize();
    }
    void IteratorStorageConstruct(void* ptr) const {
      Suffix::IteratorStorageConstruct(ptr);
    }
    void IteratorStorageDestruct(void* ptr) const {
      Suffix::IteratorStorageDestruct(ptr);
    }

    size_t TotalKeySize() const override {
      return Suffix::TotalKeySize();
    }
    std::pair<size_t, size_t> LowerBound(fstring target, size_t suffix_id, size_t suffix_count) const override {
      return Suffix::LowerBound(target, suffix_id, suffix_count);
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
    void Save(std::function<void(void*, size_t)> append) const override {
      Suffix::Save(append);
    }
  };
  struct VirtualSuffix : public ComponentBase, public SuffixLowerBound {
    typedef void* IteratorStorage;
    template<class Suffix>
    VirtualSuffix(Suffix* s) {
      suffix = new VirtualSuffixWrapper<Suffix>(s);
    }
    template<class Suffix>
    VirtualSuffix(Suffix&& s) : VirtualSuffix(&s) {}
    ~VirtualSuffix() {
      delete suffix;
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
    std::pair<size_t, size_t> LowerBound(fstring target, size_t suffix_id, size_t suffix_count) const override {
      return suffix->LowerBound(target, suffix_id, suffix_count);
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
    void Save(std::function<void(void*, size_t)> append) const override {
      suffix->Save(append);
    }
  };

  template<class Prefix, class Suffix>
  struct CompositeIndexParts {
    typedef composite_index_detail::Common Common;
    CompositeIndexParts() {}
    CompositeIndexParts(Common&& common, Prefix&& prefix, Suffix&& suffix)
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
    const ComponentBase& prefix_;
    const ComponentBase& suffix_;
    void* prefix_storage_;
    void* suffix_storage_;
    std::function<void(void*, void*)> destructor_;

    template<class Prefix, class Suffix>
    static size_t GetIteratorStorageSize(const CompositeIndexParts<Prefix, Suffix>* index) {
      return 0
        + (index->prefix_.IteratorStorageSize() + sizeof(size_t) - 1) / sizeof(size_t)
        + (index->suffix_.IteratorStorageSize() + sizeof(size_t) - 1) / sizeof(size_t)
        ;
    }

    template<class Prefix, class Suffix>
    IteratorStorage(const CompositeIndexParts<Prefix, Suffix>* index, void* iterator_storage, size_t iterator_storage_size)
      : common_(index->common_)
      , prefix_(index->prefix_)
      , suffix_(index->suffix_) {
      assert(iterator_storage_size >= GetIteratorStorageSize(index));
      prefix_storage_ = iterator_storage;
      suffix_storage_ = (void*)(uintptr_t(prefix_storage_) + (index->prefix_.IteratorStorageSize() + sizeof(size_t) - 1) / sizeof(size_t));
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

}

class CompositeIndexFactoryBase : public TerarkIndex::Factory {
public:
  typedef composite_index_detail::ComponentBase ComponentBase;
  typedef composite_index_detail::Common Common;

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
      asc_few_zero_32,
      asc_few_zero_64,
      asc_il_256,
      asc_se_512,
      asc_few_one_32,
      asc_few_one_64,
      non_desc_il_256,
      non_desc_se_512,
      non_desc_few_one_32,
      non_desc_few_one_64,
    } type;
  };

  template<class RankSelect, class InputBufferType>
  ComponentBase* BuildAscendingUintPrefix(
    InputBufferType& reader,
    const TerarkZipTableOptions& tzopt,
    const TerarkIndex::KeyStat& ks,
    const UintPrefixBuildInfo& info,
    const ImmutableCFOptions* ioption,
    std::string& name) const;


  template<class RankSelect, class InputBufferType>
  ComponentBase* BuildNonDescendingUintPrefix(
    InputBufferType& reader,
    const TerarkZipTableOptions& tzopt,
    const TerarkIndex::KeyStat& ks,
    const UintPrefixBuildInfo& info,
    const ImmutableCFOptions* ioption,
    std::string& name) const;

  template<class RankSelect, class InputBufferType>
  ComponentBase* BuildNestLoudsTriePrefix(InputBufferType& reader,
    const TerarkZipTableOptions& tzopt,
    size_t numKeys, size_t sumKeyLen,
    bool isReverse, bool isFixedLen,
    const ImmutableCFOptions* ioption,
    std::string& name) const;

  ComponentBase* BuildEmptySuffix(std::string& name) const;

  TerarkIndex* Build(NativeDataInput<InputBuffer>& reader,
    const TerarkZipTableOptions& tzopt,
    const TerarkIndex::KeyStat& ks,
    const ImmutableCFOptions* ioption = nullptr) const;

  size_t MemSizeForBuild(const TerarkIndex::KeyStat& ks) const {
    // TODO
    size_t indexSize = UintVecMin0::compute_mem_size_by_max_val(ks.sumKeyLen, ks.prefix.m_cnt_sum);
    return ks.sumKeyLen + indexSize;
  }
  unique_ptr<TerarkIndex> LoadMemory(fstring mem) const {
    // TODO;
    return nullptr;
  }
  unique_ptr<TerarkIndex> LoadFile(fstring fpath) const {
    // TODO;
    return nullptr;
  }
  virtual void SaveMmap(const TerarkIndex* index, std::function<void(const void *, size_t)> write) const {
    // TODO;
  }
protected:
  virtual TerarkIndex* CreateIndex(Common&& common,
    ComponentBase* prefix,
    ComponentBase* suffix) const {
    TERARK_RT_assert(0, std::logic_error);
    return nullptr;
  }
  virtual ComponentBase* CreatePrefix() const {
    TERARK_RT_assert(0, std::logic_error);
    return nullptr;
  }
  virtual ComponentBase* CreateSuffix() const {
    TERARK_RT_assert(0, std::logic_error);
    return nullptr;
  }
};

template<class Prefix, class Suffix>
class CompositeIndexIterator
  : public TerarkIndex::Iterator
  , public composite_index_detail::IteratorStorage {
public:
  using IteratorStorage = composite_index_detail::IteratorStorage;

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
  std::pair<void*, size_t> AllocIteratorStorage_(const composite_index_detail::CompositeIndexParts<Prefix, Suffix>* index) {
    size_t iterator_storage_size = index == nullptr ? 0 : composite_index_detail::IteratorStorage::GetIteratorStorageSize(index);
    iterator_storage_.reset(iterator_storage_size > 0 ? ::new byte_t[iterator_storage_size] : nullptr);
    return { iterator_storage_.get(), iterator_storage_size };
  }
  CompositeIndexIterator(const composite_index_detail::CompositeIndexParts<Prefix, Suffix>* index, std::pair<void*, size_t> iterator_storage)
    : IteratorStorage(index, iterator_storage.first, iterator_storage.second) {}

public:
  CompositeIndexIterator(const composite_index_detail::CompositeIndexParts<Prefix, Suffix>* index, void* iterator_storage, size_t iterator_storage_size)
    : CompositeIndexIterator(index, { iterator_storage, iterator_storage_size }) {}

  CompositeIndexIterator(const composite_index_detail::CompositeIndexParts<Prefix, Suffix>* index)
    : CompositeIndexIterator(index, AllocIteratorStorage_(index)) {}

  bool SeekToFirst() override {
    if (!prefix().IterSeekToFirst(m_id, prefix_storage())) {
      assert(m_id == size_t(-1));
      return false;
    }
    suffix().IterSet(m_id, suffix_storage());
    return true;

  }
  bool SeekToLast() override {
    if (!prefix().IterSeekToLast(m_id, prefix_storage())) {
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
    if (!prefix().IterSeek(m_id, suffix_count, target, prefix_storage())) {
      assert(m_id == size_t(-1));
      return false;
    }
    fstring prefix_key = prefix().IterGetKey(m_id, prefix_storage());
    assert(prefix_key <= target);
    if (prefix_key.size() != target.size()) {
      suffix().IterSet(m_id, suffix_storage());
      return true;
    }
    target = target.substr(prefix_key.size());
    size_t suffix_id = m_id;
    if (suffix().IterSeek(target, suffix_id, suffix_count, suffix_storage())) {
      assert(suffix_id >= m_id);
      assert(suffix_id < m_id + suffix_count);
      if (suffix_id > m_id && !prefix().IterNext(m_id, suffix_id - m_id, prefix_storage())) {
        assert(m_id == size_t(-1));
        return false;
      }
    }
    else {
      if (!prefix().IterNext(m_id, suffix_count, prefix_storage())) {
        assert(m_id == size_t(-1));
        return false;
      }
      suffix().IterSet(m_id, suffix_storage());
    }
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
    if (prefix().IterPrev(m_id, prefix_storage())) {
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
//        Mixed_XL_256
//        SE_512_64
//      AscendingUintPrefix<>
//        FewZero32
//        FewZero64
//        FewOne32
//        FewOne64
//      NonDescendingUintPrefix<>
//        FewOne32
//        FewOne64
//    AscendingUintPrefix<>
//      AllOne
//      IL_256_32
//      SE_512_64
//    NonDescendingUintPrefix<>
//      IL_256_32
//      SE_512_64
//  Suffix :
//    VirtualImpl :
//      DynamicString
//      EntropyString
//      DictZipString
//      Number<>
//        SortedUintVec
//    Empty
//    FixedString
//    Number<>
//      BigUintVecMin0

template<class Prefix, class Suffix>
class CompositeIndex : public TerarkIndex, public composite_index_detail::CompositeIndexParts<Prefix, Suffix> {
public:
  typedef composite_index_detail::CompositeIndexParts<Prefix, Suffix> CompositeIndexParts;
  typedef composite_index_detail::Common Common;
  using CompositeIndexParts::common_;
  using CompositeIndexParts::prefix_;
  using CompositeIndexParts::suffix_;

  CompositeIndex(const CompositeIndexFactoryBase* factory)
    : factory_(factory)
    , header_(nullptr) {
  }
  CompositeIndex(const CompositeIndexFactoryBase* factory, Common&& common, Prefix&& prefix, Suffix&& suffix)
    : composite_index_detail::CompositeIndexParts<Prefix, Suffix>(std::move(common), std::move(prefix), std::move(suffix))
    , factory_(factory)
    , header_(nullptr) {
  }

  const CompositeIndexFactoryBase* factory_;
  const TerarkIndexHeader* header_;

  const char* Name() const override {
    return factory_->WireName();
  }
  void SaveMmap(std::function<void(const void *, size_t)> write) const override {
    factory_->SaveMmap(this, write);
  }
  size_t Find(fstring key) const override {
    if (key.commonPrefixLen(common_) != common_.size()) {
      return size_t(-1);
    }
    key = key.substr(common_.size());
    return prefix_.Find(key, suffix_.TotalKeySize() != 0 ? &suffix_ : nullptr);
  }
  size_t DictRank(fstring key) const override {
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
    return prefix_.DictRank(key, suffix_.TotalKeySize() != 0 ? &suffix_ : nullptr);
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
  Iterator* NewIterator() const override {
    return new CompositeIndexIterator<Prefix, Suffix>(this);
  }
  bool NeedsReorder() const override {
    return prefix_.NeedsReorder();
  }
  void GetOrderMap(terark::UintVecMin0& newToOld) const  override {
  }
  void BuildCache(double cacheRatio) override {
  }
};

namespace composite_index_detail {

  template<class Prefix, size_t PV, class Suffix, size_t SV>
  struct CompositeIndexDeclare {
    typedef CompositeIndex<
      typename std::conditional<PV, VirtualPrefix, Prefix>::type,
      typename std::conditional<SV, VirtualSuffix, Suffix>::type
    > index_type;
  };


  template<class Prefix, size_t PV, class Suffix, size_t SV>
  class CompositeIndexFactory : public CompositeIndexFactoryBase {
  public:
    typedef typename CompositeIndexDeclare<Prefix, PV, Suffix, SV>::index_type index_type;

    const char* CombinName() const override {
      static std::string name =
        std::string(typeid(Prefix).name()) + std::string(typeid(Suffix).name());
      return name.c_str();
    }
  protected:
    TerarkIndex* CreateIndex(
      Common&& common,
      ComponentBase* prefix,
      ComponentBase* suffix) const override {
      return new index_type(this, std::move(common), Prefix(prefix), Suffix(suffix));
    }
    ComponentBase* CreatePrefix() const override {
      return new Prefix();
    }
    ComponentBase* CreateSuffix() const override {
      return new Suffix();
    }
  };

}

using composite_index_detail::CompositeIndexDeclare;
using composite_index_detail::CompositeIndexFactory;

#define RegisterCompositeIndex(Prefix, PV, Suffix, SV, Name, ...)                     \
typedef typename CompositeIndexDeclare<Prefix, PV, Suffix, SV>::index_type Name;      \
typedef CompositeIndexFactory<Prefix, PV, Suffix, SV> Name##Factory;                  \
TerarkIndexRegisterWithFactory(Name, Name##Factory, ##__VA_ARGS__)

#define RegisterCompositeIndexWithFactory(Prefix, PV, Suffix, SV, Name, Factory, ...) \
typedef typename CompositeIndexDeclare<Prefix, PV, Suffix, SV>::index_type Name;      \
TerarkIndexRegisterWithFactory(Name, Factory, ##__VA_ARGS__)


////////////////////////////////////////////////////////////////////////////////
// Impls
////////////////////////////////////////////////////////////////////////////////

template<class WithHint>
struct CompositeIndexUintPrefixIteratorStorage {
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
struct CompositeIndexUintPrefixIteratorStorage<std::false_type> {
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
struct CompositeIndexAscendingUintPrefix
  : public composite_index_detail::ComponentBase
  , public composite_index_detail::ComponentIteratorStorageImpl<CompositeIndexUintPrefixIteratorStorage<typename RankSelectNeedHint<RankSelect>::type>> {
  using IteratorStorage = CompositeIndexUintPrefixIteratorStorage<typename RankSelectNeedHint<RankSelect>::type>;

  CompositeIndexAscendingUintPrefix() = default;
  CompositeIndexAscendingUintPrefix(CompositeIndexAscendingUintPrefix&&) = default;
  CompositeIndexAscendingUintPrefix(ComponentBase* base) {
    assert(dynamic_cast<CompositeIndexAscendingUintPrefix<RankSelect>*>(base) != nullptr);
    auto other = static_cast<CompositeIndexAscendingUintPrefix<RankSelect>*>(base);
    rank_select.swap(other->rank_select);
    key_length = other->key_length;
    min_value = other->min_value;
    max_value = other->max_value;
    working_state = base->working_state;
    delete other;
  }
  ~CompositeIndexAscendingUintPrefix() {
    if (working_state != WorkingState::Building) {
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
  size_t Find(fstring key, composite_index_detail::SuffixLowerBound const *slb) const {
    if (key.size() < key_length) {
      return size_t(-1);
    }
    uint64_t value = ReadBigEndianUint64(key);
    if (value < min_value || value > max_value) {
      return size_t(-1);
    }
    uint64_t pos = value - min_value;
    if (!rank_select[pos]) {
      return size_t(-1);
    }
    size_t id = rank_select.rank1(pos);
    if (slb == nullptr) {
      return key.size() == key_length ? id : size_t(-1);
    }
    size_t suffix_id, common;
    std::tie(suffix_id, common) = slb->LowerBound(key.substr(key_length), id, 1);
    if (suffix_id != id || key_length + common != key.size()) {
      return size_t(-1);
    }
    return suffix_id;
  }
  size_t DictRank(fstring key, composite_index_detail::SuffixLowerBound const *slb) const {
    size_t id, pos, hint;
    bool seek_result, is_find;
    std::tie(seek_result, is_find) = SeekImpl(key, id, pos, &hint);
    if (!seek_result) {
      return rank_select.max_rank1();
    }
    if (key.size() != key_length || !is_find) {
      return id + 1;
    }
    size_t suffix_id, common;
    std::tie(suffix_id, common) = slb->LowerBound(key.substr(key_length), id, 1);
    if (suffix_id != id || key_length + common != key.size()) {
      return rank_select.max_rank1();
    }
    return suffix_id;
  }
  bool NeedsReorder() const {
    return false;
  }

  bool IterSeekToFirst(size_t& id, IteratorStorage* iter) const {
    id = 0;
    iter->pos = 0;
    UpdateBuffer(id, iter);
    return true;
  }
  bool IterSeekToLast(size_t& id, IteratorStorage* iter) const {
    id = rank_select.max_rank1() - 1;
    iter->pos = rank_select.size() - 1;
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
  bool IterPrev(size_t& id, IteratorStorage* iter) const {
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
    return false;
  }
  void Save(std::function<void(void*, size_t)> append) const override {
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
struct CompositeIndexNonDescendingUintPrefix
  : public composite_index_detail::ComponentBase
  , public composite_index_detail::ComponentIteratorStorageImpl<CompositeIndexUintPrefixIteratorStorage<typename RankSelectNeedHint<RankSelect>::type>> {
  using IteratorStorage = CompositeIndexUintPrefixIteratorStorage<typename RankSelectNeedHint<RankSelect>::type>;

  CompositeIndexNonDescendingUintPrefix() = default;
  CompositeIndexNonDescendingUintPrefix(CompositeIndexNonDescendingUintPrefix&&) = default;
  CompositeIndexNonDescendingUintPrefix(ComponentBase* base) {
    assert(dynamic_cast<CompositeIndexNonDescendingUintPrefix<RankSelect>*>(base) != nullptr);
    auto other = static_cast<CompositeIndexNonDescendingUintPrefix<RankSelect>*>(base);
    rank_select.swap(other->rank_select);
    key_length = other->key_length;
    min_value = other->min_value;
    max_value = other->max_value;
    working_state = base->working_state;
    delete other;
  }
  ~CompositeIndexNonDescendingUintPrefix() {
    if (working_state != WorkingState::Building) {
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
  size_t Find(fstring key, composite_index_detail::SuffixLowerBound const *slb) const {
    if (key.size() < key_length) {
      return size_t(-1);
    }
    uint64_t value = ReadBigEndianUint64(key);
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
    if (slb == nullptr) {
      return key.size() == key_length ? id : size_t(-1);
    }
    size_t suffix_id, common;
    std::tie(suffix_id, common) = slb->LowerBound(key.substr(key_length), id, count);
    if (suffix_id == id + count || key_length + common != key.size()) {
      return size_t(-1);
    }
    return suffix_id;
  }
  size_t DictRank(fstring key, composite_index_detail::SuffixLowerBound const *slb) const {
    size_t id, count, pos, hint;
    bool seek_result, is_find;
    std::tie(seek_result, is_find) = SeekImpl(key, id, count, pos, &hint);
    if (!seek_result) {
      return rank_select.max_rank1();
    }
    if (key.size() != key_length || !is_find) {
      return id + 1;
    }
    size_t suffix_id, common;
    std::tie(suffix_id, common) = slb->LowerBound(key.substr(key_length), id, count);
    if (suffix_id != id || key_length + common != key.size()) {
      return rank_select.max_rank1();
    }
    return suffix_id;
  }
  bool NeedsReorder() const {
    return false;
  }

  bool IterSeekToFirst(size_t& id, IteratorStorage* iter) const {
    id = 0;
    iter->pos = 0;
    UpdateBuffer(id, iter);
    assert(rank_select[iter->pos]);
    return true;
  }
  bool IterSeekToLast(size_t& id, IteratorStorage* iter) const {
    id = rank_select.max_rank1() - 1;
    iter->pos = rank_select.size() - 2;
    assert(rank_select[iter->pos]);
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
  bool IterPrev(size_t& id, IteratorStorage* iter) const {
    assert(id != size_t(-1));
    assert(rank_select[iter->pos]);
    assert(rank_select.rank1(iter->pos) == id);
    if (id == 0) {
      id = size_t(-1);
      return false;
    }
    else {
      size_t zero_seq_revlen = rank_select.zero_seq_revlen(iter->pos);
      --id;
      iter->pos -= zero_seq_revlen + 1;
      if (zero_seq_revlen > 0) {
        UpdateBuffer(id, iter);
      }
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
    return false;
  }
  void Save(std::function<void(void*, size_t)> append) const override {
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
      pos += rank_select.zero_seq_len(pos + 1);
      id = rank_select.rank1(pos);
      count = rank_select.one_seq_len(pos);
      return { true, false };
    }
  }
  void UpdateBuffer(size_t id, IteratorStorage* iter) const {
    SaveAsBigEndianUint64(iter->buffer, key_length, rank_select.rank0(iter->pos) + min_value);
  }
};

template<class NLTrie>
class CompositeIndexNestLoudsTriePrefixIterator {
protected:
  typename NLTrie::Iterator iter_;
  bool Done(size_t& id, bool ok) {
    auto dawg = static_cast<const NLTrie*>(iter_.get_dfa());
    id = ok ? dawg->state_to_word_id(iter_.word_state()) : size_t(-1);
    return ok;
  }
public:
  CompositeIndexNestLoudsTriePrefixIterator(const NLTrie* trie) : iter_(trie) {}

  fstring GetKey(size_t id) const { return iter_.word(); }
  bool SeekToFirst(size_t& id) { return Done(id, iter_.seek_begin()); }
  bool SeekToLast(size_t& id)  { return Done(id, iter_.seek_end()); }
  bool Seek(size_t& id, fstring key) { return Done(id, iter_.seek_lower_bound(key)); }
  bool Next(size_t& id) { return Done(id, iter_.incr()); }
  bool Prev(size_t& id) { return Done(id, iter_.decr()); }
  size_t DictRank(size_t id) const {
    auto dawg = static_cast<const NLTrie*>(iter_.get_dfa());
    assert(id != size_t(-1));
    return dawg->state_to_dict_rank(iter_.word_state());
  }
};
template<>
class CompositeIndexNestLoudsTriePrefixIterator<MatchingDFA> {
protected:
  unique_ptr<ADFA_LexIterator> iter_;
  const BaseDAWG* dawg_;
  bool Done(size_t& id, bool ok) {
    id = ok ? dawg_->v_state_to_word_id(iter_->word_state()) : size_t(-1);
    return ok;
  }
public:
  CompositeIndexNestLoudsTriePrefixIterator(const MatchingDFA* dfa)
    : iter_(dfa->adfa_make_iter(initial_state))
    , dawg_(dfa->get_dawg()) {}

  fstring GetKey(size_t id) const { return iter_->word(); }
  bool SeekToFirst(size_t& id) { return Done(id, iter_->seek_begin()); }
  bool SeekToLast(size_t& id) { return Done(id, iter_->seek_end()); }
  bool Seek(size_t& id, fstring key) { return Done(id, iter_->seek_lower_bound(key)); }
  bool Next(size_t& id) { return Done(id, iter_->incr()); }
  bool Prev(size_t& id) { return Done(id, iter_->decr()); }
  size_t DictRank(size_t id) const {
    assert(id != size_t(-1));
    return dawg_->state_to_dict_rank(iter_->word_state());
  }
};

template<class NLTrie>
struct CompositeIndexNestLoudsTriePrefix
  : public composite_index_detail::ComponentBase {
  using IteratorStorage = CompositeIndexNestLoudsTriePrefixIterator<NLTrie>;

  CompositeIndexNestLoudsTriePrefix() = default;
  CompositeIndexNestLoudsTriePrefix(CompositeIndexNestLoudsTriePrefix&&) = default;
  CompositeIndexNestLoudsTriePrefix(ComponentBase* base) {
    assert(dynamic_cast<CompositeIndexNestLoudsTriePrefix<NLTrie>*>(base) != nullptr);
    auto other = static_cast<CompositeIndexNestLoudsTriePrefix<NLTrie>*>(base);

    // TODO
    working_state = base->working_state;
    delete other;
  }
  ~CompositeIndexNestLoudsTriePrefix() {
    if (working_state != WorkingState::Building) {
      // TODO
    }
  }
  const BaseDAWG* dawg_;
  unique_ptr<BaseDFA> trie_;

  size_t IteratorStorageSize() const {
    return sizeof(IteratorStorage);
  }
  void IteratorStorageConstruct(void* ptr) const {
    ::new(ptr) IteratorStorage(static_cast<const NLTrie*>(trie_.get()));
  }
  void IteratorStorageDestruct(void* ptr) const {
    static_cast<IteratorStorage*>(ptr)->~IteratorStorage();
  }

  size_t KeyCount() const {
    return dawg_->num_words();
  }
  size_t TotalKeySize() const {
    return trie_->adfa_total_words_len();
  }
  size_t Find(fstring key, composite_index_detail::SuffixLowerBound const *slb) const {
    if (slb == nullptr) {
      return dawg_->index(key);
    }
    std::unique_ptr<IteratorStorage> iter(new IteratorStorage(static_cast<const NLTrie*>(trie_.get())));
    size_t id;
    if (!iter->Seek(id, key)) {
      return size_t(-1);
    }
    auto prefix_key = iter->GetKey(id);
    if (prefix_key.commonPrefixLen(key) != prefix_key.size()) {
      return size_t(-1);
    }
    size_t suffix_id, common;
    std::tie(suffix_id, common) = slb->LowerBound(key.substr(prefix_key.size()), id, 1);
    if (suffix_id != id || prefix_key.size() + common == key.size()) {
      return size_t(-1);
    }
    return id;
  }
  size_t DictRank(fstring key, composite_index_detail::SuffixLowerBound const *slb) const {
    if (slb == nullptr) {
      return dawg_->dict_rank(key);
    }
    std::unique_ptr<IteratorStorage> iter(new IteratorStorage(static_cast<const NLTrie*>(trie_.get())));
    size_t id;
    if (!iter->Seek(id, key)) {
      return KeyCount();
    }
    auto prefix_key = iter->GetKey(id);
    if (prefix_key.commonPrefixLen(key) != prefix_key.size()) {
      return iter->DictRank(id);
    }
    size_t suffix_id, common;
    std::tie(suffix_id, common) = slb->LowerBound(key.substr(prefix_key.size()), id, 1);
    if (suffix_id == id && prefix_key.size() + common == key.size()) {
      return iter->DictRank(id);
    }
    assert(suffix_id = id + 1);
    if (!iter->Next(id)) {
      return KeyCount();
    }
    return iter->DictRank(id);
  }
  bool NeedsReorder() const {
    return true;
  }

  bool IterSeekToFirst(size_t& id, IteratorStorage* iter) const {
    return iter->SeekToFirst(id);
  }
  bool IterSeekToLast(size_t& id, IteratorStorage* iter) const {
    return iter->SeekToLast(id);
  }
  bool IterSeek(size_t& id, size_t& count, fstring target, IteratorStorage* iter) const {
    count = 1;
    return iter->Seek(id, target);
  }
  bool IterNext(size_t& id, size_t count, IteratorStorage* iter) const {
    assert(count > 0);
    do {
      if (!iter->Next(id)) {
        return false;
      }
    } while (--count > 0);
    return true;
  }
  bool IterPrev(size_t& id, IteratorStorage* iter) const {
    return iter->Prev(id);
  }
  size_t IterDictRank(size_t id, const IteratorStorage* iter) const {
    return iter->DictRank(id);
  }
  fstring IterGetKey(size_t id, const IteratorStorage* iter) const {
    return iter->GetKey(id);
  }

  bool Load(fstring mem) override {
    return false;
  }
  void Save(std::function<void(void*, size_t)> append) const override {
  }
};

struct CompositeIndexEmptySuffix
  : public composite_index_detail::ComponentBase
  , public composite_index_detail::SuffixLowerBound
  , public composite_index_detail::ComponentIteratorStorageImpl<void> {
  typedef void IteratorStorage;

  CompositeIndexEmptySuffix() = default;
  CompositeIndexEmptySuffix(CompositeIndexEmptySuffix&&) = default;
  CompositeIndexEmptySuffix(ComponentBase* base) {
    working_state = WorkingState::UserMemory;
    delete base;
  }

  size_t TotalKeySize() const {
    return 0;
  }
  std::pair<size_t, size_t> LowerBound(fstring target, size_t suffix_id, size_t suffix_count) const override {
    return { suffix_id, 0 };
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
    return false;
  }
  void Save(std::function<void(void*, size_t)> append) const override {
  }
};

struct CompositeIndexDynamicStringSuffix
  : public composite_index_detail::ComponentBase
  , public composite_index_detail::SuffixLowerBound
  , public composite_index_detail::ComponentIteratorStorageImpl<void> {
  typedef void IteratorStorage;

  CompositeIndexDynamicStringSuffix() = default;
  CompositeIndexDynamicStringSuffix(CompositeIndexDynamicStringSuffix&&) = default;
  CompositeIndexDynamicStringSuffix(ComponentBase* base) {
    working_state = WorkingState::UserMemory;
    delete base;
  }
  SortedStrVec str_pool_;

  size_t TotalKeySize() const {
    return str_pool_.mem_size();
  }
  std::pair<size_t, size_t> LowerBound(fstring target, size_t suffix_id, size_t suffix_count) const override {
    size_t end = suffix_id + suffix_count;
    suffix_id = str_pool_.lower_bound(target, suffix_id, end);
    if (suffix_id == end) {
      return { suffix_id, 0 };
    }
    auto key = str_pool_[suffix_id];
    return { suffix_id, key.commonPrefixLen(target) };
  }

  void IterSet(size_t suffix_id, void*) const {
  }
  bool IterSeek(fstring target, size_t& suffix_id, size_t suffix_count, void*) const {
    size_t end = suffix_id + suffix_count;
    suffix_id = str_pool_.lower_bound(target, suffix_id, end);
    return suffix_id != end;
  }
  fstring IterGetKey(size_t suffix_id, const void*) const {
    return str_pool_[suffix_id];
  }

  bool Load(fstring mem) override {
    return false;
  }
  void Save(std::function<void(void*, size_t)> append) const override {
  }
};

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

template<class RankSelect, class InputBufferType>
void AscendingUintPrefixFillRankSelect(
  const CompositeIndexFactoryBase::UintPrefixBuildInfo& info,
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
  const CompositeIndexFactoryBase::UintPrefixBuildInfo& info,
  const TerarkIndex::KeyStat& ks,
  rank_select_allone &rs, InputBufferType& input) {
  assert(info.max_value - info.min_value < std::numeric_limits<uint64_t>::max());
  rs.resize(info.max_value - info.min_value + 1);
}

template<class T, class InputBufferType>
void AscendingUintPrefixFillRankSelect(
  const CompositeIndexFactoryBase::UintPrefixBuildInfo& info,
  const TerarkIndex::KeyStat& ks,
  rank_select_fewone<T> &rs, InputBufferType& input) {
  // TODO
}

template<class T, class InputBufferType>
void AscendingUintPrefixFillRankSelect(
  const CompositeIndexFactoryBase::UintPrefixBuildInfo& info,
  const TerarkIndex::KeyStat& ks,
  rank_select_fewzero<T> &rs, InputBufferType& input) {
  // TODO
}

template<class RankSelect, class InputBufferType>
composite_index_detail::ComponentBase*
CompositeIndexFactoryBase::BuildAscendingUintPrefix(
  InputBufferType& input,
  const TerarkZipTableOptions& tzopt,
  const TerarkIndex::KeyStat& ks,
  const UintPrefixBuildInfo& info,
  const ImmutableCFOptions* ioption,
  std::string& name) const {
  name = typeid(CompositeIndexAscendingUintPrefix<RankSelect>).name();
  RankSelect rank_select;
  assert(info.min_value <= info.max_value);
  AscendingUintPrefixFillRankSelect(info, ks, rank_select, input);
  auto prefix = new CompositeIndexAscendingUintPrefix<RankSelect>();
  prefix->rank_select.swap(rank_select);
  prefix->key_length = info.key_length;
  prefix->min_value = info.min_value;
  prefix->max_value = info.max_value;
  prefix->working_state = WorkingState::Building;
  return prefix;
}

template<class RankSelect, class InputBufferType>
void NonDescendingUintPrefixFillRankSelect(
  const CompositeIndexFactoryBase::UintPrefixBuildInfo& info,
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
    assert(last = info.max_value);
    assert(pos == bit_count);
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
    assert(last = info.min_value);
    assert(pos == 0);
  }
  rs.build_cache(true, true);
}

template<class T, class InputBufferType>
void NonDescendingUintPrefixFillRankSelect(
  const CompositeIndexFactoryBase::UintPrefixBuildInfo& info,
  const TerarkIndex::KeyStat& ks,
  rank_select_fewone<T> &rs, InputBufferType& input) {
  // TODO
}

template<class T, class InputBufferType>
void NonDescendingUintPrefixFillRankSelect(
  const CompositeIndexFactoryBase::UintPrefixBuildInfo& info,
  const TerarkIndex::KeyStat& ks,
  rank_select_fewzero<T> &rs, InputBufferType& input) {
  // TODO
}

template<class RankSelect, class InputBufferType>
composite_index_detail::ComponentBase*
CompositeIndexFactoryBase::BuildNonDescendingUintPrefix(
  InputBufferType& input,
  const TerarkZipTableOptions& tzopt,
  const TerarkIndex::KeyStat& ks,
  const UintPrefixBuildInfo& info,
  const ImmutableCFOptions* ioption,
  std::string& name) const {
  name = typeid(CompositeIndexNonDescendingUintPrefix<RankSelect>).name();
  RankSelect rank_select;
  assert(info.min_value <= info.max_value);
  NonDescendingUintPrefixFillRankSelect(info, ks, rank_select, input);
  auto prefix = new CompositeIndexNonDescendingUintPrefix<RankSelect>();
  prefix->rank_select.swap(rank_select);
  prefix->key_length = info.key_length;
  prefix->min_value = info.min_value;
  prefix->max_value = info.max_value;
  prefix->working_state = WorkingState::Building;
  return prefix;
}

void NestLoudsTriePrefixSetConfig(NestLoudsTrieConfig& conf,
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

template<class RankSelect, class InputBufferType>
composite_index_detail::ComponentBase*
CompositeIndexFactoryBase::BuildNestLoudsTriePrefix(
  InputBufferType& input,
  const TerarkZipTableOptions& tzopt,
  size_t numKeys, size_t sumKeyLen,
  bool isReverse, bool isFixedLen,
  const ImmutableCFOptions* ioption,
  std::string& name) const {
  name = typeid(CompositeIndexAscendingUintPrefix<RankSelect>).name();
  NestLoudsTrieConfig cfg;
  if (isFixedLen) {
    FixedLenStrVec keyVec;
    if (isReverse) {
      keyVec.m_size = numKeys;
      keyVec.m_strpool.resize(sumKeyLen);
      for (size_t i = numKeys; i > 0; ) {
        --i;
        reader >> keyBuf;
        memcpy(keyVec.m_strpool.data() + fixlen * i
          , fstring(keyBuf).substr(commonPrefixLen).data()
          , fixlen);
      }
    }
    else {
      keyVec.reserve(numKeys, sumRealKeyLen);
      for (size_t i = 0; i < numKeys; ++i) {
        reader >> keyBuf;
        keyVec.push_back(fstring(keyBuf).substr(commonPrefixLen));
      }
    }
    NestLoudsTriePrefixSetConfig(cfg, keyVec.mem_size(), keyVec.agv_size(), tzopt);
    return nullptr;
  }
  else {
    SortedStrVec keyVec;
    if (isReverse) {
      keyVec.m_offsets.resize_with_wire_max_val(numKeys + 1, sumKeyLen);
      keyVec.m_offsets.set_wire(numKeys, sumKeyLen);
      keyVec.m_strpool.resize(sumKeyLen);
      size_t offset = sumRealKeyLen;
      for (size_t i = numKeys; i > 0; ) {
        --i;
        reader >> keyBuf;
        fstring str = fstring(keyBuf).substr(commonPrefixLen);
        offset -= str.size();
        memcpy(keyVec.m_strpool.data() + offset, str.data(), str.size());
        keyVec.m_offsets.set_wire(i, offset);
      }
      assert(offset == 0);
    }
    else {
      keyVec.reserve(numKeys, totaKeyLen);
      for (size_t i = 0; i < numKeys; ++i) {
        reader >> keyBuf;
        keyVec.push_back(fstring(keyBuf).substr(commonPrefixLen));
      }
    }
    NestLoudsTriePrefixSetConfig(cfg, keyVec.mem_size(), keyVec.agv_size(), tzopt);
    return nullptr;
  }
}

composite_index_detail::ComponentBase*
CompositeIndexFactoryBase::BuildEmptySuffix(std::string& name) const {
  name = typeid(CompositeIndexEmptySuffix).name();
  return new CompositeIndexEmptySuffix();
}

TerarkIndex*
CompositeIndexFactoryBase::Build(
  NativeDataInput<InputBuffer>& reader,
  const TerarkZipTableOptions& tzopt,
  const TerarkIndex::KeyStat& ks,
  const ImmutableCFOptions* ioption) const {

  struct DefaultInputBuffer {
    NativeDataInput<InputBuffer> &reader;
    size_t cplen;
    valvec<byte_t> buffer;
    fstring next() {
      reader >> buffer;
      return { buffer.data() + cplen, ptrdiff_t(buffer.size() - cplen) };
    }
    DefaultInputBuffer(NativeDataInput<InputBuffer> &_reader, size_t _cplen, size_t _maxKeyLen)
      : reader(_reader), cplen(_cplen)
      , buffer(_maxKeyLen, valvec_reserve()) {
      reader.resetbuf();
      static_cast<FileStream*>(reader.getInputStream())->rewind();
    }
  };
  struct MinimizePrefixInputBuffer {
    NativeDataInput<InputBuffer> &reader;
    size_t cplen;
    size_t count;
    valvec<byte_t> last;
    valvec<byte_t> buffer;
    size_t lastSamePrefix;
    fstring next() {
      size_t maxSamePrefix;
      if (--count == 0) {
        maxSamePrefix = lastSamePrefix + 1;
      }
      else {
        reader >> buffer;
        size_t samePrefix = commonPrefixLen(buffer, last);
        last.swap(buffer);
        maxSamePrefix = std::max(samePrefix, lastSamePrefix) + 1;
        lastSamePrefix = samePrefix;
      }
      return { last.data() + cplen, ptrdiff_t(std::min(maxSamePrefix, last.size()) - cplen) };
    }
    MinimizePrefixInputBuffer(NativeDataInput<InputBuffer> &_reader, size_t _cplen, size_t _keyCount, size_t _maxKeyLen)
      : reader(_reader), cplen(_cplen), count(_keyCount)
      , last(_maxKeyLen, valvec_reserve())
      , buffer(_maxKeyLen, valvec_reserve())
      , lastSamePrefix(0) {
      reader.resetbuf();
      static_cast<FileStream*>(reader.getInputStream())->rewind();
      assert(count > 0);
      reader >> last;
    }
  };
  struct MinimizePrefixRemainingInputBuffer {
    NativeDataInput<InputBuffer> &reader;
    size_t cplen;
    size_t count;
    valvec<byte_t> last;
    valvec<byte_t> buffer;
    size_t lastSamePrefix;
    fstring next() {
      size_t maxSamePrefix;
      if (--count == 0) {
        maxSamePrefix = lastSamePrefix + 1;
      }
      else {
        reader >> buffer;
        size_t samePrefix = commonPrefixLen(buffer, last);
        last.swap(buffer);
        maxSamePrefix = std::max(samePrefix, lastSamePrefix) + 1;
        lastSamePrefix = samePrefix;
      }
      return fstring(last).substr(std::min(maxSamePrefix, last.size()));
    }
    MinimizePrefixRemainingInputBuffer(NativeDataInput<InputBuffer> &_reader, size_t _cplen, size_t _keyCount, size_t _maxKeyLen)
      : reader(_reader), cplen(_cplen), count(_keyCount)
      , last(_maxKeyLen, valvec_reserve())
      , buffer(_maxKeyLen, valvec_reserve())
      , lastSamePrefix(0) {
      reader.resetbuf();
      static_cast<FileStream*>(reader.getInputStream())->rewind();
      assert(count > 0);
      reader >> last;
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
    FixPrefixInputBuffer(NativeDataInput<InputBuffer> &_reader, size_t _cplen, size_t _prefixSize, size_t _maxKeyLen)
      : reader(_reader), cplen(_cplen), cplenPrefixSize(_cplen + _prefixSize)
      , buffer(_maxKeyLen, valvec_reserve()) {
      reader.resetbuf();
      static_cast<FileStream*>(reader.getInputStream())->rewind();
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
    FixPrefixRemainingInputBuffer(NativeDataInput<InputBuffer> &_reader, size_t _cplen, size_t _prefixSize, size_t _maxKeyLen)
      : reader(_reader), cplenPrefixSize(_cplen + _prefixSize)
      , buffer(_maxKeyLen, valvec_reserve()) {
      reader.resetbuf();
      static_cast<FileStream*>(reader.getInputStream())->rewind();
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
    FixSuffixPrefixInputBuffer(NativeDataInput<InputBuffer> &_reader, size_t _cplen, size_t _suffixSize, size_t _maxKeyLen)
      : reader(_reader), cplen(_cplen), suffixSize(_suffixSize)
      , buffer(_maxKeyLen, valvec_reserve()) {
      reader.resetbuf();
      static_cast<FileStream*>(reader.getInputStream())->rewind();
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
    FixSuffixInputBuffer(NativeDataInput<InputBuffer> &_reader, size_t _suffixSize, size_t _maxKeyLen)
      : reader(_reader), suffixSize(_suffixSize)
      , buffer(_maxKeyLen, valvec_reserve()) {
      reader.resetbuf();
      static_cast<FileStream*>(reader.getInputStream())->rewind();
    }
  };

  assert(ks.prefix.m_cnt_sum > 0);
  SortedStrVec raw_suffix;
  Common common;
  ComponentBase* prefix = nullptr;
  ComponentBase* suffix = nullptr;
  std::string combin, name;
  size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
  assert(cplen >= ks.commonPrefixLen);
  common.reset(fstring(ks.minKey).substr(ks.commonPrefixLen, cplen - ks.commonPrefixLen), true);
  auto getFixedPrefixLength = [](const TerarkIndex::KeyStat& ks, size_t cplen) {
    size_t keyCount = ks.prefix.m_cnt_sum;
    size_t maxPrefixLen = std::min<size_t>(8, ks.minKeyLen - cplen);
    size_t totalKeySize = ks.sumKeyLen - keyCount * cplen;
    size_t bestCost = totalKeySize;
    if (ks.minKeyLen != ks.maxKeyLen) {
      bestCost += keyCount;
    }
    size_t targetCost = bestCost * 10 / 6;
    UintPrefixBuildInfo result = {
      0, 0, 0, 0, 0, 0, 0, UintPrefixBuildInfo::fail
    };
    size_t entryCnt[8] = {};
    ks.diff.for_each([&](size_t len, size_t cnt) {
      if (len > cplen + 0) entryCnt[0] += cnt;
      if (len > cplen + 1) entryCnt[1] += cnt;
      if (len > cplen + 2) entryCnt[2] += cnt;
      if (len > cplen + 3) entryCnt[3] += cnt;
      if (len > cplen + 4) entryCnt[4] += cnt;
      if (len > cplen + 5) entryCnt[5] += cnt;
      if (len > cplen + 6) entryCnt[6] += cnt;
      if (len > cplen + 7) entryCnt[7] += cnt;
    });
    for (size_t &i : entryCnt) {
      i = keyCount - i;
    }
    for (size_t i = 1; i <= maxPrefixLen; ++i) {
      UintPrefixBuildInfo info;
      info.key_length = i;
      info.key_count = keyCount;
      info.min_value = ReadBigEndianUint64(ks.minKey.begin() + cplen, i);
      info.max_value = ReadBigEndianUint64(ks.maxKey.begin() + cplen, i);
      if (info.min_value > info.max_value) std::swap(info.min_value, info.max_value);
      uint64_t diff = info.max_value - info.min_value;
      info.entry_count = entryCnt[i - 1];
      assert(diff >= info.entry_count);
      if (info.entry_count == keyCount) {
        // ascending
        info.bit_count0 = diff - keyCount + 1;
        info.bit_count1 = keyCount;
      }
      else {
        // non descending
        if (keyCount + 1 > std::numeric_limits<uint64_t>::max() - diff) {
          info.bit_count0 = size_t(-1);
        }
        else {
          info.bit_count0 = diff + keyCount + 1;
        }
        info.bit_count1 = keyCount;
      }
      size_t fewCount = info.bit_count0 / 100 + info.bit_count1 / 100;
      size_t prefixCost;
      if (info.entry_count == diff) {
        info.type = UintPrefixBuildInfo::asc_allone;
        prefixCost = 0;
      }
      else if (info.bit_count1 < fewCount && info.bit_count1 < (1ULL << 48)) {
        if (diff <= std::numeric_limits<uint32_t>::max()) {
          info.type = info.entry_count == keyCount ? UintPrefixBuildInfo::asc_few_one_32 : UintPrefixBuildInfo::non_desc_few_one_32;
          prefixCost = info.bit_count1 * sizeof(uint32_t) * 33 / 32;
        }
        else {
          info.type = info.entry_count == keyCount ? UintPrefixBuildInfo::asc_few_one_64 : UintPrefixBuildInfo::non_desc_few_one_64;
          prefixCost = info.bit_count1 * sizeof(uint64_t) * 33 / 32;
        }
      }
      else if (info.bit_count0 < fewCount && info.bit_count0 < (1ULL << 48)) {
        assert(info.entry_count == keyCount);
        if (diff <= std::numeric_limits<uint32_t>::max()) {
          info.type =  UintPrefixBuildInfo::asc_few_zero_32;
          prefixCost = info.bit_count0 * sizeof(uint32_t) * 33 / 32;
        }
        else {
          info.type = UintPrefixBuildInfo::asc_few_zero_64;
          prefixCost = info.bit_count0 * sizeof(uint64_t) * 33 / 32;
        }
      }
      else {
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
      size_t suffixCost = totalKeySize - i * keyCount;
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
  UintPrefixBuildInfo uint_prefix_info = getFixedPrefixLength(ks, cplen);
  if (uint_prefix_info.key_length > 0) {
    if (ks.minKeyLen == ks.maxKeyLen && ks.maxKeyLen == cplen + uint_prefix_info.key_length) {
      DefaultInputBuffer input_reader{ reader, cplen, ks.maxKeyLen };
      switch (uint_prefix_info.type) {
      case UintPrefixBuildInfo::asc_few_zero_32:
        prefix = BuildAscendingUintPrefix<rs_fewzero_32>(
          input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_few_zero_64:
        prefix = BuildAscendingUintPrefix<rs_fewzero_64>(
          input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_allone:
        prefix = BuildAscendingUintPrefix<rank_select_allone>(
          input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_il_256:
        prefix = BuildAscendingUintPrefix<rank_select_il_256_32>(
          input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_se_512:
        prefix = BuildAscendingUintPrefix<rank_select_se_512_64>(
          input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_few_one_32:
        prefix = BuildAscendingUintPrefix<rs_fewone_32>(
          input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_few_one_64:
        prefix = BuildAscendingUintPrefix<rs_fewone_64>(
          input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::fail:
      case UintPrefixBuildInfo::non_desc_il_256:
      case UintPrefixBuildInfo::non_desc_se_512:
      case UintPrefixBuildInfo::non_desc_few_one_32:
      case UintPrefixBuildInfo::non_desc_few_one_64:
        assert(false);
        return nullptr;
      }
      combin += name;
      suffix = BuildEmptySuffix(name);
      combin += name;
      assert(dynamic_cast<const CompositeIndexFactoryBase*>(GetFactoryByCombinName(combin)) != nullptr);
      auto factory = static_cast<const CompositeIndexFactoryBase*>(GetFactoryByCombinName(combin));
      auto index = factory->CreateIndex(std::move(common), prefix, suffix);
      return index;
    }
    else {
      FixPrefixInputBuffer prefix_input_reader{ reader, cplen, uint_prefix_info.key_length, ks.maxKeyLen };
      switch (uint_prefix_info.type) {
      case UintPrefixBuildInfo::asc_few_zero_32:
        prefix = BuildAscendingUintPrefix<rs_fewzero_32>(
          prefix_input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_few_zero_64:
        prefix = BuildAscendingUintPrefix<rs_fewzero_64>(
          prefix_input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_allone:
        prefix = BuildAscendingUintPrefix<rank_select_allone>(
          prefix_input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_il_256:
        prefix = BuildAscendingUintPrefix<rank_select_il_256_32>(
          prefix_input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_se_512:
        prefix = BuildAscendingUintPrefix<rank_select_se_512_64>(
          prefix_input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_few_one_32:
        prefix = BuildAscendingUintPrefix<rs_fewone_32>(
          prefix_input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_few_one_64:
        prefix = BuildAscendingUintPrefix<rs_fewone_64>(
          prefix_input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::non_desc_il_256:
        prefix = BuildNonDescendingUintPrefix<rank_select_il_256_32>(
          prefix_input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::non_desc_se_512:
        prefix = BuildNonDescendingUintPrefix<rank_select_se_512_64>(
          prefix_input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::non_desc_few_one_32:
        prefix = BuildNonDescendingUintPrefix<rs_fewone_32>(
          prefix_input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::non_desc_few_one_64:
        prefix = BuildNonDescendingUintPrefix<rs_fewone_64>(
          prefix_input_reader, tzopt, ks, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::fail:
        assert(false);
        return nullptr;
      }
      FixPrefixRemainingInputBuffer suffix_input_reader{ reader, cplen, uint_prefix_info.key_length, ks.maxKeyLen };
      if (ks.minKeyLen == ks.maxKeyLen) {
        // BuildFixedStringSuffix
      }
      else {
        // BuildDynamicStringSuffix
      }
    }
  }
  else if (ks.sumKeyLen - ks.minSuffixLen * ks.prefix.m_cnt_sum < ks.prefix.m_total_key_len * 5 / 4) {
    size_t suffixLen = ks.minSuffixLen;
    FixSuffixPrefixInputBuffer prefix_input_reader{ reader, cplen, suffixLen, ks.maxKeyLen };
    // BuildNestLoudsTriePrefix
    FixSuffixInputBuffer suffix_input_reader{ reader, suffixLen, ks.maxKeyLen };
    // BuildFixedStringSuffix
  }
  else if (ks.prefix.m_total_key_len < ks.sumKeyLen * 31 / 32) {
    MinimizePrefixInputBuffer prefix_input_reader{ reader, cplen, ks.prefix.m_cnt_sum, ks.maxKeyLen };
    // BuildNestLoudsTriePrefix
    MinimizePrefixRemainingInputBuffer suffix_input_reader{ reader, cplen, ks.prefix.m_cnt_sum, ks.maxKeyLen };
    if (ks.minSuffixLen == ks.maxSuffixLen) {
      // BuildFixedStringSuffix
    }
    else {
      // BuildDynamicStringSuffix
    }

  } else {
    DefaultInputBuffer input_reader{ reader, cplen, ks.maxKeyLen };
    // BuildNestLoudsTriePrefix
    // BuildEmptySuffix
  }
  return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

//typedef NestLoudsTrieDAWG_IL_256 NestLoudsTrieDAWG_IL_256_32;
//typedef NestLoudsTrieDAWG_SE_512 NestLoudsTrieDAWG_SE_512_32;
//typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_IL_256_32> TrieDAWG_IL_256_32;
//typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_SE_512_64> TrieDAWG_SE_512_64;
//typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_IL_256> TrieDAWG_Mixed_IL_256;
//typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_SE_512> TrieDAWG_Mixed_SE_512;
//typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_XL_256> TrieDAWG_Mixed_XL_256;
//
//typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_IL_256_32_FL> TrieDAWG_IL_256_32_FL;
//typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_SE_512_64_FL> TrieDAWG_SE_512_64_FL;
//typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_IL_256_32_FL> TrieDAWG_Mixed_IL_256_32_FL;
//typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_SE_512_32_FL> TrieDAWG_Mixed_SE_512_32_FL;
//typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_XL_256_32_FL> TrieDAWG_Mixed_XL_256_32_FL;

//TerarkIndexRegisterImp(TrieDAWG_IL_256_32, TrieDAWG_IL_256_32::MyFactory, "NestLoudsTrieDAWG_IL", "IL_256_32", "IL_256", "NestLoudsTrieDAWG_IL_256");
//TerarkIndexRegisterNLT(SE_512_64);
//TerarkIndexRegisterNLT(Mixed_SE_512);
//TerarkIndexRegisterNLT(Mixed_IL_256);
//TerarkIndexRegisterNLT(Mixed_XL_256);

//TerarkIndexRegisterNLT(IL_256_32_FL);
//TerarkIndexRegisterNLT(SE_512_64_FL);
//TerarkIndexRegisterNLT(Mixed_SE_512_32_FL);
//TerarkIndexRegisterNLT(Mixed_IL_256_32_FL);
//TerarkIndexRegisterNLT(Mixed_XL_256_32_FL);

//typedef Min0DataCont BigUintDataCont;

typedef rank_select_allone     AllOne;
typedef rank_select_allzero    AllZero;
typedef rank_select_il_256_32  IL_256_32;
typedef rank_select_se_512_64  SE_512_64;
typedef rs_fewone_32           FewOne32;
typedef rs_fewone_64           FewOne64;
typedef rs_fewzero_32          FewZero32;
typedef rs_fewzero_64          FewZero64;

//#define RegisterIndex TerarkIndexRegister
//#define RegisterCompositeUintIndex4(rs1, rs2, Key2, _Key2)  \
//typedef CompositeUintIndex <rs1, rs2, Key2##DataCont>       \
//        CompositeUintIndex_##rs1##_##rs2##_Key2;            \
//RegisterIndex(CompositeUintIndex_##rs1##_##rs2##_Key2)
//
//#define RegisterCompositeUintIndex3(rs1, rs2, Key2)         \
//RegisterCompositeUintIndex4(rs1, rs2, Key2, _##Key2)
//
//#define RegisterCompositeUintIndex(rs1, rs2)                \
//RegisterCompositeUintIndex4(rs1, rs2, Str,);                \
//RegisterCompositeUintIndex3(rs1, rs2, SortedUint);          \
//RegisterCompositeUintIndex3(rs1, rs2,    BigUint)
//
//RegisterCompositeUintIndex(AllOne, AllZero);
//RegisterCompositeUintIndex(AllOne, FewZero32);
//RegisterCompositeUintIndex(AllOne, FewZero64);
//RegisterCompositeUintIndex(AllOne, FewOne32);
//RegisterCompositeUintIndex(AllOne, FewOne64);
//RegisterCompositeUintIndex(AllOne, IL_256_32);
//RegisterCompositeUintIndex(AllOne, SE_512_64);
//
//RegisterCompositeUintIndex(FewOne32, AllZero);
//RegisterCompositeUintIndex(FewOne64, AllZero);
//RegisterCompositeUintIndex(FewOne32, FewZero32);
//RegisterCompositeUintIndex(FewOne64, FewZero64);
//RegisterCompositeUintIndex(FewOne32, FewOne32);
//RegisterCompositeUintIndex(FewOne64, FewOne64);
//RegisterCompositeUintIndex(FewOne32, IL_256_32);
//RegisterCompositeUintIndex(FewOne64, SE_512_64);
//
//RegisterCompositeUintIndex(FewZero32, AllZero);
//RegisterCompositeUintIndex(FewZero64, AllZero);
//RegisterCompositeUintIndex(FewZero32, FewZero32);
//RegisterCompositeUintIndex(FewZero64, FewZero64);
//RegisterCompositeUintIndex(FewZero32, FewOne32);
//RegisterCompositeUintIndex(FewZero64, FewOne64);
//RegisterCompositeUintIndex(FewZero32, IL_256_32);
//RegisterCompositeUintIndex(FewZero64, SE_512_64);
//
//RegisterCompositeUintIndex(IL_256_32, AllZero);
//RegisterCompositeUintIndex(SE_512_64, AllZero);
//RegisterCompositeUintIndex(IL_256_32, FewZero32);
//RegisterCompositeUintIndex(SE_512_64, FewZero64);
//RegisterCompositeUintIndex(IL_256_32, FewOne32);
//RegisterCompositeUintIndex(SE_512_64, FewOne64);
//RegisterCompositeUintIndex(IL_256_32, IL_256_32);
//RegisterCompositeUintIndex(SE_512_64, SE_512_64);

template<class RankSelect>
using UintIndex = typename CompositeIndexDeclare<
  CompositeIndexAscendingUintPrefix<RankSelect>, 0,
  CompositeIndexEmptySuffix, 0
>::index_type;


const char* UintIndexName = "UintIndex";
template<class RankSelect>
class UintIndexFactory
  : public CompositeIndexFactory<CompositeIndexAscendingUintPrefix<RankSelect>, 0,
  CompositeIndexEmptySuffix, 0> {
  ~UintIndexFactory() {}

  typedef typename CompositeIndexFactory<
    CompositeIndexAscendingUintPrefix<RankSelect>, 0,
    CompositeIndexEmptySuffix, 0
  >::index_type index_type;

  struct FileHeader : public TerarkIndexHeader {
    uint64_t min_value;
    uint64_t max_value;
    uint64_t index_mem_size;
    uint32_t key_length;
    uint32_t common_prefix_length;

    FileHeader(size_t body_size, const std::type_info& ti) {
      memset(this, 0, sizeof *this);
      magic_len = strlen(UintIndexName);
      strncpy(magic, UintIndexName, sizeof magic);
      size_t name_i = g_TerarkIndexName.find_i(ti.name());
      strncpy(class_name, g_TerarkIndexName.val(name_i).c_str(), sizeof class_name);
      header_size = sizeof *this;
      version = 1;
      file_size = sizeof *this + body_size;
    }
  };

  unique_ptr<TerarkIndex> LoadMemory(fstring mem) const override {
    unique_ptr<index_type> index(new index_type(this));
    const FileHeader* h = (const FileHeader*)mem.data();
    if (mem.size() < sizeof(FileHeader)
      || h->magic_len != strlen(UintIndexName)
      || strcmp(h->magic, UintIndexName) != 0
      || h->header_size != sizeof(FileHeader)
      || h->version != 1
      || h->file_size != mem.size()
      ) {
      throw std::invalid_argument("UintIndex::Load(): Bad file header");
    }
    auto ptr = &index->prefix_;
    ptr->min_value = h->min_value;
    ptr->max_value = h->max_value;
    ptr->key_length = h->key_length;
    if (h->common_prefix_length > 0) {
      index->common_.reset(mem.substr(h->header_size, h->common_prefix_length), false);
    }
    ptr->rank_select.risk_mmap_from(
      (unsigned char*)mem.data() + h->header_size + align_up(h->common_prefix_length, 8), h->index_mem_size);

    index->prefix_.working_state = WorkingState::UserMemory;
    index->suffix_.working_state = WorkingState::UserMemory;
    return unique_ptr<TerarkIndex>(index.release());
  }
  unique_ptr<TerarkIndex> LoadFile(fstring fpath) const override {
    MmapWholeFile mmapFile(fpath);
    auto ret = LoadMemory(mmapFile.memory());
    assert(dynamic_cast<index_type*>(ret.get()) != nullptr);
    index_type* index = static_cast<index_type*>(ret.get());
    index->header_ = (const FileHeader*)mmapFile.base;
    mmapFile.base = nullptr;
    return ret;
  }

  void SaveMmap(const TerarkIndex* index, std::function<void(const void *, size_t)> write) const override {
    assert(dynamic_cast<const index_type*>(index) != nullptr);
    auto uint_index = static_cast<const index_type*>(index);
    auto ptr = &uint_index->prefix_;
    FileHeader header(ptr->rank_select.mem_size(), typeid(UintIndex<RankSelect>));
    header.min_value = ptr->min_value;
    header.max_value = ptr->max_value;
    header.index_mem_size = ptr->rank_select.mem_size();
    header.key_length = ptr->key_length;
    header.common_prefix_length = uint_index->common_.size();
    write(&header, sizeof header);
    if (uint_index->common_.size() > 0) {
      write(uint_index->common_.data(), uint_index->common_.size());
      Padzero<8>(write, uint_index->common_.size());
    }
    write(ptr->rank_select.data(), ptr->rank_select.mem_size());
  }
};

#define RegisterUintIndex(RankSelect)               \
RegisterCompositeIndexWithFactory(                  \
  CompositeIndexAscendingUintPrefix<RankSelect>, 0, \
  CompositeIndexEmptySuffix, 0,                     \
  UintIndex_##RankSelect,                           \
  UintIndexFactory<RankSelect>                      \
)
RegisterUintIndex(IL_256_32);
RegisterUintIndex(SE_512_64);
RegisterUintIndex(AllOne);

unique_ptr<TerarkIndex> TerarkIndex::LoadFile(fstring fpath) {
  TerarkIndex::Factory* factory = NULL;
  {
    MmapWholeFile mmap(fpath);
    auto header = (const TerarkIndexHeader*)mmap.base;
    size_t idx = g_TerarkIndexFactroy.find_i(header->class_name);
    if (idx >= g_TerarkIndexFactroy.end_i()) {
      throw std::invalid_argument(
        "TerarkIndex::LoadFile(" + fpath + "): Unknown class: "
        + header->class_name);
    }
    factory = g_TerarkIndexFactroy.val(idx).get();
  }
  return factory->LoadFile(fpath);
}
//
//unique_ptr<TerarkIndex> TerarkIndex::LoadMemory(fstring mem) {
//  auto header = (const TerarkIndexHeader*)mem.data();
//#if defined(TerocksPrivateCode)
//  if (header->file_size < mem.size()) {
//    auto dfa = loadAsLazyUnionDFA(mem, true);
//    assert(dfa);
//    return unique_ptr<TerarkIndex>(new NestLoudsTrieIndex<MatchingDFA>(dfa));
//  }
//#endif // TerocksPrivateCode
//  size_t idx = g_TerarkIndexFactroy.find_i(header->class_name);
//  if (idx >= g_TerarkIndexFactroy.end_i()) {
//    throw std::invalid_argument(
//      std::string("TerarkIndex::LoadMemory(): Unknown class: ")
//      + header->class_name);
//  }
//  TerarkIndex::Factory* factory = g_TerarkIndexFactroy.val(idx).get();
//  return factory->LoadMemory(mem);
//}


RegisterCompositeIndex(CompositeIndexAscendingUintPrefix<IL_256_32>, 0, CompositeIndexEmptySuffix, 0, Unused00);
RegisterCompositeIndex(CompositeIndexAscendingUintPrefix<IL_256_32>, 0, CompositeIndexEmptySuffix, 1, Unused01);
RegisterCompositeIndex(CompositeIndexAscendingUintPrefix<IL_256_32>, 1, CompositeIndexEmptySuffix, 0, Unused02);
RegisterCompositeIndex(CompositeIndexAscendingUintPrefix<IL_256_32>, 1, CompositeIndexEmptySuffix, 1, Unused03);

RegisterCompositeIndex(CompositeIndexNonDescendingUintPrefix<rs_fewzero_32>, 0, CompositeIndexDynamicStringSuffix, 0, Unused10);
RegisterCompositeIndex(CompositeIndexNonDescendingUintPrefix<rs_fewzero_32>, 0, CompositeIndexDynamicStringSuffix, 1, Unused11);
RegisterCompositeIndex(CompositeIndexNonDescendingUintPrefix<rs_fewzero_32>, 1, CompositeIndexDynamicStringSuffix, 0, Unused12);
RegisterCompositeIndex(CompositeIndexNonDescendingUintPrefix<rs_fewzero_32>, 1, CompositeIndexDynamicStringSuffix, 1, Unused13);

RegisterCompositeIndex(CompositeIndexNestLoudsTriePrefix<NestLoudsTrieDAWG_Mixed_XL_256_32_FL>, 0, CompositeIndexEmptySuffix, 0, Unused20);
RegisterCompositeIndex(CompositeIndexNestLoudsTriePrefix<NestLoudsTrieDAWG_Mixed_XL_256_32_FL>, 0, CompositeIndexEmptySuffix, 1, Unused21);
RegisterCompositeIndex(CompositeIndexNestLoudsTriePrefix<NestLoudsTrieDAWG_Mixed_XL_256_32_FL>, 1, CompositeIndexEmptySuffix, 0, Unused22);
RegisterCompositeIndex(CompositeIndexNestLoudsTriePrefix<NestLoudsTrieDAWG_Mixed_XL_256_32_FL>, 1, CompositeIndexEmptySuffix, 1, Unused23);

RegisterCompositeIndex(CompositeIndexNestLoudsTriePrefix<MatchingDFA>, 0, CompositeIndexDynamicStringSuffix, 0, Unused30);
RegisterCompositeIndex(CompositeIndexNestLoudsTriePrefix<MatchingDFA>, 0, CompositeIndexDynamicStringSuffix, 1, Unused31);
RegisterCompositeIndex(CompositeIndexNestLoudsTriePrefix<MatchingDFA>, 1, CompositeIndexDynamicStringSuffix, 0, Unused32);
RegisterCompositeIndex(CompositeIndexNestLoudsTriePrefix<MatchingDFA>, 1, CompositeIndexDynamicStringSuffix, 1, Unused33);


} // namespace rocksdb
