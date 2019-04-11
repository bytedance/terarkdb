
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

// 0 < cnt0 and cnt0 < 0.01 * total
inline bool IsFewZero(size_t total, size_t cnt0) {
  if (!g_indexEnableFewZero)
    return false;
  assert(total > 0);
  return (0 < cnt0) &&
    (cnt0 <= (double)total * 0.01);
}
inline bool IsFewOne(size_t total, size_t cnt1) {
  if (!g_indexEnableFewZero)
    return false;
  assert(total > 0);
  return (0 < cnt1) &&
    ((double)total * 0.005 < cnt1) &&
    (cnt1 <= (double)total * 0.01);
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

bool TerarkIndex::SeekCostEffectiveIndexLen(const KeyStat& ks, size_t& ceLen) {
  /*
    * the length of index1,
    * 1. 1 byte => too many sub-indexes under each index1
    * 2. 8 byte => a lot more gaps compared with 1 byte index1
    * !!! more bytes with less gap is preferred, in other words,
    * prefer smaller gap-ratio, larger compression-ratio
    * best case is : w1 * (1 / gap-ratio) + w2 * compress-ratio
    *        gap-ratio = (diff - numkeys) / diff,
    *   compress-ratio = compressed-part / original,
    *
    * to calculate space usage, assume 1000 keys, maxKeyLen = 16,
    *   original cost = 16,000 * 8                     = 128,000 bit
    *   8 bytes, 0.5 gap => 2000 + (16 - 8) * 8 * 1000 =  66,000
    *   2 bytes, 0.5 gap => 2000 + (16 - 2) * 8 * 1000 = 114,000
    */
  const double w1 = 0.1;
  const double w2 = 1.2;
  const double min_gap_ratio = 0.1;
  const double max_gap_ratio = 0.9;
  const double fewone_min_gap_ratio = 0.990; // fewone 0.010, 1 '1' out of 100
  const double fewone_max_gap_ratio = 0.995; // fewone 0.005, 1 '1' out of 200
  const size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
  const size_t maxLen = std::min<size_t>(8, ks.maxKeyLen - cplen);
  const double originCost = ks.prefix.m_cnt_sum * ks.maxKeyLen * 8;
  double score = 0;
  double minCost = originCost;
  size_t scoreLen = maxLen;
  size_t minCostLen = maxLen;
  ceLen = maxLen;
  for (size_t i = maxLen; i > 0; i--) {
    auto minValue = ReadBigEndianUint64(ks.minKey.begin() + cplen, i);
    auto maxValue = ReadBigEndianUint64(ks.maxKey.begin() + cplen, i);
    uint64_t diff1st = abs_diff(minValue, maxValue); // don't +1, which may cause overflow
    uint64_t diff2nd = ks.prefix.m_cnt_sum;
    // one index1st with a collection of index2nd, that's when diff < numkeys
    double gap_ratio = diff1st <= ks.prefix.m_cnt_sum ? min_gap_ratio :
      (double)(diff1st - ks.prefix.m_cnt_sum) / diff1st;
    if (g_indexEnableFewZero &&
      fewone_min_gap_ratio <= gap_ratio &&
      gap_ratio < fewone_max_gap_ratio) { // fewone branch
    // to construct rankselect for fewone, much more extra space is needed
      size_t bits = (diff1st < UINT32_MAX && ks.prefix.m_cnt_sum < UINT32_MAX) ? 32 : 64;
      double cost = ks.prefix.m_cnt_sum * bits + diff2nd * 1.2 +
        (ks.maxKeyLen - cplen - i) * ks.prefix.m_cnt_sum * 8;
      if (cost > originCost * 0.8)
        continue;
      if (cost < minCost) {
        minCost = cost;
        minCostLen = i;
      }
    }
    else if (gap_ratio > max_gap_ratio) { // skip branch
      continue;
    }
    else {
      gap_ratio = std::max(gap_ratio, min_gap_ratio);
      // diff is bitmap, * 1.2 is extra cost to build RankSelect
      double cost = ((double)diff1st + diff2nd) * 1.2 +
        (ks.maxKeyLen - cplen - i) * ks.prefix.m_cnt_sum * 8;
      if (cost > originCost * 0.8)
        continue;
      double compress_ratio = (originCost - cost) / originCost;
      double cur = w1 * (1 / gap_ratio) + w2 * compress_ratio;
      if (cur > score) {
        score = cur;
        scoreLen = i;
      }
    }
  }

  if (score > 0 || minCost <= originCost * 0.8) {
    ceLen = (score > 0) ? scoreLen : minCostLen;
    return true;
  }
  else {
    return false;
  }
}

const TerarkIndex::Factory*
  TerarkIndex::SelectFactory(const KeyStat& ks, fstring name) {
  assert(ks.prefix.m_cnt_sum > 0);
  size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
  assert(cplen >= ks.commonPrefixLen);
  size_t ceLen = 0; // cost effective index1st len if any
  if (g_indexEnableUintIndex &&
    ks.maxKeyLen == ks.minKeyLen &&
    ks.maxKeyLen - cplen <= sizeof(uint64_t)) {
    auto minValue = ReadBigEndianUint64(ks.minKey.begin() + cplen, ks.minKey.end());
    auto maxValue = ReadBigEndianUint64(ks.maxKey.begin() + cplen, ks.maxKey.end());
    if (minValue > maxValue) {
      std::swap(minValue, maxValue);
    }
    uint64_t diff = maxValue - minValue; // don't +1, may overflow
    if (diff < ks.prefix.m_cnt_sum * 30) {
      if (diff + 1 == ks.prefix.m_cnt_sum) {
        return GetFactory("UintIndex_AllOne");
      }
      else if (diff < UINT32_MAX) {
        return GetFactory("UintIndex_IL_256_32");
      }
      else {
        return GetFactory("UintIndex_SE_512_64");
      }
    }
  }
  if (g_indexEnableCompositeUintIndex &&
    ks.maxKeyLen == ks.minKeyLen &&
    ks.maxKeyLen - cplen <= 16 && // !!! plain index2nd may occupy too much space
    SeekCostEffectiveIndexLen(ks, ceLen) &&
    ks.maxKeyLen > cplen + ceLen) {
    auto minValue = ReadBigEndianUint64(ks.minKey.begin() + cplen, ceLen);
    auto maxValue = ReadBigEndianUint64(ks.maxKey.begin() + cplen, ceLen);
    if (minValue > maxValue) {
      std::swap(minValue, maxValue);
    }
    if (maxValue - minValue < UINT32_MAX - 1 &&
      ks.prefix.m_cnt_sum < UINT32_MAX - 1) { // for composite cluster key, key1:key2 maybe 1:N
      return GetFactory("CompositeUintIndex_IL_256_32_IL_256_32");
    }
    else {
      return GetFactory("CompositeUintIndex_SE_512_64_SE_512_64");
    }
  }
  if (ks.sumKeyLen - ks.prefix.m_cnt_sum * ks.commonPrefixLen > 0x1E0000000) { // 7.5G
    return GetFactory("SE_512_64");
  }
  return GetFactory(name);
}

TerarkIndex::~TerarkIndex() {}
TerarkIndex::Factory::~Factory() {}
TerarkIndex::Iterator::~Iterator() {}

template<class NLTrie>
class NestLoudsTrieIter : public TerarkIndex::Iterator {
protected:
  using TerarkIndex::Iterator::m_id;
  typename   NLTrie::Iterator  m_iter;
  bool Done(bool ok) {
    auto dawg = static_cast<const NLTrie*>(m_iter.get_dfa());
    if (ok)
      m_id = dawg->state_to_word_id(m_iter.word_state());
    else
      m_id = size_t(-1);
    return ok;
  }
public:
  NestLoudsTrieIter(const NLTrie* trie) : m_iter(trie) {}
  fstring key() const override { return m_iter.word(); }
  bool SeekToFirst() override { return Done(m_iter.seek_begin()); }
  bool SeekToLast()  override { return Done(m_iter.seek_end()); }
  bool Seek(fstring key) override { return Done(m_iter.seek_lower_bound(key)); }
  bool Next() override { return Done(m_iter.incr()); }
  bool Prev() override { return Done(m_iter.decr()); }
  size_t DictRank() const override {
    auto dawg = static_cast<const NLTrie*>(m_iter.get_dfa());
    assert(m_id != size_t(-1));
    return dawg->state_to_dict_rank(m_iter.word_state());
  }
};
template<>
class NestLoudsTrieIter<MatchingDFA> : public TerarkIndex::Iterator {
protected:
  using TerarkIndex::Iterator::m_id;
  unique_ptr<ADFA_LexIterator> m_iter;
  const BaseDAWG* m_dawg;
  bool Done(bool ok) {
    if (ok)
      m_id = m_dawg->v_state_to_word_id(m_iter->word_state());
    else
      m_id = size_t(-1);
    return ok;
  }
public:
  NestLoudsTrieIter(const MatchingDFA* dfa)
    : m_iter(dfa->adfa_make_iter(initial_state)) {
    m_dawg = dfa->get_dawg();
  }
  fstring key() const override { return m_iter->word(); }
  bool SeekToFirst() override { return Done(m_iter->seek_begin()); }
  bool SeekToLast()  override { return Done(m_iter->seek_end()); }
  bool Seek(fstring key) override { return Done(m_iter->seek_lower_bound(key)); }
  bool Next() override { return Done(m_iter->incr()); }
  bool Prev() override { return Done(m_iter->decr()); }
  size_t DictRank() const override {
    assert(m_id != size_t(-1));
    return m_dawg->state_to_dict_rank(m_iter->word_state());
  }
};

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

struct NestLoudsTrieIndexBase : public TerarkIndex {
  const BaseDAWG* m_dawg;
  unique_ptr<BaseDFA> m_trie;

  struct MyBaseFactory : public Factory {
    static
      void Read_SortedStrVec(SortedStrVec& keyVec,
        NativeDataInput<InputBuffer>& reader,
        const KeyStat& ks) {
      size_t numKeys = ks.prefix.m_cnt_sum;
      size_t commonPrefixLen = ks.commonPrefixLen;
      size_t sumPrefixLen = commonPrefixLen * numKeys;
      size_t sumRealKeyLen = ks.sumKeyLen - sumPrefixLen;
      valvec<byte_t> keyBuf;
      if (ks.minKey < ks.maxKey) {
        keyVec.reserve(numKeys, sumRealKeyLen);
        for (size_t i = 0; i < numKeys; ++i) {
          reader >> keyBuf;
          keyVec.push_back(fstring(keyBuf).substr(commonPrefixLen));
        }
      }
      else {
        keyVec.m_offsets.resize_with_wire_max_val(numKeys + 1, sumRealKeyLen);
        keyVec.m_offsets.set_wire(numKeys, sumRealKeyLen);
        keyVec.m_strpool.resize(sumRealKeyLen);
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
    }
    static
      void Read_FixedLenStrVec(FixedLenStrVec& keyVec,
        NativeDataInput<InputBuffer>& reader,
        const KeyStat& ks) {
      size_t numKeys = ks.prefix.m_cnt_sum;
      size_t commonPrefixLen = ks.commonPrefixLen;
      size_t sumPrefixLen = commonPrefixLen * numKeys;
      size_t sumRealKeyLen = ks.sumKeyLen - sumPrefixLen;
      valvec<byte_t> keyBuf;
      size_t fixlen = ks.minKeyLen - commonPrefixLen;
      keyVec.m_fixlen = fixlen;
      if (ks.minKey < ks.maxKey) {
        keyVec.reserve(numKeys, sumRealKeyLen);
        for (size_t i = 0; i < numKeys; ++i) {
          reader >> keyBuf;
          keyVec.push_back(fstring(keyBuf).substr(commonPrefixLen));
        }
      }
      else {
        keyVec.m_size = numKeys;
        keyVec.m_strpool.resize(sumRealKeyLen);
        for (size_t i = numKeys; i > 0; ) {
          --i;
          reader >> keyBuf;
          memcpy(keyVec.m_strpool.data() + fixlen * i
            , fstring(keyBuf).substr(commonPrefixLen).data()
            , fixlen);
        }
      }
    }
    template<class StrVec>
    static
      void Set_NestLoudsTrieConfig(NestLoudsTrieConfig& conf,
        const StrVec& keyVec,
        const TerarkZipTableOptions& tzopt) {
      conf.nestLevel = tzopt.indexNestLevel;
      conf.nestScale = tzopt.indexNestScale;
      if (tzopt.indexTempLevel >= 0 && tzopt.indexTempLevel < 5) {
        if (keyVec.mem_size() > tzopt.smallTaskMemory) {
          // use tmp files during index building
          conf.tmpDir = tzopt.localTempDir;
          if (0 == tzopt.indexTempLevel) {
            // adjust tmpLevel for linkVec, wihch is proportional to num of keys
            double avglen = keyVec.avg_size();
            if (keyVec.mem_size() > tzopt.smallTaskMemory * 2 && avglen <= 50) {
              // not need any mem in BFS, instead 8G file of 4G mem (linkVec)
              // this reduce 10% peak mem when avg keylen is 24 bytes
              if (avglen <= 30) {
                // write str data(each len+data) of nestStrVec to tmpfile
                conf.tmpLevel = 4;
              }
              else {
                // write offset+len of nestStrVec to tmpfile
                // which offset is ref to outer StrVec's data
                conf.tmpLevel = 3;
              }
            }
            else if (keyVec.mem_size() > tzopt.smallTaskMemory * 3 / 2) {
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

    template<class StrVec>
    static void AssertSorted(const StrVec& keyVec) {
#if !defined(NDEBUG)
      for (size_t i = 1; i < keyVec.size(); ++i) {
        fstring prev = keyVec[i - 1];
        fstring curr = keyVec[i];
        assert(prev < curr);
      }
#endif
    }

    size_t MemSizeForBuild(const KeyStat& ks) const override {
      size_t sumRealKeyLen = ks.sumKeyLen - ks.commonPrefixLen * ks.prefix.m_cnt_sum;
      if (ks.minKeyLen == ks.maxKeyLen) {
        return sumRealKeyLen;
      }
      size_t indexSize = UintVecMin0::compute_mem_size_by_max_val(ks.prefix.m_cnt_sum + 1, sumRealKeyLen);
      return indexSize + sumRealKeyLen;
    }
  };

  NestLoudsTrieIndexBase(BaseDFA* trie) : m_trie(trie) {
    m_dawg = trie->get_dawg();
  }

  const char* Name() const override {
    if (m_trie->is_mmap()) {
      auto header = (const TerarkIndexHeader*)m_trie->get_mmap().data();
      return header->class_name;
    }
    else {
      size_t name_i = g_TerarkIndexName.find_i(typeid(*this).name());
      TERARK_RT_assert(name_i < g_TerarkIndexName.end_i(), std::logic_error);
      return g_TerarkIndexName.val(name_i).c_str();
    }
  }
  void SaveMmap(std::function<void(const void *, size_t)> write) const override {
    m_trie->save_mmap(write);
  }
  size_t Find(fstring key) const override final {
    return m_dawg->index(key);
  }
  virtual size_t DictRank(fstring key) const {
    return m_dawg->dict_rank(key);
  }
  size_t NumKeys() const override final {
    return m_dawg->num_words();
  }
  size_t TotalKeySize() const override final {
    return m_trie->adfa_total_words_len();
  }
  fstring Memory() const override final {
    return m_trie->get_mmap();
  }
  bool NeedsReorder() const override final { return true; }
};

template<class NLTrie>
class NestLoudsTrieIndex : public NestLoudsTrieIndexBase {
  typedef NestLoudsTrieIter<NLTrie> MyIterator;
public:
  NestLoudsTrieIndex(NLTrie* trie) : NestLoudsTrieIndexBase(trie) {}
  Iterator* NewIterator() const override final {
    auto trie = static_cast<const NLTrie*>(m_trie.get());
    return new MyIterator(trie);
  }
  void GetOrderMap(UintVecMin0& newToOld)
    const override final {
    auto trie = static_cast<const NLTrie*>(m_trie.get());
    NestLoudsTrieGetOrderMap(trie, newToOld);
  }
  void BuildCache(double cacheRatio) {
    if (cacheRatio > 1e-8) {
      auto trie = static_cast<NLTrie*>(m_trie.get());
      NestLoudsTrieBuildCache(trie, cacheRatio);
    }
  }
  class MyFactory : public MyBaseFactory {
  public:
    TerarkIndex* Build(NativeDataInput<InputBuffer>& reader,
      const TerarkZipTableOptions& tzopt,
      const KeyStat& ks,
      const ImmutableCFOptions* ioptions) const override {
      valvec<byte_t> keyBuf;
      if (ks.minKeyLen != ks.maxKeyLen) {
        SortedStrVec keyVec;
        Read_SortedStrVec(keyVec, reader, ks);
        return BuildImpl(tzopt, keyVec);
      }
      else {
        FixedLenStrVec keyVec;
        Read_FixedLenStrVec(keyVec, reader, ks);
        return BuildImpl(tzopt, keyVec);
      }
    }
  private:
    template<class StrVec>
    TerarkIndex*
      BuildImpl(const TerarkZipTableOptions& tzopt, StrVec& keyVec) const {
      AssertSorted(keyVec);
      NestLoudsTrieConfig conf;
      Set_NestLoudsTrieConfig(conf, keyVec, tzopt);
      std::unique_ptr<NLTrie> trie(new NLTrie());
      trie->build_from(keyVec, conf);
      return new NestLoudsTrieIndex(trie.release());
    }
  public:
    unique_ptr<TerarkIndex> LoadMemory(fstring mem) const override {
      unique_ptr<BaseDFA>
        dfa(BaseDFA::load_mmap_user_mem(mem.data(), mem.size()));
      auto trie = dynamic_cast<NLTrie*>(dfa.get());
      if (NULL == trie) {
        throw std::invalid_argument("Bad trie class: " + ClassName(*dfa)
          + ", should be " + ClassName<NLTrie>());
      }
      auto index = new NestLoudsTrieIndex(trie);
      dfa.release();
      return unique_ptr<TerarkIndex>(index);
    }
    unique_ptr<TerarkIndex> LoadFile(fstring fpath) const override {
      unique_ptr<BaseDFA> dfa(BaseDFA::load_mmap(fpath));
      auto trie = dynamic_cast<NLTrie*>(dfa.get());
      if (NULL == trie) {
        throw std::invalid_argument(
          "File: " + fpath + ", Bad trie class: " + ClassName(*dfa)
          + ", should be " + ClassName<NLTrie>());
      }
      auto index = new NestLoudsTrieIndex(trie);
      dfa.release();
      return unique_ptr<TerarkIndex>(index);
    }
  };
};

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
    const UintPrefixBuildInfo& info,
    const ImmutableCFOptions* ioption,
    std::string& name) const;


  template<class RankSelect, class InputBufferType>
  ComponentBase* BuildNonDescendingUintPrefix(
    InputBufferType& reader,
    const TerarkZipTableOptions& tzopt,
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
//        FewZero32
//        FewZero64
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

struct CompositeIndexDynamicStrSuffix
  : public composite_index_detail::ComponentBase
  , public composite_index_detail::SuffixLowerBound
  , public composite_index_detail::ComponentIteratorStorageImpl<void> {
  typedef void IteratorStorage;

  CompositeIndexDynamicStrSuffix() = default;
  CompositeIndexDynamicStrSuffix(CompositeIndexDynamicStrSuffix&&) = default;
  CompositeIndexDynamicStrSuffix(ComponentBase* base) {
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
composite_index_detail::ComponentBase*
CompositeIndexFactoryBase::BuildAscendingUintPrefix(
  InputBufferType& input,
  const TerarkZipTableOptions& tzopt,
  const UintPrefixBuildInfo& info,
  const ImmutableCFOptions* ioption,
  std::string& name) const {
  name = typeid(CompositeIndexAscendingUintPrefix<RankSelect>).name();

  assert(minValue <= maxValue);
  assert(maxValue - minValue < UINT64_MAX); // must not overflow
  RankSelect rank_select;
  rank_select.resize(maxValue - minValue + 1);
  if (!std::is_same<RankSelect, rank_select_allone>::value) {
    // not 'all one' case
    for (size_t seq_id = 0; seq_id < numKeys; ++seq_id) {
      auto key = input.next();
      // even if 'cplen' contains actual data besides prefix,
      // after stripping, the left range is self-meaningful ranges
      assert(key.size() == keyLength);
      auto cur = ReadBigEndianUint64(key);
      rank_select.set1(cur - minValue);
    }
    rank_select.build_cache(false, false);
  }
  auto prefix = new CompositeIndexAscendingUintPrefix<RankSelect>();
  prefix->rank_select.swap(rank_select);
  prefix->key_length = keyLength;
  prefix->min_value = minValue;
  prefix->max_value = maxValue;
  prefix->working_state = WorkingState::Building;
  return prefix;
}

template<class RankSelect, class InputBufferType>
composite_index_detail::ComponentBase*
CompositeIndexFactoryBase::BuildNonDescendingUintPrefix(
  InputBufferType& input,
  const TerarkZipTableOptions& tzopt,
  const UintPrefixBuildInfo& info,
  const ImmutableCFOptions* ioption,
  std::string& name) const {
  name = typeid(CompositeIndexNonDescendingUintPrefix<RankSelect>).name();

  assert(minValue <= maxValue);
  assert(maxValue - minValue < UINT64_MAX); // must not overflow
  RankSelect rank_select;
  // TODO
  rank_select.build_cache(false, false);
  auto prefix = new CompositeIndexNonDescendingUintPrefix<RankSelect>();
  prefix->rank_select.swap(rank_select);
  prefix->key_length = keyLength;
  prefix->min_value = minValue;
  prefix->max_value = maxValue;
  prefix->working_state = WorkingState::Building;
  return prefix;
}

void SetNestLoudsTrieConfig(NestLoudsTrieConfig& conf,
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
    SetNestLoudsTrieConfig(cfg, keyVec.mem_size(), keyVec.agv_size(), tzopt);
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
    SetNestLoudsTrieConfig(cfg, keyVec.mem_size(), keyVec.agv_size(), tzopt);
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
  ComponentBase* prefix;
  ComponentBase* suffix;
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
          input_reader, tzopt, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_few_zero_64:
        prefix = BuildAscendingUintPrefix<rs_fewzero_64>(
          input_reader, tzopt, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_allone:
        prefix = BuildAscendingUintPrefix<rank_select_allone>(
          input_reader, tzopt, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_il_256:
        prefix = BuildAscendingUintPrefix<rank_select_il_256_32>(
          input_reader, tzopt, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_se_512:
        prefix = BuildAscendingUintPrefix<rank_select_se_512_64>(
          input_reader, tzopt, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_few_one_32:
        prefix = BuildAscendingUintPrefix<rs_fewone_32>(
          input_reader, tzopt, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_few_one_64:
        prefix = BuildAscendingUintPrefix<rs_fewone_64>(
          input_reader, tzopt, uint_prefix_info, ioption, name);
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
          prefix_input_reader, tzopt, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_few_zero_64:
        prefix = BuildAscendingUintPrefix<rs_fewzero_64>(
          prefix_input_reader, tzopt, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_allone:
        prefix = BuildAscendingUintPrefix<rank_select_allone>(
          prefix_input_reader, tzopt, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_il_256:
        prefix = BuildAscendingUintPrefix<rank_select_il_256_32>(
          prefix_input_reader, tzopt, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_se_512:
        prefix = BuildAscendingUintPrefix<rank_select_se_512_64>(
          prefix_input_reader, tzopt, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_few_one_32:
        prefix = BuildAscendingUintPrefix<rs_fewone_32>(
          prefix_input_reader, tzopt, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::asc_few_one_64:
        prefix = BuildAscendingUintPrefix<rs_fewone_64>(
          prefix_input_reader, tzopt, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::non_desc_il_256:
        prefix = BuildNonDescendingUintPrefix<rank_select_il_256_32>(
          prefix_input_reader, tzopt, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::non_desc_se_512:
        prefix = BuildNonDescendingUintPrefix<rank_select_se_512_64>(
          prefix_input_reader, tzopt, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::non_desc_few_one_32:
        prefix = BuildNonDescendingUintPrefix<rs_fewone_32>(
          prefix_input_reader, tzopt, uint_prefix_info, ioption, name);
        break;
      case UintPrefixBuildInfo::non_desc_few_one_64:
        prefix = BuildNonDescendingUintPrefix<rs_fewone_64>(
          prefix_input_reader, tzopt, uint_prefix_info, ioption, name);
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

class SortedUintDataCont {
private:
  size_t get_val(size_t idx) const {
    return container_[idx] + min_value_;
  }
  uint64_t to_uint64(fstring val) const {
    byte_t targetBuffer[8] = { 0 };
    memcpy(targetBuffer + (8 - key_len_), val.data(),
      std::min(key_len_, val.size()));
    return ReadBigEndianUint64(targetBuffer, 8);
  }
public:
  void swap(SortedUintDataCont& other) {
    container_.swap(other.container_);
    std::swap(min_value_, other.min_value_);
    std::swap(key_len_, other.key_len_);
  }
  void swap(SortedUintVec& other) {
    container_.swap(other);
  }
  void set_min_value(uint64_t min_value) {
    min_value_ = min_value;
  }
  void set_key_len(size_t key_len) {
    key_len_ = key_len;
  }
  void set_size(size_t sz) { assert(0); }
  void risk_release_ownership() {
    container_.risk_release_ownership();
  }
  const byte_t* data() const { return container_.data(); }
  size_t mem_size() const { return container_.mem_size(); }
  size_t size() const { return container_.size(); }
  bool equals(size_t idx, fstring val) const {
    assert(idx < container_.size());
    if (val.size() != key_len_)
      return false;
    uint64_t n = to_uint64(val);
    return get_val(idx) == n;
  }
  void risk_set_data(byte_t* data, size_t num, size_t maxValue) {
    assert(0);
  }
  void risk_set_data(byte_t* data, size_t sz) {
    assert(data != nullptr);
    container_.risk_set_data(data, sz);
  }

  void copy_to(size_t idx, byte_t* data) const {
    assert(idx < container_.size());
    size_t v = get_val(idx);
    SaveAsBigEndianUint64(data, key_len_, v);
  }
  int compare(size_t idx, fstring another) const {
    assert(idx < container_.size());
    byte_t arr[8] = { 0 };
    copy_to(idx, arr);
    fstring me(arr, arr + key_len_);
    return fstring_func::compare3()(me, another);
  }
  /*
    * 1. m_len == 8,
    * 2. m_len < 8, should align
    */
  size_t lower_bound(size_t lo, size_t hi, fstring val) const {
    uint64_t n = to_uint64(val);
    if (n < min_value_) // if equal, val len may > 8
      return lo;
    n -= min_value_;
    size_t pos = container_.lower_bound(lo, hi, n);
    while (pos != hi) {
      if (compare(pos, val) >= 0)
        return pos;
      pos++;
    }
    return pos;
  }


private:
  SortedUintVec container_;
  uint64_t min_value_;
  size_t key_len_;
};

class Min0DataCont {
private:
  size_t get_val(size_t idx) const {
    return container_[idx] + min_value_;
  }
  uint64_t to_uint64(fstring val) const {
    byte_t targetBuffer[8] = { 0 };
    memcpy(targetBuffer + (8 - key_len_), val.data(),
      std::min(key_len_, val.size()));
    return ReadBigEndianUint64(targetBuffer, 8);
  }
public:
  void swap(Min0DataCont& other) {
    container_.swap(other.container_);
    std::swap(min_value_, other.min_value_);
    std::swap(key_len_, other.key_len_);
  }
  void swap(BigUintVecMin0& other) {
    container_.swap(other);
  }
  void set_min_value(uint64_t min_value) {
    min_value_ = min_value;
  }
  void set_key_len(size_t key_len) {
    key_len_ = key_len;
  }
  void set_size(size_t sz) { assert(0); }
  void risk_release_ownership() {
    container_.risk_release_ownership();
  }
  const byte_t* data() const { return container_.data(); }
  size_t mem_size() const { return container_.mem_size(); }
  size_t size() const { return container_.size(); }
  bool equals(size_t idx, fstring val) const {
    assert(idx < container_.size());
    if (val.size() != key_len_)
      return false;
    uint64_t n = to_uint64(val);
    return get_val(idx) == n;
  }
  void risk_set_data(byte_t* data, size_t num, size_t maxValue) {
    size_t bits = BigUintVecMin0::compute_uintbits(maxValue);
    container_.risk_set_data(data, num, bits);
  }
  void risk_set_data(byte_t* data, size_t sz) {
    assert(0);
  }

  void copy_to(size_t idx, byte_t* data) const {
    assert(idx < container_.size());
    size_t v = get_val(idx);
    SaveAsBigEndianUint64(data, key_len_, v);
  }
  int compare(size_t idx, fstring another) const {
    assert(idx < container_.size());
    alignas(8) byte_t arr[8] = { 0 };
    copy_to(idx, arr);
    fstring me(arr, arr + key_len_);
    return fstring_func::compare3()(me, another);
  }
  size_t lower_bound(size_t lo, size_t hi, fstring val) const {
    uint64_t n = to_uint64(val);
    if (n < min_value_) // if equal, val len may > key_len_
      return lo;
    n -= min_value_;
    size_t pos = lower_bound_n<const BigUintVecMin0&>(container_, lo, hi, n);
    while (pos != hi) {
      if (compare(pos, val) >= 0)
        return pos;
      pos++;
    }
    return pos;
  }

private:
  BigUintVecMin0 container_;
  uint64_t min_value_;
  size_t key_len_;
};

class StrDataCont {
public:
  void swap(StrDataCont& other) {
    container_.swap(other.container_);
  }
  void swap(FixedLenStrVec& other) {
    container_.swap(other);
  }
  void risk_release_ownership() {
    container_.risk_release_ownership();
  }
  void set_key_len(size_t len) {
    container_.m_fixlen = len;
  }
  void set_min_value(uint64_t min_value) { assert(0); }
  void set_size(size_t sz) {
    container_.m_size = sz;
  }
  const byte_t* data() const { return container_.data(); }
  size_t mem_size() const { return container_.mem_size(); }
  size_t size() const { return container_.size(); }
  bool equals(size_t idx, fstring other) const {
    return container_[idx] == other;
  }
  void risk_set_data(byte_t* data, size_t sz) {
    assert(data != nullptr);
    // here, sz == count, since <byte_t>
    container_.m_strpool.risk_set_data(data, sz);
  }
  void risk_set_data(byte_t* data, size_t num, size_t maxValue) {
    assert(0);
  }

  void copy_to(size_t idx, byte_t* data) const {
    assert(idx < container_.size());
    memcpy(data, container_[idx].data(), container_[idx].size());
  }
  int compare(size_t idx, fstring another) const {
    assert(idx < container_.size());
    return fstring_func::compare3()(container_[idx], another);
  }
  size_t lower_bound(size_t lo, size_t hi, fstring val) const {
    return container_.lower_bound(lo, hi, val);
  }

private:
  FixedLenStrVec container_;
};
// -- fast zero-seq-len
template<class RankSelect>
size_t fast_zero_seq_len(const RankSelect& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_len(pos);
}
template<class Uint>
size_t
  fast_zero_seq_len(const rank_select_fewzero<Uint>& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_len(pos, hint);
}
template<class Uint>
size_t
  fast_zero_seq_len(const rank_select_fewone<Uint>& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_len(pos, hint);
}
// -- fast zero-seq-revlen
template<class RankSelect>
size_t fast_zero_seq_revlen(const RankSelect& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_revlen(pos);
}
template<class Uint>
size_t
  fast_zero_seq_revlen(const rank_select_fewzero<Uint>& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_revlen(pos, hint);
}
template<class Uint>
size_t
  fast_zero_seq_revlen(const rank_select_fewone<Uint>& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_revlen(pos, hint);
}

struct CompositeUintIndexBase : public TerarkIndex {
  static const char* index_name;
  struct MyBaseFileHeader : public TerarkIndexHeader {
    uint64_t key1_min_value;
    uint64_t key1_max_value;
    uint64_t rankselect1_mem_size;
    uint64_t rankselect2_mem_size;
    uint64_t key2_data_mem_size;
    /*
      * per key length = common_prefix_len + key1_fixed_len + key2_fixed_len
      */
    uint32_t key1_fixed_len;
    uint32_t key2_fixed_len;
    /*
      * (Rocksdb) For one huge index, we'll split it into multipart-index for the sake of RAM,
      * and each sub-index could have longer commonPrefix compared with ks.commonPrefix.
      * what's more, under such circumstances, ks.commonPrefix may have been rewritten
      * be upper-level builder to '0'. here,
      * common_prefix_length = sub-index.commonPrefixLen - whole-index.commonPrefixLen
      */
    uint32_t common_prefix_length;
    uint32_t reserved32;
    uint64_t reserved64;
    /*
      * For backward compatibility.
      * do NOT use reserved64/reserved_102_24 any more
      */
#define key2_min_value reserved64
#define key2_max_value reserved_102_24

    MyBaseFileHeader(size_t body_size, const std::type_info& ti) {
      memset(this, 0, sizeof *this);
      magic_len = strlen(index_name);
      strncpy(magic, index_name, sizeof magic);
      size_t name_i = g_TerarkIndexName.find_i(ti.name());
      assert(name_i < g_TerarkIndexFactroy.end_i());
      strncpy(class_name, g_TerarkIndexName.val(name_i).c_str(), sizeof class_name);
      header_size = sizeof *this;
      version = 1;
      file_size = sizeof *this + body_size;
    }
  };
  enum ContainerUsedT {
    kFixedLenStr = 0,
    kUintMin0,
    kSortedUint
  };
  // handlers
  static uint64_t Read1stKey(const valvec<byte_t>& key, size_t cplen, size_t key1_len) {
    return ReadBigEndianUint64(key.begin() + cplen, key1_len);
  }
  static uint64_t Read1stKey(fstring key, size_t cplen, size_t key1_len) {
    return ReadBigEndianUint64((const byte_t*)key.data() + cplen, key1_len);
  }
  static uint64_t Read1stKey(const byte_t* key, size_t cplen, size_t key1_len) {
    return ReadBigEndianUint64(key + cplen, key1_len);
  }
  class MyBaseFactory : public TerarkIndex::Factory {
  public:
    // composite index as cluster index
    // secondary index which contain 'id' as key2
    enum AmazingCombinationT {
      kAllOne_AllZero = 0, // secondary index no gap
      kAllOne_FewZero,     // c-cluster index
      kAllOne_FewOne,
      kAllOne_Normal,      // c-cluster index
      kFewZero_AllZero = 4,// secondary index with gap
      kFewZero_FewZero,    //
      kFewZero_FewOne,
      kFewZero_Normal,
      kFewOne_AllZero = 8, // secondary index with lots of gap
      kFewOne_FewZero,     // c-cluster index with lots of gap
      kFewOne_FewOne,
      kFewOne_Normal,      // c-cluster index with lots of gap
      kNormal_AllZero = 12,
      kNormal_FewZero,
      kNormal_FewOne,
      kNormal_Normal
    };
    size_t MemSizeForBuild(const KeyStat& ks) const override {
      assert(ks.minKeyLen == ks.maxKeyLen);
      size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
      size_t key1_len = 0;
      bool check = SeekCostEffectiveIndexLen(ks, key1_len);
      assert(check && ks.maxKeyLen > cplen + key1_len);
      TERARK_UNUSED_VAR(check);
      uint64_t minValue = Read1stKey(ks.minKey, cplen, key1_len);
      uint64_t maxValue = Read1stKey(ks.maxKey, cplen, key1_len);
      if (minValue > maxValue) {
        std::swap(minValue, maxValue);
      }
      uint64_t diff = maxValue - minValue + 1;
      size_t key2_len = ks.minKey.size() - cplen - key1_len;
      // maximum
      size_t rankselect_1st_sz = size_t(std::ceil(diff * 1.25 / 8));
      size_t rankselect_2nd_sz = size_t(std::ceil(ks.prefix.m_cnt_sum * 1.25 / 8));
      size_t sum_key2_sz = std::ceil(ks.prefix.m_cnt_sum * key2_len) * 1.25; // sorteduint as the extra cost
      return rankselect_1st_sz + rankselect_2nd_sz + sum_key2_sz;
    }
    static void
      updateMinMax(fstring data, valvec<byte_t>& minData, valvec<byte_t>& maxData) {
      if (minData.empty() || data < fstring(minData.begin(), minData.size())) {
        minData.assign(data.data(), data.size());
      }
      if (maxData.empty() || data > fstring(maxData.begin(), maxData.size())) {
        maxData.assign(data.data(), data.size());
      }
    }
    template<class RankSelect1, class RankSelect2>
    static AmazingCombinationT
      figureCombination(const RankSelect1& rs1, const RankSelect2& rs2) {
      bool isRS1FewZero = IsFewZero(rs1.size(), rs1.max_rank0());
      bool isRS2FewZero = IsFewZero(rs2.size(), rs2.max_rank0());
      bool isRS1FewOne = IsFewOne(rs1.size(), rs1.max_rank1());
      bool isRS2FewOne = IsFewOne(rs2.size(), rs2.max_rank1());
      // all one && ...
      if (rs1.isall1() && rs2.isall0())
        return kAllOne_AllZero;
      else if (rs1.isall1() && isRS2FewZero)
        return kAllOne_FewZero;
      else if (rs1.isall1() && isRS2FewOne)
        return kAllOne_FewOne;
      else if (rs1.isall1())
        return kAllOne_Normal;
      // few zero && ...
      else if (isRS1FewZero && rs2.isall0())
        return kFewZero_AllZero;
      else if (isRS1FewZero && isRS2FewZero)
        return kFewZero_FewZero;
      else if (isRS1FewZero && isRS2FewOne)
        return kFewZero_FewOne;
      else if (isRS1FewZero)
        return kFewZero_Normal;
      // few one && ...
      else if (isRS1FewOne && rs2.isall0())
        return kFewOne_AllZero;
      else if (isRS1FewOne && isRS2FewZero)
        return kFewOne_FewZero;
      else if (isRS1FewOne && isRS2FewOne)
        return kFewOne_FewOne;
      else if (isRS1FewOne)
        return kFewOne_Normal;
      // normal && ...
      else if (rs2.isall0())
        return kNormal_AllZero;
      else if (isRS2FewZero)
        return kNormal_FewZero;
      else if (isRS2FewOne)
        return kNormal_FewOne;
      else
        return kNormal_Normal;
    }
    template<class RankSelect1, class RankSelect2>
    static TerarkIndex*
      CreateIndexWithSortedUintCont(RankSelect1& rankselect1,
        RankSelect2& rankselect2,
        FixedLenStrVec& keyVec,
        const KeyStat& ks,
        uint64_t key1MinValue,
        uint64_t key1MaxValue,
        size_t key1_len,
        valvec<byte_t>& minKey2Data,
        valvec<byte_t>& maxKey2Data) {
      const size_t kBlockUnits = 128;
      const size_t kLimit = (1ull << 48) - 1;
      uint64_t key2MinValue = ReadBigEndianUint64(minKey2Data);
      uint64_t key2MaxValue = ReadBigEndianUint64(maxKey2Data);
      //if (key2MinValue == key2MaxValue) // MyRock UT will fail on this condition
      //  return nullptr;
      unique_ptr<SortedUintVec::Builder> builder(SortedUintVec::createBuilder(false, kBlockUnits));
      uint64_t prev = ReadBigEndianUint64(keyVec[0]) - key2MinValue;
      builder->push_back(prev);
      for (size_t i = 1; i < keyVec.size(); i++) {
        fstring str = keyVec[i];
        uint64_t key2 = ReadBigEndianUint64(str) - key2MinValue;
        if (terark_unlikely(abs_diff(key2, prev) > kLimit)) // should not use sorted uint vec
          return nullptr;
        builder->push_back(key2);
        prev = key2;
      }
      SortedUintVec uintVec;
      auto rs = builder->finish(&uintVec);
      if (rs.mem_size > keyVec.mem_size() / 2.0) // too much ram consumed
        return nullptr;
      SortedUintDataCont container;
      container.swap(uintVec);
      //container.init(minKey2Data.size(), key2MinValue);
      container.set_key_len(minKey2Data.size());
      container.set_min_value(key2MinValue);
      return CreateIndex(rankselect1, rankselect2, container, ks,
        key1MinValue, key1MaxValue,
        key1_len, key2MinValue, key2MaxValue);
    }
    template<class RankSelect1, class RankSelect2>
    static TerarkIndex*
      CreateIndexWithUintCont(RankSelect1& rankselect1,
        RankSelect2& rankselect2,
        FixedLenStrVec& keyVec,
        const KeyStat& ks,
        uint64_t key1MinValue,
        uint64_t key1MaxValue,
        size_t key1_len,
        valvec<byte_t>& minKey2Data,
        valvec<byte_t>& maxKey2Data) {
      uint64_t key2MinValue = ReadBigEndianUint64(minKey2Data);
      uint64_t key2MaxValue = ReadBigEndianUint64(maxKey2Data);
      uint64_t diff = key2MaxValue - key2MinValue + 1;
      size_t bitUsed = BigUintVecMin0::compute_uintbits(diff);
      if (bitUsed > keyVec.m_fixlen * 8 * 0.9) // compress ratio just so so
        return nullptr;
      // reuse memory from keyvec, since vecMin0 should consume less mem
      // compared with fixedlenvec
      BigUintVecMin0 vecMin0;
      vecMin0.risk_set_data(const_cast<byte_t*>(keyVec.data()), keyVec.size(), bitUsed);
      for (size_t i = 0; i < keyVec.size(); i++) {
        fstring str = keyVec[i];
        uint64_t key2 = ReadBigEndianUint64(str);
        vecMin0.set_wire(i, key2 - key2MinValue);
      }
      keyVec.risk_release_ownership();
      Min0DataCont container;
      container.swap(vecMin0);
      //container.init(minKey2Data.size(), key2MinValue);
      container.set_key_len(minKey2Data.size());
      container.set_min_value(key2MinValue);
      return CreateIndex(rankselect1, rankselect2, container, ks,
        key1MinValue, key1MaxValue,
        key1_len, key2MinValue, key2MaxValue);
    }
    template<class RankSelect1, class RankSelect2>
    static TerarkIndex*
      CreateIndexWithStrCont(RankSelect1& rankselect1,
        RankSelect2& rankselect2,
        FixedLenStrVec& keyVec,
        const KeyStat& ks,
        uint64_t key1MinValue,
        uint64_t key1MaxValue,
        size_t key1_len) {
      StrDataCont container;
      container.swap(keyVec);
      return CreateIndex(rankselect1, rankselect2, container, ks,
        key1MinValue, key1MaxValue, key1_len, 0, 0);
    }
    template<class RankSelect1, class RankSelect2, class DataCont>
    static TerarkIndex*
      CreateIndex(RankSelect1& rankselect1,
        RankSelect2& rankselect2,
        DataCont& container, const KeyStat& ks,
        uint64_t key1MinValue, uint64_t key1MaxValue, size_t key1_len,
        uint64_t key2MinValue, uint64_t key2MaxValue);
    bool verifyBaseHeader(fstring mem) const {
      const MyBaseFileHeader* header = (const MyBaseFileHeader*)mem.data();
      if (mem.size() < sizeof(MyBaseFileHeader)
        || header->magic_len != strlen(index_name)
        || strcmp(header->magic, index_name) != 0
        || header->header_size != sizeof(MyBaseFileHeader)
        || header->version != 1
        || header->file_size != mem.size()
        ) {
        throw std::invalid_argument("CompositeUintIndex::Load(): Bad file header");
      }
      return true;
    }

#define Disable_BuildImpl(rs1, rs2) \
  TerarkIndex* BuildImpl(NativeDataInput<InputBuffer>& reader, \
                          const TerarkZipTableOptions& tzopt, \
                          const KeyStat& ks, \
                          rs1*, rs2*) \
  const { TERARK_IF_DEBUG(assert(0), abort()); return NULL; }

    template<class rs2> Disable_BuildImpl(rs_fewone_32, rs2);
    template<class rs2> Disable_BuildImpl(rs_fewone_64, rs2);
    template<class rs2> Disable_BuildImpl(rs_fewzero_32, rs2);
    template<class rs2> Disable_BuildImpl(rs_fewzero_64, rs2);
    template<class rs2> Disable_BuildImpl(rank_select_allone, rs2);
    //template<class rs2> Disable_BuildImpl(rank_select_allzero, rs2);

    template<class rs1> Disable_BuildImpl(rs1, rs_fewone_32);
    template<class rs1> Disable_BuildImpl(rs1, rs_fewone_64);
    template<class rs1> Disable_BuildImpl(rs1, rs_fewzero_32);
    template<class rs1> Disable_BuildImpl(rs1, rs_fewzero_64);
    //template<class rs1> Disable_BuildImpl(rs1, rank_select_allone);
    template<class rs1> Disable_BuildImpl(rs1, rank_select_allzero);

    Disable_BuildImpl(rs_fewone_32, rs_fewone_32);
    Disable_BuildImpl(rs_fewone_32, rs_fewzero_32);
    //Disable_BuildImpl(rs_fewone_32, rank_select_allone);
    Disable_BuildImpl(rs_fewone_32, rank_select_allzero);

    Disable_BuildImpl(rs_fewzero_32, rs_fewone_32);
    Disable_BuildImpl(rs_fewzero_32, rs_fewzero_32);
    //Disable_BuildImpl(rs_fewzero_32, rank_select_allone);
    Disable_BuildImpl(rs_fewzero_32, rank_select_allzero);

    Disable_BuildImpl(rs_fewone_64, rs_fewone_64);
    Disable_BuildImpl(rs_fewone_64, rs_fewzero_64);
    //Disable_BuildImpl(rs_fewone_64, rank_select_allone);
    Disable_BuildImpl(rs_fewone_64, rank_select_allzero);

    Disable_BuildImpl(rs_fewzero_64, rs_fewone_64);
    Disable_BuildImpl(rs_fewzero_64, rs_fewzero_64);
    //Disable_BuildImpl(rs_fewzero_64, rank_select_allone);
    Disable_BuildImpl(rs_fewzero_64, rank_select_allzero);

    Disable_BuildImpl(rank_select_allone, rs_fewone_32);
    Disable_BuildImpl(rank_select_allone, rs_fewzero_32);
    //Disable_BuildImpl(rank_select_allone, rank_select_allone);
    Disable_BuildImpl(rank_select_allone, rank_select_allzero);

    //Disable_BuildImpl(rank_select_allzero, rs_fewone_32);
    //Disable_BuildImpl(rank_select_allzero, rs_fewzero_32);
    //Disable_BuildImpl(rank_select_allzero, rank_select_allone);
    //Disable_BuildImpl(rank_select_allzero, rank_select_allzero);

    Disable_BuildImpl(rank_select_allone, rs_fewone_64);
    Disable_BuildImpl(rank_select_allone, rs_fewzero_64);

    //Disable_BuildImpl(rank_select_allzero, rs_fewone_64);
    //Disable_BuildImpl(rank_select_allzero, rs_fewzero_64);

    template<class RankSelect1, class RankSelect2>
    TerarkIndex* BuildImpl(NativeDataInput<InputBuffer>& reader,
      const TerarkZipTableOptions& tzopt,
      const KeyStat& ks,
      RankSelect1*, RankSelect2*,
      const ImmutableCFOptions* ioptions) const {
      size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
      assert(cplen >= ks.commonPrefixLen);
      if (ks.maxKeyLen != ks.minKeyLen ||
        ks.maxKeyLen <= cplen + 1) {
        printf("diff ken len, maxlen %zu, minlen %zu\n", ks.maxKeyLen, ks.minKeyLen);
        abort();
      }

      size_t key1_len = 0;
      bool check = SeekCostEffectiveIndexLen(ks, key1_len);
      assert(check && ks.maxKeyLen > cplen + key1_len);
      TERARK_UNUSED_VAR(check);
      uint64_t minValue = Read1stKey(ks.minKey, cplen, key1_len);
      uint64_t maxValue = Read1stKey(ks.maxKey, cplen, key1_len);
      // if rocksdb reverse comparator is used, then minValue
      // is actually the largest one
      if (minValue > maxValue) {
        std::swap(minValue, maxValue);
      }
      /*
        * 1stRS stores bitmap [minValue, maxValue] for index1st
        * 2ndRS stores bitmap for StaticMap<key1, list<key2>>
        * order of rs2 follows the order of index data, and order
        * of rs1 follows the order of rs2
        */
      RankSelect1 rankselect1(maxValue - minValue + 1);
      RankSelect2 rankselect2(ks.prefix.m_cnt_sum + 1); // append extra '0' at back
      valvec<byte_t> keyBuf, minKey2Data, maxKey2Data;
      uint64_t prev = size_t(-1);
      size_t key2_len = ks.maxKeyLen - cplen - key1_len;
      size_t sumKey2Len = key2_len * ks.prefix.m_cnt_sum;
      FixedLenStrVec keyVec(key2_len);
      keyVec.m_strpool.reserve(sumKey2Len + 8);
      if (ks.minKey < ks.maxKey) { // ascend
        for (size_t i = 0; i < ks.prefix.m_cnt_sum; ++i) {
          reader >> keyBuf;
          uint64_t offset = Read1stKey(keyBuf, cplen, key1_len) - minValue;
          rankselect1.set1(offset);
          if (terark_unlikely(i == 0)) // make sure 1st prev != offset
            prev = offset - 1;
          if (offset != prev) { // new key1 encountered
            rankselect2.set0(i);
          }
          else {
            rankselect2.set1(i);
          }
          prev = offset;
          fstring key2Data = fstring(keyBuf).substr(cplen + key1_len);
          updateMinMax(key2Data, minKey2Data, maxKey2Data);
          keyVec.push_back(key2Data);
        }
      }
      else { // descend, reverse comparator
        size_t pos = sumKey2Len;
        keyVec.m_size = ks.prefix.m_cnt_sum;
        keyVec.m_strpool.resize(sumKey2Len);
        // compare with '0', do NOT use size_t
        for (size_t i = ks.prefix.m_cnt_sum; i > 0; ) {
          i--;
          reader >> keyBuf;
          uint64_t offset = Read1stKey(keyBuf, cplen, key1_len) - minValue;
          rankselect1.set1(offset);
          if (terark_unlikely(i == ks.prefix.m_cnt_sum - 1)) { // make sure 1st prev != offset
            prev = offset - 1;
          }
          if (offset != prev) { // next index1 is new one
            rankselect2.set0(i + 1);
          }
          else {
            rankselect2.set1(i + 1);
          }
          prev = offset;
          // save index data
          fstring key2Data = fstring(keyBuf).substr(cplen + key1_len);
          updateMinMax(key2Data, minKey2Data, maxKey2Data);
          pos -= key2Data.size();
          memcpy(keyVec.m_strpool.data() + pos, key2Data.data(), key2Data.size());
        }
        rankselect2.set0(0); // set 1st element to 0
        assert(pos == 0);
      }
      rankselect2.set0(ks.prefix.m_cnt_sum);
      // TBD: build histogram, which should set as 'true'
      rankselect1.build_cache(false, false);
      rankselect2.build_cache(false, false);
      // try order: 1. sorteduint; 2. uintmin0; 3. fixlen
      // TBD: following skips are only for test right now
#ifndef INDEX_UT
      if (ioptions) {
        INFO(ioptions->info_log,
          "TerarkCompositeUintIndex::Build(): key1Min %zu, key1Max %zu, "
          "cplen %zu, key1_len %zu, key2_len %zu,\n"
          "key1 size %zu, key2 size %zu, "
          "rankselect combination is %d\n",
          minValue, maxValue, cplen, key1_len, key2_len,
          rankselect1.size(), rankselect2.size(),
          figureCombination(rankselect1, rankselect2)
        );
      }
#endif
      if (key2_len <= 8) {
        TerarkIndex* index = nullptr;
        if (g_indexEnableSortedUint)
          index = CreateIndexWithSortedUintCont(
            rankselect1, rankselect2, keyVec, ks,
            minValue, maxValue, key1_len, minKey2Data, maxKey2Data);
        if (!index && g_indexEnableBigUint0) {
          index = CreateIndexWithUintCont(
            rankselect1, rankselect2, keyVec, ks,
            minValue, maxValue, key1_len, minKey2Data, maxKey2Data);
        }
        if (index)
          return index;
      }
      return CreateIndexWithStrCont(
        rankselect1, rankselect2, keyVec, ks,
        minValue, maxValue, key1_len);
    }

    void loadImplBase(fstring& mem, fstring fpath, CompositeUintIndexBase* ptr)
      const {
      if (mem.data() == nullptr) {
        MmapWholeFile mmapFile(fpath);
        mem = mmapFile.memory();
        mmapFile.base = nullptr;
        ptr->workingState_ = WorkingState::MmapFile;
      }
      else {
        ptr->workingState_ = WorkingState::UserMemory;
      }
      // make sure header is valid
      verifyBaseHeader(mem);
      // construct composite index
      const MyBaseFileHeader* header = (const MyBaseFileHeader*)mem.data();
      ptr->header_ = header;
      ptr->minValue_ = header->key1_min_value;
      ptr->maxValue_ = header->key1_max_value;
      ptr->key2_min_value_ = header->key2_min_value; // old version do NOT has this attribute
      ptr->key2_max_value_ = header->key2_max_value;
      ptr->key1_len_ = header->key1_fixed_len;
      ptr->key2_len_ = header->key2_fixed_len;
      if (header->common_prefix_length > 0) {
        ptr->commonPrefix_.risk_set_data((byte_t*)mem.data() + header->header_size,
          header->common_prefix_length);
      }
    }
  };
  static int CommonSeek(fstring target,
    const CompositeUintIndexBase& index,
    uint64_t* pKey1,
    fstring* pKey2,
    size_t& id,
    size_t& rankselect1_idx) {
    size_t cplen = target.commonPrefixLen(index.commonPrefix_);
    if (cplen != index.commonPrefix_.size()) {
      assert(target.size() >= cplen);
      assert(target.size() == cplen ||
        byte_t(target[cplen]) != byte_t(index.commonPrefix_[cplen]));
      if (target.size() == cplen ||
        byte_t(target[cplen]) < byte_t(index.commonPrefix_[cplen])) {
        rankselect1_idx = 0;
        id = 0;
        return 1;
      }
      else {
        id = size_t(-1);
        return 0;
      }
    }
    uint64_t key1 = 0;
    fstring  key2;
    if (target.size() <= cplen + index.key1_len_) {
      fstring sub = target.substr(cplen);
      byte_t targetBuffer[8] = { 0 };
      /*
        * do not think hard about int, think about string instead.
        * assume key1_len is 6 byte len like 'abcdef', target without
        * commpref is 'b', u should compare 'b' with 'a' instead of 'f'.
        * that's why assign sub starting in the middle instead at tail.
        */
      memcpy(targetBuffer + (8 - index.key1_len_), sub.data(), sub.size());
      *pKey1 = key1 = Read1stKey(targetBuffer, 0, 8);
      *pKey2 = key2 = fstring(); // empty
    }
    else {
      *pKey1 = key1 = Read1stKey(target, cplen, index.key1_len_);
      *pKey2 = key2 = target.substr(cplen + index.key1_len_);
    }
    if (key1 > index.maxValue_) {
      id = size_t(-1);
      return 0;
    }
    else if (key1 < index.minValue_) {
      rankselect1_idx = 0;
      id = 0;
      return 1;
    }
    else {
      return 2;
    }
  }

  class MyBaseIterator : public TerarkIndex::Iterator {
  protected:
    const CompositeUintIndexBase& index_;
    size_t rankselect1_idx_; // used to track & decode index1 value
    size_t access_hint_;
    valvec<byte_t> buffer_;

    MyBaseIterator(const CompositeUintIndexBase& index) : index_(index) {
      rankselect1_idx_ = size_t(-1);
      m_id = size_t(-1);
      buffer_.resize_no_init(index.KeyLen());
      memcpy(buffer_.data(), index.commonPrefix_.data(), index.commonPrefix_.size());
      access_hint_ = size_t(-1);
    }
    /*
      * Various combinations of <RankSelect1, RankSelect2, DataCont> may cause tooooo many template instantiations,
      * which will cause code expansion & compiler errors (warnings). That's why we lift some methods from
      * NonBaseIterator to BaseIterator, like UpdateBuffer/UpdateBufferKey2/CommonSeek/SeekToFirst(), even though
      * it seems strange.
      */
    void UpdateBuffer() {
      // key = commonprefix + key1s + key2
      // assign key1
      size_t offset = index_.commonPrefix_.size();
      auto len1 = index_.key1_len_;
      auto key1 = rankselect1_idx_ + index_.minValue_;
      SaveAsBigEndianUint64(buffer_.data() + offset, len1, key1);
      UpdateBufferKey2(offset + len1);
    }
    virtual void UpdateBufferKey2(size_t offset) = 0;
    size_t DictRank() const override {
      assert(m_id != size_t(-1));
      return m_id;
    }
    fstring key() const override {
      assert(m_id != size_t(-1));
      return buffer_;
    }
    bool SeekToFirst() override {
      rankselect1_idx_ = 0;
      m_id = 0;
      UpdateBuffer();
      return true;
    }
  };

  size_t KeyLen() const {
    return commonPrefix_.size() + key1_len_ + key2_len_;
  }
  const char* Name() const override {
    return header_->class_name;
  }
  fstring Memory() const override {
    return fstring((const char*)header_, (ptrdiff_t)header_->file_size);
  }
  bool NeedsReorder() const override {
    return false;
  }
  void GetOrderMap(UintVecMin0& newToOld) const override {
    assert(false);
  }
  void BuildCache(double cacheRatio) override {
    //do nothing
  }

  ~CompositeUintIndexBase() {
    if (workingState_ == WorkingState::Building) {
      delete header_;
    }
    else {
      if (workingState_ == WorkingState::MmapFile) {
        terark::mmap_close((void*)header_, header_->file_size);
      }
      commonPrefix_.risk_release_ownership();
    }
  }

  const MyBaseFileHeader* header_;
  valvec<byte_t>    commonPrefix_;
  uint64_t          minValue_;
  uint64_t          maxValue_;
  uint64_t          key2_min_value_;
  uint64_t          key2_max_value_;
  uint32_t          key1_len_;
  uint32_t          key2_len_;
  WorkingState      workingState_;
};
const char* CompositeUintIndexBase::index_name = "CompositeIndex";

/*
  * For simplicity, let's take composite index => index1:index2. Then following
  * compositeindexes like,
  *   4:6, 4:7, 4:8, 7:19, 7:20, 8:3
  * index1: {4, 7}, use bitmap (also UintIndex's form) to represent
  *   4(1), 5(0), 6(0), 7(1), 8(1)
  * index1:index2: use bitmap to respresent 4:6(0), 4:7(1), 4:8(1), 7:19(0), 7:20(1), 8:3(0)
  * to search 7:20, use bitmap1 to rank1(7) = 2, then use bitmap2 to select0(2) = position-3,
  * use bitmap2 to select0(3) = position-5. That is, we should search within [3, 5).
  * iter [3 to 5), 7:20 is found, done.
  */
template<class RankSelect1, class RankSelect2, class Key2DataContainer>
class CompositeUintIndex : public CompositeUintIndexBase {
public:
  struct FileHeader : MyBaseFileHeader {
    FileHeader(size_t file_size)
      : MyBaseFileHeader(file_size, typeid(CompositeUintIndex))
    {}
  };
public:
  static bool SeekImpl(fstring target,
    const CompositeUintIndex& index,
    size_t& id,
    size_t& rankselect1_idx) {
    uint64_t key1 = 0;
    fstring  key2;
    int ret = CompositeUintIndexBase::CommonSeek(target, index, &key1, &key2, id, rankselect1_idx);
    if (ret < 2) {
      return ret ? true : false;
    }
    // find the corresponding bit within 2ndRS
    uint64_t order, pos0, cnt;
    rankselect1_idx = key1 - index.minValue_;
    if (index.rankselect1_[rankselect1_idx]) {
      // find within this index
      order = index.rankselect1_.rank1(rankselect1_idx);
      pos0 = index.rankselect2_.select0(order);
      if (pos0 == index.key2_data_.size() - 1) { // last elem
        id = (index.key2_data_.compare(pos0, key2) >= 0) ? pos0 : size_t(-1);
        goto out;
      }
      else {
        cnt = index.rankselect2_.one_seq_len(pos0 + 1) + 1;
        id = index.Locate(index.key2_data_, pos0, cnt, key2);
        if (id != size_t(-1)) {
          goto out;
        }
        else if (pos0 + cnt == index.key2_data_.size()) {
          goto out;
        }
        else {
          // try next offset
          rankselect1_idx++;
        }
      }
    }
    // no such index, use the lower_bound form
    cnt = index.rankselect1_.zero_seq_len(rankselect1_idx);
    if (rankselect1_idx + cnt >= index.rankselect1_.size()) {
      id = size_t(-1);
      return false;
    }
    rankselect1_idx += cnt;
    order = index.rankselect1_.rank1(rankselect1_idx);
    id = index.rankselect2_.select0(order);
  out:
    if (id == size_t(-1)) {
      return false;
    }
    return true;
  }
  class CompositeUintIndexIterator : public MyBaseIterator {
  public:
    CompositeUintIndexIterator(const CompositeUintIndex& index)
      : MyBaseIterator(index) {}
    void UpdateBufferKey2(size_t offset) override {
      auto& index_ = static_cast<const CompositeUintIndex&>(MyBaseIterator::index_);
      index_.key2_data_.copy_to(m_id, buffer_.data() + offset);
    }

    bool SeekToLast() override {
      auto& index_ = static_cast<const CompositeUintIndex&>(MyBaseIterator::index_);
      rankselect1_idx_ = index_.rankselect1_.size() - 1;
      m_id = index_.key2_data_.size() - 1;
      UpdateBuffer();
      return true;
    }
    bool Seek(fstring target) override {
      auto& index = static_cast<const CompositeUintIndex&>(MyBaseIterator::index_);
      if (CompositeUintIndex::SeekImpl(target, index, m_id, rankselect1_idx_)) {
        UpdateBuffer();
        return true;
      }
      return false;
    }

    bool Next() override {
      auto& index_ = static_cast<const CompositeUintIndex&>(MyBaseIterator::index_);
      assert(m_id != size_t(-1));
      assert(index_.rankselect1_[rankselect1_idx_] != 0);
      if (terark_unlikely(m_id + 1 == index_.key2_data_.size())) {
        m_id = size_t(-1);
        return false;
      }
      else {
        if (Is1stKeyDiff(m_id + 1)) {
          assert(rankselect1_idx_ + 1 < index_.rankselect1_.size());
          //uint64_t cnt = index_.rankselect1_.zero_seq_len(rankselect1_idx_ + 1);
          uint64_t cnt = fast_zero_seq_len(index_.rankselect1_, rankselect1_idx_ + 1, access_hint_);
          rankselect1_idx_ += cnt + 1;
          assert(rankselect1_idx_ < index_.rankselect1_.size());
        }
        ++m_id;
        UpdateBuffer();
        return true;
      }
    }
    bool Prev() override {
      auto& index_ = static_cast<const CompositeUintIndex&>(MyBaseIterator::index_);
      assert(m_id != size_t(-1));
      assert(index_.rankselect1_[rankselect1_idx_] != 0);
      if (terark_unlikely(m_id == 0)) {
        m_id = size_t(-1);
        return false;
      }
      else {
        if (Is1stKeyDiff(m_id)) {
          /*
            * zero_seq_ has [a, b) range, hence next() need (pos_ + 1), whereas
            * prev() just called with (pos_) is enough
            * case1: 1 0 1, ...
            * case2: 1 1, ...
            */
          assert(rankselect1_idx_ > 0);
          //uint64_t cnt = index_.rankselect1_.zero_seq_revlen(rankselect1_idx_);
          uint64_t cnt = fast_zero_seq_revlen(index_.rankselect1_, rankselect1_idx_, access_hint_);
          assert(rankselect1_idx_ >= cnt + 1);
          rankselect1_idx_ -= (cnt + 1);
        }
        --m_id;
        UpdateBuffer();
        return true;
      }
    }

  protected:
    //use 2nd index bitmap to check if 1st index changed
    bool Is1stKeyDiff(size_t curr_id) {
      auto& index_ = static_cast<const CompositeUintIndex&>(MyBaseIterator::index_);
      return index_.rankselect2_.is0(curr_id);
    }
  };

public:
  class MyFactory : public MyBaseFactory {
  public:
    // no option.keyPrefixLen
    TerarkIndex* Build(NativeDataInput<InputBuffer>& reader,
      const TerarkZipTableOptions& tzopt,
      const KeyStat& ks,
      const ImmutableCFOptions* ioption) const override {
      return BuildImpl(reader, tzopt, ks,
        (RankSelect1*)(NULL), (RankSelect2*)(NULL), ioption);
    }
  public:
    unique_ptr<TerarkIndex> LoadMemory(fstring mem) const override {
      return UniquePtrOf(loadImpl(mem, {}));
    }
    unique_ptr<TerarkIndex> LoadFile(fstring fpath) const override {
      return UniquePtrOf(loadImpl({}, fpath));
    }
  protected:
    TerarkIndex* loadImpl(fstring mem, fstring fpath) const {
      auto ptr = UniquePtrOf(
        new CompositeUintIndex<RankSelect1, RankSelect2, Key2DataContainer>());
      loadImplBase(mem, fpath, ptr.get());
      const MyBaseFileHeader* header = (const MyBaseFileHeader*)mem.data();
      assert(VerifyClassName<CompositeUintIndex>(header->class_name));
      size_t offset = header->header_size +
        align_up(header->common_prefix_length, 8);
      ptr->rankselect1_.risk_mmap_from((unsigned char*)mem.data() + offset,
        header->rankselect1_mem_size);
      offset += header->rankselect1_mem_size;
      ptr->rankselect2_.risk_mmap_from((unsigned char*)mem.data() + offset,
        header->rankselect2_mem_size);
      offset += header->rankselect2_mem_size;
      if (std::is_same<Key2DataContainer, SortedUintDataCont>::value) {
        ptr->key2_data_.risk_set_data((unsigned char*)mem.data() + offset,
          header->key2_data_mem_size);
        //ptr->key2_data_.init(header->key2_fixed_len, header->key2_min_value);
        ptr->key2_data_.set_key_len(header->key2_fixed_len);
        ptr->key2_data_.set_min_value(header->key2_min_value);
      }
      else if (std::is_same<Key2DataContainer, Min0DataCont>::value) {
        size_t num = ptr->rankselect2_.size() - 1; // sub the extra append '0'
        size_t diff = header->key2_max_value - header->key2_min_value + 1;
        ptr->key2_data_.risk_set_data((unsigned char*)mem.data() + offset,
          num, diff);
        //ptr->key2_data_.init(header->key2_fixed_len, header->key2_min_value);
        ptr->key2_data_.set_key_len(header->key2_fixed_len);
        ptr->key2_data_.set_min_value(header->key2_min_value);
      }
      else { // FixedLenStr
        ptr->key2_data_.risk_set_data((unsigned char*)mem.data() + offset,
          header->key2_data_mem_size);
        //ptr->key2_data_.init(header->key2_fixed_len,
        //                   header->key2_data_mem_size / header->key2_fixed_len);
        ptr->key2_data_.set_key_len(header->key2_fixed_len);
        ptr->key2_data_.set_size(header->key2_data_mem_size / header->key2_fixed_len);
      }
      return ptr.release();
    }
  };

public:
  using TerarkIndex::FactoryPtr;
  CompositeUintIndex() {}
  CompositeUintIndex(RankSelect1& rankselect1, RankSelect2& rankselect2,
    Key2DataContainer& key2Container, const KeyStat& ks,
    uint64_t minValue, uint64_t maxValue, size_t key1_len,
    uint64_t minKey2Value = 0, uint64_t maxKey2Value = 0) {
    //isBuilding_ = true;
    workingState_ = WorkingState::Building;
    size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
    // save meta into header
    FileHeader* header = new FileHeader(
      rankselect1.mem_size() +
      rankselect2.mem_size() +
      align_up(key2Container.mem_size(), 8));
    header->key1_min_value = minValue;
    header->key1_max_value = maxValue;
    header->rankselect1_mem_size = rankselect1.mem_size();
    header->rankselect2_mem_size = rankselect2.mem_size();
    header->key2_data_mem_size = key2Container.mem_size();
    header->key1_fixed_len = key1_len;
    header->key2_min_value = minKey2Value;
    header->key2_max_value = maxKey2Value;
    header->key2_fixed_len = ks.minKeyLen - cplen - key1_len;
    if (cplen > ks.commonPrefixLen) {
      // upper layer didn't handle common prefix, we'll do it
      // ourselves. actually here ks.commonPrefixLen == 0
      header->common_prefix_length = cplen - ks.commonPrefixLen;
      commonPrefix_.assign(ks.minKey.data() + ks.commonPrefixLen,
        header->common_prefix_length);
      header->file_size += align_up(header->common_prefix_length, 8);
    }
    header_ = header;
    rankselect1_.swap(rankselect1);
    rankselect2_.swap(rankselect2);
    key2_data_.swap(key2Container);
    key1_len_ = key1_len;
    key2_len_ = header->key2_fixed_len;
    minValue_ = minValue;
    maxValue_ = maxValue;
    key2_min_value_ = minKey2Value;
    key2_max_value_ = maxKey2Value;
    //isUserMemory_ = false;
  }

  virtual ~CompositeUintIndex() {
    if (workingState_ == WorkingState::Building) {
      // do nothing, will destruct in base destructor
    }
    else {
      rankselect1_.risk_release_ownership();
      rankselect2_.risk_release_ownership();
      key2_data_.risk_release_ownership();
    }
  }
  void SaveMmap(std::function<void(const void *, size_t)> write) const override {
    write(header_, sizeof *header_);
    if (!commonPrefix_.empty()) {
      write(commonPrefix_.data(), commonPrefix_.size());
      Padzero<8>(write, commonPrefix_.size());
    }
    write(rankselect1_.data(), rankselect1_.mem_size());
    write(rankselect2_.data(), rankselect2_.mem_size());
    write(key2_data_.data(), key2_data_.mem_size());
    Padzero<8>(write, key2_data_.mem_size());
  }
  size_t Find(fstring key) const override {
    size_t cplen = commonPrefix_.size();
    if (key.size() != key2_len_ + key1_len_ + cplen ||
      key.commonPrefixLen(commonPrefix_) != cplen) {
      return size_t(-1);
    }
    uint64_t key1 = Read1stKey(key, cplen, key1_len_);
    if (key1 < minValue_ || key1 > maxValue_) {
      return size_t(-1);
    }
    uint64_t offset = key1 - minValue_;
    if (!rankselect1_[offset]) {
      return size_t(-1);
    }
    uint64_t order = rankselect1_.rank1(offset);
    uint64_t pos0 = rankselect2_.select0(order);
    assert(pos0 != size_t(-1));
    size_t cnt = rankselect2_.one_seq_len(pos0 + 1);
    fstring key2 = key.substr(cplen + key1_len_);
    size_t id = Locate(key2_data_, pos0, cnt + 1, key2);
    if (id != size_t(-1) && key2_data_.equals(id, key2)) {
      return id;
    }
    return size_t(-1);
  }
  size_t DictRank(fstring key) const override {
    size_t id, rankselect1_idx;
    if (CompositeUintIndex::SeekImpl(key, *this, id, rankselect1_idx)) {
      return id;
    }
    return key2_data_.size();
  }
  size_t NumKeys() const override {
    return key2_data_.size();
  }
  size_t TotalKeySize() const override final {
    return (commonPrefix_.size() + key1_len_ + key2_len_) * key2_data_.size();
  }
  Iterator* NewIterator() const override {
    return new CompositeUintIndexIterator(*this);
  }

public:
  size_t Locate(const Key2DataContainer& arr,
    size_t start, size_t cnt, fstring target) const {
    size_t lo = start, hi = start + cnt;
    size_t pos = arr.lower_bound(lo, hi, target);
    return (pos < hi) ? pos : size_t(-1);
  }

protected:
  RankSelect1       rankselect1_;
  RankSelect2       rankselect2_;
  Key2DataContainer key2_data_;
};

template<class RankSelect1, class RankSelect2, class DataCont>
TerarkIndex*
  CompositeUintIndexBase::MyBaseFactory::CreateIndex(
    RankSelect1& rankselect1,
    RankSelect2& rankselect2,
    DataCont& container,
    const KeyStat& ks,
    uint64_t key1MinValue, uint64_t key1MaxValue, size_t key1_len,
    uint64_t key2MinValue, uint64_t key2MaxValue)
{
  const size_t kRS1Cnt = rankselect1.size(); // to accompany ks2cnt
  const size_t kRS2Cnt = rankselect2.size(); // extra '0' append to rs2, included as well
  AmazingCombinationT cob = figureCombination(rankselect1, rankselect2);
  switch (cob) {
    // -- all one & ...
  case kAllOne_AllZero: {
    rank_select_allone rs1(kRS1Cnt);
    rank_select_allzero rs2(kRS2Cnt);
    return new CompositeUintIndex<rank_select_allone, rank_select_allzero, DataCont>(
      rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kAllOne_FewZero: {
    rank_select_allone rs1(kRS1Cnt);
    rank_select_fewzero<typename RankSelect2::index_t> rs2(kRS2Cnt);
    rs2.build_from(rankselect2);
    return new CompositeUintIndex<rank_select_allone, rank_select_fewzero<typename RankSelect2::index_t>, DataCont>(
      rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kAllOne_FewOne: {
    rank_select_allone rs1(kRS1Cnt);
    rank_select_fewone<typename RankSelect2::index_t> rs2(kRS2Cnt);
    rs2.build_from(rankselect2);
    return new CompositeUintIndex<rank_select_allone, rank_select_fewone<typename RankSelect2::index_t>, DataCont>(
      rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kAllOne_Normal: {
    rank_select_allone rs1(kRS1Cnt);
    return new CompositeUintIndex<rank_select_allone, RankSelect2, DataCont>(
      rs1, rankselect2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
                        // -- few zero & ...
  case kFewZero_AllZero: {
    rank_select_fewzero<typename RankSelect1::index_t> rs1(kRS1Cnt);
    rs1.build_from(rankselect1);
    rank_select_allzero rs2(kRS2Cnt);
    return new CompositeUintIndex<rank_select_fewzero<typename RankSelect1::index_t>, rank_select_allzero, DataCont>(
      rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kFewZero_FewZero: {
    rank_select_fewzero<typename RankSelect1::index_t> rs1(kRS1Cnt);
    rs1.build_from(rankselect1);
    rank_select_fewzero<typename RankSelect2::index_t> rs2(kRS2Cnt);
    rs2.build_from(rankselect2);
    return new CompositeUintIndex<rank_select_fewzero<typename RankSelect1::index_t>,
      rank_select_fewzero<typename RankSelect2::index_t>, DataCont>(
        rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kFewZero_FewOne: {
    rank_select_fewzero<typename RankSelect1::index_t> rs1(kRS1Cnt);
    rs1.build_from(rankselect1);
    rank_select_fewone<typename RankSelect2::index_t> rs2(kRS2Cnt);
    rs2.build_from(rankselect2);
    return new CompositeUintIndex<rank_select_fewzero<typename RankSelect1::index_t>,
      rank_select_fewone<typename RankSelect2::index_t>, DataCont>(
        rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kFewZero_Normal: {
    rank_select_fewzero<typename RankSelect1::index_t> rs1(kRS1Cnt);
    rs1.build_from(rankselect1);
    return new CompositeUintIndex<rank_select_fewzero<typename RankSelect1::index_t>, RankSelect2, DataCont>(
      rs1, rankselect2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
                        // few one & ...
  case kFewOne_AllZero: {
    rank_select_fewone<typename RankSelect1::index_t> rs1(kRS1Cnt);
    rs1.build_from(rankselect1);
    rank_select_allzero rs2(kRS2Cnt);
    return new CompositeUintIndex<rank_select_fewone<typename RankSelect1::index_t>, rank_select_allzero, DataCont>(
      rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kFewOne_FewZero: {
    rank_select_fewone<typename RankSelect1::index_t> rs1(kRS1Cnt);
    rs1.build_from(rankselect1);
    rank_select_fewzero<typename RankSelect2::index_t> rs2(kRS2Cnt);
    rs2.build_from(rankselect2);
    return new CompositeUintIndex<rank_select_fewone<typename RankSelect1::index_t>,
      rank_select_fewzero<typename RankSelect2::index_t>, DataCont>(
        rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kFewOne_FewOne: {
    rank_select_fewone<typename RankSelect1::index_t> rs1(kRS1Cnt);
    rs1.build_from(rankselect1);
    rank_select_fewone<typename RankSelect2::index_t> rs2(kRS2Cnt);
    rs2.build_from(rankselect2);
    return new CompositeUintIndex<rank_select_fewone<typename RankSelect1::index_t>,
      rank_select_fewone<typename RankSelect2::index_t>, DataCont>(
        rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kFewOne_Normal: {
    rank_select_fewone<typename RankSelect1::index_t> rs1(kRS1Cnt);
    rs1.build_from(rankselect1);
    return new CompositeUintIndex<rank_select_fewone<typename RankSelect1::index_t>, RankSelect2, DataCont>(
      rs1, rankselect2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
                        // normal & ...
  case kNormal_AllZero: {
    rank_select_allzero rs2(kRS2Cnt);
    return new CompositeUintIndex<RankSelect1, rank_select_allzero, DataCont>(
      rankselect1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kNormal_FewZero: {
    rank_select_fewzero<typename RankSelect2::index_t> rs2(kRS2Cnt);
    rs2.build_from(rankselect2);
    return new CompositeUintIndex<RankSelect1, rank_select_fewzero<typename RankSelect2::index_t>, DataCont>(
      rankselect1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kNormal_FewOne: {
    rank_select_fewone<typename RankSelect2::index_t> rs2(kRS2Cnt);
    rs2.build_from(rankselect2);
    return new CompositeUintIndex<RankSelect1, rank_select_fewone<typename RankSelect2::index_t>, DataCont>(
      rankselect1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kNormal_Normal: {
    return new CompositeUintIndex<RankSelect1, RankSelect2, DataCont>(
      rankselect1, rankselect2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  default:
    assert(0);
    return nullptr;
  }
}

typedef NestLoudsTrieDAWG_IL_256 NestLoudsTrieDAWG_IL_256_32;
typedef NestLoudsTrieDAWG_SE_512 NestLoudsTrieDAWG_SE_512_32;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_IL_256_32> TrieDAWG_IL_256_32;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_SE_512_64> TrieDAWG_SE_512_64;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_IL_256> TrieDAWG_Mixed_IL_256;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_SE_512> TrieDAWG_Mixed_SE_512;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_XL_256> TrieDAWG_Mixed_XL_256;

typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_IL_256_32_FL> TrieDAWG_IL_256_32_FL;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_SE_512_64_FL> TrieDAWG_SE_512_64_FL;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_IL_256_32_FL> TrieDAWG_Mixed_IL_256_32_FL;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_SE_512_32_FL> TrieDAWG_Mixed_SE_512_32_FL;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_XL_256_32_FL> TrieDAWG_Mixed_XL_256_32_FL;

TerarkIndexRegisterImp(TrieDAWG_IL_256_32, TrieDAWG_IL_256_32::MyFactory, "NestLoudsTrieDAWG_IL", "IL_256_32", "IL_256", "NestLoudsTrieDAWG_IL_256");
TerarkIndexRegisterNLT(SE_512_64);
TerarkIndexRegisterNLT(Mixed_SE_512);
TerarkIndexRegisterNLT(Mixed_IL_256);
TerarkIndexRegisterNLT(Mixed_XL_256);

TerarkIndexRegisterNLT(IL_256_32_FL);
TerarkIndexRegisterNLT(SE_512_64_FL);
TerarkIndexRegisterNLT(Mixed_SE_512_32_FL);
TerarkIndexRegisterNLT(Mixed_IL_256_32_FL);
TerarkIndexRegisterNLT(Mixed_XL_256_32_FL);

typedef Min0DataCont BigUintDataCont;

typedef rank_select_allone     AllOne;
typedef rank_select_allzero    AllZero;
typedef rank_select_il_256_32  IL_256_32;
typedef rank_select_se_512_64  SE_512_64;
typedef rs_fewone_32           FewOne32;
typedef rs_fewone_64           FewOne64;
typedef rs_fewzero_32          FewZero32;
typedef rs_fewzero_64          FewZero64;

#define RegisterIndex TerarkIndexRegister
#define RegisterCompositeUintIndex4(rs1, rs2, Key2, _Key2)  \
typedef CompositeUintIndex <rs1, rs2, Key2##DataCont>       \
        CompositeUintIndex_##rs1##_##rs2##_Key2;            \
RegisterIndex(CompositeUintIndex_##rs1##_##rs2##_Key2)

#define RegisterCompositeUintIndex3(rs1, rs2, Key2)         \
RegisterCompositeUintIndex4(rs1, rs2, Key2, _##Key2)

#define RegisterCompositeUintIndex(rs1, rs2)                \
RegisterCompositeUintIndex4(rs1, rs2, Str,);                \
RegisterCompositeUintIndex3(rs1, rs2, SortedUint);          \
RegisterCompositeUintIndex3(rs1, rs2,    BigUint)

RegisterCompositeUintIndex(AllOne, AllZero);
RegisterCompositeUintIndex(AllOne, FewZero32);
RegisterCompositeUintIndex(AllOne, FewZero64);
RegisterCompositeUintIndex(AllOne, FewOne32);
RegisterCompositeUintIndex(AllOne, FewOne64);
RegisterCompositeUintIndex(AllOne, IL_256_32);
RegisterCompositeUintIndex(AllOne, SE_512_64);

RegisterCompositeUintIndex(FewOne32, AllZero);
RegisterCompositeUintIndex(FewOne64, AllZero);
RegisterCompositeUintIndex(FewOne32, FewZero32);
RegisterCompositeUintIndex(FewOne64, FewZero64);
RegisterCompositeUintIndex(FewOne32, FewOne32);
RegisterCompositeUintIndex(FewOne64, FewOne64);
RegisterCompositeUintIndex(FewOne32, IL_256_32);
RegisterCompositeUintIndex(FewOne64, SE_512_64);

RegisterCompositeUintIndex(FewZero32, AllZero);
RegisterCompositeUintIndex(FewZero64, AllZero);
RegisterCompositeUintIndex(FewZero32, FewZero32);
RegisterCompositeUintIndex(FewZero64, FewZero64);
RegisterCompositeUintIndex(FewZero32, FewOne32);
RegisterCompositeUintIndex(FewZero64, FewOne64);
RegisterCompositeUintIndex(FewZero32, IL_256_32);
RegisterCompositeUintIndex(FewZero64, SE_512_64);

RegisterCompositeUintIndex(IL_256_32, AllZero);
RegisterCompositeUintIndex(SE_512_64, AllZero);
RegisterCompositeUintIndex(IL_256_32, FewZero32);
RegisterCompositeUintIndex(SE_512_64, FewZero64);
RegisterCompositeUintIndex(IL_256_32, FewOne32);
RegisterCompositeUintIndex(SE_512_64, FewOne64);
RegisterCompositeUintIndex(IL_256_32, IL_256_32);
RegisterCompositeUintIndex(SE_512_64, SE_512_64);

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

#define RegisterUintIndex(RankSelect)       \
RegisterCompositeIndexWithFactory(          \
  CompositeIndexAscendingUintPrefix<RankSelect>, 0,  \
  CompositeIndexEmptySuffix, 0,             \
  UintIndex_##RankSelect,                   \
  UintIndexFactory<RankSelect>              \
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

unique_ptr<TerarkIndex> TerarkIndex::LoadMemory(fstring mem) {
  auto header = (const TerarkIndexHeader*)mem.data();
#if defined(TerocksPrivateCode)
  if (header->file_size < mem.size()) {
    auto dfa = loadAsLazyUnionDFA(mem, true);
    assert(dfa);
    return unique_ptr<TerarkIndex>(new NestLoudsTrieIndex<MatchingDFA>(dfa));
  }
#endif // TerocksPrivateCode
  size_t idx = g_TerarkIndexFactroy.find_i(header->class_name);
  if (idx >= g_TerarkIndexFactroy.end_i()) {
    throw std::invalid_argument(
      std::string("TerarkIndex::LoadMemory(): Unknown class: ")
      + header->class_name);
  }
  TerarkIndex::Factory* factory = g_TerarkIndexFactroy.val(idx).get();
  return factory->LoadMemory(mem);
}


RegisterCompositeIndex(CompositeIndexAscendingUintPrefix<IL_256_32>, 0, CompositeIndexEmptySuffix, 0, Unused00);
RegisterCompositeIndex(CompositeIndexAscendingUintPrefix<IL_256_32>, 0, CompositeIndexEmptySuffix, 1, Unused01);
RegisterCompositeIndex(CompositeIndexAscendingUintPrefix<IL_256_32>, 1, CompositeIndexEmptySuffix, 0, Unused02);
RegisterCompositeIndex(CompositeIndexAscendingUintPrefix<IL_256_32>, 1, CompositeIndexEmptySuffix, 1, Unused03);

RegisterCompositeIndex(CompositeIndexNonDescendingUintPrefix<rs_fewzero_32>, 0, CompositeIndexDynamicStrSuffix, 0, Unused10);
RegisterCompositeIndex(CompositeIndexNonDescendingUintPrefix<rs_fewzero_32>, 0, CompositeIndexDynamicStrSuffix, 1, Unused11);
RegisterCompositeIndex(CompositeIndexNonDescendingUintPrefix<rs_fewzero_32>, 1, CompositeIndexDynamicStrSuffix, 0, Unused12);
RegisterCompositeIndex(CompositeIndexNonDescendingUintPrefix<rs_fewzero_32>, 1, CompositeIndexDynamicStrSuffix, 1, Unused13);

RegisterCompositeIndex(CompositeIndexNestLoudsTriePrefix<NestLoudsTrieDAWG_Mixed_XL_256_32_FL>, 0, CompositeIndexEmptySuffix, 0, Unused20);
RegisterCompositeIndex(CompositeIndexNestLoudsTriePrefix<NestLoudsTrieDAWG_Mixed_XL_256_32_FL>, 0, CompositeIndexEmptySuffix, 1, Unused21);
RegisterCompositeIndex(CompositeIndexNestLoudsTriePrefix<NestLoudsTrieDAWG_Mixed_XL_256_32_FL>, 1, CompositeIndexEmptySuffix, 0, Unused22);
RegisterCompositeIndex(CompositeIndexNestLoudsTriePrefix<NestLoudsTrieDAWG_Mixed_XL_256_32_FL>, 1, CompositeIndexEmptySuffix, 1, Unused23);

RegisterCompositeIndex(CompositeIndexNestLoudsTriePrefix<MatchingDFA>, 0, CompositeIndexDynamicStrSuffix, 0, Unused30);
RegisterCompositeIndex(CompositeIndexNestLoudsTriePrefix<MatchingDFA>, 0, CompositeIndexDynamicStrSuffix, 1, Unused31);
RegisterCompositeIndex(CompositeIndexNestLoudsTriePrefix<MatchingDFA>, 1, CompositeIndexDynamicStrSuffix, 0, Unused32);
RegisterCompositeIndex(CompositeIndexNestLoudsTriePrefix<MatchingDFA>, 1, CompositeIndexDynamicStrSuffix, 1, Unused33);


} // namespace rocksdb
