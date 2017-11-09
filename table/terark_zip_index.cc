#include "terark_zip_index.h"
#include "terark_zip_table.h"
#include "terark_zip_common.h"
#include <terark/hash_strmap.hpp>
#include <terark/fsa/dfa_mmap_header.hpp>
#include <terark/fsa/fsa_cache.hpp>
#include <terark/fsa/nest_trie_dawg.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/num_to_str.hpp>
#if defined(TerocksPrivateCode)
#include <terark/fsa/fsa_for_union_dfa.hpp>
#endif // TerocksPrivateCode


namespace rocksdb {

using terark::initial_state;
using terark::BaseDFA;
using terark::NestLoudsTrieDAWG_SE_512;
using terark::NestLoudsTrieDAWG_SE_512_64;
using terark::NestLoudsTrieDAWG_IL_256;
using terark::NestLoudsTrieDAWG_Mixed_SE_512;
using terark::NestLoudsTrieDAWG_Mixed_IL_256;
using terark::NestLoudsTrieDAWG_Mixed_XL_256;

using terark::NestLoudsTrieDAWG_SE_512_32_FL;
using terark::NestLoudsTrieDAWG_SE_512_64_FL;
using terark::NestLoudsTrieDAWG_IL_256_32_FL;
using terark::NestLoudsTrieDAWG_Mixed_SE_512_32_FL;
using terark::NestLoudsTrieDAWG_Mixed_IL_256_32_FL;
using terark::NestLoudsTrieDAWG_Mixed_XL_256_32_FL;

using terark::SortedStrVec;
using terark::FixedLenStrVec;
using terark::MmapWholeFile;
using terark::UintVecMin0;
using terark::MatchingDFA;
using terark::commonPrefixLen;

static terark::hash_strmap<TerarkIndex::FactoryPtr> g_TerarkIndexFactroy;
static terark::hash_strmap<std::string>             g_TerarkIndexName;

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
  for (const char* name : names) {
    g_TerarkIndexFactroy.insert_i(name, FactoryPtr(factory));
    g_TerarkIndexName.insert_i(rtti_name, *names.begin());
  }
}

const TerarkIndex::Factory* TerarkIndex::GetFactory(fstring name) {
  size_t idx = g_TerarkIndexFactroy.find_i(name);
  if (idx < g_TerarkIndexFactroy.end_i()) {
    auto factory = g_TerarkIndexFactroy.val(idx).get();
    return factory;
  }
  return NULL;
}

bool TerarkIndex::SeekCostEffectiveIndexLen(const KeyStat& ks, size_t& ceLen) {
  assert(ks.minKey.size() > 8);
  /*
   * the length of index1,
   * 1. 1 byte => too many sub-indexes under each index1
   * 2. 8 byte => a lot more gaps compared with 1 byte index1
   * !!! more bytes with less gap is preferred, in other words,
   * prefer smaller gap-ratio, larger compression-ratio
   * best case is : w1 * (1 / gap-ratio) + w2 * compress-ratio 
   *   gap-ratio = (diff - numkeys) / diff,
   *   compress-ratio = compressed-part / original,
   *
   * to calculate space usage, assume 1000 keys, maxKeyLen = 16,
   *   original cost = 16,000 * 8                     = 128,000 bit
   *   8 bytes, 0.5 gap => 2000 + (16 - 8) * 8 * 1000 = 66,000
   *   2 bytes, 0.5 gap => 2000 + (16 - 2) * 8 * 1000 = 114,000
   */
  static const int maxLen = 8;
  static const double w1 = 0.1, w2 = 0.5,
    max_gap_ratio = 0.9, min_gap_ratio = 0.1;
  size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
  size_t originCost = ks.numKeys * ks.maxKeyLen * 8;
  double score = 0;
  ceLen = maxLen;
  for (int i = maxLen; i > 0; i--) {
    int offset = cplen, end = cplen + i;
    uint64_t
      minValue = ReadUint64(ks.minKey.begin() + offset,
                            ks.minKey.begin() + end),
      maxValue = ReadUint64(ks.maxKey.begin() + offset,
                            ks.maxKey.begin() + end);
    uint64_t diff = std::max(minValue, maxValue) - std::min(minValue, maxValue) + 1;
    // one index1st with a collection of index2nd, that's when diff < numkeys
    double gap_ratio = diff <= ks.numKeys ? min_gap_ratio : 
      (double)(diff - ks.numKeys) / diff;
    if (gap_ratio > max_gap_ratio)
      continue;
    gap_ratio = std::max(gap_ratio, min_gap_ratio);
    // diff is bitmap, * 1.2 is extra cost to build RankSelect
    uint64_t cost = diff * 1.2 + (ks.maxKeyLen - i) * ks.numKeys;
    if (diff == ks.numKeys) {
      cost -= (diff * 1.2);
    }
    if (cost > originCost * 0.8)
      continue;
    double compress_ratio = (originCost - cost) / originCost;
    double cur = w1 * (1 / gap_ratio) + w2 * compress_ratio;
    if (cur > score) {
      score = cur;
      ceLen = i;
    }
  }
  return score > 0;
}

const TerarkIndex::Factory*
TerarkIndex::SelectFactory(const KeyStat& ks, fstring name) {
  assert(ks.numKeys > 0);
  assert(!ks.minKey.empty() && !ks.maxKey.empty());
#if defined(TerocksPrivateCode)
  size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
  assert(cplen >= ks.commonPrefixLen);
  size_t ceLen = 0; // cost effective index1st len if any
  if (ks.maxKeyLen == ks.minKeyLen && ks.maxKeyLen - cplen <= sizeof(uint64_t)) {
    uint64_t
      minValue = ReadUint64(ks.minKey.begin() + cplen, ks.minKey.end()),
      maxValue = ReadUint64(ks.maxKey.begin() + cplen, ks.maxKey.end());
    uint64_t diff = (minValue < maxValue ? maxValue - minValue : minValue - maxValue) + 1;
    if (diff < ks.numKeys * 30) {
      if (diff == ks.numKeys) {
        return GetFactory("UintIndex_AllOne");
      }
      else if (diff < (4ull << 30)) {
        return GetFactory("UintIndex");
      }
      else {
        return GetFactory("UintIndex_SE_512_64");
      }
    }
  } else if (ks.maxKeyLen == ks.minKeyLen &&
             ks.maxKeyLen - cplen > sizeof(uint64_t) &&
             ks.maxKeyLen - cplen <= 16 && // !!! plain index2nd may occupy too much space
             SeekCostEffectiveIndexLen(ks, ceLen)) {
    uint64_t
      minValue = ReadUint64(ks.minKey.begin() + cplen,
                            ks.minKey.begin() + cplen + ceLen),
      maxValue = ReadUint64(ks.maxKey.begin() + cplen,
                            ks.maxKey.begin() + cplen + ceLen);
    uint64_t diff = std::max(minValue, maxValue) - std::min(minValue, maxValue) + 1;
    const char* facname = nullptr;
    if (ks.numKeys < (4ull << 30)) {
      facname = diff == ks.numKeys ? "CompositeIndex_AllOne_IL85" :
        "CompositeIndex_IL85_IL85";
    } else {
      facname = diff == ks.numKeys ? "CompositeIndex_AllOne_SE96" :
        "CompositeIndex_SE96_SE96";
    }
    //printf("Factory used: %s\n", facname);
    return GetFactory(facname);
  }
#endif // TerocksPrivateCode
  /*
   * 32bit, 2G nodes could store:
   * 1. 2 children,
   *    => 1 + 2**1 + 2**2 + ... + 2**30 == 2**31 => leaf cnt == 2**30, height = 31
   *    1G * 31 = 31G
   * 2. 64 children,
   *    => 1 + 2**6 + 2**12 + ... + 2**30 + N = 2**31 => 2**30 with h = 7, 2**30 with h == 6
   *    1G * 13 = 13G,
   * 3. every node has 256 children, 
   *    => 1 + 2**8 + 2**16 + 2**24 + N == 2**31 => N ~= 2**31 with h == 5,
   *    2G * 5 = 10G
   */
  if (ks.sumKeyLen - ks.numKeys * ks.commonPrefixLen > 0x1E0000000) { // 7.5G
    return GetFactory("SE_512_64");
  }
  return GetFactory(name);
}

TerarkIndex::~TerarkIndex() {}
TerarkIndex::Factory::~Factory() {}
TerarkIndex::Iterator::~Iterator() {}

class NestLoudsTrieIterBase : public TerarkIndex::Iterator {
protected:
  unique_ptr<terark::ADFA_LexIterator> m_iter;
  fstring key() const override {
	  return fstring(m_iter->word());
  }
  NestLoudsTrieIterBase(terark::ADFA_LexIterator* iter)
   : m_iter(iter) {}
};

template<class NLTrie>
class NestLoudsTrieIterBaseTpl : public NestLoudsTrieIterBase {
protected:
  using TerarkIndex::Iterator::m_id;
  NestLoudsTrieIterBaseTpl(const NLTrie* trie)
    : NestLoudsTrieIterBase(trie->adfa_make_iter(initial_state)) {
    m_dawg = trie;
  }
  const NLTrie* m_dawg;
  bool Done(bool ok) {
    if (ok)
      m_id = m_dawg->state_to_word_id(m_iter->word_state());
    else
      m_id = size_t(-1);
    return ok;
  }
};
template<>
class NestLoudsTrieIterBaseTpl<MatchingDFA> : public NestLoudsTrieIterBase {
protected:
  using TerarkIndex::Iterator::m_id;
  NestLoudsTrieIterBaseTpl(const MatchingDFA* dfa)
    : NestLoudsTrieIterBase(dfa->adfa_make_iter(initial_state)) {
    m_dawg = dfa->get_dawg();
  }
  const terark::BaseDAWG* m_dawg;
  bool Done(bool ok) {
    if (ok)
      m_id = m_dawg->v_state_to_word_id(m_iter->word_state());
    else
      m_id = size_t(-1);
    return ok;
  }
};

template<class NLTrie>
void NestLoudsTrieBuildCache(NLTrie* trie, double cacheRatio) {
  trie->build_fsa_cache(cacheRatio, NULL);
}
void NestLoudsTrieBuildCache(MatchingDFA* dfa, double cacheRatio) {
}


template<class NLTrie>
void NestLoudsTrieGetOrderMap(NLTrie* trie, UintVecMin0& newToOld) {
  terark::NonRecursiveDictionaryOrderToStateMapGenerator gen;
  gen(*trie, [&](size_t dictOrderOldId, size_t state) {
    size_t newId = trie->state_to_word_id(state);
    //assert(trie->state_to_dict_index(state) == dictOrderOldId);
    //assert(trie->dict_index_to_state(dictOrderOldId) == state);
    newToOld.set_wire(newId, dictOrderOldId);
  });
}
void NestLoudsTrieGetOrderMap(MatchingDFA* dfa, UintVecMin0& newToOld) {
  assert(0);
}


template<class NLTrie>
class NestLoudsTrieIndex : public TerarkIndex {
  const terark::BaseDAWG* m_dawg;
  unique_ptr<NLTrie> m_trie;
  class MyIterator : public NestLoudsTrieIterBaseTpl<NLTrie> {
  protected:
    using NestLoudsTrieIterBaseTpl<NLTrie>::m_dawg;
    using NestLoudsTrieIterBaseTpl<NLTrie>::Done;
    using NestLoudsTrieIterBase::m_iter;
    using TerarkIndex::Iterator::m_id;
  public:
    explicit MyIterator(NLTrie* trie)
      : NestLoudsTrieIterBaseTpl<NLTrie>(trie)
    {}
    bool SeekToFirst() override { return Done(m_iter->seek_begin()); }
    bool SeekToLast()  override { return Done(m_iter->seek_end()); }
    bool Seek(fstring key) override { return Done(m_iter->seek_lower_bound(key)); }
    bool Next() override { return Done(m_iter->incr()); }
    bool Prev() override { return Done(m_iter->decr()); }
    size_t DictRank() const override {
      assert(m_id != size_t(-1));
#if defined(TerocksPrivateCode)
      return m_dawg->state_to_dict_rank(m_iter->word_state());
#endif // TerocksPrivateCode
      return m_id;
    }
  };
public:
  NestLoudsTrieIndex(NLTrie* trie) : m_trie(trie) {
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
    MY_THREAD_LOCAL(terark::MatchContext, ctx);
    ctx.root = 0;
    ctx.pos = 0;
    ctx.zidx = 0;
    ctx.zbuf_state = size_t(-1);
    return m_dawg->index(ctx, key);
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
  Iterator* NewIterator() const override final {
    return new MyIterator(m_trie.get());
  }
  bool NeedsReorder() const override final { return true; }
  void GetOrderMap(UintVecMin0& newToOld)
  const override final {
    NestLoudsTrieGetOrderMap(m_trie.get(), newToOld);
  }
  void BuildCache(double cacheRatio) {
    if (cacheRatio > 1e-8) {
      NestLoudsTrieBuildCache(m_trie.get(), cacheRatio);
    }
  }
  class MyFactory : public Factory {
  public:
    TerarkIndex* Build(NativeDataInput<InputBuffer>& reader,
                       const TerarkZipTableOptions& tzopt,
                       const KeyStat& ks) const override {
      size_t numKeys = ks.numKeys;
      size_t commonPrefixLen = ks.commonPrefixLen;
      size_t sumPrefixLen = commonPrefixLen * numKeys;
      size_t sumRealKeyLen = ks.sumKeyLen - sumPrefixLen;
      valvec<byte_t> keyBuf;
      if (ks.minKeyLen != ks.maxKeyLen) {
        SortedStrVec keyVec;
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
        return BuildImpl(tzopt, keyVec);
      }
      else {
        size_t fixlen = ks.minKeyLen - commonPrefixLen;
        FixedLenStrVec keyVec(fixlen);
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
        return BuildImpl(tzopt, keyVec);
      }
    }
  private:
    template<class StrVec>
    TerarkIndex* BuildImpl(const TerarkZipTableOptions& tzopt,
                           StrVec& keyVec) const {
#if !defined(NDEBUG)
      for (size_t i = 1; i < keyVec.size(); ++i) {
        fstring prev = keyVec[i - 1];
        fstring curr = keyVec[i];
        assert(prev < curr);
      }
      //    backupKeys = keyVec;
#endif
      terark::NestLoudsTrieConfig conf;
      conf.nestLevel = tzopt.indexNestLevel;
      conf.nestScale = tzopt.indexNestScale;
      if (tzopt.indexTempLevel >= 0 && tzopt.indexTempLevel < 5) {
        if (keyVec.mem_size() > tzopt.smallTaskMemory) {
          // use tmp files during index building
          conf.tmpDir = tzopt.localTempDir;
          if (0 == tzopt.indexTempLevel) {
            // adjust tmpLevel for linkVec, wihch is proportional to num of keys
            double avglen = keyVec.avg_size();
            if (keyVec.mem_size() > tzopt.smallTaskMemory*2 && avglen <= 50) {
              // not need any mem in BFS, instead 8G file of 4G mem (linkVec)
              // this reduce 10% peak mem when avg keylen is 24 bytes
              if (avglen <= 30) {
                // write str data(each len+data) of nestStrVec to tmpfile
                conf.tmpLevel = 4;
              } else {
                // write offset+len of nestStrVec to tmpfile
                // which offset is ref to outer StrVec's data
                conf.tmpLevel = 3;
              }
            }
            else if (keyVec.mem_size() > tzopt.smallTaskMemory*3/2) {
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
      unique_ptr<TerarkIndex> index(new NestLoudsTrieIndex(trie));
      dfa.release();
      return std::move(index);
    }
    unique_ptr<TerarkIndex> LoadFile(fstring fpath) const override {
      unique_ptr<BaseDFA> dfa(BaseDFA::load_mmap(fpath));
      auto trie = dynamic_cast<NLTrie*>(dfa.get());
      if (NULL == trie) {
        throw std::invalid_argument(
            "File: " + fpath + ", Bad trie class: " + ClassName(*dfa)
            + ", should be " + ClassName<NLTrie>());
      }
      unique_ptr<TerarkIndex> index(new NestLoudsTrieIndex(trie));
      dfa.release();
      return std::move(index);
    }
    size_t MemSizeForBuild(const KeyStat& ks) const override {
      size_t sumRealKeyLen = ks.sumKeyLen - ks.commonPrefixLen * ks.numKeys;
      if (ks.minKeyLen == ks.maxKeyLen) {
        return sumRealKeyLen;
      }
      size_t indexSize = UintVecMin0::compute_mem_size_by_max_val(ks.numKeys + 1, sumRealKeyLen);
      return indexSize + sumRealKeyLen;
    }
  };
};


#if defined(TerocksPrivateCode)

/*
 * special impl for all one/zero UintIndex
 */
class rank_select_allone : boost::noncopyable {
public:
  rank_select_allone() : m_size(-1), m_placeholder(nullptr) {}
  rank_select_allone(size_t sz) : m_size(sz), m_placeholder(nullptr) {}
  ~rank_select_allone() = default;

  void resize(size_t newsize) { m_size = newsize; }
  void set1(size_t i) { assert(i < m_size); }
  size_t select1(size_t i) const { return i; }
  void build_cache(bool, bool) {};
  void swap(rank_select_allone& another) {
    std::swap(m_size, another.m_size);
    std::swap(m_placeholder, another.m_placeholder);
  }

  void risk_release_ownership() {}
  void risk_mmap_from(unsigned char* base, size_t length) {
    assert(base != nullptr);
    assert(length == sizeof(*this));
    m_size = *((size_t*)base);
  }

  const void* data() const { return this; }
  bool operator[](long n) const { // alias of 'is1'
    assert(n >= 0 && (size_t)n < m_size);
    return true;
  }

  size_t mem_size() const { return sizeof(*this); }
  size_t max_rank1() const { return m_size; }
  size_t size() const { return m_size; }
  size_t rank1(size_t bitpos) const { return bitpos; }

  ///@returns number of continuous one/zero bits starts at bitpos
  size_t zero_seq_len(size_t bitpos) const { return 0; }
  size_t zero_seq_revlen(size_t endpos) const { return 0; }

private:
  size_t m_size;
  unsigned char* m_placeholder;
};

class rank_select_allzero : boost::noncopyable {
public:
  rank_select_allzero() : m_size(-1), m_placeholder(nullptr) {}
  rank_select_allzero(size_t sz) : m_size(sz), m_placeholder(nullptr) {}
  ~rank_select_allzero() = default;

  void clear() {}
  void resize(size_t newsize) { m_size = newsize; }
  void set0(size_t i) { assert(i < m_size); }
  void set1(size_t i) { assert(i < m_size); }
  size_t select0(size_t i) const { return i; }
  void build_cache(bool, bool) {};
  void swap(rank_select_allzero& another) {
    std::swap(m_size, another.m_size);
    std::swap(m_placeholder, another.m_placeholder);
  }

  void risk_release_ownership() {}
  void risk_mmap_from(unsigned char* base, size_t length) {
    assert(base != nullptr);
    assert(length == sizeof(*this));
    m_size = *((size_t*)base);
  }

  const void* data() const { return this; }
  bool operator[](long n) const { // alias of 'is1'
    assert(n >= 0 && (size_t)n < m_size);
    return false;
  }

  size_t mem_size() const { return sizeof(*this); }
  size_t max_rank0() const { return m_size; }
  size_t max_rank1() const { return 0; }
  size_t size() const { return m_size; }
  size_t rank0(size_t bitpos) const { return bitpos; }

  ///@returns number of continuous one/zero bits starts at bitpos
  size_t one_seq_len(size_t bitpos) const { return 0; }
  size_t one_seq_revlen(size_t endpos) const { return 0; }

private:
  size_t m_size;
  unsigned char* m_placeholder;
};

/*
 * For simplicity, let's take composite index => index1:index2. Then following 
 * compositeindexes like,
 *   4:6, 4:7, 4:8, 7:19, 7:20, 8:3
 * index1: {4, 7}, use bitmap (also UintIndex's form) to respresent
 *   4(1), 5(0), 6(0), 7(1), 8(1)
 * index1:index2: use bitmap to respresent 4:6(0), 4:7(1), 4:8(1), 7:19(0), 7:20(1), 8:3(0)
 * to search 7:20, use bitmap1 to rank1(7) = 2, then use bitmap2 to select0(2) = position-3,
 * use bitmap2 to select0(3) = position-5. That is, we should search within [3, 5). 
 * iter [3 to 5), 7:20 is found, done.
 */
template<class RankSelect1st, class RankSelect2nd>
class TerarkCompositeIndex : public TerarkIndex {
public:
  //static const int kCommonPrefixLen = 4;
  static const char* index_name;
  struct FileHeader : public TerarkIndexHeader {
    uint64_t min_value;
    uint64_t max_value;
    uint64_t index_1st_mem_size;
    uint64_t index_2nd_mem_size;
    uint64_t index_data_mem_size;
    /*
     * per key length = common_prefix_len + index_1st_len + index_2nd_len
     */
    uint32_t index_1st_len;
    uint32_t index_2nd_len;
    /*
     * (Rocksdb) For one huge index, we'll split it into multipart-index for the sake of RAM, 
     * and each sub-index could have longer commonPrefix compared with ks.commonPrefix.
     * what's more, under such circumstances, ks.commonPrefix may have been rewritten
     * be upper-level builder to '0'. here, 
     * common_prefix_length = sub-index.commonPrefixLen - whole-index.commonPrefixLen
     */
    uint32_t common_prefix_length;

    FileHeader(size_t body_size, fstring c_name = fstring()) {
      memset(this, 0, sizeof *this);
      magic_len = strlen(index_name);
      strncpy(magic, index_name, sizeof magic);
      size_t name_i;
      if (c_name.empty()) {
        name_i = g_TerarkIndexName.find_i(
          typeid(TerarkCompositeIndex<RankSelect1st, RankSelect2nd>).name());
      } else {
        name_i = g_TerarkIndexName.find_i(c_name);
      }
      assert(name_i < g_TerarkIndexFactroy.end_i());
      strncpy(class_name, g_TerarkIndexName.val(name_i).c_str(), sizeof class_name);
      header_size = sizeof *this;
      version = 1;

      file_size = sizeof *this + body_size;
    }
  };

public:
  class TerarkCompositeIndexIterator : public TerarkIndex::Iterator {
  public:
    TerarkCompositeIndexIterator(const TerarkCompositeIndex& index) : index_(index) {
      m_id = size_t(-1);
      buffer_.resize_no_init(index_.commonPrefix_.size() +
        index_.index1stLen_ + index_.index2ndLen_);
      memcpy(buffer_.data(), index_.commonPrefix_.data(), index_.commonPrefix_.size());
    }
    virtual ~TerarkCompositeIndexIterator() {}
      
    bool SeekToFirst() override {
      offset_1st_ = index_.index1stRS_.select1(0);
      assert(offset_1st_ != size_t(-1));
      m_id = 0;
      UpdateBuffer();
      return true;
    }
    bool SeekToLast() override {
      /*
       * max_rank1() is size,
       * max_rank1() - 1 is last position
       */
      assert(index_.index1stRS_.max_rank1() > 0);
      size_t last = index_.index1stRS_.max_rank1() - 1;
      offset_1st_ = index_.index1stRS_.select1(last);
      assert(offset_1st_ != size_t(-1));
      m_id = index_.index2ndRS_.size() - 1;
      UpdateBuffer();
      return true;
    }
    bool Seek(fstring target) override {
      size_t cplen = target.commonPrefixLen(index_.commonPrefix_);
      if (cplen != index_.commonPrefix_.size()) {
        assert(target.size() >= cplen);
        assert(target.size() == cplen || target[cplen] != index_.commonPrefix_[cplen]);
        if (target.size() == cplen ||
            byte_t(target[cplen]) < byte_t(index_.commonPrefix_[cplen])) {
          return SeekToFirst();
        } else {
          m_id = size_t(-1);
          return false;
        }
      }
      uint64_t index1st = 0;
      fstring index2nd;
      if (target.size() <= cplen + index_.index1stLen_) {
        fstring sub = target.substr(index_.commonPrefix_.size());
        byte_t targetBuffer[8] = { 0 };
        memcpy(targetBuffer + (8 - sub.size()),
               sub.data(), sub.size());
        index1st = Read1stIndex(fstring(targetBuffer, targetBuffer + 8), 
                                0, 8);
      } else {
        index1st = Read1stIndex(target, cplen, index_.index1stLen_);
        index2nd = target.substr(cplen + index_.index1stLen_);
      }
      if (index1st > index_.maxValue_) {
        m_id = size_t(-1);
        return false;
      } else if (index1st < index_.minValue_) {
        return SeekToFirst();
      }
      
      // find the corresponding bit within 2ndRS
      offset_1st_ = index1st - index_.minValue_;
      if (index_.index1stRS_[offset_1st_]) {
        // find within this index
        uint64_t
          order = index_.index1stRS_.rank1(offset_1st_),
          pos0 = index_.index2ndRS_.select0(order);
        if (pos0 == index_.index2ndRS_.size() - 1) { // last elem
          m_id = (index2nd <= index_.indexData_[pos0]) ? pos0 : size_t(-1);
        } else {
          uint64_t cnt = index_.index2ndRS_.one_seq_len(pos0 + 1) + 1;
          m_id = index_.Locate(index_.indexData_, pos0, cnt, index2nd);
          if (m_id == size_t(-1) && pos0 + cnt < index_.indexData_.size()) {
            // try next offset
            m_id = pos0 + cnt;
          }
        }
      } else {
        // no such index, use the lower_bound form
        uint64_t cnt = index_.index1stRS_.zero_seq_len(offset_1st_);
        if (offset_1st_ + cnt >= index_.index1stRS_.size()) {
          m_id = size_t(-1);
          return false;
        }
        offset_1st_ += cnt;
        uint64_t order = index_.index1stRS_.rank1(offset_1st_);
        m_id = index_.index2ndRS_.select0(order);
      }
      if (m_id == size_t(-1)) {
        return false;
      }
      UpdateBuffer();
      return true;
    }

    bool Next() override {
      assert(m_id != size_t(-1));
      assert(index_.index1stRS_[offset_1st_] != 0);
      if (m_id + 1 == index_.index2ndRS_.size()) {
        m_id = size_t(-1);
        return false;
      } else {
        if (IsIndexDiff(m_id, m_id + 1)) {
          assert(offset_1st_ + 1 <  index_.index1stRS_.size());
          uint64_t cnt = index_.index1stRS_.zero_seq_len(offset_1st_ + 1);
          offset_1st_ += cnt + 1;
          assert(offset_1st_ < index_.index1stRS_.size());
        }
        ++m_id;
        UpdateBuffer();
        return true;
      }
    }
    bool Prev() override {
      assert(m_id != size_t(-1));
      assert(index_.index1stRS_[offset_1st_] != 0);
      if (m_id == 0) {
        m_id = size_t(-1);
        return false;
      } else {
        if (IsIndexDiff(m_id - 1, m_id)) {
          /*
           * zero_seq_ has [a, b) range, hence next() need (pos_ + 1), whereas
           * prev() just called with (pos_) is enough
           * case1: 1 0 1, ... 
           * case2: 1 1, ...
           */
          assert(offset_1st_ > 0);
          uint64_t cnt = index_.index1stRS_.zero_seq_revlen(offset_1st_);
          assert(offset_1st_ >= cnt + 1);
          offset_1st_ -= (cnt + 1);
        }
        --m_id;
        UpdateBuffer();
        return true;
      }
    }

    size_t DictRank() const override {
      assert(m_id != size_t(-1));
      return m_id;
    }
    fstring key() const override {
      assert(m_id != size_t(-1));
      return buffer_;
    }

  protected:
    //use 2nd index bitmap to check if 1st index changed
    bool IsIndexDiff(size_t lid, size_t rid) {
      /*
       * case1: 0 1 1 0, ...
       * case2: 0 0, ...
       *   the 2nd 0 means another index started
       */
      int li = (int)index_.index2ndRS_[lid],
        ri = (int)index_.index2ndRS_[rid];
      return (li > ri ||
              li + ri == 0);
    }
    void UpdateBuffer() {
      // key = commonprefix + index1st + index2nd
      // assign index 1st
      size_t offset = index_.commonPrefix_.size();
      AssignUint64(buffer_.data() + offset,
                   buffer_.data() + offset + index_.index1stLen_,
                   offset_1st_ + index_.minValue_);
      // assign index 2nd
      offset += index_.index1stLen_;
      fstring data = index_.indexData_[m_id];
      memcpy(buffer_.data() + offset,
             data.data(), data.size());
    }

    size_t offset_1st_; // used to track & decode index1 value
    valvec<byte_t> buffer_;
    const TerarkCompositeIndex& index_;
  };
    
public:
  class MyFactory : public TerarkIndex::Factory {
  public:
    /*
     * no option.keyPrefixLen
     */
    TerarkIndex* Build(NativeDataInput<InputBuffer>& reader,
                       const TerarkZipTableOptions& tzopt,
                       const KeyStat& ks) const override {
      size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
      assert(cplen >= ks.commonPrefixLen);
      if (ks.maxKeyLen != ks.minKeyLen ||
          ks.maxKeyLen - cplen <= sizeof(uint64_t)) {
        printf("diff ken len, maxlen %zu, minlen %zu\n", ks.maxKeyLen, ks.minKeyLen);
        abort();
      }

      size_t index1stLen = 0;
      assert(SeekCostEffectiveIndexLen(ks, index1stLen));
      uint64_t
        minValue = Read1stIndex(ks.minKey, cplen, index1stLen),
        maxValue = Read1stIndex(ks.maxKey, cplen, index1stLen);
      /*
       * if rocksdb reverse comparator is used, then minValue
       * is actually the largetst one
       */
      if (minValue > maxValue) {
        std::swap(minValue, maxValue);
      }
      uint64_t cnt = maxValue - minValue + 1;
      /*
       * 1stRS stores bitmap [minvalue, maxvalue] for index1st
       * 2ndRS stores bitmap [keystart, keyend] for composite index
       * order of rs2 follows the order of index data, and order 
       * of rs1 follows the order of rs2
       */
      RankSelect1st index1stRS(cnt);
      RankSelect2nd index2ndRS(ks.numKeys);
      valvec<byte_t> keyBuf;
      uint64_t prev = size_t(-1);
      size_t index2ndLen = ks.maxKeyLen - cplen - index1stLen,
        sumRealKeyLen = index2ndLen * ks.numKeys;
      FixedLenStrVec keyVec(index2ndLen);
      keyVec.reserve(ks.numKeys, sumRealKeyLen);
      if (ks.minKey < ks.maxKey) {
        for (size_t i = 0; i < ks.numKeys; ++i) {
          reader >> keyBuf;
          uint64_t offset = Read1stIndex(keyBuf, cplen, index1stLen) - minValue;
          index1stRS.set1(offset);
          if (offset != prev) { // new index1 encountered
            index2ndRS.set0(i);
          } else {
            index2ndRS.set1(i);
          }
          prev = offset;
          keyVec.push_back(fstring(keyBuf).substr(cplen + index1stLen));
        }
      } else {
        size_t pos = sumRealKeyLen;
        keyVec.m_size = ks.numKeys;
        keyVec.m_strpool.resize(sumRealKeyLen);
        // compare with '0', do NOT use size_t
        for (long i = ks.numKeys - 1; i >= 0; --i) {
          reader >> keyBuf;
          uint64_t offset = Read1stIndex(keyBuf, cplen, index1stLen) - minValue;
          index1stRS.set1(offset);
          if (i < (long)ks.numKeys - 1) {
            if (offset != prev) { // next index1 is new one
              index2ndRS.set0(i + 1);
            } else {
              index2ndRS.set1(i + 1);
            }
          }
          prev = offset;
          // save index data
          fstring str = fstring(keyBuf).substr(cplen + index1stLen);
          pos -= str.size();
          memcpy(keyVec.m_strpool.data() + pos, str.data(), str.size());
        }
        index2ndRS.set0(0); // set 1st element to 0
        assert(pos == 0);
      }
      //
      std::string c_name = BuildAndCheckRS(index1stRS, index2ndRS);
      // construct index, set meta, index, data
      unique_ptr< TerarkCompositeIndex<RankSelect1st, RankSelect2nd> > ptr(
        new TerarkCompositeIndex<RankSelect1st, RankSelect2nd>());
      ptr->isUserMemory_ = false;
      ptr->isBuilding_ = true;
      {
        // save meta into header
        FileHeader *header = new FileHeader(
          index1stRS.mem_size() + index2ndRS.mem_size() + keyVec.mem_size(),
          c_name);
        header->min_value = minValue;
        header->max_value = maxValue;
        header->index_1st_mem_size = index1stRS.mem_size();
        header->index_2nd_mem_size = index2ndRS.mem_size();
        header->index_data_mem_size = keyVec.mem_size();
        header->index_1st_len = index1stLen;
        header->index_2nd_len = ks.minKeyLen - cplen - index1stLen;
        if (cplen > ks.commonPrefixLen) {
          // upper layer didn't handle common prefix, we'll do it
          // ourselves. actually here ks.commonPrefixLen == 0
          header->common_prefix_length = cplen - ks.commonPrefixLen;
          ptr->commonPrefix_.assign(ks.minKey.data() + ks.commonPrefixLen,
                                    header->common_prefix_length);
          header->file_size += terark::align_up(header->common_prefix_length, 8);
        }
        ptr->header_ = header;
      }
      ptr->index1stRS_.swap(index1stRS);
      ptr->index2ndRS_.swap(index2ndRS);
      ptr->indexData_.swap(keyVec);
      return ptr.release();
    }
    unique_ptr<TerarkIndex> LoadMemory(fstring mem) const override {
      return unique_ptr<TerarkIndex>(loadImpl(mem, {}).release());
    }
    unique_ptr<TerarkIndex> LoadFile(fstring fpath) const override {
      return unique_ptr<TerarkIndex>(loadImpl({}, fpath).release());
    }
    size_t MemSizeForBuild(const KeyStat& ks) const override {
      assert(ks.minKeyLen == ks.maxKeyLen);
      size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
      size_t index1stLen = 0;
      assert(SeekCostEffectiveIndexLen(ks, index1stLen));
      uint64_t
        minValue = Read1stIndex(ks.minKey, cplen, index1stLen),
        maxValue = Read1stIndex(ks.maxKey, cplen, index1stLen);
      if (minValue > maxValue) {
        std::swap(minValue, maxValue);
      }
      uint64_t diff = maxValue - minValue + 1;
      size_t index1stsz = size_t(std::ceil(diff * 1.25 / 8));
      size_t index2ndsz = size_t(std::ceil(ks.numKeys * 1.25 / 8));
      size_t indexDatasz = 
        size_t(std::ceil(ks.numKeys * (ks.minKey.size() - cplen - index1stLen)));
      return index1stsz + index2ndsz + indexDatasz;
    }
  protected:
    unique_ptr<TerarkCompositeIndex<RankSelect1st, RankSelect2nd>>
      loadImpl(fstring mem, fstring fpath) const {
      unique_ptr<TerarkCompositeIndex<RankSelect1st, RankSelect2nd>>
        ptr(new TerarkCompositeIndex<RankSelect1st, RankSelect2nd>());
      ptr->isUserMemory_ = false;
      ptr->isBuilding_ = false;
      if (mem.data() == nullptr) {
        MmapWholeFile(fpath).swap(ptr->file_);
        mem = {(const char*)ptr->file_.base, (ptrdiff_t)ptr->file_.size};
      } else {
        ptr->isUserMemory_ = true;
      }
      // make sure header is valid
      verifyHeader(mem);
      // construct composite index
      const FileHeader* header = (const FileHeader*)mem.data();
      ptr->header_ = header;
      ptr->minValue_ = header->min_value;
      ptr->maxValue_ = header->max_value;
      ptr->index1stLen_ = header->index_1st_len;
      ptr->index2ndLen_ = header->index_2nd_len;
      if (header->common_prefix_length > 0) {
        ptr->commonPrefix_.risk_set_data((char*)mem.data() + header->header_size,
                                         header->common_prefix_length);
      }
      size_t offset = header->header_size +
        terark::align_up(header->common_prefix_length, 8);
      ptr->index1stRS_.risk_mmap_from((unsigned char*)mem.data() + offset,
                                      header->index_1st_mem_size);
      offset += header->index_1st_mem_size;
      if (header->index_2nd_mem_size > 0) {
        ptr->index2ndRS_.risk_mmap_from((unsigned char*)mem.data() + offset,
                                      header->index_2nd_mem_size);
        offset += header->index_2nd_mem_size;
      } else { // all zero
        ptr->index2ndRS_.resize(ptr->index1stRS_.max_rank1());
      }
      ptr->indexData_.m_strpool.risk_set_data((unsigned char*)mem.data() + offset,
                                              header->index_data_mem_size);
      ptr->indexData_.m_fixlen = header->index_2nd_len;
      ptr->indexData_.m_size = header->index_data_mem_size / header->index_2nd_len;
      return ptr;
    }
    bool verifyHeader(fstring mem) const {
      const FileHeader* header = (const FileHeader*)mem.data();
      if (mem.size() < sizeof(FileHeader)
          || header->magic_len != strlen(index_name)
          || strcmp(header->magic, index_name) != 0
          || header->header_size != sizeof(FileHeader)
          || header->version != 1
          || header->file_size != mem.size()
        ) {
        throw std::invalid_argument("TerarkCompositeIndex::Load(): Bad file header");
      }
      //printf("\ntypename is: %s\n", typeid(TerarkCompositeIndex<RankSelect1st, RankSelect2nd>).name());
      auto verifyClassName = [&] {
        size_t name_i = g_TerarkIndexName.find_i(
          typeid(TerarkCompositeIndex<RankSelect1st, RankSelect2nd>).name());
        size_t self_i = g_TerarkIndexFactroy.find_i(g_TerarkIndexName.val(name_i));
        assert(self_i < g_TerarkIndexFactroy.end_i());
        size_t head_i = g_TerarkIndexFactroy.find_i(header->class_name);
        return head_i < g_TerarkIndexFactroy.end_i() &&
                        g_TerarkIndexFactroy.val(head_i) == g_TerarkIndexFactroy.val(self_i);
      };
      assert(verifyClassName()), (void)verifyClassName;
      return true;
    }
    /*
     * TBD: overwrite the given indexRS type with our designated one
     */
    std::string BuildAndCheckRS(RankSelect1st& index1stRS, RankSelect2nd& index2ndRS) const {
      index1stRS.build_cache(false, false);
      index2ndRS.build_cache(false, false);
      if (index2ndRS.max_rank1() != 0) {
        return string();
      } else {
        index2ndRS.clear();
        return std::string(typeid(TerarkCompositeIndex<RankSelect1st, rank_select_allzero>).name());
      }
    }
  };

public:    
  using TerarkIndex::FactoryPtr;
  virtual ~TerarkCompositeIndex() {
    if (isBuilding_) {
      delete (FileHeader*)header_;
    } else if (file_.base != nullptr || isUserMemory_) {
      index1stRS_.risk_release_ownership();
      index2ndRS_.risk_release_ownership();
      indexData_.m_strpool.risk_release_ownership();
      commonPrefix_.risk_release_ownership();
    }
  }
  const char* Name() const override {
    return header_->class_name;
  }
  void SaveMmap(std::function<void(const void *, size_t)> write) const override {
    write(header_, sizeof *header_);
    if (!commonPrefix_.empty()) {
      write(commonPrefix_.data(), terark::align_up(commonPrefix_.size(), 8));
    }
    write(index1stRS_.data(), index1stRS_.mem_size());
    if (index2ndRS_.mem_size() > 0) {
      write(index2ndRS_.data(), index2ndRS_.mem_size());
    }
    write(indexData_.m_strpool.data(), indexData_.mem_size());
  }
  size_t Find(fstring key) const override {
    size_t cplen = commonPrefix_.size();
    if (key.size() != index2ndLen_ + index1stLen_ + cplen ||
        key.commonPrefixLen(commonPrefix_) != cplen) {
      return size_t(-1);
    }
    uint64_t index1st = Read1stIndex(key, cplen, index1stLen_);
    if (index1st < minValue_ || index1st > maxValue_) {
      return size_t(-1);
    }
    uint64_t offset = index1st - minValue_;
    if (!index1stRS_[offset]) {
      return size_t(-1);
    }
    uint64_t
      order = index1stRS_.rank1(offset),
      pos0 = index2ndRS_.select0(order);
    assert(pos0 != size_t(-1));
    size_t cnt = index2ndRS_.one_seq_len(pos0 + 1);
    fstring index2nd = key.substr(cplen + index1stLen_);
    size_t id = Locate(indexData_, pos0, cnt + 1, index2nd);
    if (id != size_t(-1) && index2nd == indexData_[id]) {
      return id;
    }
    return size_t(-1);
  }
  size_t NumKeys() const override {
    return indexData_.size();
  }
  size_t TotalKeySize() const override final {
    return (commonPrefix_.size() + index1stLen_ + index2ndLen_) * indexData_.size();
  }
  fstring Memory() const override {
    return fstring((const char*)header_, (ptrdiff_t)header_->file_size);
  }
  Iterator* NewIterator() const override {
    return new TerarkCompositeIndexIterator(*this);
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

public:
  // handlers
  static uint64_t Read1stIndex(valvec<byte_t>& key, size_t cplen, size_t index1stLen) {
    return ReadUint64(key.begin() + cplen,
                      key.begin() + cplen + index1stLen);
  }
  static uint64_t Read1stIndex(fstring key, size_t cplen, size_t index1stLen) {
    return ReadUint64((const byte_t*)key.data() + cplen,
                      (const byte_t*)key.data() + cplen + index1stLen);
  }
  size_t Locate(const FixedLenStrVec& arr,
                size_t start, size_t cnt, fstring target) const {
    size_t lo = start, hi = start + cnt;
    size_t pos = arr.lower_bound(lo, hi, target);
    return (pos < hi) ? pos : size_t(-1);
  }

protected:
  const FileHeader* header_;
  MmapWholeFile     file_;
  valvec<char>      commonPrefix_;
  RankSelect1st     index1stRS_;
  RankSelect2nd     index2ndRS_;
  FixedLenStrVec    indexData_;
  uint64_t          minValue_;
  uint64_t          maxValue_;
  uint32_t          index1stLen_;
  uint32_t          index2ndLen_;
  bool              isUserMemory_;
  bool              isBuilding_;
};
template<class RankSelect1st, class RankSelect2nd>
const char* TerarkCompositeIndex<RankSelect1st, RankSelect2nd>::index_name = "CompositeIndex";

template<class RankSelect>
class TerarkUintIndex : public TerarkIndex {
public:
  static const char* index_name;
  struct FileHeader : public TerarkIndexHeader
  {
    uint64_t min_value;
    uint64_t max_value;
    uint64_t index_mem_size;
    uint32_t key_length;
    /*
     * (Rocksdb) For one huge index, we'll split it into multipart-index for the sake of RAM,
     * and each sub-index could have longer commonPrefix compared with ks.commonPrefix.
     * what's more, under such circumstances, ks.commonPrefix may have been rewritten
     * by upper-level builder to '0'. here,
     * common_prefix_length = sub-index.commonPrefixLen - whole-index.commonPrefixLen
     */
    uint32_t common_prefix_length;

    FileHeader(size_t body_size) {
      memset(this, 0, sizeof *this);
      magic_len = strlen(index_name);
      strncpy(magic, index_name, sizeof magic);
      size_t name_i = g_TerarkIndexName.find_i(typeid(TerarkUintIndex<RankSelect>).name());
      strncpy(class_name, g_TerarkIndexName.val(name_i).c_str(), sizeof class_name);

      header_size = sizeof *this;
      version = 1;

      file_size = sizeof *this + body_size;
    }
  };

  class UIntIndexIterator : public TerarkIndex::Iterator {
  public:
    UIntIndexIterator(const TerarkUintIndex& index) : index_(index) {
      pos_ = size_t(-1);
      buffer_.resize_no_init(index_.commonPrefix_.size() + index_.keyLength_);
      memcpy(buffer_.data(), index_.commonPrefix_.data(), index_.commonPrefix_.size());
    }
    virtual ~UIntIndexIterator() {}

    bool SeekToFirst() override {
      m_id = 0;
      pos_ = 0;
      UpdateBuffer();
      return true;
    }
    bool SeekToLast() override {
      m_id = index_.indexSeq_.max_rank1() - 1;
      pos_ = index_.indexSeq_.size() - 1;
      UpdateBuffer();
      return true;
    }
    bool Seek(fstring target) override {
      size_t cplen = target.commonPrefixLen(index_.commonPrefix_);
      if (cplen != index_.commonPrefix_.size()) {
        assert(target.size() >= cplen);
        assert(target.size() == cplen || target[cplen] != index_.commonPrefix_[cplen]);
        if (target.size() == cplen || byte_t(target[cplen]) < byte_t(index_.commonPrefix_[cplen])) {
          SeekToFirst();
          return true;
        }
        else {
          m_id = size_t(-1);
          return false;
        }
      }
      target = target.substr(index_.commonPrefix_.size());
      byte_t targetBuffer[8] = {};
      memcpy(targetBuffer + (8 - index_.keyLength_),
          target.data(), std::min<size_t>(index_.keyLength_, target.size()));
      uint64_t targetValue = ReadUint64Aligned(targetBuffer, targetBuffer + 8);
      if (targetValue > index_.maxValue_) {
        m_id = size_t(-1);
        return false;
      }
      if (targetValue < index_.minValue_) {
        SeekToFirst();
        return true;
      }
      pos_ = targetValue - index_.minValue_;
      m_id = index_.indexSeq_.rank1(pos_);
      if (!index_.indexSeq_[pos_]) {
        pos_ += index_.indexSeq_.zero_seq_len(pos_);
      }
      else if (target.size() > index_.keyLength_) {
        // minValue:  target
        // targetVal: targetvalue.1
        // maxValue:  targetvau
        if (pos_ == index_.indexSeq_.size() - 1) {
          m_id = size_t(-1);
          return false;
        }
        ++m_id;
        pos_ = pos_ + index_.indexSeq_.zero_seq_len(pos_ + 1) + 1;
      }
      UpdateBuffer();
      return true;
    }
    bool Next() override {
      assert(m_id != size_t(-1));
      assert(index_.indexSeq_[pos_]);
      assert(index_.indexSeq_.rank1(pos_) == m_id);
      if (m_id == index_.indexSeq_.max_rank1() - 1) {
        m_id = size_t(-1);
        return false;
      }
      else {
        ++m_id;
        pos_ = pos_ + index_.indexSeq_.zero_seq_len(pos_ + 1) + 1;
        UpdateBuffer();
        return true;
      }
    }
    bool Prev() override {
      assert(m_id != size_t(-1));
      assert(index_.indexSeq_[pos_]);
      assert(index_.indexSeq_.rank1(pos_) == m_id);
      if (m_id == 0) {
        m_id = size_t(-1);
        return false;
      }
      else {
        --m_id;
        pos_ = pos_ - index_.indexSeq_.zero_seq_revlen(pos_) - 1;
        UpdateBuffer();
        return true;
      }
    }
    size_t DictRank() const override {
      assert(m_id != size_t(-1));
      return m_id;
    }
    fstring key() const override {
      assert(m_id != size_t(-1));
      return buffer_;
    }
  protected:
    void UpdateBuffer() {
      AssignUint64(buffer_.data() + index_.commonPrefix_.size(),
        buffer_.data() + buffer_.size(), pos_ + index_.minValue_);
    }
    size_t pos_;
    valvec<byte_t> buffer_;
    const TerarkUintIndex& index_;
  };
  class MyFactory : public TerarkIndex::Factory {
  public:
    TerarkIndex* Build(NativeDataInput<InputBuffer>& reader,
                       const TerarkZipTableOptions& tzopt,
                       const KeyStat& ks) const override {
      size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
      assert(cplen >= ks.commonPrefixLen);
      if (ks.maxKeyLen != ks.minKeyLen ||
          ks.maxKeyLen - cplen > sizeof(uint64_t)) {
        abort();
      }
      uint64_t
        minValue = ReadUint64(ks.minKey.begin() + cplen, ks.minKey.end()),
        maxValue = ReadUint64(ks.maxKey.begin() + cplen, ks.maxKey.end());
      if (minValue > maxValue) {
        std::swap(minValue, maxValue);
      }
      uint64_t diff = maxValue - minValue + 1;
      RankSelect indexSeq;
      indexSeq.resize(diff);
      if (!std::is_same<RankSelect, rank_select_allone>::value) {
        // not 'all one' case
        valvec<byte_t> keyBuf;
        for (size_t seq_id = 0; seq_id < ks.numKeys; ++seq_id) {
          reader >> keyBuf;
          // even if 'cplen' contains actual data besides prefix,
          // after stripping, the left range is self-meaningful ranges
          indexSeq.set1(ReadUint64(keyBuf.begin() + cplen,
            keyBuf.end()) - minValue);
        }
      } else {
        printf("== all one index used\n");
      }
      indexSeq.build_cache(false, false);
      unique_ptr<TerarkUintIndex<RankSelect>> ptr(new TerarkUintIndex<RankSelect>());
      ptr->isUserMemory_ = false;
      ptr->isBuilding_ = true;
      FileHeader *header = new FileHeader(indexSeq.mem_size());
      header->min_value = minValue;
      header->max_value = maxValue;
      header->index_mem_size = indexSeq.mem_size();
      header->key_length = ks.minKeyLen - cplen;
      if (cplen > ks.commonPrefixLen) {
        header->common_prefix_length = cplen - ks.commonPrefixLen;
        ptr->commonPrefix_.assign(ks.minKey.data() + ks.commonPrefixLen,
          header->common_prefix_length);
        header->file_size += terark::align_up(header->common_prefix_length, 8);
      }
      ptr->header_ = header;
      ptr->indexSeq_.swap(indexSeq);
      return ptr.release();
    }
    unique_ptr<TerarkIndex> LoadMemory(fstring mem) const override {
      return unique_ptr<TerarkIndex>(loadImpl(mem, {}).release());
    }
    unique_ptr<TerarkIndex> LoadFile(fstring fpath) const override {
      return unique_ptr<TerarkIndex>(loadImpl({}, fpath).release());
    }
    size_t MemSizeForBuild(const KeyStat& ks) const override {
      assert(ks.minKeyLen == ks.maxKeyLen);
      size_t length = ks.maxKeyLen - commonPrefixLen(ks.minKey, ks.maxKey);
      uint64_t
        minValue = ReadUint64(ks.minKey.begin(), ks.minKey.begin() + length),
        maxValue = ReadUint64(ks.maxKey.begin(), ks.maxKey.begin() + length);
      if (minValue > maxValue) {
        std::swap(minValue, maxValue);
      }
      uint64_t diff = maxValue - minValue + 1;
      return size_t(std::ceil(diff * 1.25 / 8));
    }
  protected:
    unique_ptr<TerarkUintIndex<RankSelect>> loadImpl(fstring mem, fstring fpath) const {
      unique_ptr<TerarkUintIndex<RankSelect>> ptr(new TerarkUintIndex<RankSelect>());
      ptr->isUserMemory_ = false;
      ptr->isBuilding_ = false;

      if (mem.data() == nullptr) {
        MmapWholeFile(fpath).swap(ptr->file_);
        mem = {(const char*)ptr->file_.base, (ptrdiff_t)ptr->file_.size};
      }
      else {
        ptr->isUserMemory_ = true;
      }
      const FileHeader* header = (const FileHeader*)mem.data();

      if (mem.size() < sizeof(FileHeader)
        || header->magic_len != strlen(index_name)
        || strcmp(header->magic, index_name) != 0
        || header->header_size != sizeof(FileHeader)
        || header->version != 1
        || header->file_size != mem.size()
        ) {
        throw std::invalid_argument("TerarkUintIndex::Load(): Bad file header");
      }
      auto verifyClassName = [&] {
        size_t name_i = g_TerarkIndexName.find_i(typeid(TerarkUintIndex<RankSelect>).name());
        size_t self_i = g_TerarkIndexFactroy.find_i(g_TerarkIndexName.val(name_i));
        assert(self_i < g_TerarkIndexFactroy.end_i());
        size_t head_i = g_TerarkIndexFactroy.find_i(header->class_name);
        return head_i < g_TerarkIndexFactroy.end_i()
          && g_TerarkIndexFactroy.val(head_i) == g_TerarkIndexFactroy.val(self_i);
      };
      assert(verifyClassName()), (void)verifyClassName;
      ptr->header_ = header;
      ptr->minValue_ = header->min_value;
      ptr->maxValue_ = header->max_value;
      ptr->keyLength_ = header->key_length;
      if (header->common_prefix_length > 0) {
        ptr->commonPrefix_.risk_set_data((char*)mem.data() + header->header_size,
          header->common_prefix_length);
      }
      ptr->indexSeq_.risk_mmap_from((unsigned char*)mem.data() + header->header_size
        + terark::align_up(header->common_prefix_length, 8), header->index_mem_size);
      return ptr;
    }
  };
  using TerarkIndex::FactoryPtr;
  virtual ~TerarkUintIndex() {
    if (isBuilding_) {
      delete (FileHeader*)header_;
    }
    else if (file_.base != nullptr || isUserMemory_) {
      indexSeq_.risk_release_ownership();
      commonPrefix_.risk_release_ownership();
    }
  }
  const char* Name() const override {
    return header_->class_name;
  }
  void SaveMmap(std::function<void(const void *, size_t)> write) const override {
    write(header_, sizeof *header_);
    if (!commonPrefix_.empty()) {
      write(commonPrefix_.data(), terark::align_up(commonPrefix_.size(), 8));
    }
    write(indexSeq_.data(), indexSeq_.mem_size());
  }
  size_t Find(fstring key) const override {
    if (key.size() != keyLength_ + commonPrefix_.size()) {
      return size_t(-1);
    }
    if (key.commonPrefixLen(commonPrefix_) != commonPrefix_.size()) {
      return size_t(-1);
    }
    key = key.substr(commonPrefix_.size());
    assert(key.n == keyLength_);
    uint64_t findValue = ReadUint64((const byte_t*)key.begin(), (const byte_t*)key.end());
    if (findValue < minValue_ || findValue > maxValue_) {
      return size_t(-1);
    }
    uint64_t findPos = findValue - minValue_;
    if (!indexSeq_[findPos]) {
      return size_t(-1);
    }
    return indexSeq_.rank1(findPos);
  }
  size_t NumKeys() const override {
    return indexSeq_.max_rank1();
  }
  size_t TotalKeySize() const override final {
    return (commonPrefix_.size() + keyLength_) * indexSeq_.max_rank1();
  }
  fstring Memory() const override {
    return fstring((const char*)header_, (ptrdiff_t)header_->file_size);
  }
  Iterator* NewIterator() const override {
    return new UIntIndexIterator(*this);
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
protected:
  const FileHeader* header_;
  MmapWholeFile     file_;
  valvec<char>      commonPrefix_;
  RankSelect        indexSeq_;
  uint64_t          minValue_;
  uint64_t          maxValue_;
  bool              isUserMemory_;
  bool              isBuilding_;
  uint32_t          keyLength_;
};
template<class RankSelect>
const char* TerarkUintIndex<RankSelect>::index_name = "UintIndex";

#endif // TerocksPrivateCode

typedef NestLoudsTrieDAWG_IL_256 NestLoudsTrieDAWG_IL_256_32;
typedef NestLoudsTrieDAWG_SE_512 NestLoudsTrieDAWG_SE_512_32;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_IL_256_32> TerocksIndex_NestLoudsTrieDAWG_IL_256_32;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_SE_512_32> TerocksIndex_NestLoudsTrieDAWG_SE_512_32;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_SE_512_64> TerocksIndex_NestLoudsTrieDAWG_SE_512_64;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_IL_256> TerocksIndex_NestLoudsTrieDAWG_Mixed_IL_256;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_SE_512> TerocksIndex_NestLoudsTrieDAWG_Mixed_SE_512;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_XL_256> TerocksIndex_NestLoudsTrieDAWG_Mixed_XL_256;

typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_IL_256_32_FL> TerocksIndex_NestLoudsTrieDAWG_IL_256_32_FL;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_SE_512_32_FL> TerocksIndex_NestLoudsTrieDAWG_SE_512_32_FL;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_SE_512_64_FL> TerocksIndex_NestLoudsTrieDAWG_SE_512_64_FL;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_IL_256_32_FL> TerocksIndex_NestLoudsTrieDAWG_Mixed_IL_256_32_FL;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_SE_512_32_FL> TerocksIndex_NestLoudsTrieDAWG_Mixed_SE_512_32_FL;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_XL_256_32_FL> TerocksIndex_NestLoudsTrieDAWG_Mixed_XL_256_32_FL;

TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_IL_256_32, "NestLoudsTrieDAWG_IL", "IL_256_32", "IL_256", "NestLoudsTrieDAWG_IL_256");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_SE_512_32, "NestLoudsTrieDAWG_SE_512", "SE_512_32", "SE_512");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_SE_512_64, "NestLoudsTrieDAWG_SE_512_64", "SE_512_64");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_SE_512, "NestLoudsTrieDAWG_Mixed_SE_512", "Mixed_SE_512");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_IL_256, "NestLoudsTrieDAWG_Mixed_IL_256", "Mixed_IL_256");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_XL_256, "NestLoudsTrieDAWG_Mixed_XL_256", "Mixed_XL_256");

TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_IL_256_32_FL, "NestLoudsTrieDAWG_IL_256_32_FL", "IL_256_32_FL", "IL_256_FL");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_SE_512_32_FL, "NestLoudsTrieDAWG_SE_512_32_FL", "SE_512_32_FL", "SE_512_FL");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_SE_512_64_FL, "NestLoudsTrieDAWG_SE_512_64_FL", "SE_512_64_FL");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_SE_512_32_FL, "NestLoudsTrieDAWG_Mixed_SE_512_32_FL", "Mixed_SE_512_32_FL");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_IL_256_32_FL, "NestLoudsTrieDAWG_Mixed_IL_256_32_FL", "Mixed_IL_256_32_FL");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_XL_256_32_FL, "NestLoudsTrieDAWG_Mixed_XL_256_32_FL", "Mixed_XL_256_32_FL");

#if defined(TerocksPrivateCode)
  // IL_85: IL_2**8_2**5
typedef TerarkCompositeIndex<terark::rank_select_il_256, terark::rank_select_il_256>       TerarkCompositeIndex_IL85_IL85;
typedef TerarkCompositeIndex<terark::rank_select_il_256, rank_select_allzero>              TerarkCompositeIndex_IL85_AllZero;
typedef TerarkCompositeIndex<rank_select_allone, terark::rank_select_il_256>               TerarkCompositeIndex_AllOne_IL85;

typedef TerarkCompositeIndex<terark::rank_select_se_512_64, terark::rank_select_se_512_64> TerarkCompositeIndex_SE96_SE96;
typedef TerarkCompositeIndex<terark::rank_select_se_512_64, rank_select_allzero>           TerarkCompositeIndex_SE96_AllZero;
typedef TerarkCompositeIndex<rank_select_allone, terark::rank_select_se_512_64>            TerarkCompositeIndex_AllOne_SE96;
typedef TerarkCompositeIndex<rank_select_allone, rank_select_allzero>               TerarkCompositeIndex_AllOne_AllZero;
  
TerarkIndexRegister(TerarkCompositeIndex_IL85_IL85, "CompositeIndex_IL85_IL85");
TerarkIndexRegister(TerarkCompositeIndex_IL85_AllZero, "CompositeIndex_IL85_AllZero");
TerarkIndexRegister(TerarkCompositeIndex_AllOne_IL85, "CompositeIndex_AllOne_IL85");

TerarkIndexRegister(TerarkCompositeIndex_SE96_SE96, "CompositeIndex_SE96_SE96");
TerarkIndexRegister(TerarkCompositeIndex_SE96_AllZero, "CompositeIndex_SE96_AllZero");
TerarkIndexRegister(TerarkCompositeIndex_AllOne_SE96, "CompositeIndex_AllOne_SE96");
TerarkIndexRegister(TerarkCompositeIndex_AllOne_AllZero, "CompositeIndex_AllOne_AllZero");

typedef TerarkUintIndex<terark::rank_select_il_256_32> TerarkUintIndex_IL_256_32;
typedef TerarkUintIndex<terark::rank_select_se_256_32> TerarkUintIndex_SE_256_32;
typedef TerarkUintIndex<terark::rank_select_se_512_32> TerarkUintIndex_SE_512_32;
typedef TerarkUintIndex<terark::rank_select_se_512_64> TerarkUintIndex_SE_512_64;
typedef TerarkUintIndex<rank_select_allone> TerarkUintIndex_AllOne;
TerarkIndexRegister(TerarkUintIndex_IL_256_32, "UintIndex_IL_256_32", "UintIndex");
TerarkIndexRegister(TerarkUintIndex_SE_256_32, "UintIndex_SE_256_32");
TerarkIndexRegister(TerarkUintIndex_SE_512_32, "UintIndex_SE_512_32");
TerarkIndexRegister(TerarkUintIndex_SE_512_64, "UintIndex_SE_512_64");
TerarkIndexRegister(TerarkUintIndex_AllOne, "UintIndex_AllOne");
#endif // TerocksPrivateCode

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

} // namespace rocksdb
