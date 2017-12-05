#include "terark_zip_index.h"
#include "terark_zip_table.h"
#include "terark_zip_common.h"
#include <terark/bitmap.hpp>
#include <terark/hash_strmap.hpp>
#include <terark/fsa/dfa_mmap_header.hpp>
#include <terark/fsa/fsa_cache.hpp>
#include <terark/fsa/nest_trie_dawg.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/num_to_str.hpp>
#if defined(TerocksPrivateCode)
#include <terark/fsa/fsa_for_union_dfa.hpp>
#include "tests/rank_select_fewzero.h"
#endif // TerocksPrivateCode

namespace rocksdb {

using namespace terark;

static hash_strmap<TerarkIndex::FactoryPtr> g_TerarkIndexFactroy;
static hash_strmap<std::string>             g_TerarkIndexName;

template<class IndexClass>
bool VerifyClassName(fstring class_name) {
  size_t name_i = g_TerarkIndexName.find_i(typeid(IndexClass).name());
  size_t self_i = g_TerarkIndexFactroy.find_i(g_TerarkIndexName.val(name_i));
  assert(self_i < g_TerarkIndexFactroy.end_i());
  size_t head_i = g_TerarkIndexFactroy.find_i(class_name);
  return head_i < g_TerarkIndexFactroy.end_i() &&
                  g_TerarkIndexFactroy.val(head_i) == g_TerarkIndexFactroy.val(self_i);
}

void AppendExtraZero(std::function<void(const void *, size_t)> write, size_t len) {
  assert(len <= 8);
  static const char zeros[8] = { 0 };
  if (0 < len && len < 8) {
    write(zeros, len);
  }
}

// 0 < cnt0 and cnt0 < 0.01 * total
bool IsFewZero(size_t total, size_t cnt0) {
  assert(total > 0);
  return (0 < cnt0) && 
    (cnt0 <= (double)total * 0.01);
}
bool IsFewOne(size_t total, size_t cnt1) {
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
  for (const char* name : names) {
    g_TerarkIndexFactroy.insert_i(name, FactoryPtr(factory));
  }
  g_TerarkIndexName.insert_i(rtti_name, *names.begin());
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
  const double w1 = 0.1;
  const double w2 = 1.2;
  const double min_gap_ratio = 0.1;
  const double max_gap_ratio = 0.9;
  const double fewone_min_gap_ratio = 0.99; // fewone 0.01, that's 1 '1' out of 100
  const double fewone_max_gap_ratio = 0.995; // fewone 0.005, 1 '1' out of 200
  const size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
  const size_t maxLen = std::min<size_t>(8, ks.maxKeyLen - cplen);
  const double originCost = ks.numKeys * ks.maxKeyLen * 8;
  double score = 0;
  double minCost = originCost;
  size_t scoreLen = maxLen;
  size_t minCostLen = maxLen;
  ceLen = maxLen;
  for (size_t i = maxLen; i > 0; i--) {
    auto minValue = ReadBigEndianUint64(ks.minKey.begin() + cplen, i);
    auto maxValue = ReadBigEndianUint64(ks.maxKey.begin() + cplen, i);
    uint64_t diff1st = abs_diff(minValue, maxValue) + 1;
    uint64_t diff2nd = ks.numKeys;
    // one index1st with a collection of index2nd, that's when diff < numkeys
    double gap_ratio = diff1st <= ks.numKeys ? min_gap_ratio : 
      (double)(diff1st - ks.numKeys) / diff1st;
    if (fewone_min_gap_ratio <= gap_ratio &&
        gap_ratio < fewone_max_gap_ratio) { // fewone branch
      // to construct rankselect for fewone, much more extra space is needed
      // TBD: maybe we could prescan ?
      size_t bits = (diff1st < UINT32_MAX && ks.numKeys < UINT32_MAX) ? 32 : 64;
      double cost = ks.numKeys * bits + diff2nd * 1.2 + 
        (ks.maxKeyLen - cplen - i) * ks.numKeys * 8;
      if (cost > originCost * 0.8)
        continue;
      if (cost < minCost) {
        minCost = cost;
        minCostLen = i;
      }
    } else if (gap_ratio > max_gap_ratio) { // skip branch
      continue;
    } else {
      gap_ratio = std::max(gap_ratio, min_gap_ratio);
      // diff is bitmap, * 1.2 is extra cost to build RankSelect
      double cost = (diff1st + diff2nd) * 1.2 + 
        (ks.maxKeyLen - cplen - i) * ks.numKeys * 8;
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
  } else {
    return false;
  }
}

const TerarkIndex::Factory*
TerarkIndex::SelectFactory(const KeyStat& ks, fstring name) {
  assert(ks.numKeys > 0);
  //assert(!ks.minKey.empty() && !ks.maxKey.empty());
#if defined(TerocksPrivateCode)
  static bool disableUintIndex =
    terark::getEnvBool("TerarkZipTable_disableUintIndex", false);
  static bool disableCompositeUintIndex =
    terark::getEnvBool("TerarkZipTable_disableCompositeUintIndex", false);
  size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
  assert(cplen >= ks.commonPrefixLen);
  size_t ceLen = 0; // cost effective index1st len if any
  if (!disableUintIndex && ks.maxKeyLen == ks.minKeyLen && ks.maxKeyLen - cplen <= sizeof(uint64_t)) {
    auto minValue = ReadBigEndianUint64(ks.minKey.begin() + cplen, ks.minKey.end());
    auto maxValue = ReadBigEndianUint64(ks.maxKey.begin() + cplen, ks.maxKey.end());
    uint64_t diff = (minValue < maxValue ? maxValue - minValue : minValue - maxValue) + 1;
    if (diff < ks.numKeys * 30) {
      if (diff == ks.numKeys) {
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
  if (!disableCompositeUintIndex &&
      ks.maxKeyLen == ks.minKeyLen &&
      ks.maxKeyLen - cplen <= 16 && // !!! plain index2nd may occupy too much space
      SeekCostEffectiveIndexLen(ks, ceLen) &&
      ks.maxKeyLen > cplen + ceLen) {
    auto minValue = ReadBigEndianUint64(ks.minKey.begin() + cplen, ceLen);
    auto maxValue = ReadBigEndianUint64(ks.maxKey.begin() + cplen, ceLen);
    uint64_t diff = (minValue < maxValue ? maxValue - minValue : minValue - maxValue) + 1;
    if (diff < UINT32_MAX &&
        ks.numKeys < UINT32_MAX) { // for composite cluster key, key1:key2 maybe 1:N
      return GetFactory("CompositeUintIndex_IL_256_32_IL_256_32");
    } else {
      return GetFactory("CompositeUintIndex_SE_512_64_SE_512_64");
    }
  }
#endif // TerocksPrivateCode
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
  unique_ptr<ADFA_LexIterator> m_iter;
  fstring key() const override {
	  return fstring(m_iter->word());
  }
  NestLoudsTrieIterBase(ADFA_LexIterator* iter)
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
  const BaseDAWG* m_dawg;
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


template<class NLTrie>
class NestLoudsTrieIndex : public TerarkIndex {
  const BaseDAWG* m_dawg;
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
      NestLoudsTrieConfig conf;
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

template<class Container>
class CompositeKeyDataContainer {
};

template<>
class CompositeKeyDataContainer<SortedUintVec> {
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
  void swap(CompositeKeyDataContainer<SortedUintVec>& other) {
    container_.swap(other.container_);
    std::swap(min_value_, other.min_value_);
    std::swap(key_len_, other.key_len_);
  }
  void swap(SortedUintVec& other) {
    container_.swap(other);
  }
  void init(size_t len, size_t minval) {
    key_len_ = len;
    min_value_ = minval;
  }
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

template<>
class CompositeKeyDataContainer<UintVecMin0> {
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
  void swap(CompositeKeyDataContainer<UintVecMin0>& other) {
    container_.swap(other.container_);
    std::swap(min_value_, other.min_value_);
    std::swap(key_len_, other.key_len_);
  }
  void swap(UintVecMin0& other) {
    container_.swap(other);
  }
  void init(size_t len, size_t minval) {
    key_len_ = len;
    min_value_ = minval;
  }
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
    size_t bits = UintVecMin0::compute_uintbits(maxValue);
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
    byte_t arr[8] = { 0 };
    copy_to(idx, arr);
    fstring me(arr, arr + key_len_);
    return fstring_func::compare3()(me, another);
  }
  size_t lower_bound(size_t lo, size_t hi, fstring val) const {
    uint64_t n = to_uint64(val);
    if (n < min_value_) // if equal, val len may > key_len_
      return lo;
    n -= min_value_;
    size_t pos = lower_bound_n<const UintVecMin0&>(container_, lo, hi, n);
    while (pos != hi) {
      if (compare(pos, val) >= 0)
        return pos;
      pos++;
    }
    return pos;
  }

private:
  UintVecMin0 container_;
  uint64_t min_value_;
  size_t key_len_;
};

template<>
class CompositeKeyDataContainer<FixedLenStrVec> {
public:
  void swap(CompositeKeyDataContainer<FixedLenStrVec>& other) {
    container_.swap(other.container_);
  }
  void swap(FixedLenStrVec& other) {
    container_.swap(other);
  }
  void risk_release_ownership() {
    container_.risk_release_ownership();
  }
  void init(size_t len, size_t sz) {
    container_.m_fixlen = len;
    container_.m_size = sz;
  }
  void init(size_t minval) {
    assert(0);
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
size_t fast_zero_seq_len(RankSelect& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_len(pos);
}
template<>
size_t fast_zero_seq_len<rank_select_fewzero<uint32_t>>(
  rank_select_fewzero<uint32_t>& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_len(pos, hint);
}
template<>
size_t fast_zero_seq_len<rank_select_fewzero<uint64_t>>(
  rank_select_fewzero<uint64_t>& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_len(pos, hint);
}
template<>
size_t fast_zero_seq_len<rank_select_fewone<uint32_t>>(
  rank_select_fewone<uint32_t>& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_len(pos, hint);
}
template<>
size_t fast_zero_seq_len<rank_select_fewone<uint64_t>>(
  rank_select_fewone<uint64_t>& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_len(pos, hint);
}
// -- fast zero-seq-revlen
template<class RankSelect>
size_t fast_zero_seq_revlen(RankSelect& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_revlen(pos);
}
template<>
size_t fast_zero_seq_revlen<rank_select_fewzero<uint32_t>>(
  rank_select_fewzero<uint32_t>& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_revlen(pos, hint);
}
template<>
size_t fast_zero_seq_revlen<rank_select_fewzero<uint64_t>>(
  rank_select_fewzero<uint64_t>& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_revlen(pos, hint);
}
template<>
size_t fast_zero_seq_revlen<rank_select_fewone<uint32_t>>(
  rank_select_fewone<uint32_t>& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_revlen(pos, hint);
}
template<>
size_t fast_zero_seq_revlen<rank_select_fewone<uint64_t>>(
  rank_select_fewone<uint64_t>& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_revlen(pos, hint);
}


struct CompositeUintIndexBase : public TerarkIndex {
  static const char* index_name;
  struct MyBaseFileHeader : public TerarkIndexHeader {
    uint64_t min_value;
    uint64_t max_value;
    uint64_t rankselect1_mem_size;
    uint64_t rankselect2_mem_size;
    uint64_t key2_data_mem_size;
    uint64_t key2_min_value;
    uint64_t key2_max_value;
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
};
const char* CompositeUintIndexBase::index_name = "CompositeIndex";

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
template<class RankSelect1, class RankSelect2, class Key2DataContainer>
class CompositeUintIndex : public CompositeUintIndexBase {
public:
  struct FileHeader : MyBaseFileHeader {
    FileHeader(size_t file_size)
      : MyBaseFileHeader(file_size, typeid(CompositeUintIndex))
    {}
  };
  typedef CompositeKeyDataContainer<SortedUintVec> SortedUintDataCont;
  typedef CompositeKeyDataContainer<UintVecMin0> Min0DataCont;
  typedef CompositeKeyDataContainer<FixedLenStrVec> StrDataCont;
public:
  class CompositeUintIndexIterator : public TerarkIndex::Iterator {
  public:
    CompositeUintIndexIterator(const CompositeUintIndex& index) : index_(index) {
      rankselect1_idx_ = size_t(-1);
      m_id = size_t(-1);
      buffer_.resize_no_init(index_.commonPrefix_.size() +
        index_.key1_len_ + index_.key2_len_);
      memcpy(buffer_.data(), index_.commonPrefix_.data(), index_.commonPrefix_.size());
    }
    virtual ~CompositeUintIndexIterator() {}

    bool SeekToFirst() override {
      rankselect1_idx_ = 0;
      m_id = 0;
      UpdateBuffer();
      return true;
    }
    bool SeekToLast() override {
      rankselect1_idx_ = index_.rankselect1_.size() - 1;
      m_id = index_.key2_data_.size() - 1;
      UpdateBuffer();
      return true;
    }
    bool Seek(fstring target) override {
      size_t cplen = target.commonPrefixLen(index_.commonPrefix_);
      if (cplen != index_.commonPrefix_.size()) {
        assert(target.size() >= cplen);
        assert(target.size() == cplen ||
            byte_t(target[cplen]) != byte_t(index_.commonPrefix_[cplen]));
        if (target.size() == cplen ||
            byte_t(target[cplen]) < byte_t(index_.commonPrefix_[cplen])) {
          return SeekToFirst();
        } else {
          m_id = size_t(-1);
          return false;
        }
      }
      uint64_t key1 = 0;
      fstring  key2;
      if (target.size() <= cplen + index_.key1_len_) {
        fstring sub = target.substr(cplen);
        byte_t targetBuffer[8] = { 0 };
        /*
         * do not think hard about int, think about string instead. 
         * assume key1_len is 6 byte len like 'abcdef', target without
         * commpref is 'b', u should compare 'b' with 'a' instead of 'f'.
         * that's why assign sub starting in the middle instead at tail.
         */
        memcpy(targetBuffer + (8 - index_.key1_len_), sub.data(), sub.size());
        key1 = Read1stKey(targetBuffer, 0, 8);
        key2 = fstring(); // empty
      } else {
        key1 = Read1stKey(target, cplen, index_.key1_len_);
        key2 = target.substr(cplen + index_.key1_len_);
      }
      if (key1 > index_.maxValue_) {
        m_id = size_t(-1);
        return false;
      } else if (key1 < index_.minValue_) {
        return SeekToFirst();
      }
      
      // find the corresponding bit within 2ndRS
      uint64_t order, pos0, cnt;
      rankselect1_idx_ = key1 - index_.minValue_;
      if (index_.rankselect1_[rankselect1_idx_]) {
        // find within this index
        order = index_.rankselect1_.rank1(rankselect1_idx_);
        pos0 = index_.rankselect2_.select0(order);
        if (pos0 == index_.key2_data_.size() - 1) { // last elem
          m_id = (index_.key2_data_.compare(pos0, key2) >= 0) ? pos0 : size_t(-1);
          goto out;
        } else {
          cnt = index_.rankselect2_.one_seq_len(pos0 + 1) + 1;
          m_id = index_.Locate(index_.key2_data_, pos0, cnt, key2);
          if (m_id != size_t(-1)) {
            goto out;
          } else if (pos0 + cnt == index_.key2_data_.size()) {
            goto out;
          } else {
            // try next offset
            rankselect1_idx_++;
          }
        }
      }
      // no such index, use the lower_bound form
      cnt = index_.rankselect1_.zero_seq_len(rankselect1_idx_);
      if (rankselect1_idx_ + cnt >= index_.rankselect1_.size()) {
        m_id = size_t(-1);
        return false;
      }
      rankselect1_idx_ += cnt;
      order = index_.rankselect1_.rank1(rankselect1_idx_);
      m_id = index_.rankselect2_.select0(order);
    out:
      if (m_id == size_t(-1)) {
        return false;
      }
      UpdateBuffer();
      return true;
    }

    bool Next() override {
      assert(m_id != size_t(-1));
      assert(index_.rankselect1_[rankselect1_idx_] != 0);
      if (terark_unlikely(m_id + 1 == index_.key2_data_.size())) {
        m_id = size_t(-1);
        return false;
      } else {
        if (Is1stKeyDiff(m_id + 1)) {
          assert(rankselect1_idx_ + 1 <  index_.rankselect1_.size());
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
      assert(m_id != size_t(-1));
      assert(index_.rankselect1_[rankselect1_idx_] != 0);
      if (terark_unlikely(m_id == 0)) {
        m_id = size_t(-1);
        return false;
      } else {
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
    bool Is1stKeyDiff(size_t curr_id) {
      return index_.rankselect2_.is0(curr_id);
    }
    void UpdateBuffer() {
      // key = commonprefix + key1s + key2
      // assign key1
      size_t offset = index_.commonPrefix_.size();
      auto len1 = index_.key1_len_;
      auto key1 = rankselect1_idx_ + index_.minValue_;
      SaveAsBigEndianUint64(buffer_.data() + offset, len1, key1);
      // assign key2
      offset += len1;
      index_.key2_data_.copy_to(m_id, buffer_.data() + offset);
    }

    size_t rankselect1_idx_; // used to track & decode index1 value
    size_t access_hint_;
    valvec<byte_t> buffer_;
    const CompositeUintIndex& index_;
  };
    
public:
  class MyFactory : public TerarkIndex::Factory {
  public:
    // composite index as cluster index
    // secondary index which contain 'id' as key2
    enum AmazingCombinationT {
      kAllOne_AllZero = 0, // secondary index no gap
      kAllOne_FewZero,     // c-cluster index
      kAllOne_FewOne,
      kAllOne_Normal,      // c-cluster index
      kFewZero_AllZero,    // secondary index with gap
      kFewZero_FewZero,    //
      kFewZero_FewOne,
      kFewZero_Normal,
      kFewOne_AllZero,     // secondary index with lots of gap
      kFewOne_FewZero,     // c-cluster index with lots of gap
      kFewOne_FewOne,
      kFewOne_Normal,      // c-cluster index with lots of gap
      kNormal_AllZero,
      kNormal_FewZero,
      kNormal_FewOne,
      kNormal_Normal
    };
  public:
    // no option.keyPrefixLen
    TerarkIndex* Build(NativeDataInput<InputBuffer>& reader,
                       const TerarkZipTableOptions& tzopt,
                       const KeyStat& ks) const override {
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
      // is actually the largetst one
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
      RankSelect2 rankselect2(ks.numKeys + 1); // append extra '0' at back
      valvec<byte_t> keyBuf, minKey2Data, maxKey2Data;
      uint64_t prev = size_t(-1);
      size_t key2_len = ks.maxKeyLen - cplen - key1_len;
      size_t sumKey2Len = key2_len * ks.numKeys;
      FixedLenStrVec keyVec(key2_len);
      keyVec.reserve(ks.numKeys, sumKey2Len);
      if (ks.minKey < ks.maxKey) { // ascend
        for (size_t i = 0; i < ks.numKeys; ++i) {
          reader >> keyBuf;
          uint64_t offset = Read1stKey(keyBuf, cplen, key1_len) - minValue;
          rankselect1.set1(offset);
          if (terark_unlikely(i == 0)) // make sure 1st prev != offset
            prev = offset - 1;
          if (offset != prev) { // new key1 encountered
            rankselect2.set0(i);
          } else {
            rankselect2.set1(i);
          }
          prev = offset;
          fstring key2Data = fstring(keyBuf).substr(cplen + key1_len);
          updateMinMax(key2Data, minKey2Data, maxKey2Data);
          keyVec.push_back(key2Data);
        }
      } else { // descend, reverse comparator
        size_t pos = sumKey2Len;
        keyVec.m_size = ks.numKeys;
        keyVec.m_strpool.resize(sumKey2Len);
        // compare with '0', do NOT use size_t
        for (long i = ks.numKeys - 1; i >= 0; --i) {
          reader >> keyBuf;
          uint64_t offset = Read1stKey(keyBuf, cplen, key1_len) - minValue;
          rankselect1.set1(offset);
          if (terark_unlikely(i == ks.numKeys - 1)) // make sure 1st prev != offset
            prev = offset - 1;
          if (offset != prev) { // next index1 is new one
            rankselect2.set0(i + 1);
          } else {
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
      rankselect2.set0(ks.numKeys);
      // TBD: build histogram, which should set as 'true'
      rankselect1.build_cache(false, false);
      rankselect2.build_cache(false, false);
      // try order: 1. sorteduint; 2. uintmin0; 3. fixlen
      // TBD: following skips are only for test right now
      bool skipSorted =
        terark::getEnvBool("TerarkZipTable_skipSorted", false);
      bool skipUint =
        terark::getEnvBool("TerarkZipTable_skipUint", false);
      if (key2_len <= 8) {
        TerarkIndex* index = nullptr;
        if (!skipSorted)
          index = CreateIndexWithSortedUintCont(rankselect1, rankselect2, 
                                                keyVec, ks, minValue, maxValue, key1_len, minKey2Data, maxKey2Data);
        if (!index && !skipUint) {
          index = CreateIndexWithUintCont(rankselect1, rankselect2, 
            keyVec, ks, minValue, maxValue, key1_len, minKey2Data, maxKey2Data);
        }
        if (index)
          return index;
      }
      return CreateIndexWithStrCont(rankselect1, rankselect2, keyVec, ks,
                                    minValue, maxValue, key1_len);
    }
  private:
    static void updateMinMax(fstring data, valvec<byte_t>& minData, valvec<byte_t>& maxData) {
      if (minData.empty() || data < fstring(minData.begin(), minData.size())) {
        minData.assign(data.data(), data.size());
      }
      if (maxData.empty() || data > fstring(maxData.begin(), maxData.size())) {
        maxData.assign(data.data(), data.size());
      }
    }
    static AmazingCombinationT figureCombination(RankSelect1& rs1, RankSelect2& rs2) {
      bool isRS1FewZero = IsFewZero(rs1.size(), rs1.max_rank0());
      bool isRS2FewZero = IsFewZero(rs2.size(), rs2.max_rank0());
      bool isRS1FewOne  = IsFewOne(rs1.size(), rs1.max_rank1());
      bool isRS2FewOne  = IsFewOne(rs2.size(), rs2.max_rank1());
      if (rs1.max_rank0() == 0 && rs2.max_rank1() == 0)
        return kAllOne_AllZero;
      else if (rs1.max_rank0() == 0 && isRS2FewZero)
        return kAllOne_FewZero;
      else if (rs1.max_rank0() == 0 && isRS2FewOne)
        return kAllOne_FewOne;
      else if (rs1.max_rank0() == 0)
        return kAllOne_Normal; 
      //
      else if (isRS1FewZero && rs2.max_rank1() == 0)
        return kFewZero_AllZero;
      else if (isRS1FewZero && isRS2FewZero)
        return kFewZero_FewZero;
      else if (isRS1FewZero && isRS2FewOne)
        return kFewZero_FewOne;
      else if (isRS1FewZero)
        return kFewZero_Normal;
      //
      else if (isRS1FewOne && rs2.max_rank1() == 0)
        return kFewOne_AllZero;
      else if (isRS1FewOne && isRS2FewZero)
        return kFewOne_FewZero;
      else if (isRS1FewOne && isRS2FewOne)
        return kFewOne_FewOne;
      else if (isRS1FewOne)
        return kFewOne_Normal;
      //
      else if (rs2.max_rank1() == 0)
        return kNormal_AllZero;
      else if (isRS2FewZero)
        return kNormal_FewZero;
      else if (isRS2FewOne)
        return kNormal_FewOne;
      else
        return kNormal_Normal;
    }
    static TerarkIndex* CreateIndexWithSortedUintCont(RankSelect1& rankselect1, RankSelect2& rankselect2,
                                                      FixedLenStrVec& keyVec, const KeyStat& ks, 
                                                      uint64_t key1MinValue, uint64_t key1MaxValue, size_t key1_len,
                                                      valvec<byte_t>& minKey2Data, valvec<byte_t>& maxKey2Data) {
      const size_t kBlockUnits = 128;
      const size_t kLimit = (1ull << 48) - 1;
      uint64_t key2MinValue = ReadBigEndianUint64(minKey2Data);
      uint64_t key2MaxValue = ReadBigEndianUint64(maxKey2Data);
      unique_ptr<SortedUintVec:: Builder> builder(SortedUintVec::createBuilder(false, kBlockUnits));
      uint64_t prev = ReadBigEndianUint64(keyVec[0]) - key2MinValue;
      builder->push_back(prev);
      for (size_t i = 1; i < keyVec.size(); i++) {
        fstring str = keyVec[i];
        uint64_t key2 = ReadBigEndianUint64(str) - key2MinValue;
        if (terark_unlikely(abs_diff(key2, prev) > kLimit)) // should not use sorted uint vec
          return nullptr;
        prev = key2;
        builder->push_back(prev);
      }
      SortedUintVec uintVec;
      auto rs = builder->finish(&uintVec);
      if (rs.mem_size > keyVec.mem_size() / 2.0) // too much ram consumed
        return nullptr;
      SortedUintDataCont container;
      container.swap(uintVec);
      container.init(minKey2Data.size(), key2MinValue);
      return CreateIndex(rankselect1, rankselect2, container, ks, key1MinValue, key1MaxValue, 
                         key1_len, key2MinValue, key2MaxValue);
    }
    static TerarkIndex* CreateIndexWithUintCont(RankSelect1& rankselect1, RankSelect2& rankselect2,
                                                FixedLenStrVec& keyVec, const KeyStat& ks, 
                                                uint64_t key1MinValue, uint64_t key1MaxValue, size_t key1_len,
                                                valvec<byte_t>& minKey2Data, valvec<byte_t>& maxKey2Data) {
      uint64_t key2MinValue = ReadBigEndianUint64(minKey2Data);
      uint64_t key2MaxValue = ReadBigEndianUint64(maxKey2Data);
      uint64_t diff = key2MaxValue - key2MinValue + 1;
      size_t bitUsed = UintVecMin0::compute_uintbits(diff);
      if (bitUsed > keyVec.m_fixlen * 8 * 0.9) // compress ratio just so so
        return nullptr;
      // reuse memory from keyvec, since vecMin0 should consume less mem
      // compared with fixedlenvec
      UintVecMin0 vecMin0;
      vecMin0.risk_set_data(const_cast<byte_t*>(keyVec.data()), keyVec.size(), bitUsed);
      for (size_t i = 0; i < keyVec.size(); i++) {
        fstring str = keyVec[i];
        uint64_t key2 = ReadBigEndianUint64(str);
        vecMin0.set_wire(i, key2 - key2MinValue);
      }
      keyVec.risk_release_ownership();
      Min0DataCont container;
      container.swap(vecMin0);
      container.init(minKey2Data.size(), key2MinValue);
      return CreateIndex(rankselect1, rankselect2, container, ks, key1MinValue, key1MaxValue,
                         key1_len, key2MinValue, key2MaxValue);
    }
    static TerarkIndex* CreateIndexWithStrCont(RankSelect1& rankselect1, RankSelect2& rankselect2,
                                               FixedLenStrVec& keyVec, const KeyStat& ks, 
                                               uint64_t key1MinValue, uint64_t key1MaxValue, size_t key1_len) {
      StrDataCont container;
      container.swap(keyVec);
      return CreateIndex(rankselect1, rankselect2, container, ks, 
                         key1MinValue, key1MaxValue, key1_len, 0, 0);
    }

    template<class DataCont>
    static TerarkIndex* CreateIndex(RankSelect1& rankselect1, RankSelect2& rankselect2,
                                    DataCont& container, const KeyStat& ks,
                                    uint64_t key1MinValue, uint64_t key1MaxValue, size_t key1_len,
                                    uint64_t key2MinValue, uint64_t key2MaxValue) {
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

  public:
    unique_ptr<TerarkIndex> LoadMemory(fstring mem) const override {
      return UniquePtrOf(loadImpl(mem, {}));
    }
    unique_ptr<TerarkIndex> LoadFile(fstring fpath) const override {
      return UniquePtrOf(loadImpl({}, fpath));
    }
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
      size_t rankselect_2nd_sz = size_t(std::ceil(ks.numKeys * 1.25 / 8));
      size_t sum_key2_sz = std::ceil(ks.numKeys * key2_len);
      return rankselect_1st_sz + rankselect_2nd_sz + sum_key2_sz;
    }
  protected:
    TerarkIndex* loadImpl(fstring mem, fstring fpath) const {
      auto ptr = UniquePtrOf(
        new CompositeUintIndex<RankSelect1, RankSelect2, Key2DataContainer>());
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
      ptr->key2_min_value_ = header->key2_min_value;
      ptr->key2_max_value_ = header->key2_max_value;
      ptr->key1_len_ = header->key1_fixed_len;
      ptr->key2_len_ = header->key2_fixed_len;
      if (header->common_prefix_length > 0) {
        ptr->commonPrefix_.risk_set_data((char*)mem.data() + header->header_size,
                                         header->common_prefix_length);
      }
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
        ptr->key2_data_.init(header->key2_fixed_len, header->key2_min_value);
      } else if (std::is_same<Key2DataContainer, Min0DataCont>::value) {
        size_t num = ptr->rankselect2_.size() - 1; // sub the extra append '0'
        size_t diff = header->key2_max_value - header->key2_min_value + 1;
        ptr->key2_data_.risk_set_data((unsigned char*)mem.data() + offset,
                                      num, diff);
        ptr->key2_data_.init(header->key2_fixed_len, header->key2_min_value);
      } else { // FixedLenStr
        ptr->key2_data_.risk_set_data((unsigned char*)mem.data() + offset,
                                      header->key2_data_mem_size);
        ptr->key2_data_.init(header->key2_fixed_len,
                             header->key2_data_mem_size / header->key2_fixed_len);
      }
      return ptr.release();
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
        throw std::invalid_argument("CompositeUintIndex::Load(): Bad file header");
      }
      assert(VerifyClassName<CompositeUintIndex>(header->class_name));
      return true;
    }
  };

public:
  using TerarkIndex::FactoryPtr;
  CompositeUintIndex() {}
  CompositeUintIndex(RankSelect1& rankselect1, RankSelect2& rankselect2,
                     Key2DataContainer& key2Container, const KeyStat& ks,
                     uint64_t minValue, uint64_t maxValue, size_t key1_len,
                     uint64_t minKey2Value = 0, uint64_t maxKey2Value = 0) {
    isBuilding_ = true;
    size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
    // save meta into header
    FileHeader* header = new FileHeader(
      rankselect1.mem_size() + 
      rankselect2.mem_size() + 
      align_up(key2Container.mem_size(), 8));
    header->min_value = minValue;
    header->max_value = maxValue;
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
    isUserMemory_ = false;
  }

  virtual ~CompositeUintIndex() {
    if (isBuilding_) {
      delete (FileHeader*)header_;
    } else if (file_.base != nullptr || isUserMemory_) {
      rankselect1_.risk_release_ownership();
      rankselect2_.risk_release_ownership();
      key2_data_.risk_release_ownership();
      commonPrefix_.risk_release_ownership();
    }
  }
  const char* Name() const override {
    return header_->class_name;
  }
  void SaveMmap(std::function<void(const void *, size_t)> write) const override {
    write(header_, sizeof *header_);
    if (!commonPrefix_.empty()) {
      write(commonPrefix_.data(), commonPrefix_.size());
      AppendExtraZero(write, 8 - commonPrefix_.size() % 8);
    }
    write(rankselect1_.data(), rankselect1_.mem_size());
    write(rankselect2_.data(), rankselect2_.mem_size());
    write(key2_data_.data(), key2_data_.mem_size());
    AppendExtraZero(write, 8 - key2_data_.mem_size() % 8);
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
  size_t NumKeys() const override {
    return key2_data_.size();
  }
  size_t TotalKeySize() const override final {
    return (commonPrefix_.size() + key1_len_ + key2_len_) * key2_data_.size();
  }
  fstring Memory() const override {
    return fstring((const char*)header_, (ptrdiff_t)header_->file_size);
  }
  Iterator* NewIterator() const override {
    return new CompositeUintIndexIterator(*this);
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
  static uint64_t Read1stKey(const valvec<byte_t>& key, size_t cplen, size_t key1_len) {
    return ReadBigEndianUint64(key.begin() + cplen, key1_len);
  }
  static uint64_t Read1stKey(fstring key, size_t cplen, size_t key1_len) {
    return ReadBigEndianUint64((const byte_t*)key.data() + cplen, key1_len);
  }
  static uint64_t Read1stKey(const byte_t* key, size_t cplen, size_t key1_len) {
    return ReadBigEndianUint64(key + cplen, key1_len);
  }
  size_t Locate(const Key2DataContainer& arr,
                size_t start, size_t cnt, fstring target) const {
    size_t lo = start, hi = start + cnt;
    size_t pos = arr.lower_bound(lo, hi, target);
    return (pos < hi) ? pos : size_t(-1);
  }

protected:
  const FileHeader* header_;
  MmapWholeFile     file_;
  valvec<char>      commonPrefix_;
  RankSelect1       rankselect1_;
  RankSelect2       rankselect2_;
  Key2DataContainer key2_data_;
  uint64_t          minValue_;
  uint64_t          maxValue_;
  uint64_t          key2_min_value_;
  uint64_t          key2_max_value_;
  uint32_t          key1_len_;
  uint32_t          key2_len_;
  bool              isUserMemory_;
  bool              isBuilding_;
};

struct UintIndexBase : public TerarkIndex{
  static const char* index_name;
  struct MyBaseFileHeader : public TerarkIndexHeader
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

    MyBaseFileHeader(size_t body_size, const std::type_info& ti) {
      memset(this, 0, sizeof *this);
      magic_len = strlen(index_name);
      strncpy(magic, index_name, sizeof magic);
      size_t name_i = g_TerarkIndexName.find_i(ti.name());
      strncpy(class_name, g_TerarkIndexName.val(name_i).c_str(), sizeof class_name);
      header_size = sizeof *this;
      version = 1;
      file_size = sizeof *this + body_size;
    }
  };
};
const char* UintIndexBase::index_name = "UintIndex";

template<class RankSelect>
class UintIndex : public UintIndexBase {
public:
  struct FileHeader : public MyBaseFileHeader {
    FileHeader(size_t body_size)
      : MyBaseFileHeader(body_size, typeid(UintIndex)) {}
  };
  class UIntIndexIterator : public TerarkIndex::Iterator {
  public:
    UIntIndexIterator(const UintIndex& index) : index_(index) {
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
        assert(target.size() == cplen
            || byte_t(target[cplen]) != byte_t(index_.commonPrefix_[cplen]));
        if (target.size() == cplen
            || byte_t(target[cplen]) < byte_t(index_.commonPrefix_[cplen])) {
          SeekToFirst();
          return true;
        }
        else {
          m_id = size_t(-1);
          return false;
        }
      }
      target = target.substr(index_.commonPrefix_.size());
      /*
       *    target.size()     == 4;
       *    index_.keyLength_ == 6;
       *    | - - - - - - - - |  <- buffer
       *        | - - - - - - |  <- index
       *        | - - - - |      <- target
       */
      byte_t targetBuffer[8] = {};
      memcpy(targetBuffer + (8 - index_.keyLength_),
          target.data(), std::min<size_t>(index_.keyLength_, target.size()));
      uint64_t targetValue = ReadBigEndianUint64Aligned(targetBuffer, 8);
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
      SaveAsBigEndianUint64(buffer_.data() + index_.commonPrefix_.size(),
        buffer_.data() + buffer_.size(), pos_ + index_.minValue_);
    }
    size_t pos_;
    valvec<byte_t> buffer_;
    const UintIndex& index_;
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
      auto minValue = ReadBigEndianUint64(ks.minKey.begin() + cplen, ks.minKey.end());
      auto maxValue = ReadBigEndianUint64(ks.maxKey.begin() + cplen, ks.maxKey.end());
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
          auto cur = ReadBigEndianUint64(keyBuf.begin() + cplen, keyBuf.end());
          indexSeq.set1(cur - minValue);
        }
      }
      indexSeq.build_cache(false, false);

      auto ptr = UniquePtrOf(new UintIndex<RankSelect>());
      ptr->workingState_ = WorkingState::Building;

      FileHeader *header = new FileHeader(indexSeq.mem_size());
      header->min_value = minValue;
      header->max_value = maxValue;
      header->index_mem_size = indexSeq.mem_size();
      header->key_length = ks.minKeyLen - cplen;
      if (cplen > ks.commonPrefixLen) {
        header->common_prefix_length = cplen - ks.commonPrefixLen;
        ptr->commonPrefix_.assign(ks.minKey.data() + ks.commonPrefixLen,
          header->common_prefix_length);
        header->file_size += align_up(header->common_prefix_length, 8);
      }
      ptr->header_ = header;
      ptr->indexSeq_.swap(indexSeq);
      ptr->minValue_ = minValue;
      ptr->maxValue_ = maxValue;
      ptr->keyLength_ = header->key_length;
      return ptr.release();
    }
    unique_ptr<TerarkIndex> LoadMemory(fstring mem) const override {
      return UniquePtrOf(loadImpl(mem, {}));
    }
    unique_ptr<TerarkIndex> LoadFile(fstring fpath) const override {
      return UniquePtrOf(loadImpl({}, fpath));
    }
    size_t MemSizeForBuild(const KeyStat& ks) const override {
      assert(ks.minKeyLen == ks.maxKeyLen);
      size_t length = ks.maxKeyLen - commonPrefixLen(ks.minKey, ks.maxKey);
      auto minValue = ReadBigEndianUint64(ks.minKey.begin(), length);
      auto maxValue = ReadBigEndianUint64(ks.maxKey.begin(), length);
      if (minValue > maxValue) {
        std::swap(minValue, maxValue);
      }
      uint64_t diff = maxValue - minValue + 1;
      return size_t(std::ceil(diff * 1.25 / 8));
    }
  protected:
    TerarkIndex* loadImpl(fstring mem, fstring fpath) const {
      auto getHeader = [](fstring m) {
        const FileHeader* h = (const FileHeader*)m.data();
        if (m.size() < sizeof(FileHeader)
          || h->magic_len != strlen(index_name)
          || strcmp(h->magic, index_name) != 0
          || h->header_size != sizeof(FileHeader)
          || h->version != 1
          || h->file_size != m.size()
          ) {
          throw std::invalid_argument("UintIndex::Load(): Bad file header");
        }
        assert(VerifyClassName<UintIndex>(h->class_name));
        return h;
      };

      auto ptr = UniquePtrOf(new UintIndex());
      ptr->workingState_ = WorkingState::UserMemory;

      const FileHeader* header;
      if (mem.data() == nullptr) {
        MmapWholeFile mmapFile(fpath);
        mem = mmapFile.memory();
        ptr->header_ = header = getHeader(mem);
        MmapWholeFile().swap(mmapFile);
        ptr->workingState_ = WorkingState::MmapFile;
      }
      else {
        ptr->header_ = header = getHeader(mem);
      }
      ptr->minValue_ = header->min_value;
      ptr->maxValue_ = header->max_value;
      ptr->keyLength_ = header->key_length;
      if (header->common_prefix_length > 0) {
        ptr->commonPrefix_.risk_set_data((char*)mem.data() + header->header_size,
          header->common_prefix_length);
      }
      ptr->indexSeq_.risk_mmap_from((unsigned char*)mem.data() + header->header_size
        + align_up(header->common_prefix_length, 8), header->index_mem_size);
      return ptr.release();
    }
  };
  using TerarkIndex::FactoryPtr;
  virtual ~UintIndex() {
    if (workingState_ == WorkingState::Building) {
      delete (FileHeader*)header_;
    }
    else {
      indexSeq_.risk_release_ownership();
      commonPrefix_.risk_release_ownership();
      if (workingState_ == WorkingState::MmapFile) {
        terark::mmap_close((void*)header_, header_->file_size);
      }
    }
  }
  const char* Name() const override {
    return header_->class_name;
  }
  void SaveMmap(std::function<void(const void *, size_t)> write) const override {
    write(header_, sizeof *header_);
    if (!commonPrefix_.empty()) {
      write(commonPrefix_.data(), commonPrefix_.size());
      AppendExtraZero(write, 8 - commonPrefix_.size() % 8);
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
    uint64_t findValue = ReadBigEndianUint64(key);
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
  valvec<char>      commonPrefix_;
  RankSelect        indexSeq_;
  uint64_t          minValue_;
  uint64_t          maxValue_;
  uint32_t          keyLength_;
  enum class WorkingState : uint32_t {
    Building = 1,
    UserMemory = 2,
    MmapFile = 3,
  }                 workingState_;
};

#endif // TerocksPrivateCode

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

TerarkIndexRegisterImp(TrieDAWG_IL_256_32, "NestLoudsTrieDAWG_IL", "IL_256_32", "IL_256", "NestLoudsTrieDAWG_IL_256");
TerarkIndexRegisterNLT(SE_512_64);
TerarkIndexRegisterNLT(Mixed_SE_512);
TerarkIndexRegisterNLT(Mixed_IL_256);
TerarkIndexRegisterNLT(Mixed_XL_256);

TerarkIndexRegisterNLT(IL_256_32_FL);
TerarkIndexRegisterNLT(SE_512_64_FL);
TerarkIndexRegisterNLT(Mixed_SE_512_32_FL);
TerarkIndexRegisterNLT(Mixed_IL_256_32_FL);
TerarkIndexRegisterNLT(Mixed_XL_256_32_FL);

#if defined(TerocksPrivateCode)

/*enum AmazingCombinationT {
  kAllOne_AllZero = 0,
  kAllOne_FewZero,
  kAllOne_Normal,

  kFewZero_AllZero,
  kFewZero_FewZero,
  kFewZero_Normal,
    
  kFewOne_AllZero,
  kFewOne_FewZero,
  kFewOne_Normal,      

  kNormal_AllZero,
  kNormal_FewZero,
  kNormal_Normal
  };*/

typedef rank_select_fewzero<uint32_t> rs_fewzero_32;
typedef rank_select_fewzero<uint64_t> rs_fewzero_64;
typedef rank_select_fewone<uint32_t> rs_fewone_32;
typedef rank_select_fewone<uint64_t> rs_fewone_64;
typedef CompositeKeyDataContainer<SortedUintVec>  CKSortedUintDataCont;
typedef CompositeKeyDataContainer<UintVecMin0>    CKMin0DataCont;
typedef CompositeKeyDataContainer<FixedLenStrVec> CKStrDataCont;

// ---- CKSortedUintDataCont
// allone allzero
typedef CompositeUintIndex<rank_select_allone   , rank_select_allzero  , CKSortedUintDataCont> CompositeUintIndex_AllOne_AllZero_SortedUint;
TerarkIndexRegister(CompositeUintIndex_AllOne_AllZero_SortedUint);
// allone fewzero
typedef CompositeUintIndex<rank_select_allone   , rs_fewzero_32  ,       CKSortedUintDataCont> CompositeUintIndex_AllOne_FewZero32_SortedUint;
typedef CompositeUintIndex<rank_select_allone   , rs_fewzero_64  ,       CKSortedUintDataCont> CompositeUintIndex_AllOne_FewZero64_SortedUint;
TerarkIndexRegister(CompositeUintIndex_AllOne_FewZero32_SortedUint);
TerarkIndexRegister(CompositeUintIndex_AllOne_FewZero64_SortedUint);
// allone fewone
typedef CompositeUintIndex<rank_select_allone   , rs_fewone_32  ,       CKSortedUintDataCont> CompositeUintIndex_AllOne_FewOne32_SortedUint;
typedef CompositeUintIndex<rank_select_allone   , rs_fewone_64  ,       CKSortedUintDataCont> CompositeUintIndex_AllOne_FewOne64_SortedUint;
TerarkIndexRegister(CompositeUintIndex_AllOne_FewOne32_SortedUint);
TerarkIndexRegister(CompositeUintIndex_AllOne_FewOne64_SortedUint);
// allone normal
typedef CompositeUintIndex<rank_select_allone   , rank_select_il_256_32, CKSortedUintDataCont> CompositeUintIndex_AllOne_IL_256_32_SortedUint;
typedef CompositeUintIndex<rank_select_allone   , rank_select_se_512_64, CKSortedUintDataCont> CompositeUintIndex_AllOne_SE_512_64_SortedUint;
TerarkIndexRegister(CompositeUintIndex_AllOne_IL_256_32_SortedUint);
TerarkIndexRegister(CompositeUintIndex_AllOne_SE_512_64_SortedUint);

// fewzero allzero
typedef CompositeUintIndex<rs_fewzero_32        , rank_select_allzero  , CKSortedUintDataCont> CompositeUintIndex_FewZero32_AllZero_SortedUint;
typedef CompositeUintIndex<rs_fewzero_64        , rank_select_allzero  , CKSortedUintDataCont> CompositeUintIndex_FewZero64_AllZero_SortedUint;
TerarkIndexRegister(CompositeUintIndex_FewZero32_AllZero_SortedUint);
TerarkIndexRegister(CompositeUintIndex_FewZero64_AllZero_SortedUint);
// fewzero fewzero
typedef CompositeUintIndex<rs_fewzero_32        , rs_fewzero_32        , CKSortedUintDataCont> CompositeUintIndex_FewZero32_FewZero32_SortedUint;
typedef CompositeUintIndex<rs_fewzero_64        , rs_fewzero_64        , CKSortedUintDataCont> CompositeUintIndex_FewZero64_FewZero64_SortedUint;
TerarkIndexRegister(CompositeUintIndex_FewZero32_FewZero32_SortedUint);
TerarkIndexRegister(CompositeUintIndex_FewZero64_FewZero64_SortedUint);
// fewzero fewone
typedef CompositeUintIndex<rs_fewzero_32        , rs_fewone_32        , CKSortedUintDataCont> CompositeUintIndex_FewZero32_FewOne32_SortedUint;
typedef CompositeUintIndex<rs_fewzero_64        , rs_fewone_64        , CKSortedUintDataCont> CompositeUintIndex_FewZero64_FewOne64_SortedUint;
TerarkIndexRegister(CompositeUintIndex_FewZero32_FewOne32_SortedUint);
TerarkIndexRegister(CompositeUintIndex_FewZero64_FewOne64_SortedUint);
// fewzero normal
typedef CompositeUintIndex<rs_fewzero_32        , rank_select_il_256_32, CKSortedUintDataCont> CompositeUintIndex_FewZero32_IL_256_32_SortedUint;
typedef CompositeUintIndex<rs_fewzero_64        , rank_select_se_512_64, CKSortedUintDataCont> CompositeUintIndex_FewZero64_SE_512_64_SortedUint;
TerarkIndexRegister(CompositeUintIndex_FewZero32_IL_256_32_SortedUint);
TerarkIndexRegister(CompositeUintIndex_FewZero64_SE_512_64_SortedUint);

// fewone allzero
typedef CompositeUintIndex<rs_fewone_32        , rank_select_allzero  , CKSortedUintDataCont> CompositeUintIndex_FewOne32_AllZero_SortedUint;
typedef CompositeUintIndex<rs_fewone_64        , rank_select_allzero  , CKSortedUintDataCont> CompositeUintIndex_FewOne64_AllZero_SortedUint;
TerarkIndexRegister(CompositeUintIndex_FewOne32_AllZero_SortedUint);
TerarkIndexRegister(CompositeUintIndex_FewOne64_AllZero_SortedUint);
// fewone fewzero
typedef CompositeUintIndex<rs_fewone_32        , rs_fewzero_32        , CKSortedUintDataCont> CompositeUintIndex_FewOne32_FewZero32_SortedUint;
typedef CompositeUintIndex<rs_fewone_64        , rs_fewzero_64        , CKSortedUintDataCont> CompositeUintIndex_FewOne64_FewZero64_SortedUint;
TerarkIndexRegister(CompositeUintIndex_FewOne32_FewZero32_SortedUint);
TerarkIndexRegister(CompositeUintIndex_FewOne64_FewZero64_SortedUint);
// fewone fewone
typedef CompositeUintIndex<rs_fewone_32        , rs_fewone_32        , CKSortedUintDataCont> CompositeUintIndex_FewOne32_FewOne32_SortedUint;
typedef CompositeUintIndex<rs_fewone_64        , rs_fewone_64        , CKSortedUintDataCont> CompositeUintIndex_FewOne64_FewOne64_SortedUint;
TerarkIndexRegister(CompositeUintIndex_FewOne32_FewOne32_SortedUint);
TerarkIndexRegister(CompositeUintIndex_FewOne64_FewOne64_SortedUint);
// fewone normal
typedef CompositeUintIndex<rs_fewone_32        , rank_select_il_256_32, CKSortedUintDataCont> CompositeUintIndex_FewOne32_IL_256_32_SortedUint;
typedef CompositeUintIndex<rs_fewone_64        , rank_select_se_512_64, CKSortedUintDataCont> CompositeUintIndex_FewOne64_SE_512_64_SortedUint;
TerarkIndexRegister(CompositeUintIndex_FewOne32_IL_256_32_SortedUint);
TerarkIndexRegister(CompositeUintIndex_FewOne64_SE_512_64_SortedUint);

// normal allzero
typedef CompositeUintIndex<rank_select_il_256_32, rank_select_allzero  , CKSortedUintDataCont> CompositeUintIndex_IL_256_32_AllZero_SortedUint;
typedef CompositeUintIndex<rank_select_se_512_64, rank_select_allzero  , CKSortedUintDataCont> CompositeUintIndex_SE_512_64_AllZero_SortedUint;
TerarkIndexRegister(CompositeUintIndex_IL_256_32_AllZero_SortedUint);
TerarkIndexRegister(CompositeUintIndex_SE_512_64_AllZero_SortedUint);
// normal fewzero
typedef CompositeUintIndex<rank_select_il_256_32, rs_fewzero_32        , CKSortedUintDataCont> CompositeUintIndex_IL_256_32_FewZero32_SortedUint;
typedef CompositeUintIndex<rank_select_se_512_64, rs_fewzero_64        , CKSortedUintDataCont> CompositeUintIndex_SE_512_64_FewZero64_SortedUint;
TerarkIndexRegister(CompositeUintIndex_IL_256_32_FewZero32_SortedUint);
TerarkIndexRegister(CompositeUintIndex_SE_512_64_FewZero64_SortedUint);
// normal fewone
typedef CompositeUintIndex<rank_select_il_256_32, rs_fewone_32        , CKSortedUintDataCont> CompositeUintIndex_IL_256_32_FewOne32_SortedUint;
typedef CompositeUintIndex<rank_select_se_512_64, rs_fewone_64        , CKSortedUintDataCont> CompositeUintIndex_SE_512_64_FewOne64_SortedUint;
TerarkIndexRegister(CompositeUintIndex_IL_256_32_FewOne32_SortedUint);
TerarkIndexRegister(CompositeUintIndex_SE_512_64_FewOne64_SortedUint);
// normal normal
typedef CompositeUintIndex<rank_select_il_256_32, rank_select_il_256_32, CKSortedUintDataCont> CompositeUintIndex_IL_256_32_IL_256_32_SortedUint;
typedef CompositeUintIndex<rank_select_se_512_64, rank_select_se_512_64, CKSortedUintDataCont> CompositeUintIndex_SE_512_64_SE_512_64_SortedUint;
TerarkIndexRegister(CompositeUintIndex_IL_256_32_IL_256_32_SortedUint);
TerarkIndexRegister(CompositeUintIndex_SE_512_64_SE_512_64_SortedUint);

// ---- CKMin0DataCont, _Uint
// allone allzero
typedef CompositeUintIndex<rank_select_allone   , rank_select_allzero  , CKMin0DataCont> CompositeUintIndex_AllOne_AllZero_Uint;
TerarkIndexRegister(CompositeUintIndex_AllOne_AllZero_Uint);
// allone fewzero
typedef CompositeUintIndex<rank_select_allone   , rs_fewzero_32  ,       CKMin0DataCont> CompositeUintIndex_AllOne_FewZero32_Uint;
typedef CompositeUintIndex<rank_select_allone   , rs_fewzero_64  ,       CKMin0DataCont> CompositeUintIndex_AllOne_FewZero64_Uint;
TerarkIndexRegister(CompositeUintIndex_AllOne_FewZero32_Uint);
TerarkIndexRegister(CompositeUintIndex_AllOne_FewZero64_Uint);
// allone fewone
typedef CompositeUintIndex<rank_select_allone   , rs_fewone_32  ,       CKMin0DataCont> CompositeUintIndex_AllOne_FewOne32_Uint;
typedef CompositeUintIndex<rank_select_allone   , rs_fewone_64  ,       CKMin0DataCont> CompositeUintIndex_AllOne_FewOne64_Uint;
TerarkIndexRegister(CompositeUintIndex_AllOne_FewOne32_Uint);
TerarkIndexRegister(CompositeUintIndex_AllOne_FewOne64_Uint);
// allone normal
typedef CompositeUintIndex<rank_select_allone   , rank_select_il_256_32, CKMin0DataCont> CompositeUintIndex_AllOne_IL_256_32_Uint;
typedef CompositeUintIndex<rank_select_allone   , rank_select_se_512_64, CKMin0DataCont> CompositeUintIndex_AllOne_SE_512_64_Uint;
TerarkIndexRegister(CompositeUintIndex_AllOne_IL_256_32_Uint);
TerarkIndexRegister(CompositeUintIndex_AllOne_SE_512_64_Uint);

// fewzero allzero
typedef CompositeUintIndex<rs_fewzero_32        , rank_select_allzero  , CKMin0DataCont> CompositeUintIndex_FewZero32_AllZero_Uint;
typedef CompositeUintIndex<rs_fewzero_64        , rank_select_allzero  , CKMin0DataCont> CompositeUintIndex_FewZero64_AllZero_Uint;
TerarkIndexRegister(CompositeUintIndex_FewZero32_AllZero_Uint);
TerarkIndexRegister(CompositeUintIndex_FewZero64_AllZero_Uint);
// fewzero fewzero
typedef CompositeUintIndex<rs_fewzero_32        , rs_fewzero_32        , CKMin0DataCont> CompositeUintIndex_FewZero32_FewZero32_Uint;
typedef CompositeUintIndex<rs_fewzero_64        , rs_fewzero_64        , CKMin0DataCont> CompositeUintIndex_FewZero64_FewZero64_Uint;
TerarkIndexRegister(CompositeUintIndex_FewZero32_FewZero32_Uint);
TerarkIndexRegister(CompositeUintIndex_FewZero64_FewZero64_Uint);
// fewzero fewone
typedef CompositeUintIndex<rs_fewzero_32        , rs_fewone_32        , CKMin0DataCont> CompositeUintIndex_FewZero32_FewOne32_Uint;
typedef CompositeUintIndex<rs_fewzero_64        , rs_fewone_64        , CKMin0DataCont> CompositeUintIndex_FewZero64_FewOne64_Uint;
TerarkIndexRegister(CompositeUintIndex_FewZero32_FewOne32_Uint);
TerarkIndexRegister(CompositeUintIndex_FewZero64_FewOne64_Uint);
// fewzero normal
typedef CompositeUintIndex<rs_fewzero_32        , rank_select_il_256_32, CKMin0DataCont> CompositeUintIndex_FewZero32_IL_256_32_Uint;
typedef CompositeUintIndex<rs_fewzero_64        , rank_select_se_512_64, CKMin0DataCont> CompositeUintIndex_FewZero64_SE_512_64_Uint;
TerarkIndexRegister(CompositeUintIndex_FewZero32_IL_256_32_Uint);
TerarkIndexRegister(CompositeUintIndex_FewZero64_SE_512_64_Uint);

// fewone allzero
typedef CompositeUintIndex<rs_fewone_32        , rank_select_allzero  , CKMin0DataCont> CompositeUintIndex_FewOne32_AllZero_Uint;
typedef CompositeUintIndex<rs_fewone_64        , rank_select_allzero  , CKMin0DataCont> CompositeUintIndex_FewOne64_AllZero_Uint;
TerarkIndexRegister(CompositeUintIndex_FewOne32_AllZero_Uint);
TerarkIndexRegister(CompositeUintIndex_FewOne64_AllZero_Uint);
// fewone fewzero
typedef CompositeUintIndex<rs_fewone_32        , rs_fewzero_32        , CKMin0DataCont> CompositeUintIndex_FewOne32_FewZero32_Uint;
typedef CompositeUintIndex<rs_fewone_64        , rs_fewzero_64        , CKMin0DataCont> CompositeUintIndex_FewOne64_FewZero64_Uint;
TerarkIndexRegister(CompositeUintIndex_FewOne32_FewZero32_Uint);
TerarkIndexRegister(CompositeUintIndex_FewOne64_FewZero64_Uint);
// fewone fewone
typedef CompositeUintIndex<rs_fewone_32        , rs_fewone_32        , CKMin0DataCont> CompositeUintIndex_FewOne32_FewOne32_Uint;
typedef CompositeUintIndex<rs_fewone_64        , rs_fewone_64        , CKMin0DataCont> CompositeUintIndex_FewOne64_FewOne64_Uint;
TerarkIndexRegister(CompositeUintIndex_FewOne32_FewOne32_Uint);
TerarkIndexRegister(CompositeUintIndex_FewOne64_FewOne64_Uint);
// fewone normal
typedef CompositeUintIndex<rs_fewone_32        , rank_select_il_256_32, CKMin0DataCont> CompositeUintIndex_FewOne32_IL_256_32_Uint;
typedef CompositeUintIndex<rs_fewone_64        , rank_select_se_512_64, CKMin0DataCont> CompositeUintIndex_FewOne64_SE_512_64_Uint;
TerarkIndexRegister(CompositeUintIndex_FewOne32_IL_256_32_Uint);
TerarkIndexRegister(CompositeUintIndex_FewOne64_SE_512_64_Uint);

// normal allzero
typedef CompositeUintIndex<rank_select_il_256_32, rank_select_allzero  , CKMin0DataCont> CompositeUintIndex_IL_256_32_AllZero_Uint;
typedef CompositeUintIndex<rank_select_se_512_64, rank_select_allzero  , CKMin0DataCont> CompositeUintIndex_SE_512_64_AllZero_Uint;
TerarkIndexRegister(CompositeUintIndex_IL_256_32_AllZero_Uint);
TerarkIndexRegister(CompositeUintIndex_SE_512_64_AllZero_Uint);
// normal fewzero
typedef CompositeUintIndex<rank_select_il_256_32, rs_fewzero_32        , CKMin0DataCont> CompositeUintIndex_IL_256_32_FewZero32_Uint;
typedef CompositeUintIndex<rank_select_se_512_64, rs_fewzero_64        , CKMin0DataCont> CompositeUintIndex_SE_512_64_FewZero64_Uint;
TerarkIndexRegister(CompositeUintIndex_IL_256_32_FewZero32_Uint);
TerarkIndexRegister(CompositeUintIndex_SE_512_64_FewZero64_Uint);
// normal fewone
typedef CompositeUintIndex<rank_select_il_256_32, rs_fewone_32        , CKMin0DataCont> CompositeUintIndex_IL_256_32_FewOne32_Uint;
typedef CompositeUintIndex<rank_select_se_512_64, rs_fewone_64        , CKMin0DataCont> CompositeUintIndex_SE_512_64_FewOne64_Uint;
TerarkIndexRegister(CompositeUintIndex_IL_256_32_FewOne32_Uint);
TerarkIndexRegister(CompositeUintIndex_SE_512_64_FewOne64_Uint);
// normal normal
typedef CompositeUintIndex<rank_select_il_256_32, rank_select_il_256_32, CKMin0DataCont> CompositeUintIndex_IL_256_32_IL_256_32_Uint;
typedef CompositeUintIndex<rank_select_se_512_64, rank_select_se_512_64, CKMin0DataCont> CompositeUintIndex_SE_512_64_SE_512_64_Uint;
TerarkIndexRegister(CompositeUintIndex_IL_256_32_IL_256_32_Uint);
TerarkIndexRegister(CompositeUintIndex_SE_512_64_SE_512_64_Uint);




// ---- CKStrDataCont, 
// allone allzero
typedef CompositeUintIndex<rank_select_allone   , rank_select_allzero  , CKStrDataCont> CompositeUintIndex_AllOne_AllZero;
TerarkIndexRegister(CompositeUintIndex_AllOne_AllZero);
// allone fewzero
typedef CompositeUintIndex<rank_select_allone   , rs_fewzero_32  ,       CKStrDataCont> CompositeUintIndex_AllOne_FewZero32;
typedef CompositeUintIndex<rank_select_allone   , rs_fewzero_64  ,       CKStrDataCont> CompositeUintIndex_AllOne_FewZero64;
TerarkIndexRegister(CompositeUintIndex_AllOne_FewZero32);
TerarkIndexRegister(CompositeUintIndex_AllOne_FewZero64);
// allone fewone
typedef CompositeUintIndex<rank_select_allone   , rs_fewone_32  ,       CKStrDataCont> CompositeUintIndex_AllOne_FewOne32;
typedef CompositeUintIndex<rank_select_allone   , rs_fewone_64  ,       CKStrDataCont> CompositeUintIndex_AllOne_FewOne64;
TerarkIndexRegister(CompositeUintIndex_AllOne_FewOne32);
TerarkIndexRegister(CompositeUintIndex_AllOne_FewOne64);
// allone normal
typedef CompositeUintIndex<rank_select_allone   , rank_select_il_256_32, CKStrDataCont> CompositeUintIndex_AllOne_IL_256_32;
typedef CompositeUintIndex<rank_select_allone   , rank_select_se_512_64, CKStrDataCont> CompositeUintIndex_AllOne_SE_512_64;
TerarkIndexRegister(CompositeUintIndex_AllOne_IL_256_32);
TerarkIndexRegister(CompositeUintIndex_AllOne_SE_512_64);

// fewzero allzero
typedef CompositeUintIndex<rs_fewzero_32        , rank_select_allzero  , CKStrDataCont> CompositeUintIndex_FewZero32_AllZero;
typedef CompositeUintIndex<rs_fewzero_64        , rank_select_allzero  , CKStrDataCont> CompositeUintIndex_FewZero64_AllZero;
TerarkIndexRegister(CompositeUintIndex_FewZero32_AllZero);
TerarkIndexRegister(CompositeUintIndex_FewZero64_AllZero);
// fewzero fewzero
typedef CompositeUintIndex<rs_fewzero_32        , rs_fewzero_32        , CKStrDataCont> CompositeUintIndex_FewZero32_FewZero32;
typedef CompositeUintIndex<rs_fewzero_64        , rs_fewzero_64        , CKStrDataCont> CompositeUintIndex_FewZero64_FewZero64;
TerarkIndexRegister(CompositeUintIndex_FewZero32_FewZero32);
TerarkIndexRegister(CompositeUintIndex_FewZero64_FewZero64);
// fewzero fewone
typedef CompositeUintIndex<rs_fewzero_32        , rs_fewone_32        , CKStrDataCont> CompositeUintIndex_FewZero32_FewOne32;
typedef CompositeUintIndex<rs_fewzero_64        , rs_fewone_64        , CKStrDataCont> CompositeUintIndex_FewZero64_FewOne64;
TerarkIndexRegister(CompositeUintIndex_FewZero32_FewOne32);
TerarkIndexRegister(CompositeUintIndex_FewZero64_FewOne64);
// fewzero normal
typedef CompositeUintIndex<rs_fewzero_32        , rank_select_il_256_32, CKStrDataCont> CompositeUintIndex_FewZero32_IL_256_32;
typedef CompositeUintIndex<rs_fewzero_64        , rank_select_se_512_64, CKStrDataCont> CompositeUintIndex_FewZero64_SE_512_64;
TerarkIndexRegister(CompositeUintIndex_FewZero32_IL_256_32);
TerarkIndexRegister(CompositeUintIndex_FewZero64_SE_512_64);

// fewone allzero
typedef CompositeUintIndex<rs_fewone_32        , rank_select_allzero  , CKStrDataCont> CompositeUintIndex_FewOne32_AllZero;
typedef CompositeUintIndex<rs_fewone_64        , rank_select_allzero  , CKStrDataCont> CompositeUintIndex_FewOne64_AllZero;
TerarkIndexRegister(CompositeUintIndex_FewOne32_AllZero);
TerarkIndexRegister(CompositeUintIndex_FewOne64_AllZero);
// fewone fewzero
typedef CompositeUintIndex<rs_fewone_32        , rs_fewzero_32        , CKStrDataCont> CompositeUintIndex_FewOne32_FewZero32;
typedef CompositeUintIndex<rs_fewone_64        , rs_fewzero_64        , CKStrDataCont> CompositeUintIndex_FewOne64_FewZero64;
TerarkIndexRegister(CompositeUintIndex_FewOne32_FewZero32);
TerarkIndexRegister(CompositeUintIndex_FewOne64_FewZero64);
// fewone fewone
typedef CompositeUintIndex<rs_fewone_32        , rs_fewone_32        , CKStrDataCont> CompositeUintIndex_FewOne32_FewOne32;
typedef CompositeUintIndex<rs_fewone_64        , rs_fewone_64        , CKStrDataCont> CompositeUintIndex_FewOne64_FewOne64;
TerarkIndexRegister(CompositeUintIndex_FewOne32_FewOne32);
TerarkIndexRegister(CompositeUintIndex_FewOne64_FewOne64);
// fewone normal
typedef CompositeUintIndex<rs_fewone_32        , rank_select_il_256_32, CKStrDataCont> CompositeUintIndex_FewOne32_IL_256_32;
typedef CompositeUintIndex<rs_fewone_64        , rank_select_se_512_64, CKStrDataCont> CompositeUintIndex_FewOne64_SE_512_64;
TerarkIndexRegister(CompositeUintIndex_FewOne32_IL_256_32);
TerarkIndexRegister(CompositeUintIndex_FewOne64_SE_512_64);

// normal allzero
typedef CompositeUintIndex<rank_select_il_256_32, rank_select_allzero  , CKStrDataCont> CompositeUintIndex_IL_256_32_AllZero;
typedef CompositeUintIndex<rank_select_se_512_64, rank_select_allzero  , CKStrDataCont> CompositeUintIndex_SE_512_64_AllZero;
TerarkIndexRegister(CompositeUintIndex_IL_256_32_AllZero);
TerarkIndexRegister(CompositeUintIndex_SE_512_64_AllZero);
// normal fewzero
typedef CompositeUintIndex<rank_select_il_256_32, rs_fewzero_32        , CKStrDataCont> CompositeUintIndex_IL_256_32_FewZero32;
typedef CompositeUintIndex<rank_select_se_512_64, rs_fewzero_64        , CKStrDataCont> CompositeUintIndex_SE_512_64_FewZero64;
TerarkIndexRegister(CompositeUintIndex_IL_256_32_FewZero32);
TerarkIndexRegister(CompositeUintIndex_SE_512_64_FewZero64);
// normal fewone
typedef CompositeUintIndex<rank_select_il_256_32, rs_fewone_32        , CKStrDataCont> CompositeUintIndex_IL_256_32_FewOne32;
typedef CompositeUintIndex<rank_select_se_512_64, rs_fewone_64        , CKStrDataCont> CompositeUintIndex_SE_512_64_FewOne64;
TerarkIndexRegister(CompositeUintIndex_IL_256_32_FewOne32);
TerarkIndexRegister(CompositeUintIndex_SE_512_64_FewOne64);
// normal normal
typedef CompositeUintIndex<rank_select_il_256_32, rank_select_il_256_32, CKStrDataCont> CompositeUintIndex_IL_256_32_IL_256_32;
typedef CompositeUintIndex<rank_select_se_512_64, rank_select_se_512_64, CKStrDataCont> CompositeUintIndex_SE_512_64_SE_512_64;
TerarkIndexRegister(CompositeUintIndex_IL_256_32_IL_256_32);
TerarkIndexRegister(CompositeUintIndex_SE_512_64_SE_512_64);





typedef UintIndex<rank_select_il_256_32> UintIndex_IL_256_32;
typedef UintIndex<rank_select_se_512_64> UintIndex_SE_512_64;
typedef UintIndex<rank_select_allone>    UintIndex_AllOne;
TerarkIndexRegister(UintIndex_IL_256_32);
TerarkIndexRegister(UintIndex_SE_512_64);
TerarkIndexRegister(UintIndex_AllOne);

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
