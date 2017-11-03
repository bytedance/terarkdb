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

const TerarkIndex::Factory*
TerarkIndex::SelectFactory(const KeyStat& ks, fstring name) {
#if defined(TerocksPrivateCode)
  if (ks.maxKeyLen == ks.minKeyLen &&
      ks.maxKeyLen - ks.commonPrefixLen <= sizeof(uint64_t)) {
    uint64_t
      minValue = ReadUint64(ks.minKey.begin() + ks.commonPrefixLen, ks.minKey.end()),
      maxValue = ReadUint64(ks.maxKey.begin() + ks.commonPrefixLen, ks.maxKey.end());
    uint64_t diff = (minValue < maxValue ? maxValue - minValue : minValue - maxValue) + 1;
    if (diff < ks.numKeys * 30) {
      if (diff < (4ull << 30)) {
        return GetFactory("UintIndex");
      }
      else {
        return GetFactory("UintIndex_SE_512_64");
      }
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
  /*
   * 1. assume index1_len = 8, ks.commonPrefixLen = 0, assume at least 2 index1 exist. right now.
   * 2. add indexlen detector, dynamically detect the length of index1, index2...
   * 3. for fixed len index_2nd+, we could have their common prefix as well.
   */
  template<class RankSelect1st, class RankSelect2nd>
  class TerarkCompositeIndex : public TerarkIndex {
  public:
    static const int kCommonPrefixLen = 4;
    static const int kIndex1stLen = 8;
    static const int kIndex2ndLen = 8;

    static const char* index_name;
    struct FileHeader : public TerarkIndexHeader {
      uint64_t min_value;
      uint64_t max_value;
      uint64_t index_1st_mem_size;
      uint64_t index_2nd_mem_size;
      uint64_t index_data_mem_size;
      uint32_t key_length;
      /*
       * (Rocksdb) For one huge index, we'll split it into multipart-index for the sake of RAM, 
       * and each sub-index could have longer commonPrefix compared with ks.commonPrefix.
       * what's more, under such circumstances, ks.commonPrefix may have been rewritten
       * be upper-level builder to '0'. here, 
       * common_prefix_length = sub-index.commonPrefixLen - whole-index.commonPrefixLen
       */
      uint32_t common_prefix_length;

      FileHeader(size_t body_size) {
        memset(this, 0, sizeof *this);
        magic_len = strlen(index_name);
        strncpy(magic, index_name, sizeof magic);
        size_t name_i = g_TerarkIndexName.find_i(
          typeid(TerarkCompositeIndex<RankSelect1st, RankSelect2nd>).name());
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
        //pos_ = size_t(-1);
        m_id = size_t(-1);
        buffer_.resize_no_init(index_.commonPrefix_.size() + index_.keyLength_);
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
        if (target.size() <= kIndex1stLen) {
          byte_t targetBuffer[8] = { 0 };
          memcpy(targetBuffer + (8 - target.size()),
                 target.data(), target.size());
          index1st = Read1stIndex(fstring(targetBuffer, targetBuffer + 8), cplen);
        } else {
          index1st = Read1stIndex(target, cplen);
        }
        if (index1st > index_.maxValue_) {
          m_id = size_t(-1);
          return false;
        } else if (index1st < index_.minValue_) {
          return SeekToFirst();
        }
        // find the corresponding bit within 2ndRS
        offset_1st_ = index1st - index_.minValue_;
        fstring index2nd = target.substr(kIndex1stLen);
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

      /*
       * use 2nd index bitmap to check if 1st index changed
       */
      bool IsIndexDiff(size_t lid, size_t rid) {
        /*
         * case1: 0 1 1 0, ...
         * case2: 0 0, ...
         *   the 2nd 0 means another index started
         */
        return (index_.index2ndRS_[lid] > index_.index2ndRS_[rid] ||
                index_.index2ndRS_[lid] == index_.index2ndRS_[rid] == 0);
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
        // key = commonprefix + index1st + index2nd
        // assign index 1st
        size_t offset = index_.commonPrefix_.size();
        AssignUint64(buffer_.data() + offset,
                     buffer_.data() + kIndex1stLen,
                     offset_1st_ + index_.minValue_);
        // assign index 2nd
        offset = kIndex1stLen;
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
            ks.maxKeyLen <= sizeof(uint64_t)) {
          // if (ks.maxKeyLen - cplen > sizeof(uint64_t)) {
          printf("diff ken len, maxlen %d, minlen %d\n", ks.maxKeyLen, ks.minKeyLen);
          abort();
        }
        /*
         * if rocksdb reverse comparator is used, then minValue
         * is actually the largetst one
         */
        uint64_t
          minValue = Read1stIndex(ks.minKey, cplen),
          maxValue = Read1stIndex(ks.maxKey, cplen);
        if (minValue > maxValue) {
          std::swap(minValue, maxValue);
        }
        uint64_t diff = maxValue - minValue + 1;
        /*
         * 1stRS stores bitmap [minvalue, maxvalue] for index1st
         * 2ndRS stores bitmap [keystart, keyend] for composite index
         * order of rs2 follows the order of index data, and order 
         * of rs1 follows the order of rs2
         */
        RankSelect1st index1stRS(diff);
        RankSelect2nd index2ndRS(ks.numKeys + 1);
        valvec<byte_t> keyBuf;
        uint64_t prev = size_t(-1);
        size_t sumRealKeyLen = (ks.maxKeyLen - kIndex1stLen) * ks.numKeys;
        FixedLenStrVec keyVec(ks.maxKeyLen - kIndex1stLen);
        keyVec.reserve(ks.numKeys, sumRealKeyLen);
        if (ks.minKey < ks.maxKey) {
          for (size_t i = 0; i < ks.numKeys; ++i) {
            reader >> keyBuf;
            uint64_t offset = Read1stIndex(keyBuf, cplen) - minValue;
            index1stRS.set1(offset);
            if (offset != prev) { // new index1 encountered
              index2ndRS.set0(i);
            } else {
              index2ndRS.set1(i);
            }
            prev = offset;
            // save index data
            keyVec.push_back(fstring(keyBuf).substr(kIndex1stLen));
          }
        } else {
          //keyVec.m_offsets.resize_with_wire_max_val(numKeys + 1, sumRealKeyLen);
          //keyVec.m_offsets.set_wire(numKeys, sumRealKeyLen);
          //keyVec.m_strpool.resize(sumRealKeyLen);
          size_t offset = sumRealKeyLen;
          for (size_t i = ks.numKeys - 1; i >= 0; --i) {
            reader >> keyBuf;
            uint64_t offset = Read1stIndex(keyBuf, cplen) - minValue;
            index1stRS.set1(offset);
            if (offset != prev) { // next index1 is new one
              index2ndRS.set0(i + 1);
            } else {
              index2ndRS.set1(i + 1);
            }
            prev = offset;
            // save index data
            fstring str =  fstring(keyBuf).substr(kIndex1stLen);
            offset -= str.size();
            memcpy(keyVec.m_strpool.data() + offset, str.data(), str.size());
            //keyVec.m_offsets.set_wire(i, offset);
          }
          index2ndRS.set0(0); // set 1st element to 0
          index2ndRS.resize(ks.numKeys); // TBD: if resize if necessary ?
          assert(offset == 0);
        }
        index1stRS.build_cache(false, false);
        index2ndRS.build_cache(false, false);
        // construct index, set meta, index, data
        unique_ptr< TerarkCompositeIndex<RankSelect1st, RankSelect2nd> > ptr(
          new TerarkCompositeIndex<RankSelect1st, RankSelect2nd>());
        ptr->isUserMemory_ = false;
        ptr->isBuilding_ = true;
        {
          // save meta into header
          FileHeader *header = new FileHeader(
            index1stRS.mem_size() +
            index2ndRS.mem_size() +
            keyVec.mem_size());
          header->min_value = minValue;
          header->max_value = maxValue;
          header->index_1st_mem_size = index1stRS.mem_size();
          header->index_2nd_mem_size = index2ndRS.mem_size();
          header->index_data_mem_size = keyVec.mem_size();
          header->key_length = ks.minKeyLen - cplen;
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
        uint64_t
          minValue = Read1stIndex(ks.minKey, cplen),
          maxValue = Read1stIndex(ks.maxKey, cplen);
        if (minValue > maxValue) {
          std::swap(minValue, maxValue);
        }
        uint64_t diff = maxValue - minValue + 1;
        size_t index1stsz = size_t(std::ceil(diff * 1.25 / 8));
        size_t index2ndsz = size_t(std::ceil(ks.numKeys * 1.25 / 8));
        size_t indexDatasz = size_t(std::ceil(ks.numKeys * (ks.minKey.size() - kIndex1stLen)));
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
        ptr->keyLength_ = header->key_length;
        if (header->common_prefix_length > 0) {
          ptr->commonPrefix_.risk_set_data((char*)mem.data() + header->header_size,
                                           header->common_prefix_length);
        }
        size_t offset = header->header_size +
          terark::align_up(header->common_prefix_length, 8);
        ptr->index1stRS_.risk_mmap_from((unsigned char*)mem.data() + offset,
                                        header->index_1st_mem_size);
        offset += header->index_1st_mem_size;
        ptr->index2ndRS_.risk_mmap_from((unsigned char*)mem.data() + offset,
                                        header->index_2nd_mem_size);
        offset += header->index_2nd_mem_size;
        ptr->indexData_.m_strpool.risk_set_data((unsigned char*)mem.data() + offset,
                                                header->index_data_mem_size);
        // TBD: confirm this
        size_t fixlen = header->key_length + 
          header->common_prefix_length - kIndex1stLen;
        ptr->indexData_.m_fixlen = fixlen;
        ptr->indexData_.m_size = header->index_data_mem_size / fixlen;
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
      
    };
    
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
      write(index2ndRS_.data(), index2ndRS_.mem_size());
      write(indexData_.m_strpool.data(), indexData_.mem_size());
    }
    size_t Find(fstring key) const override {
      if (key.size() != keyLength_ + commonPrefix_.size() ||
          key.commonPrefixLen(commonPrefix_) != commonPrefix_.size()) {
        return size_t(-1);
      }
      uint64_t index1st = Read1stIndex(key, 0);
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
      fstring index2nd = key.substr(kIndex1stLen);
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
      return (commonPrefix_.size() + keyLength_) * indexData_.size();
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
    static uint64_t Read1stIndex(valvec<byte_t>& key, size_t cplen) {
      return ReadUint64(key.begin() + cplen,
                        key.begin() + kIndex1stLen);
    }
    static uint64_t Read1stIndex(fstring key, size_t cplen) {
      return ReadUint64((const byte_t*)key.data() + cplen,
                        (const byte_t*)key.data() + kIndex1stLen);
    }
    size_t Locate(const FixedLenStrVec& arr,
                  size_t start, size_t cnt, fstring target) const {
      /*
       * Find within index data, 
       *   items > 64, use binary search
       *   items <= 64, iterate search
       */
      static const size_t limit = 64;
      if (cnt > limit) {
        // bsearch
        size_t lo = start, hi = start + cnt;
        while (lo < hi) {
          size_t mid = (lo + hi) / 2;
          if (arr[mid] < target)
            lo = mid + 1;
          else
            hi = mid;
        }
        return (lo < start + cnt) ? lo : size_t(-1);
      } else {
        for (size_t i = 0; i < cnt; i++) {
          if (target <= arr[start + i]) {
            return start + i;
          }
        }
      }
      return size_t(-1);
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
    bool              isUserMemory_;
    bool              isBuilding_;
    uint32_t          keyLength_;
  };
  template<class RankSelect1st, class RankSelect2nd>
  const char* TerarkCompositeIndex<RankSelect1st, RankSelect2nd>::index_name = "CompositeIndex";

  
#if defined(TerocksPrivateCode)

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
      target.n -= index_.commonPrefix_.size();
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
      valvec<byte_t> keyBuf;
      indexSeq.resize(diff);
      for (size_t seq_id = 0; seq_id < ks.numKeys; ++seq_id) {
        reader >> keyBuf;
        indexSeq.set1(ReadUint64(keyBuf.begin() + cplen,
            keyBuf.end()) - minValue);
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
    key.n -= commonPrefix_.size();
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

  typedef TerarkCompositeIndex<terark::rank_select_il_256, terark::rank_select_il_256> TerarkCompositeIndex_IL_256_32;

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

  TerarkIndexRegister(TerarkCompositeIndex_IL_256_32, "CompositeIndex_IL_256_32", "CompositeIndex");

#if defined(TerocksPrivateCode)
typedef TerarkUintIndex<terark::rank_select_il_256_32> TerarkUintIndex_IL_256_32;
typedef TerarkUintIndex<terark::rank_select_se_256_32> TerarkUintIndex_SE_256_32;
typedef TerarkUintIndex<terark::rank_select_se_512_32> TerarkUintIndex_SE_512_32;
typedef TerarkUintIndex<terark::rank_select_se_512_64> TerarkUintIndex_SE_512_64;
TerarkIndexRegister(TerarkUintIndex_IL_256_32, "UintIndex_IL_256_32", "UintIndex");
TerarkIndexRegister(TerarkUintIndex_SE_256_32, "UintIndex_SE_256_32");
TerarkIndexRegister(TerarkUintIndex_SE_512_32, "UintIndex_SE_512_32");
TerarkIndexRegister(TerarkUintIndex_SE_512_64, "UintIndex_SE_512_64");
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
