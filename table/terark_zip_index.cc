#include "terark_zip_index.h"
#include "terark_zip_table.h"
#include "terark_zip_common.h"
#include <terark/hash_strmap.hpp>
#include <terark/fsa/dfa_mmap_header.hpp>
#include <terark/fsa/fsa_cache.hpp>
#include <terark/fsa/nest_trie_dawg.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/sortable_strvec.hpp>
#if defined(TerocksPrivateCode)
#include <terark/fsa/fsa_for_union_dfa.hpp>
#endif // TerocksPrivateCode


//#define DEBUG_ITERATOR

namespace rocksdb {

using terark::initial_state;
using terark::BaseDFA;
using terark::NestLoudsTrieDAWG_SE_512;
using terark::NestLoudsTrieDAWG_SE_512_64;
using terark::NestLoudsTrieDAWG_IL_256;
using terark::NestLoudsTrieDAWG_Mixed_SE_512;
using terark::NestLoudsTrieDAWG_Mixed_IL_256;
using terark::NestLoudsTrieDAWG_Mixed_XL_256;
using terark::SortedStrVec;
using terark::FixedLenStrVec;
using terark::MmapWholeFile;
using terark::UintVecMin0;
using terark::MatchingDFA;

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
    : NestLoudsTrieIterBase(trie->adfa_make_iter(initial_state)) {}
  bool Done(const NLTrie* trie, bool ok) {
    if (ok)
      m_id = trie->state_to_word_id(m_iter->word_state());
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
  bool Done(const MatchingDFA* trie, bool ok) {
    assert(trie->get_dawg() == m_dawg);
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
    const NLTrie* m_trie;
  protected:
    using NestLoudsTrieIterBaseTpl<NLTrie>::Done;
    using NestLoudsTrieIterBase::m_iter;
  public:
    explicit MyIterator(NLTrie* trie)
      : NestLoudsTrieIterBaseTpl<NLTrie>(trie)
      , m_trie(trie)
    {}
    bool SeekToFirst() override { return Done(m_trie, m_iter->seek_begin()); }
    bool SeekToLast()  override { return Done(m_trie, m_iter->seek_end()); }
    bool Seek(fstring key) override {
      return Done(m_trie, m_iter->seek_lower_bound(key));
    }
    bool Next() override {
#ifdef DEBUG_ITERATOR
      auto key_ref = key();
      std::string saved_key(key_ref.data(), key_ref.size());
      bool ret = Done(m_trie, m_iter->incr());
      if (ret && key() < saved_key) {
        assert(0);
        //throw std::exception("NestLoudsTrieIndex::Next() error");
      }
      return ret;
#else
      return Done(m_trie, m_iter->incr());
#endif
    }
    bool Prev() override {
#ifdef DEBUG_ITERATOR
      auto key_ref = key();
      std::string saved_key(key_ref.data(), key_ref.size());
      bool ret = Done(m_trie, m_iter->decr());
      if (ret && key() > saved_key) {
        assert(0);
        //throw std::exception("NestLoudsTrieIndex::Prev() error");
      }
      return ret;
#else
      return Done(m_trie, m_iter->decr());
#endif
    }
  };
public:
  NestLoudsTrieIndex(NLTrie* trie) : m_trie(trie) {
    m_dawg = trie->get_dawg();
  }
  const char* Name() const override {
    auto header = (const TerarkIndexHeader*)m_trie->get_mmap().data();
    return header->class_name;
  }
  void SaveMmap(std::function<void(const void *, size_t)> write) const override {
    m_trie->save_mmap(write);
  }
  size_t Find(fstring key) const override final {
    MY_THREAD_LOCAL(terark::MatchContext, ctx);
    ctx.root = 0;
    ctx.pos = 0;
    ctx.zidx = 0;
  //ctx.zbuf_state = size_t(-1);
    return m_dawg->index(ctx, key);
  }
  size_t NumKeys() const override final {
    return m_dawg->num_words();
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
        if (target.size() == cplen || target[cplen] < index_.commonPrefix_[cplen]) {
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
      size_t commonPrefixLen = fstring(ks.minKey).commonPrefixLen(ks.maxKey);
      assert(commonPrefixLen >= ks.commonPrefixLen);
      if (ks.maxKeyLen != ks.minKeyLen ||
          ks.maxKeyLen - commonPrefixLen > sizeof(uint64_t)) {
        abort();
      }
      uint64_t
        minValue = ReadUint64(ks.minKey.begin() + commonPrefixLen, ks.minKey.end()),
        maxValue = ReadUint64(ks.maxKey.begin() + commonPrefixLen, ks.maxKey.end());
      if (minValue > maxValue) {
        std::swap(minValue, maxValue);
      }
      uint64_t diff = maxValue - minValue + 1;
      RankSelect indexSeq;
      valvec<byte_t> keyBuf;
      indexSeq.resize(diff);
      for (size_t seq_id = 0; seq_id < ks.numKeys; ++seq_id) {
        reader >> keyBuf;
        indexSeq.set1(ReadUint64(keyBuf.begin() + commonPrefixLen,
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
      header->key_length = ks.minKeyLen - commonPrefixLen;
      if (commonPrefixLen > ks.commonPrefixLen) {
        header->common_prefix_length = commonPrefixLen - ks.commonPrefixLen;
        ptr->commonPrefix_.assign(ks.minKey.data(), header->common_prefix_length);
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
      size_t length = ks.maxKeyLen - fstring(ks.minKey).commonPrefixLen(ks.maxKey);
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
        return nullptr;
      }
      size_t name_i = g_TerarkIndexName.find_i(typeid(TerarkUintIndex<RankSelect>).name());
      if (strcmp(header->class_name, g_TerarkIndexName.val(name_i).c_str()) != 0) {
        return nullptr;
      }
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
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_SE_512_32> TerocksIndex_NestLoudsTrieDAWG_SE_512_32;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_SE_512_64> TerocksIndex_NestLoudsTrieDAWG_SE_512_64;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_IL_256_32> TerocksIndex_NestLoudsTrieDAWG_IL_256_32;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_SE_512> TerocksIndex_NestLoudsTrieDAWG_Mixed_SE_512;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_IL_256> TerocksIndex_NestLoudsTrieDAWG_Mixed_IL_256;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_XL_256> TerocksIndex_NestLoudsTrieDAWG_Mixed_XL_256;
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_SE_512_32, "NestLoudsTrieDAWG_SE_512", "SE_512_32", "SE_512");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_SE_512_64, "NestLoudsTrieDAWG_SE_512_64", "SE_512_64", "SE_512");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_IL_256_32, "NestLoudsTrieDAWG_IL_256", "IL_256_32", "IL_256", "NestLoudsTrieDAWG_IL");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_SE_512, "NestLoudsTrieDAWG_Mixed_SE_512", "Mixed_SE_512");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_IL_256, "NestLoudsTrieDAWG_Mixed_IL_256", "Mixed_IL_256");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_XL_256, "NestLoudsTrieDAWG_Mixed_XL_256", "Mixed_XL_256");

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
