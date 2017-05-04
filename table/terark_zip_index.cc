#include "terark_zip_index.h"
#include "terark_zip_table.h"
#include "terark_zip_common.h"
#include <terark/hash_strmap.hpp>
#include <terark/fsa/dfa_mmap_header.hpp>
#include <terark/fsa/fsa_cache.hpp>
#include <terark/fsa/nest_trie_dawg.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/sortable_strvec.hpp>

namespace rocksdb {

using terark::initial_state;
using terark::BaseDFA;
using terark::NestLoudsTrieDAWG_SE_512;
using terark::NestLoudsTrieDAWG_IL_256;
using terark::NestLoudsTrieDAWG_Mixed_SE_512;
using terark::NestLoudsTrieDAWG_Mixed_IL_256;
using terark::NestLoudsTrieDAWG_Mixed_XL_256;
using terark::SortableStrVec;
using terark::MmapWholeFile;

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
    const char* riit_name,
    Factory* factory) {
  for (const char* name : names) {
//  STD_INFO("AutoRegisterFactory: %s\n", name);
    g_TerarkIndexFactroy.insert_i(name, FactoryPtr(factory));
    g_TerarkIndexName.insert_i(riit_name, *names.begin());
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
  if (ks.maxKeyLen == ks.minKeyLen && ks.minKeyLen > 0 && ks.maxKeyLen <= 8) {
    uint64_t
      minValue = ReadUint64(ks.minKey.begin(), ks.minKey.end()),
      maxValue = ReadUint64(ks.maxKey.begin(), ks.maxKey.end());
    uint64_t diff = (minValue < maxValue ? maxValue - minValue : minValue - maxValue) + 1;
    if (diff < ks.numKeys * 30 && ks.numKeys < (4ULL << 30)) {
      return GetFactory("UintIndex");
    }
  }
  return GetFactory(name);
}

TerarkIndex::~TerarkIndex() {}
TerarkIndex::Factory::~Factory() {}
TerarkIndex::Iterator::~Iterator() {}

class NestLoudsTrieIterBase : public TerarkIndex::Iterator {
protected:
  unique_ptr<terark::ADFA_LexIterator> m_iter;
  template<class NLTrie>
  bool Done(const NLTrie* trie, bool ok) {
    if (ok)
      m_id = trie->state_to_word_id(m_iter->word_state());
    else
      m_id = size_t(-1);
    return ok;
  }
  fstring key() const override {
    return fstring(m_iter->word());
  }
  NestLoudsTrieIterBase(terark::ADFA_LexIterator* iter)
   : m_iter(iter) {}
};
template<class NLTrie>
class NestLoudsTrieIndex : public TerarkIndex {
  unique_ptr<NLTrie> m_trie;
  class MyIterator : public NestLoudsTrieIterBase {
    const NLTrie* m_trie;
  public:
    explicit MyIterator(NLTrie* trie)
     : NestLoudsTrieIterBase(trie->adfa_make_iter(initial_state))
     , m_trie(trie)
    {}
    bool SeekToFirst() override { return Done(m_trie, m_iter->seek_begin()); }
    bool SeekToLast()  override { return Done(m_trie, m_iter->seek_end()); }
    bool Seek(fstring key) override {
        return Done(m_trie, m_iter->seek_lower_bound(key));
    }
    bool Next() override { return Done(m_trie, m_iter->incr()); }
    bool Prev() override { return Done(m_trie, m_iter->decr()); }
  };
public:
  NestLoudsTrieIndex(NLTrie* trie) : m_trie(trie) {}
  size_t Find(fstring key) const override final {
    MY_THREAD_LOCAL(terark::MatchContext, ctx);
    ctx.root = 0;
    ctx.pos = 0;
    ctx.zidx = 0;
  //ctx.zbuf_state = size_t(-1);
    return m_trie->index(ctx, key);
  }
  size_t NumKeys() const override final {
    return m_trie->num_words();
  }
  fstring Memory() const override final {
    return m_trie->get_mmap();
  }
  Iterator* NewIterator() const override final {
    return new MyIterator(m_trie.get());
  }
  bool NeedsReorder() const override final { return true; }
  void GetOrderMap(uint32_t* newToOld)
  const override final {
    terark::NonRecursiveDictionaryOrderToStateMapGenerator gen;
    gen(*m_trie, [&](size_t dictOrderOldId, size_t state) {
      size_t newId = m_trie->state_to_word_id(state);
      newToOld[newId] = uint32_t(dictOrderOldId);
    });
  }
  void BuildCache(double cacheRatio) {
    if (cacheRatio > 1e-8) {
        m_trie->build_fsa_cache(cacheRatio, NULL);
    }
  }
  class MyFactory : public Factory {
  public:
    void Build(TempFileDeleteOnClose& tmpKeyFile,
               const TerarkZipTableOptions& tzopt,
               std::function<void(const void *, size_t)> write,
               KeyStat& ks) const override {
      NativeDataInput<InputBuffer> reader(&tmpKeyFile.fp);
#if !defined(NDEBUG)
      SortableStrVec backupKeys;
#endif
      size_t sumPrefixLen = ks.commonPrefixLen * ks.numKeys;
      SortableStrVec keyVec;
      keyVec.m_index.reserve(ks.numKeys);
      keyVec.m_strpool.reserve(ks.sumKeyLen - sumPrefixLen);
      valvec<byte_t> keyBuf;
      for (size_t seq_id = 0; seq_id < ks.numKeys; ++seq_id) {
        reader >> keyBuf;
        keyVec.push_back(fstring(keyBuf).substr(ks.commonPrefixLen));
      }
      if (keyVec[0] > keyVec.back()) {
        std::reverse(keyVec.m_index.begin(), keyVec.m_index.end());
      }
//      for(size_t i = 0, ei = keyVec.size(); i < ei)

#if !defined(NDEBUG)
      for (size_t i = 1; i < keyVec.size(); ++i) {
        fstring prev = keyVec[i-1];
        fstring curr = keyVec[i];
        assert(prev < curr);
      }
      backupKeys = keyVec;
#endif
      terark::NestLoudsTrieConfig conf;
      conf.nestLevel = tzopt.indexNestLevel;
      const size_t smallmem = 1000*1024*1024;
      const size_t myWorkMem = keyVec.mem_size();
      if (myWorkMem > smallmem) {
        // use tmp files during index building
        conf.tmpDir = tzopt.localTempDir;
        // adjust tmpLevel for linkVec, wihch is proportional to num of keys
        if (ks.numKeys > 1ul<<30) {
          // not need any mem in BFS, instead 8G file of 4G mem (linkVec)
          // this reduce 10% peak mem when avg keylen is 24 bytes
          conf.tmpLevel = 3;
        }
        else if (myWorkMem > 256ul<<20) {
          // 1G mem in BFS, swap to 1G file after BFS and before build nextStrVec
          conf.tmpLevel = 2;
        }
      }
      conf.isInputSorted = true;
      std::unique_ptr<NLTrie> trie(new NLTrie());
      trie->build_from(keyVec, conf);
      trie->save_mmap(write);
    }
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
      return sizeof(SortableStrVec::SEntry) * ks.numKeys + ks.sumKeyLen
          - ks.commonPrefixLen * ks.numKeys;
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
    uint32_t reserved_28_4;

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
    UIntIndexIterator(const TerarkUintIndex& index) : index_(index) {}
    virtual ~UIntIndexIterator() {}

    virtual bool SeekToFirst() {
      m_id = 0;
      pos_ = 0;
      UpdateBuffer();
      return true;
    }
    virtual bool SeekToLast() {
      m_id = index_.indexSeq_.max_rank1() - 1;
      pos_ = index_.indexSeq_.size() - 1;
      UpdateBuffer();
      return true;
    }
    virtual bool Seek(fstring target) {
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
      if (!index_.indexSeq_[pos_] || target.size() > index_.keyLength_) {
        if (m_id == index_.indexSeq_.max_rank1() - 1) {
          m_id = size_t(-1);
          return false;
        }
        ++m_id;
        pos_ = pos_ + index_.indexSeq_.zero_seq_len(pos_ + 1) + 1;
      }
      UpdateBuffer();
      return true;
    }
    virtual bool Next() {
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
    virtual bool Prev() {
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
    virtual fstring key() const {
      assert(m_id != size_t(-1));
      return fstring(buffer_, index_.keyLength_);
    }
  protected:
    void UpdateBuffer() {
      AssignUint64(buffer_, buffer_ + index_.keyLength_, pos_ + index_.minValue_);
    }
    uint64_t pos_;
    byte_t buffer_[8];
    const TerarkUintIndex& index_;
  };
  class MyFactory : public TerarkIndex::Factory {
  public:
    virtual ~MyFactory() {
    }
    void Build(TempFileDeleteOnClose& tmpKeyFile,
               const TerarkZipTableOptions& tzopt,
               std::function<void(const void *, size_t)> write,
               KeyStat& ks) const override {
      if (ks.maxKeyLen != ks.minKeyLen || ks.minKeyLen == 0 || ks.maxKeyLen > 8) {
        abort();
      }
      uint64_t
        minValue = ReadUint64(ks.minKey.begin(), ks.minKey.end()),
        maxValue = ReadUint64(ks.maxKey.begin(), ks.maxKey.end());
      if (minValue > maxValue) {
        std::swap(minValue, maxValue);
      }
      uint64_t diff = maxValue - minValue + 1;
      RankSelect indexSeq_;
      NativeDataInput<InputBuffer> reader(&tmpKeyFile.fp);
      valvec<byte_t> keyBuf;
      indexSeq_.resize(diff);
      for (size_t seq_id = 0; seq_id < ks.numKeys; ++seq_id) {
        reader >> keyBuf;
        indexSeq_.set1(ReadUint64(keyBuf.begin(), keyBuf.end()) - minValue);
      }
      indexSeq_.build_cache(false, false);
      FileHeader header(indexSeq_.mem_size());
      header.min_value = minValue;
      header.max_value = maxValue;
      header.index_mem_size = indexSeq_.mem_size();
      header.key_length = ks.minKeyLen;
      write(&header, sizeof header);
      write(indexSeq_.data(), indexSeq_.mem_size());
      ks.commonPrefixLen = 0;
    }
    unique_ptr<TerarkIndex> LoadMemory(fstring mem) const override {
      return unique_ptr<TerarkIndex>(loadImpl(mem, {}).release());
    }
    unique_ptr<TerarkIndex> LoadFile(fstring fpath) const override {
      return unique_ptr<TerarkIndex>(loadImpl({}, fpath).release());
    }
    size_t MemSizeForBuild(const KeyStat& ks) const override {
      uint64_t
        minValue = ReadUint64(ks.minKey.begin(), ks.minKey.end()),
        maxValue = ReadUint64(ks.maxKey.begin(), ks.maxKey.end());
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
      ptr->indexSeq_.risk_mmap_from((unsigned char*)mem.data() + header->header_size,
        header->index_mem_size);
      return ptr;
    }
  };
  using TerarkIndex::FactoryPtr;
  virtual ~TerarkUintIndex() {
    if (file_.base != nullptr || isUserMemory_) {
      indexSeq_.risk_release_ownership();
    }
  }
  virtual size_t Find(fstring key) const {
    if (key.size() != keyLength_) {
      return size_t(-1);
    }
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
  virtual size_t NumKeys() const {
    return indexSeq_.max_rank1();
  }
  virtual fstring Memory() const {
    return fstring((const char*)header_, (ptrdiff_t)header_->file_size);
  }
  virtual Iterator* NewIterator() const {
    return new UIntIndexIterator(*this);
  }
  virtual bool NeedsReorder() const {
    return false;
  }
  virtual void GetOrderMap(uint32_t* newToOld) const {
    assert(false);
  }
  virtual void BuildCache(double cacheRatio) {
    //do nothing
  }
protected:
  const FileHeader* header_;
  MmapWholeFile     file_;
  RankSelect        indexSeq_;
  uint64_t          minValue_;
  uint64_t          maxValue_;
  bool              isUserMemory_;
  uint32_t          keyLength_;
};
template<class RankSelect>
const char* TerarkUintIndex<RankSelect>::index_name = "UintIndex";

#endif // TerocksPrivateCode

typedef NestLoudsTrieDAWG_IL_256 NestLoudsTrieDAWG_IL_256_32;
typedef NestLoudsTrieDAWG_SE_512 NestLoudsTrieDAWG_SE_512_32;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_SE_512_32> TerocksIndex_NestLoudsTrieDAWG_SE_512_32;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_IL_256_32> TerocksIndex_NestLoudsTrieDAWG_IL_256_32;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_SE_512> TerocksIndex_NestLoudsTrieDAWG_Mixed_SE_512;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_IL_256> TerocksIndex_NestLoudsTrieDAWG_Mixed_IL_256;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_XL_256> TerocksIndex_NestLoudsTrieDAWG_Mixed_XL_256;
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_SE_512_32, "NestLoudsTrieDAWG_SE_512", "SE_512_32", "SE_512");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_IL_256_32, "NestLoudsTrieDAWG_IL_256", "IL_256_32", "IL_256", "NestLoudsTrieDAWG_IL");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_SE_512, "NestLoudsTrieDAWG_Mixed_SE_512", "Mixed_SE_512");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_IL_256, "NestLoudsTrieDAWG_Mixed_IL_256", "Mixed_IL_256");
TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_XL_256, "NestLoudsTrieDAWG_Mixed_XL_256", "Mixed_XL_256");

#if defined(TerocksPrivateCode)
typedef TerarkUintIndex<terark::rank_select_il_256_32> TerarkUintIndex_IL_256_32;
typedef TerarkUintIndex<terark::rank_select_se_256_32> TerarkUintIndex_SE_256_32;
typedef TerarkUintIndex<terark::rank_select_se_512_32> TerarkUintIndex_SE_512_32;
TerarkIndexRegister(TerarkUintIndex_IL_256_32, "UintIndex_IL_256_32", "UintIndex");
TerarkIndexRegister(TerarkUintIndex_SE_256_32, "UintIndex_SE_256_32");
TerarkIndexRegister(TerarkUintIndex_SE_512_32, "UintIndex_SE_512_32");
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
