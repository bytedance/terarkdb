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

static terark::hash_strmap<TerocksIndex::FactoryPtr> g_TerocksIndexFactroy;

TerocksIndex::AutoRegisterFactory::AutoRegisterFactory(
    std::initializer_list<const char*> names,
    Factory* factory) {
  for (const char* name : names) {
//  STD_INFO("AutoRegisterFactory: %s\n", name);
    g_TerocksIndexFactroy.insert_i(name, FactoryPtr(factory));
  }
}

const TerocksIndex::Factory* TerocksIndex::GetFactory(fstring name) {
  size_t idx = g_TerocksIndexFactroy.find_i(name);
  if (idx < g_TerocksIndexFactroy.end_i()) {
    auto factory = g_TerocksIndexFactroy.val(idx).get();
    return factory;
  }
  return NULL;
}

const TerocksIndex::Factory*
TerocksIndex::SelectFactory(const KeyStat& ks, fstring name) {
  return GetFactory(name);
}

TerocksIndex::~TerocksIndex() {}
TerocksIndex::Factory::~Factory() {}
TerocksIndex::Iterator::~Iterator() {}

class NestLoudsTrieIterBase : public TerocksIndex::Iterator {
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
class NestLoudsTrieIndex : public TerocksIndex {
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
               fstring tmpFilePath,
               const KeyStat& ks) const override {
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
      if (tzopt.debugLevel != 1 && tzopt.debugLevel != 2) {
        tmpKeyFile.close();
      }
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
      if (keyVec[0] < keyVec.back()) {
        conf.isInputSorted = true;
      }
      std::unique_ptr<NLTrie> trie(new NLTrie());
      trie->build_from(keyVec, conf);
      trie->save_mmap(tmpFilePath);
    }
    unique_ptr<TerocksIndex> LoadMemory(fstring mem) const override {
      unique_ptr<BaseDFA>
      dfa(BaseDFA::load_mmap_user_mem(mem.data(), mem.size()));
      auto trie = dynamic_cast<NLTrie*>(dfa.get());
      if (NULL == trie) {
        throw std::invalid_argument("Bad trie class: " + ClassName(*dfa)
            + ", should be " + ClassName<NLTrie>());
      }
      unique_ptr<TerocksIndex> index(new NestLoudsTrieIndex(trie));
      dfa.release();
      return std::move(index);
    }
    unique_ptr<TerocksIndex> LoadFile(fstring fpath) const override {
      unique_ptr<BaseDFA> dfa(BaseDFA::load_mmap(fpath));
      auto trie = dynamic_cast<NLTrie*>(dfa.get());
      if (NULL == trie) {
        throw std::invalid_argument(
            "File: " + fpath + ", Bad trie class: " + ClassName(*dfa)
            + ", should be " + ClassName<NLTrie>());
      }
      unique_ptr<TerocksIndex> index(new NestLoudsTrieIndex(trie));
      dfa.release();
      return std::move(index);
    }
    size_t MemSizeForBuild(const KeyStat& ks) const override {
      return sizeof(SortableStrVec::SEntry) * ks.numKeys + ks.sumKeyLen
          - ks.commonPrefixLen * ks.numKeys;
    }
  };
};
typedef NestLoudsTrieDAWG_IL_256 NestLoudsTrieDAWG_IL_256_32;
typedef NestLoudsTrieDAWG_SE_512 NestLoudsTrieDAWG_SE_512_32;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_SE_512_32> TerocksIndex_NestLoudsTrieDAWG_SE_512_32;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_IL_256_32> TerocksIndex_NestLoudsTrieDAWG_IL_256_32;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_SE_512> TerocksIndex_NestLoudsTrieDAWG_Mixed_SE_512;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_IL_256> TerocksIndex_NestLoudsTrieDAWG_Mixed_IL_256;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_XL_256> TerocksIndex_NestLoudsTrieDAWG_Mixed_XL_256;
TerocksIndexRegister(TerocksIndex_NestLoudsTrieDAWG_SE_512_32, "NestLoudsTrieDAWG_SE_512", "SE_512_32", "SE_512");
TerocksIndexRegister(TerocksIndex_NestLoudsTrieDAWG_IL_256_32, "NestLoudsTrieDAWG_IL_256", "IL_256_32", "IL_256", "NestLoudsTrieDAWG_IL");
TerocksIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_SE_512, "NestLoudsTrieDAWG_Mixed_SE_512", "Mixed_SE_512");
TerocksIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_IL_256, "NestLoudsTrieDAWG_Mixed_IL_256", "Mixed_IL_256");
TerocksIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_XL_256, "NestLoudsTrieDAWG_Mixed_XL_256", "Mixed_XL_256");

unique_ptr<TerocksIndex> TerocksIndex::LoadFile(fstring fpath) {
  TerocksIndex::Factory* factory = NULL;
  {
    terark::MmapWholeFile mmap(fpath);
    auto header = (const terark::DFA_MmapHeader*)mmap.base;
    size_t idx = g_TerocksIndexFactroy.find_i(header->dfa_class_name);
    if (idx >= g_TerocksIndexFactroy.end_i()) {
      throw std::invalid_argument(
          "TerocksIndex::LoadFile(" + fpath + "): Unknown trie class: "
          + header->dfa_class_name);
    }
    factory = g_TerocksIndexFactroy.val(idx).get();
  }
  return factory->LoadFile(fpath);
}

unique_ptr<TerocksIndex> TerocksIndex::LoadMemory(fstring mem) {
  auto header = (const terark::DFA_MmapHeader*)mem.data();
  size_t idx = g_TerocksIndexFactroy.find_i(header->dfa_class_name);
  if (idx >= g_TerocksIndexFactroy.end_i()) {
    throw std::invalid_argument(
        std::string("TerocksIndex::LoadMemory(): Unknown trie class: ")
        + header->dfa_class_name);
  }
  TerocksIndex::Factory* factory = g_TerocksIndexFactroy.val(idx).get();
  return factory->LoadMemory(mem);
}


} // namespace rocksdb
