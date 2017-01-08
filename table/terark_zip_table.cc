/*
 * terark_zip_table.cc
 *
 *  Created on: 2016-08-09
 *      Author: leipeng
 */

#include "terark_zip_table.h"
#include <rocksdb/comparator.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <table/get_context.h>
#include <table/internal_iterator.h>
#include <table/table_builder.h>
#include <table/table_reader.h>
#include <table/meta_blocks.h>
#include <db/compaction_iterator.h>
#include <terark/stdtypes.hpp>
#include <terark/lcast.hpp>
#include <terark/util/crc.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/throw.hpp>
#include <terark/util/profiling.hpp>
#include <terark/zbs/fast_zip_blob_store.hpp>
#include <terark/fsa/nest_trie_dawg.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/hash_strmap.hpp>
#include <terark/fsa/dfa_mmap_header.hpp>
#include <terark/fsa/fsa_cache.hpp>
#include <boost/scope_exit.hpp>
#include <future>
#include <memory>
#include <random>
#include <stdlib.h>
#include <stdint.h>
#include <util/arena.h> // for #include <sys/mman.h>
#include <cxxabi.h>

namespace rocksdb {

using std::unique_ptr;
using std::unordered_map;
using std::vector;
using std::string;

template<class T>
unique_ptr<T> UniquePtrOf(T* p) { return unique_ptr<T>(p); }

using terark::BaseDFA;
using terark::NestLoudsTrieDAWG_SE_512;
using terark::NestLoudsTrieDAWG_IL_256;
using terark::NestLoudsTrieDAWG_Mixed_SE_512;
using terark::NestLoudsTrieDAWG_Mixed_IL_256;
using terark::NestLoudsTrieDAWG_Mixed_XL_256;
using terark::DictZipBlobStore;
using terark::byte_t;
using terark::valvec;
using terark::valvec_no_init;
using terark::valvec_reserve;
using terark::fstring;
using terark::initial_state;
using terark::FileStream;
using terark::InputBuffer;
using terark::OutputBuffer;
using terark::LittleEndianDataInput;
using terark::LittleEndianDataOutput;
using terark::SortableStrVec;
using terark::UintVecMin0;
using terark::BadCrc32cException;

static terark::profiling g_pf;

static const uint64_t kTerarkZipTableMagicNumber = 0x1122334455667788;

static const std::string kTerarkZipTableIndexBlock = "TerarkZipTableIndexBlock";
static const std::string kTerarkZipTableValueTypeBlock = "TerarkZipTableValueTypeBlock";
static const std::string kTerarkZipTableValueDictBlock = "TerarkZipTableValueDictBlock";
static const std::string kTerarkZipTableCommonPrefixBlock = "TerarkZipTableCommonPrefixBlock";

class TerarkZipTableIterator;

#if defined(IOS_CROSS_COMPILE) || defined(__DARWIN_C_LEVEL)
  #define MY_THREAD_LOCAL(Type, Var)  Type Var
//#elif defined(_WIN32)
//  #define MY_THREAD_LOCAL(Type, Var)  static __declspec(thread) Type Var
#else
  #define MY_THREAD_LOCAL(Type, Var)  static thread_local Type Var
#endif

#ifdef TERARK_ZIP_TRIAL_VERSION
const char g_trail_rand_delete[] = "TERARK_ZIP_TRIAL_VERSION random deleted this row";
#endif

#define STD_INFO(format, ...) fprintf(stderr, "%s INFO: " format, StrDateTimeNow(), ##__VA_ARGS__)
#define STD_WARN(format, ...) fprintf(stderr, "%s WARN: " format, StrDateTimeNow(), ##__VA_ARGS__)

#undef INFO
#undef WARN
#if defined(NDEBUG) && 0
  #define INFO(logger, format, ...) Info(logger, format, ##__VA_ARGS__)
  #define WARN(logger, format, ...) Warn(logger, format, ##__VA_ARGS__)
#else
  #define INFO(logger, format, ...) STD_INFO(format, ##__VA_ARGS__)
  #define WARN(logger, format, ...) STD_WARN(format, ##__VA_ARGS__)
#endif

static const char* StrDateTimeNow() {
  thread_local char buf[64];
  time_t rawtime;
  time(&rawtime);
  struct tm* timeinfo = localtime(&rawtime);
  strftime(buf, sizeof(buf), "%F %T",timeinfo);
  return buf;
}

static std::string demangle(const char* name) {
  int status = -4; // some arbitrary value to eliminate the compiler warning
  terark::AutoFree<char> res(abi::__cxa_demangle(name, NULL, NULL, &status));
  return (status==0) ? res.p : name ;
}
template<class T>
static std::string ClassName() {
  return demangle(typeid(T).name());
}
template<class T>
static std::string ClassName(const T& x) {
  return demangle(typeid(x).name());
}

template<class ByteArray>
inline Slice SliceOf(const ByteArray& ba) {
  BOOST_STATIC_ASSERT(sizeof(ba[0] == 1));
  return Slice((const char*)ba.data(), ba.size());
}

inline static fstring fstringOf(const Slice& x) {
  return fstring(x.data(), x.size());
}

template<class ByteArrayView>
inline ByteArrayView SubStr(const ByteArrayView& x, size_t pos) {
  assert(pos <= x.size());
  return ByteArrayView(x.data() + pos, x.size() - pos);
}

class AutoDeleteFile {
public:
  std::string fpath;
  operator fstring() const { return fpath; }
  void Delete() {
    ::remove(fpath.c_str());
    fpath.clear();
  }
  ~AutoDeleteFile() {
    if (!fpath.empty()) {
      ::remove(fpath.c_str());
    }
  }
};
class TempFileDeleteOnClose {
public:
  std::string path;
  FileStream  fp;
  NativeDataOutput<OutputBuffer> writer;
  void open() {
    fp.open(path.c_str(), "wb+");
    fp.disbuf();
    writer.attach(&fp);
  }
  void dopen(int fd) {
    fp.dopen(fd, "wb+");
    fp.disbuf();
    writer.attach(&fp);
  }
  ~TempFileDeleteOnClose() {
    if (fp)
      this->close();
  }
  void close() {
    assert(nullptr != fp);
    fp.close();
    ::remove(path.c_str());
  }
  void complete_write() {
    writer.flush_buffer();
    fp.rewind();
  }
};

class TerocksIndex : boost::noncopyable {
public:
  class Iterator : boost::noncopyable {
  protected:
    size_t m_id = size_t(-1);
  public:
    virtual ~Iterator();
    virtual bool SeekToFirst() = 0;
    virtual bool SeekToLast() = 0;
    virtual bool Seek(fstring target) = 0;
    virtual bool Next() = 0;
    virtual bool Prev() = 0;
    inline bool Valid() const { return size_t(-1) != m_id; }
    inline size_t id() const { return m_id; }
    virtual Slice key() const = 0;
    inline void SetInvalid() { m_id = size_t(-1); }
  };
  class Factory : public terark::RefCounter {
  public:
    virtual ~Factory();
    virtual void Build(TempFileDeleteOnClose& tmpKeyFile,
                       const TerarkZipTableOptions& tzopt,
                       fstring tmpFilePath,
                       size_t commonPrefixLen,
                       size_t minKeyLen,
                       size_t maxKeyLen,
                       size_t numKeys,
                       size_t sumKeyLen) const = 0;
    virtual unique_ptr<TerocksIndex> LoadMemory(fstring mem) const = 0;
    virtual unique_ptr<TerocksIndex> LoadFile(fstring fpath) const = 0;
    virtual size_t MemSizeForBuild(size_t numKeys, size_t sumKeyLen) const = 0;
  };
  typedef boost::intrusive_ptr<Factory> FactoryPtr;
  struct AutoRegisterFactory {
    AutoRegisterFactory(std::initializer_list<const char*> names, Factory* factory);
  };
  static const Factory* GetFactory(fstring name);
  static unique_ptr<TerocksIndex> LoadFile(fstring fpath);
  static unique_ptr<TerocksIndex> LoadMemory(fstring mem);
  virtual ~TerocksIndex();
  virtual size_t Find(fstring key) const = 0;
  virtual size_t NumKeys() const = 0;
  virtual fstring Memory() const = 0;
  virtual Iterator* NewIterator() const = 0;
  virtual bool NeedsReorder() const = 0;
  virtual void GetOrderMap(uint32_t* newToOld) const = 0;
  virtual void BuildCache(double cacheRatio) = 0;
};
static terark::hash_strmap<TerocksIndex::FactoryPtr> g_TerocksIndexFactroy;
#define TerocksIndexRegister(clazz, ...) \
    TerocksIndex::AutoRegisterFactory \
    g_AutoRegister_##clazz({#clazz,##__VA_ARGS__}, new clazz::MyFactory())

TerocksIndex::AutoRegisterFactory::AutoRegisterFactory(
    std::initializer_list<const char*> names,
    Factory* factory) {
  for (const char* name : names) {
//  STD_INFO("AutoRegisterFactory: %s\n", name);
    g_TerocksIndexFactroy.insert_i(name, factory);
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
  Slice key() const override {
    return SliceOf(m_iter->word());
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
               size_t commonPrefixLen,
               size_t minKeyLen,
               size_t maxKeyLen,
               size_t numKeys,
               size_t sumKeyLen) const override {
      NativeDataInput<InputBuffer> reader(&tmpKeyFile.fp);
#if !defined(NDEBUG)
      SortableStrVec backupKeys;
#endif
      size_t sumPrefixLen = commonPrefixLen * numKeys;
      SortableStrVec keyVec;
      keyVec.m_index.reserve(numKeys);
      keyVec.m_strpool.reserve(sumKeyLen - sumPrefixLen);
      valvec<byte_t> keyBuf;
      for (size_t seq_id = 0; seq_id < numKeys; ++seq_id) {
        reader >> keyBuf;
        keyVec.push_back(fstring(keyBuf).substr(commonPrefixLen));
      }
      tmpKeyFile.close();
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
        if (numKeys > 1ul<<30) {
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
    size_t MemSizeForBuild(size_t numKeys, size_t sumKeyLen)
    const override {
      return sizeof(SortableStrVec::SEntry) * numKeys + sumKeyLen;
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

///////////////////////////////////////////////////////////////////////////////

enum class ZipValueType : unsigned char {
	kZeroSeq = 0,
	kDelete = 1,
	kValue = 2,
	kMulti = 3,
};
const size_t kZipValueTypeBits = 2;

struct ZipValueMultiValue {
	uint32_t num;
	uint32_t offsets[1];

	Slice getValueData(size_t nth) const {
		assert(nth < num);
		size_t offset0 = offsets[nth+0];
		size_t offset1 = offsets[nth+1];
		size_t dlength = offset1 - offset0;
		const char* base = (const char*)(offsets + num + 1);
		return Slice(base + offset0, dlength);
	}
	static size_t calcHeaderSize(size_t n) {
		return sizeof(uint32_t) * (n + 2);
	}
};

/**
 * one user key map to a record id: the index NO. of a key in NestLoudsTrie,
 * the record id is used to direct index a type enum(small integer) array,
 * the record id is also used to access the value store
 */
class TerarkZipTableReader: public TableReader, boost::noncopyable {
public:
  InternalIterator*
  NewIterator(const ReadOptions&, Arena*, bool skip_filters) override;

  void Prepare(const Slice& target) override {}

  Status Get(const ReadOptions&, const Slice& key, GetContext*,
             bool skip_filters) override;

  uint64_t ApproximateOffsetOf(const Slice& key) override { return 0; }
  void SetupForCompaction() override {}

  std::shared_ptr<const TableProperties>
  GetTableProperties() const override { return table_properties_; }

  size_t ApproximateMemoryUsage() const override { return file_data_.size(); }

  ~TerarkZipTableReader();
  TerarkZipTableReader(const TableReaderOptions&, const TerarkZipTableOptions&);
  Status Open(RandomAccessFileReader* file, uint64_t file_size);

private:
  unique_ptr<DictZipBlobStore> valstore_;
  unique_ptr<TerocksIndex> keyIndex_;
  Slice commonPrefix_;
  terark::UintVecMin0 typeArray_;
  static const size_t kNumInternalBytes = 8;
  Slice  file_data_;
  unique_ptr<RandomAccessFileReader> file_;
  const TableReaderOptions table_reader_options_;
  std::shared_ptr<const TableProperties> table_properties_;
  const TerarkZipTableOptions& tzto_;
  bool isReverseBytewiseOrder_;
  friend class TerarkZipTableIterator;
  Status LoadIndex(Slice mem);
};


class TerarkZipTableBuilder: public TableBuilder, boost::noncopyable {
public:
  TerarkZipTableBuilder(
		  const TerarkZipTableOptions&,
		  const ImmutableCFOptions& ioptions,
		  const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*,
		  uint32_t column_family_id,
		  WritableFileWriter* file,
		  const std::string& column_family_name);

  ~TerarkZipTableBuilder();

  void Add(const Slice& key, const Slice& value) override;
  Status status() const override { return status_; }
  Status Finish() override;
  void Abandon() override;
  uint64_t NumEntries() const override { return properties_.num_entries; }
  uint64_t FileSize() const override;
  TableProperties GetTableProperties() const override { return properties_; }
  void SetSecondPassIterator(InternalIterator* reader) override {
    second_pass_iter_ = reader;
  }

private:
  void AddPrevUserKey();

  Arena arena_;
  const TerarkZipTableOptions& table_options_;
  const ImmutableCFOptions& ioptions_;
  std::vector<unique_ptr<IntTblPropCollector>> table_properties_collectors_;

  InternalIterator* second_pass_iter_ = nullptr;
  valvec<byte_t> prevUserKey_;
  terark::febitvec valueBits_;
  TempFileDeleteOnClose tmpKeyFile_;
  TempFileDeleteOnClose tmpValueFile_;
  TempFileDeleteOnClose tmpSampleFile_;
  std::mt19937_64 randomGenerator_;
  uint64_t sampleUpperBound_;
  size_t lenUserKeys_ = size_t(-1);
  size_t numUserKeys_ = size_t(-1);
  size_t minUserKeyLen_ = 0;
  size_t maxUserKeyLen_ = size_t(-1);
  size_t commonPrefixLen_ = 0;
  size_t sampleLenSum_ = 0;
  WritableFileWriter* file_;
  uint64_t offset_ = 0;
  Status status_;
  TableProperties properties_;
  bool closed_ = false;  // Either Finish() or Abandon() has been called.

  long long t0 = 0;
};

///////////////////////////////////////////////////////////////////////////////

class TerarkZipTableIterator : public InternalIterator, boost::noncopyable {
public:
  explicit TerarkZipTableIterator(const TerarkZipTableReader* table)
	: table_(table), iter_(table->keyIndex_->NewIterator())
	, commonPrefix_(fstringOf(table->commonPrefix_))
	, reverse_(table->isReverseBytewiseOrder_)
  {
    // isReverseBytewiseOrder_ just reverse key order
    // do not reverse multi values of a single key
    // so it can not use a reverse wrapper iterator
	  zValtype_ = ZipValueType::kZeroSeq;
	  pinned_iters_mgr_ = NULL;
	  SetIterInvalid();
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) {
    if (pinned_iters_mgr_ && pinned_iters_mgr_ != pinned_iters_mgr) {
      pinned_buffer_.clear();
    }
    pinned_iters_mgr_ = pinned_iters_mgr;
  }

  bool Valid() const override {
	  return iter_->Valid();
  }

  void SeekToFirst() override {
	  if (UnzipIterRecord(IndexIterSeekToFirst())) {
		  DecodeCurrKeyValue();
		  validx_ = 1;
	  }
  }

  void SeekToLast() override {
	  if (UnzipIterRecord(IndexIterSeekToLast())) {
		  validx_ = valnum_ - 1;
		  DecodeCurrKeyValue();
	  }
  }

  void SeekForPrev(const Slice& target) override {
#if 0
    assert(false);
    status_ =
        Status::NotSupported("SeekForPrev() is not supported in TerarkZipTableIterator");
#else
    SeekForPrevImpl(target, &table_->table_reader_options_.internal_comparator);
#endif
  }

  void Seek(const Slice& target) override {
	  ParsedInternalKey pikey;
	  if (!ParseInternalKey(target, &pikey)) {
		  status_ = Status::InvalidArgument("TerarkZipTableIterator::Seek()",
				  "param target.size() < 8");
		  SetIterInvalid();
		  return;
	  }
	  TryPinBuffer(interKeyBuf_xx_);
	  // Damn MySQL-rocksdb may use "rev:" comparator
	  size_t cplen = commonPrefix_.commonPrefixLen(fstringOf(pikey.user_key));
    if (commonPrefix_.size() != cplen) {
      if (pikey.user_key.size() == cplen) {
        assert(pikey.user_key.size() < commonPrefix_.size());
        if (reverse_) {
          SeekToLast();
          if (iter_->Valid()) {
            this->Next(); // move  to EOF
            assert(!this->Valid());
          }
        }
        else {
          SeekToFirst();
        }
      }
      else {
        assert(pikey.user_key.size() > cplen);
        assert(pikey.user_key[cplen] != commonPrefix_[cplen]);
        if ((byte_t(pikey.user_key[cplen]) < commonPrefix_[cplen]) ^ reverse_) {
          SeekToFirst();
        } else {
          SeekToLast();
        }
      }
      return;
    }
    bool ok = iter_->Seek(fstringOf(pikey.user_key).substr(cplen));
    if (!ok) { // searchKey is bytewise greater than all keys in database
      if (reverse_) {
        // searchKey is reverse_bytewise less than all keys in database
        iter_->SeekToLast();
        ok = iter_->Valid();
      }
    }
    else { // now iter is at bytewise lower bound position
      int cmp = iter_->key().compare(SubStr(pikey.user_key, cplen));
      assert(cmp >= 0); // iterKey >= searchKey
      if (cmp > 0 && reverse_) {
        iter_->Prev();
        ok = iter_->Valid();
      }
    }
	  if (UnzipIterRecord(ok)) {
		  do {
			  DecodeCurrKeyValue();
			  validx_++;
			  if (pInterKey_.sequence <= pikey.sequence) {
				  return; // done
			  }
		  } while (validx_ < valnum_);
		  // no visible version/sequence for target, use Next();
		  // if using Next(), version check is not needed
		  Next();
	  }
  }

  void Next() override {
	  assert(iter_->Valid());
	  if (validx_ < valnum_) {
		  DecodeCurrKeyValue();
		  validx_++;
	  }
	  else {
		  if (UnzipIterRecord(IndexIterNext())) {
			  DecodeCurrKeyValue();
			  validx_ = 1;
		  }
	  }
  }

  void Prev() override {
    assert(iter_->Valid());
	  if (validx_ > 0) {
		  validx_--;
		  DecodeCurrKeyValue();
	  }
	  else {
		  if (UnzipIterRecord(IndexIterPrev())) {
			  validx_ = valnum_ - 1;
			  DecodeCurrKeyValue();
		  }
	  }
  }

  Slice key() const override {
    assert(iter_->Valid());
	  return SliceOf(interKeyBuf_xx_);
  }

  Slice value() const override {
    assert(iter_->Valid());
	  return userValue_;
  }

  Status status() const override {
	  return status_;
  }

  bool IsKeyPinned() const {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled();
  }
  bool IsValuePinned() const {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled();
  }

private:
  void SetIterInvalid() {
    TryPinBuffer(interKeyBuf_xx_);
    iter_->SetInvalid();
	  validx_ = 0;
	  valnum_ = 0;
	  pInterKey_.user_key = Slice();
	  pInterKey_.sequence = uint64_t(-1);
	  pInterKey_.type = kMaxValue;
  }
  bool IndexIterSeekToFirst() {
    TryPinBuffer(interKeyBuf_xx_);
    if (reverse_)
      return iter_->SeekToLast();
    else
      return iter_->SeekToFirst();
  }
  bool IndexIterSeekToLast() {
    TryPinBuffer(interKeyBuf_xx_);
    if (reverse_)
      return iter_->SeekToFirst();
    else
      return iter_->SeekToLast();
  }
  bool IndexIterPrev() {
    TryPinBuffer(interKeyBuf_xx_);
    if (reverse_)
      return iter_->Next();
    else
      return iter_->Prev();
  }
  bool IndexIterNext() {
    TryPinBuffer(interKeyBuf_xx_);
    if (reverse_)
      return iter_->Prev();
    else
      return iter_->Next();
  }
  void TryPinBuffer(valvec<byte_t>& buf) {
    if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
      pinned_buffer_.push_back();
      pinned_buffer_.back().swap(buf);
    }
  }
  bool UnzipIterRecord(bool hasRecord) {
	  if (hasRecord) {
		  size_t recId = iter_->id();
		  try {
		    TryPinBuffer(valueBuf_);
			  table_->valstore_->get_record(recId, &valueBuf_);
		  }
		  catch (const BadCrc32cException& ex) { // crc checksum error
			  SetIterInvalid();
			  status_ = Status::Corruption(
				"TerarkZipTableIterator::UnzipIterRecord()", ex.what());
			  return false;
		  }
		  zValtype_ = ZipValueType(table_->typeArray_[recId]);
		  if (ZipValueType::kMulti == zValtype_) {
			  auto zmValue = (ZipValueMultiValue*)(valueBuf_.data());
			  assert(zmValue->num > 0);
			  valnum_ = zmValue->num;
		  } else {
			  valnum_ = 1;
		  }
		  validx_ = 0;
		  pInterKey_.user_key = SliceOf(iter_->key());
		  return true;
	  }
	  else {
		  SetIterInvalid();
		  return false;
	  }
  }
  void DecodeCurrKeyValue() {
	assert(status_.ok());
	assert(iter_->id() < table_->keyIndex_->NumKeys());
	switch (zValtype_) {
	default:
		status_ = Status::Aborted("TerarkZipTableReader::Get()",
				"Bad ZipValueType");
		abort(); // must not goes here, if it does, it should be a bug!!
		break;
	case ZipValueType::kZeroSeq:
		pInterKey_.sequence = 0;
		pInterKey_.type = kTypeValue;
		userValue_ = SliceOf(valueBuf_);
		break;
	case ZipValueType::kValue: // should be a kTypeValue, the normal case
		// little endian uint64_t
		pInterKey_.sequence = *(uint64_t*)valueBuf_.data() & kMaxSequenceNumber;
		pInterKey_.type = kTypeValue;
		userValue_ = SliceOf(fstring(valueBuf_).substr(7));
		break;
	case ZipValueType::kDelete:
		// little endian uint64_t
		pInterKey_.sequence = *(uint64_t*)valueBuf_.data() & kMaxSequenceNumber;
		pInterKey_.type = kTypeDeletion;
		userValue_ = Slice();
		break;
	case ZipValueType::kMulti: { // more than one value
		auto zmValue = (const ZipValueMultiValue*)(valueBuf_.data());
		assert(0 != valnum_);
		assert(validx_ < valnum_);
		assert(valnum_ == zmValue->num);
		Slice d = zmValue->getValueData(validx_);
		auto snt = unaligned_load<SequenceNumber>(d.data());
		UnPackSequenceAndType(snt, &pInterKey_.sequence, &pInterKey_.type);
		d.remove_prefix(sizeof(SequenceNumber));
		userValue_ = d;
		break; }
	}
	interKeyBuf_.assign(commonPrefix_.data(), commonPrefix_.size());
	AppendInternalKey(&interKeyBuf_, pInterKey_);
	interKeyBuf_xx_.assign((byte_t*)interKeyBuf_.data(), interKeyBuf_.size());
  }

  const TerarkZipTableReader* const table_;
  const unique_ptr<TerocksIndex::Iterator> iter_;
  const fstring commonPrefix_;
  const bool reverse_;
  ParsedInternalKey pInterKey_;
  std::string interKeyBuf_;
  valvec<byte_t> interKeyBuf_xx_;
  valvec<byte_t> valueBuf_;
  Slice  userValue_;
  ZipValueType zValtype_;
  size_t valnum_;
  size_t validx_;
  Status status_;
  PinnedIteratorsManager*  pinned_iters_mgr_;
  valvec<valvec<byte_t> >  pinned_buffer_;
};

TerarkZipTableReader::~TerarkZipTableReader() {
	typeArray_.risk_release_ownership();
}

TerarkZipTableReader::TerarkZipTableReader(const TableReaderOptions& tro,
      const TerarkZipTableOptions& tzto)
 : table_reader_options_(tro)
 , tzto_(tzto)
{
  isReverseBytewiseOrder_ = false;
}

static void MmapWarmUpBytes(const void* addr, size_t len) {
  auto base = (const byte_t*)(uintptr_t(addr) & uintptr_t(~4095));
  auto size = terark::align_up((size_t(addr) & 4095) + len, 4096);
#ifdef POSIX_MADV_WILLNEED
  posix_madvise((void*)addr, len, POSIX_MADV_WILLNEED);
#endif
  for (size_t i = 0; i < size; i += 4096) {
    volatile byte_t unused = ((const volatile byte_t*)base)[i];
    (void)unused;
  }
}
template<class T>
static void MmapWarmUp(const T* addr, size_t len) {
  MmapWarmUpBytes((T*)addr, sizeof(T)*len);
}
static void MmapWarmUp(fstring mem) {
  MmapWarmUpBytes(mem.data(), mem.size());
}
//static fstring memBlockOf(const UintVecMin0& uv) {
//  return fstring(uv.data(), uv.mem_size());
//}
static void MmapWarmUp(const UintVecMin0& uv) {
  MmapWarmUpBytes(uv.data(), uv.mem_size());
}

Status
TerarkZipTableReader::Open(RandomAccessFileReader* file, uint64_t file_size) {
  file_.reset(file); // take ownership
  const auto& ioptions = table_reader_options_.ioptions;
  TableProperties* props = nullptr;
  Status s = ReadTableProperties(file, file_size,
		  	  kTerarkZipTableMagicNumber, ioptions, &props);
  if (!s.ok()) {
    return s;
  }
  assert(nullptr != props);
  unique_ptr<TableProperties> uniqueProps(props);
  Slice file_data;
  if (table_reader_options_.env_options.use_mmap_reads) {
	s = file->Read(0, file_size, &file_data, nullptr);
	if (!s.ok())
		return s;
  } else {
    return Status::InvalidArgument("TerarkZipTableReader::Open()",
			"EnvOptions::use_mmap_reads must be true");
  }
  file_data_ = file_data;
  table_properties_.reset(uniqueProps.release());
  isReverseBytewiseOrder_ =
      fstring(ioptions.user_comparator->Name()).startsWith("rev:");
  BlockContents valueDictBlock, indexBlock, zValueTypeBlock;
  BlockContents commonPrefixBlock;
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
		  kTerarkZipTableValueDictBlock, &valueDictBlock);
  if (!s.ok()) {
	  return s;
  }
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
		  kTerarkZipTableIndexBlock, &indexBlock);
  if (!s.ok()) {
	  return s;
  }
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
		  kTerarkZipTableValueTypeBlock, &zValueTypeBlock);
  if (!s.ok()) {
	  return s;
  }
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
      kTerarkZipTableCommonPrefixBlock, &commonPrefixBlock);
  if (s.ok()) {
    commonPrefix_ = commonPrefixBlock.data;
  }
  else {
    // some error, usually is
    // Status::Corruption("Cannot find the meta block", meta_block_name)
    WARN(ioptions.info_log
        , "Read %s block failed, treat as old SST version, error: %s\n"
        , kTerarkZipTableCommonPrefixBlock.c_str()
        , s.ToString().c_str());
  }
  try {
	  valstore_.reset(new DictZipBlobStore());
	  valstore_->load_user_memory(
			  fstringOf(valueDictBlock.data),
			  fstring(file_data.data(), props->data_size));
  }
  catch (const BadCrc32cException& ex) {
	  return Status::Corruption("TerarkZipTableReader::Open()", ex.what());
  }
  s = LoadIndex(indexBlock.data);
  if (!s.ok()) {
	  return s;
  }
  size_t recNum = keyIndex_->NumKeys();
  typeArray_.risk_set_data((byte_t*)zValueTypeBlock.data.data(),
		  recNum, kZipValueTypeBits);
  long long t0 = g_pf.now();
  MmapWarmUp(fstringOf(indexBlock.data));
  MmapWarmUp(valstore_->get_dict());
  MmapWarmUp(valstore_->get_index());
  long long t1 = g_pf.now();
  keyIndex_->BuildCache(tzto_.indexCacheRatio);
  long long t2 = g_pf.now();
	INFO(ioptions.info_log
    , "TerarkZipTableReader::Open(): fsize=%zd, entries=%zd keys=%zd indexSize=%zd valueSize=%zd, warm up time = %6.3f'sec, build cache time = %6.3f'sec\n"
		, size_t(file_size), size_t(table_properties_->num_entries)
		, keyIndex_->NumKeys()
		, size_t(table_properties_->index_size)
		, size_t(table_properties_->data_size)
		, g_pf.sf(t0, t1)
		, g_pf.sf(t1, t2)
	);
  return Status::OK();
}

Status TerarkZipTableReader::LoadIndex(Slice mem) {
  auto func = "TerarkZipTableReader::LoadIndex()";
  try {
	  keyIndex_ = TerocksIndex::LoadMemory(fstringOf(mem));
  }
  catch (const BadCrc32cException& ex) {
	  return Status::Corruption(func, ex.what());
  }
  catch (const std::exception& ex) {
	  return Status::InvalidArgument(func, ex.what());
  }
  return Status::OK();
}

InternalIterator*
TerarkZipTableReader::
NewIterator(const ReadOptions& ro, Arena* arena, bool skip_filters) {
	(void)skip_filters; // unused
	if (arena) {
		return new(arena->AllocateAligned(sizeof(TerarkZipTableIterator)))
				TerarkZipTableIterator(this);
	}
	else {
		return new TerarkZipTableIterator(this);
	}
}

Status
TerarkZipTableReader::Get(const ReadOptions& ro, const Slice& ikey,
						  GetContext* get_context, bool skip_filters) {
  MY_THREAD_LOCAL(valvec<byte_t>, g_tbuf);
	ParsedInternalKey pikey;
	if (!ParseInternalKey(ikey, &pikey)) {
	  return Status::InvalidArgument("TerarkZipTableReader::Get()",
	      "bad internal key causing ParseInternalKey() failed");
	}
  size_t cplen = pikey.user_key.difference_offset(commonPrefix_);
  if (commonPrefix_.size() != cplen) {
    return Status::OK();
  }
	size_t recId = keyIndex_->Find(fstringOf(pikey.user_key).substr(cplen));
	if (size_t(-1) == recId) {
		return Status::OK();
	}
	try {
		valstore_->get_record(recId, &g_tbuf);
	}
	catch (const terark::BadChecksumException& ex) {
		return Status::Corruption("TerarkZipTableReader::Get()", ex.what());
	}
	switch (ZipValueType(typeArray_[recId])) {
	default:
		return Status::Aborted("TerarkZipTableReader::Get()", "Bad ZipValueType");
	case ZipValueType::kZeroSeq:
		get_context->SaveValue(Slice((char*)g_tbuf.data(), g_tbuf.size()), 0);
		break;
	case ZipValueType::kValue: { // should be a kTypeValue, the normal case
		// little endian uint64_t
		uint64_t seq = *(uint64_t*)g_tbuf.data() & kMaxSequenceNumber;
		if (seq <= pikey.sequence) {
			get_context->SaveValue(SliceOf(fstring(g_tbuf).substr(7)), seq);
		}
		break; }
	case ZipValueType::kDelete: {
		// little endian uint64_t
		uint64_t seq = *(uint64_t*)g_tbuf.data() & kMaxSequenceNumber;
		if (seq <= pikey.sequence) {
			get_context->SaveValue(
				ParsedInternalKey(pikey.user_key, seq, kTypeDeletion),
				Slice());
		}
		break; }
	case ZipValueType::kMulti: { // more than one value
		auto mVal = (const ZipValueMultiValue*)g_tbuf.data();
		for(size_t i = 0; i < size_t(mVal->num); ++i) {
			Slice val = mVal->getValueData(i);
			SequenceNumber sn;
			ValueType valtype;
			{
				auto snt = unaligned_load<SequenceNumber>(val.data());
				UnPackSequenceAndType(snt, &sn, &valtype);
			}
			if (sn <= pikey.sequence) {
				val.remove_prefix(sizeof(SequenceNumber));
				// only kTypeMerge will return true
				bool hasMoreValue = get_context->SaveValue(
					ParsedInternalKey(pikey.user_key, sn, valtype), val);
				if (!hasMoreValue) {
					break;
				}
			}
		}
		break; }
	}
	if (g_tbuf.capacity() > 512*1024) {
	  g_tbuf.clear(); // free large thread local memory
	}
	return Status::OK();
}

///////////////////////////////////////////////////////////////////////////////

TerarkZipTableBuilder::TerarkZipTableBuilder(
		const TerarkZipTableOptions& tzto,
		const ImmutableCFOptions& ioptions,
		const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*,
		uint32_t column_family_id,
		WritableFileWriter* file,
		const std::string& column_family_name)
  : table_options_(tzto)
  , ioptions_(ioptions)
{
  file_ = file;
  sampleUpperBound_ = randomGenerator_.max() * table_options_.sampleRatio;
  tmpValueFile_.path = tzto.localTempDir + "/Terocks-XXXXXX";
  int fd = mkstemp(&tmpValueFile_.path[0]);
  if (fd < 0) {
    int err = errno;
    THROW_STD(invalid_argument
        , "ERROR: TerarkZipTableBuilder::TerarkZipTableBuilder(): mkstemp(%s) = %s\n"
        , tmpValueFile_.path.c_str(), strerror(err));
  }
  tmpValueFile_.dopen(fd);
  tmpKeyFile_.path = tmpValueFile_.path + ".keydata";
  tmpKeyFile_.open();
  tmpSampleFile_.path = tmpValueFile_.path + ".sample";
  tmpSampleFile_.open();

  properties_.fixed_key_len = 0;
  properties_.num_data_blocks = 1;
  properties_.column_family_id = column_family_id;
  properties_.column_family_name = column_family_name;
}

TerarkZipTableBuilder::~TerarkZipTableBuilder() {
}

uint64_t TerarkZipTableBuilder::FileSize() const {
	if (0 == offset_) {
		// for compaction caller to split file by increasing size
		auto kvLen = properties_.raw_key_size +  properties_.raw_value_size;
		return uint64_t(kvLen * table_options_.estimateCompressionRatio);
	} else {
		return offset_;
	}
}

void TerarkZipTableBuilder::Add(const Slice& key, const Slice& value) {
	assert(key.size() >= 8);
	fstring userKey(key.data(), key.size()-8);
	if (terark_likely(size_t(-1) != numUserKeys_)) {
		if (prevUserKey_ != userKey) {
			assert(prevUserKey_ < userKey);
			commonPrefixLen_ = fstring(prevUserKey_.data(), commonPrefixLen_)
			                 . commonPrefixLen(userKey);
      minUserKeyLen_ = std::min(userKey.size(), minUserKeyLen_);
      maxUserKeyLen_ = std::max(userKey.size(), maxUserKeyLen_);
			AddPrevUserKey();
			prevUserKey_.assign(userKey);
		}
	}
	else {
    commonPrefixLen_ = userKey.size();
    minUserKeyLen_ = userKey.size();
    maxUserKeyLen_ = userKey.size();
		prevUserKey_.assign(userKey);
		lenUserKeys_ = 0;
		numUserKeys_ = 0;
		t0 = g_pf.now();
	}
	valueBits_.push_back(true);
	if (!value.empty() && randomGenerator_() < sampleUpperBound_) {
	  tmpSampleFile_.writer << fstringOf(value);
		sampleLenSum_ += value.size();
	}
	if (!second_pass_iter_) {
	  tmpValueFile_.writer.ensureWrite(userKey.end(), 8);
	  tmpValueFile_.writer << fstringOf(value);
	}
	properties_.num_entries++;
	properties_.raw_key_size += key.size();
	properties_.raw_value_size += value.size();
}

template<class ByteArray>
static
Status WriteBlock(const ByteArray& blockData, WritableFileWriter* file,
                  uint64_t* offset, BlockHandle* block_handle) {
  block_handle->set_offset(*offset);
  block_handle->set_size(blockData.size());
  Status s = file->Append(SliceOf(blockData));
  if (s.ok()) {
    *offset += blockData.size();
  }
  return s;
}

Status TerarkZipTableBuilder::Finish() {
	assert(!closed_);
	closed_ = true;

	valvec<byte_t> commonPrefix(prevUserKey_.data(), commonPrefixLen_);
	AddPrevUserKey();
	tmpKeyFile_.complete_write();
  if (!second_pass_iter_) {
    tmpValueFile_.complete_write();
  }
  tmpSampleFile_.complete_write();

	AutoDeleteFile tmpIndexFile{tmpValueFile_.path + ".index"};
	AutoDeleteFile tmpStoreFile{tmpValueFile_.path + ".zbs"};
	AutoDeleteFile tmpStoreDict{tmpValueFile_.path + ".zbs-dict"};
  DictZipBlobStore::ZipStat dzstat;

	long long rawBytes = properties_.raw_key_size + properties_.raw_value_size;
	{
	  long long tt = g_pf.now();
	  INFO(ioptions_.info_log
	      , "TerarkZipTableBuilder::Finish():this=%p:  first pass time =%7.2f's, %8.3f'MB/sec\n"
	      , this, g_pf.sf(t0,tt), rawBytes*1.0/g_pf.uf(t0,tt)
	      );
	}
  static std::mutex zipMutex;
  static std::condition_variable zipCond;
  static size_t sumWorkingMem = 0;
  const  size_t softMemLimit = table_options_.softZipWorkingMemLimit;
  const  size_t hardMemLimit = std::max(table_options_.hardZipWorkingMemLimit, softMemLimit);

// indexing is also slow, run it in parallel
std::future<void> asyncIndexResult = std::async(std::launch::async, [&]()
{
  const size_t myWorkMem = lenUserKeys_ +
              sizeof(SortableStrVec::SEntry) * numUserKeys_;
  const size_t smallmem = 1000*1024*1024;
  {
    std::unique_lock<std::mutex> zipLock(zipMutex);
    if (myWorkMem < softMemLimit) {
      while ( (sumWorkingMem + myWorkMem >= softMemLimit && myWorkMem >= smallmem)
          ||  (sumWorkingMem + myWorkMem >= hardMemLimit) ) {
        INFO(ioptions_.info_log
            , "TerarkZipTableBuilder::Finish():this=%p: wait, sumWorkingMem = %f'GB, indexWorkingMem = %f'GB\n"
            , this, sumWorkingMem/1e9, myWorkMem/1e9
            );
        zipCond.wait(zipLock);
      }
    }
    else {
      // relaxed condition is for preventing large job starvation
      // and, even for very large index, we should allowing it to be built
      // because even very large index is unlikely hit NestLoudsTrie limit
      while (sumWorkingMem > softMemLimit/2) {
        zipCond.wait(zipLock);
      }
    }
    sumWorkingMem += myWorkMem;
  }
  BOOST_SCOPE_EXIT(myWorkMem){
    std::unique_lock<std::mutex> zipLock(zipMutex);
    assert(sumWorkingMem >= myWorkMem);
    sumWorkingMem -= myWorkMem;
    zipCond.notify_all();
  }BOOST_SCOPE_EXIT_END;

  long long t1 = g_pf.now();
  {
    auto factory = TerocksIndex::GetFactory(table_options_.indexType);
    if (!factory) {
      THROW_STD(invalid_argument,
          "invalid indexType: %s", table_options_.indexType.c_str());
    }
    factory->Build(tmpKeyFile_, table_options_, tmpIndexFile.fpath
        , commonPrefixLen_, minUserKeyLen_, maxUserKeyLen_
        , numUserKeys_, lenUserKeys_);
  }
  long long tt = g_pf.now();
  INFO(ioptions_.info_log
      , "TerarkZipTableBuilder::Finish():this=%p:  index pass time =%7.2f's, %8.3f'MB/sec\n"
      , this, g_pf.sf(t1,tt), properties_.raw_key_size*1.0/g_pf.uf(t1,tt)
      );
});
  long long t3 = 0;
  size_t realsampleLenSum = 0;
	unique_ptr<DictZipBlobStore> zstore;
	UintVecMin0 zvType(properties_.num_entries, kZipValueTypeBits);
{
  const  size_t smalldictMem = 6*200*1024*1024;
  const  size_t myDictMem = std::min<size_t>(sampleLenSum_, INT32_MAX) * 6; // include samples self
  {
    std::unique_lock<std::mutex> zipLock(zipMutex);
    if (myDictMem < softMemLimit) {
      while ( (sumWorkingMem + myDictMem >= softMemLimit && myDictMem >= smalldictMem)
          ||  (sumWorkingMem + myDictMem >= hardMemLimit) ) {
        INFO(ioptions_.info_log
            , "TerarkZipTableBuilder::Finish():this=%p: wait, sumWorkingMem = %f'GB, dictZipWorkingMem = %f'GB\n"
            , this, sumWorkingMem/1e9, myDictMem/1e9
            );
        zipCond.wait(zipLock);
      }
    }
    else {
      while (sumWorkingMem > 0) {
        INFO(ioptions_.info_log
            , "TerarkZipTableBuilder::Finish():this=%p: wait, sumWorkingMem = %f'GB, dictZipWorkingMem = %f'GB\n"
            , this, sumWorkingMem/1e9, myDictMem/1e9
            );
        zipCond.wait(zipLock);
      }
    }
    sumWorkingMem += myDictMem;
  }
  BOOST_SCOPE_EXIT(myDictMem){
    std::unique_lock<std::mutex> zipLock(zipMutex);
    assert(sumWorkingMem >= myDictMem);
    sumWorkingMem -= myDictMem;
    zipCond.notify_all();
  }BOOST_SCOPE_EXIT_END;

  t3 = g_pf.now();
  DictZipBlobStore::Options dzopt;
  dzopt.entropyAlgo = DictZipBlobStore::Options::EntropyAlgo(table_options_.entropyAlgo);
  dzopt.checksumLevel = table_options_.checksumLevel;
  dzopt.useSuffixArrayLocalMatch = table_options_.useSuffixArrayLocalMatch;
  auto zbuilder = UniquePtrOf(DictZipBlobStore::createZipBuilder(dzopt));
{
#if defined(TERARK_ZIP_TRIAL_VERSION)
  zbuilder->addSample(g_trail_rand_delete);
#endif
  valvec<byte_t> sample;
  NativeDataInput<InputBuffer> input(&tmpSampleFile_.fp);
  if (sampleLenSum_ < INT32_MAX) {
    for (size_t len = 0; len < sampleLenSum_; ) {
      input >> sample;
      zbuilder->addSample(sample);
      len += sample.size();
    }
    realsampleLenSum = sampleLenSum_;
  }
  else {
    uint64_t upperBound2 = uint64_t(
        randomGenerator_.max() * double(INT32_MAX) / sampleLenSum_);
    for (size_t len = 0; len < sampleLenSum_; ) {
      input >> sample;
      if (randomGenerator_() < upperBound2) {
        zbuilder->addSample(sample);
        realsampleLenSum += sample.size();
      }
      len += sample.size();
    }
  }
  tmpSampleFile_.close();
  if (0 == realsampleLenSum) { // prevent from empty
    zbuilder->addSample("Hello World!");
  }
  zbuilder->finishSample();
  zbuilder->prepare(properties_.num_entries, tmpStoreFile);
}
	if (nullptr == second_pass_iter_)
{
	NativeDataInput<InputBuffer> input(&tmpValueFile_.fp);
	valvec<byte_t> value;
#if defined(TERARK_ZIP_TRIAL_VERSION)
  valvec<byte_t> tmpValueBuf;
#endif
	size_t entryId = 0;
	size_t bitPos = 0;
	for (size_t recId = 0; recId < numUserKeys_; recId++) {
		uint64_t seqType = input.load_as<uint64_t>();
		uint64_t seqNum;
		ValueType vType;
		UnPackSequenceAndType(seqType, &seqNum, &vType);
		size_t oneSeqLen = valueBits_.one_seq_len(bitPos);
		assert(oneSeqLen >= 1);
		if (1==oneSeqLen && (kTypeDeletion==vType || kTypeValue==vType)) {
			if (0 == seqNum && kTypeValue==vType) {
				zvType.set_wire(recId, size_t(ZipValueType::kZeroSeq));
#if defined(TERARK_ZIP_TRIAL_VERSION)
				if (randomGenerator_() < randomGenerator_.max()/1000) {
				  input >> tmpValueBuf;
				  value.assign(fstring(g_trail_rand_delete));
				} else
#endif
				  input >> value;
			} else {
				if (kTypeValue==vType) {
					zvType.set_wire(recId, size_t(ZipValueType::kValue));
				} else {
					zvType.set_wire(recId, size_t(ZipValueType::kDelete));
				}
				value.erase_all();
				value.append((byte_t*)&seqNum, 7);
				input.load_add(value);
			}
		}
		else {
			zvType.set_wire(recId, size_t(ZipValueType::kMulti));
			size_t headerSize = ZipValueMultiValue::calcHeaderSize(oneSeqLen);
			value.resize(headerSize);
			((ZipValueMultiValue*)value.data())->num = oneSeqLen;
			((ZipValueMultiValue*)value.data())->offsets[0] = 0;
			for (size_t j = 0; j < oneSeqLen; j++) {
				if (j > 0) {
					seqType = input.load_as<uint64_t>();
				}
				value.append((byte_t*)&seqType, 8);
				input.load_add(value);
				((ZipValueMultiValue*)value.data())->offsets[j+1] = value.size() - headerSize;
			}
		}
    zbuilder->addRecord(value);
		bitPos += oneSeqLen + 1;
		entryId += oneSeqLen;
	}
  assert(entryId == properties_.num_entries);
}
	else
{
  valvec<byte_t> value;
  size_t entryId = 0;
  size_t bitPos = 0;
  for (size_t recId = 0; recId < numUserKeys_; recId++) {
    value.erase_all();
    assert(second_pass_iter_->Valid());
    Slice curKey = second_pass_iter_->key();
    Slice curVal = second_pass_iter_->value();
    ParsedInternalKey pikey;
    ParseInternalKey(curKey, &pikey);
    size_t oneSeqLen = valueBits_.one_seq_len(bitPos);
    assert(oneSeqLen >= 1);
    if (1==oneSeqLen && (kTypeDeletion==pikey.type || kTypeValue==pikey.type)) {
    //  assert(fstringOf(pikey.user_key) == backupKeys[recId]);
      if (0 == pikey.sequence && kTypeValue==pikey.type) {
        zvType.set_wire(recId, size_t(ZipValueType::kZeroSeq));
        zbuilder->addRecord(fstringOf(curVal));
      } else {
        if (kTypeValue==pikey.type) {
          zvType.set_wire(recId, size_t(ZipValueType::kValue));
        } else {
          zvType.set_wire(recId, size_t(ZipValueType::kDelete));
        }
        value.append((byte_t*)&pikey.sequence, 7);
        value.append(fstringOf(curVal));
        zbuilder->addRecord(value);
      }
      second_pass_iter_->Next();
    }
    else {
      zvType.set_wire(recId, size_t(ZipValueType::kMulti));
      size_t headerSize = ZipValueMultiValue::calcHeaderSize(oneSeqLen);
      value.resize(headerSize);
      ((ZipValueMultiValue*)value.data())->num = oneSeqLen;
      ((ZipValueMultiValue*)value.data())->offsets[0] = 0;
      for (size_t j = 0; j < oneSeqLen; j++) {
        curKey = second_pass_iter_->key();
        curVal = second_pass_iter_->value();
        ParseInternalKey(curKey, &pikey);
      //  assert(fstringOf(pikey.user_key) == backupKeys[recId]);
        uint64_t seqType = PackSequenceAndType(pikey.sequence, pikey.type);
        value.append((byte_t*)&seqType, 8);
        value.append(fstringOf(curVal));
        ((ZipValueMultiValue*)value.data())->offsets[j+1] = value.size() - headerSize;
        second_pass_iter_->Next();
      }
      zbuilder->addRecord(value);
    }
    bitPos += oneSeqLen + 1;
    entryId += oneSeqLen;
  }
  assert(entryId == properties_.num_entries);
}

  tmpValueFile_.close();
  zstore.reset(zbuilder->finish());
  dzstat = zbuilder->getZipStat();
  zbuilder.reset();
}

  long long t4 = g_pf.now();

  // wait for indexing complete, if indexing is slower than value compressing
  asyncIndexResult.get();
  long long t5 = g_pf.now();
	unique_ptr<TerocksIndex> index(TerocksIndex::LoadFile(tmpIndexFile));
	assert(index->NumKeys() == numUserKeys_);
	Status s;
  BlockHandle dataBlock, dictBlock, indexBlock, zvTypeBlock;
  BlockHandle commonPrefixBlock;
{
  size_t real_size = index->Memory().size() + zstore->mem_size() + zvType.mem_size();
  size_t block_size, last_allocated_block;
  file_->writable_file()->GetPreallocationStatus(&block_size, &last_allocated_block);
  INFO(ioptions_.info_log
    , "TerarkZipTableBuilder::Finish():this=%p: old prealloc_size = %zd, real_size = %zd\n"
    , this, block_size, real_size
    );
  file_->writable_file()->SetPreallocationBlockSize(1*1024*1024 + real_size);
}
  long long t6, t7;
  offset_ = 0;
  if (index->NeedsReorder()) {
		UintVecMin0 zvType2(numUserKeys_, kZipValueTypeBits);
		terark::AutoFree<uint32_t> newToOld(numUserKeys_, UINT32_MAX);
		index->GetOrderMap(newToOld.p);
		t6 = g_pf.now();
		if (fstring(ioptions_.user_comparator->Name()).startsWith("rev:")) {
		  // Damn reverse bytewise order
      for (size_t newId = 0; newId < numUserKeys_; ++newId) {
        size_t dictOrderOldId = newToOld.p[newId];
        size_t reverseOrderId = numUserKeys_ - dictOrderOldId - 1;
        newToOld.p[newId] = reverseOrderId;
        zvType2.set_wire(newId, zvType[reverseOrderId]);
      }
		}
		else {
		  for (size_t newId = 0; newId < numUserKeys_; ++newId) {
		    size_t dictOrderOldId = newToOld.p[newId];
		    zvType2.set_wire(newId, zvType[dictOrderOldId]);
		  }
		}
		t7 = g_pf.now();
		try {
		  dataBlock.set_offset(offset_);
		  zstore->reorder_zip_data(newToOld, [&](const void* data, size_t size) {
		    s = file_->Append(Slice((const char*)data, size));
		    if (!s.ok()) {
		      throw s;
		    }
		    offset_ += size;
		  });
		  dataBlock.set_size(offset_ - dataBlock.offset());
		} catch (const Status&) {
		  return s;
		}
		zvType.clear();
		zvType.swap(zvType2);
	}
  else {
    t7 = t6 = t5;
    WriteBlock(zstore->get_data(), file_, &offset_, &dataBlock);
  }
  WriteBlock(commonPrefix, file_, &offset_, &commonPrefixBlock);
	properties_.data_size = dataBlock.size();
	s = WriteBlock(zstore->get_dict(), file_, &offset_, &dictBlock);
	if (!s.ok()) {
		return s;
	}
	s = WriteBlock(index->Memory(), file_, &offset_, &indexBlock);
	if (!s.ok()) {
		return s;
	}
	fstring zvTypeMem(zvType.data(), zvType.mem_size());
	s = WriteBlock(zvTypeMem, file_, &offset_, &zvTypeBlock);
	if (!s.ok()) {
		return s;
	}
	index.reset();
	zstore.reset();
	tmpStoreFile.Delete();
	tmpStoreDict.Delete();
	tmpIndexFile.Delete();
	properties_.index_size = indexBlock.size();
	MetaIndexBuilder metaindexBuiler;
	metaindexBuiler.Add(kTerarkZipTableValueDictBlock, dictBlock);
	metaindexBuiler.Add(kTerarkZipTableIndexBlock, indexBlock);
	metaindexBuiler.Add(kTerarkZipTableValueTypeBlock, zvTypeBlock);
	metaindexBuiler.Add(kTerarkZipTableCommonPrefixBlock, commonPrefixBlock);
	PropertyBlockBuilder propBlockBuilder;
	propBlockBuilder.AddTableProperty(properties_);
	propBlockBuilder.Add(properties_.user_collected_properties);
	NotifyCollectTableCollectorsOnFinish(table_properties_collectors_,
	                                     ioptions_.info_log,
	                                     &propBlockBuilder);
	BlockHandle propBlock, metaindexBlock;
	s = WriteBlock(propBlockBuilder.Finish(), file_, &offset_, &propBlock);
	if (!s.ok()) {
		return s;
	}
	metaindexBuiler.Add(kPropertiesBlock, propBlock);
	s = WriteBlock(metaindexBuiler.Finish(), file_, &offset_, &metaindexBlock);
	if (!s.ok()) {
		return s;
	}
	Footer footer(kTerarkZipTableMagicNumber, 0);
	footer.set_metaindex_handle(metaindexBlock);
	footer.set_index_handle(BlockHandle::NullBlockHandle());
	std::string footer_encoding;
	footer.EncodeTo(&footer_encoding);
	s = file_->Append(footer_encoding);
	if (s.ok()) {
		offset_ += footer_encoding.size();
	}
  long long t8 = g_pf.now();
  INFO(ioptions_.info_log
    ,
R"EOS(TerarkZipTableBuilder::Finish():this=%p: second pass time =%7.2f's, %8.3f'MB/sec, value only(%4.1f%% of KV)
   wait indexing time = %7.2f's,
  remap KeyValue time = %7.2f's, %8.3f'MB/sec (all stages of remap)
    Get OrderMap time = %7.2f's, %8.3f'MB/sec (index lex order gen)
  rebuild zvType time = %7.2f's, %8.3f'MB/sec
  write SST data time = %7.2f's, %8.3f'MB/sec
    z-dict build time = %7.2f's, sample length = %7.3f'MB, throughput = %6.3f'MB/sec
    zip my value time = %7.2f's, unzip  length = %7.3f'GB
    zip my value throughput = %7.3f'MB/sec
    zip pipeline throughput = %7.3f'MB/sec
    entries = %zd  keys = %zd  avg-key = %.2f  avg-zkey = %.2f  avg-val = %.2f  avg-zval = %.2f
    UnZipSize{ index =%9.4f GB  value =%9.4f GB  all =%9.4f GB }
    __ZipSize{ index =%9.4f GB  value =%9.4f GB  all =%9.4f GB }
    UnZip/Zip{ index =%9.4f     value =%9.4f     all =%9.4f    }
    Zip/UnZip{ index =%9.4f     value =%9.4f     all =%9.4f    }
)EOS"
    , this, g_pf.sf(t3,t4)
    , properties_.raw_value_size*1.0/g_pf.uf(t3,t4)
    , properties_.raw_value_size*100.0/rawBytes

    , g_pf.sf(t4,t5) // wait indexing time
    , g_pf.sf(t5,t8), double(offset_) / g_pf.uf(t5,t8)

    , g_pf.sf(t5,t6), properties_.index_size/g_pf.uf(t5,t6) // index lex walk

    , g_pf.sf(t6,t7), numUserKeys_*2/8/(g_pf.uf(t6,t7)+1.0) // rebuild zvType

    , g_pf.sf(t7,t8), double(offset_) / g_pf.uf(t7,t8) // write SST data

    , dzstat.dictBuildTime, realsampleLenSum / 1e6
    , realsampleLenSum / dzstat.dictBuildTime / 1e6

    , dzstat.dictZipTime, properties_.raw_value_size / 1e9
    , properties_.raw_value_size  / dzstat.dictZipTime / 1e6
    , dzstat.pipelineThroughBytes / dzstat.dictZipTime / 1e6

    , size_t(properties_.num_entries), numUserKeys_
    , double(lenUserKeys_) / numUserKeys_
    , double(properties_.index_size) / numUserKeys_
    , double(properties_.raw_value_size) / numUserKeys_
    , double(properties_.data_size) / numUserKeys_

    , lenUserKeys_/1e9, properties_.raw_value_size/1e9, rawBytes/1e9

    , properties_.index_size/1e9, properties_.data_size/1e9, offset_/1e9

    , double(lenUserKeys_) / properties_.index_size
    , double(properties_.raw_value_size) / properties_.data_size
    , double(rawBytes) / offset_

    , properties_.index_size / double(lenUserKeys_)
    , properties_.data_size  / double(properties_.raw_value_size)
    , offset_ / double(rawBytes)
  );
	return s;
}

void TerarkZipTableBuilder::Abandon() {
  closed_ = true;
  tmpKeyFile_.complete_write();
  tmpValueFile_.complete_write();
  tmpSampleFile_.complete_write();
}

void TerarkZipTableBuilder::AddPrevUserKey() {
  tmpKeyFile_.writer << prevUserKey_;
	valueBits_.push_back(false);
	lenUserKeys_ += prevUserKey_.size();
	numUserKeys_++;
}

/////////////////////////////////////////////////////////////////////////////

class TerarkZipTableFactory : public TableFactory, boost::noncopyable {
 public:
  explicit
  TerarkZipTableFactory(const TerarkZipTableOptions& tzto, TableFactory* fallback)
  : table_options_(tzto), fallback_factory_(fallback) {
    adaptive_factory_ = NewAdaptiveTableFactory();
  }

  const char* Name() const override { return "TerarkZipTable"; }

  Status
  NewTableReader(const TableReaderOptions& table_reader_options,
                 unique_ptr<RandomAccessFileReader>&& file,
                 uint64_t file_size,
				 unique_ptr<TableReader>* table,
				 bool prefetch_index_and_filter_in_cache) const override;

  TableBuilder*
  NewTableBuilder(const TableBuilderOptions& table_builder_options,
				  uint32_t column_family_id,
				  WritableFileWriter* file) const override;

  std::string GetPrintableTableOptions() const override;

  // Sanitizes the specified DB Options.
  Status SanitizeOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;

  void* GetOptions() override { return &table_options_; }

 private:
  TerarkZipTableOptions table_options_;
  TableFactory* fallback_factory_;
  TableFactory* adaptive_factory_; // just for open table
  mutable size_t nth_new_terark_table_ = 0;
  mutable size_t nth_new_fallback_table_ = 0;
};

class TableFactory*
NewTerarkZipTableFactory(const TerarkZipTableOptions& tzto,
						 class TableFactory* fallback) {
  STD_INFO("NewTerarkZipTableFactory()\n");
	return new TerarkZipTableFactory(tzto, fallback);
}

inline static
bool IsBytewiseComparator(const Comparator* cmp) {
#if 1
  const fstring name = cmp->Name();
  if (name.startsWith("RocksDB_SE_")) {
    return true;
  }
  if (name.startsWith("rev:RocksDB_SE_")) {
    // reverse bytewise compare, needs reverse in iterator
    return true;
  }
	return fstring(cmp->Name()) == "leveldb.BytewiseComparator";
#else
	return BytewiseComparator() == cmp;
#endif
}

Status
TerarkZipTableFactory::NewTableReader(
		const TableReaderOptions& table_reader_options,
		unique_ptr<RandomAccessFileReader>&& file,
		uint64_t file_size, unique_ptr<TableReader>* table,
		bool prefetch_index_and_filter_in_cache)
const {
  auto userCmp = table_reader_options.internal_comparator.user_comparator();
	if (!IsBytewiseComparator(userCmp)) {
		return Status::InvalidArgument("TerarkZipTableFactory::NewTableReader()",
				"user comparator must be 'leveldb.BytewiseComparator'");
	}
#if 0
  if (fstring(userCmp->Name()).startsWith("rev:")) {
    return Status::InvalidArgument("TerarkZipTableFactory::NewTableReader",
        "Damn, fuck out the reverse bytewise comparator");
  }
#endif
	Footer footer;
	Status s = ReadFooterFromFile(file.get(), file_size, &footer);
	if (!s.ok()) {
		return s;
	}
	if (footer.table_magic_number() != kTerarkZipTableMagicNumber) {
	  if (adaptive_factory_) {
	    // just for open table
	    return adaptive_factory_->NewTableReader(table_reader_options,
	              std::move(file), file_size, table,
	              prefetch_index_and_filter_in_cache);
	  }
		if (fallback_factory_) {
			return fallback_factory_->NewTableReader(table_reader_options,
					std::move(file), file_size, table,
					prefetch_index_and_filter_in_cache);
		}
		return Status::InvalidArgument(
			"TerarkZipTableFactory::NewTableReader()",
			"fallback_factory is null and magic_number is not kTerarkZipTable"
			);
	}
#if 0
	if (!prefetch_index_and_filter_in_cache) {
		WARN(table_reader_options.ioptions.info_log
				, "TerarkZipTableFactory::NewTableReader(): "
				  "prefetch_index_and_filter_in_cache = false is ignored, "
				  "all index and data will be loaded in memory\n");
	}
#endif
	std::unique_ptr<TerarkZipTableReader>
	t(new TerarkZipTableReader(table_reader_options, table_options_));
	s = t->Open(file.release(), file_size);
	if (s.ok()) {
	  *table = std::move(t);
	}
	return s;
}

TableBuilder*
TerarkZipTableFactory::NewTableBuilder(
		const TableBuilderOptions& table_builder_options,
		uint32_t column_family_id,
		WritableFileWriter* file)
const {
  auto userCmp = table_builder_options.internal_comparator.user_comparator();
	if (!IsBytewiseComparator(userCmp)) {
		THROW_STD(invalid_argument,
				"TerarkZipTableFactory::NewTableBuilder(): "
				"user comparator must be 'leveldb.BytewiseComparator'");
	}
#if 0
  if (fstring(userCmp->Name()).startsWith("rev:")) {
    THROW_STD(invalid_argument,
        "TerarkZipTableFactory::NewTableBuilder(): "
        "user comparator must be 'leveldb.BytewiseComparator'");
  }
#endif
  int curlevel = table_builder_options.level;
  int numlevel = table_builder_options.ioptions.num_levels;
  int minlevel = table_options_.terarkZipMinLevel;
  if (minlevel < 0) {
    minlevel = numlevel-1;
  }
#if 1
  INFO(table_builder_options.ioptions.info_log
      , "nth_new_talbe{ terark = %zd fallback = %zd } curlevel = %d minlevel = %d numlevel = %d fallback = %p\n"
      , nth_new_terark_table_, nth_new_fallback_table_, curlevel, minlevel, numlevel, fallback_factory_
      );
#endif
	if (fallback_factory_) {
    if (curlevel >= 0 && curlevel < minlevel) {
      nth_new_fallback_table_++;
      TableBuilder* tb = fallback_factory_->NewTableBuilder(table_builder_options,
          column_family_id, file);
      INFO(table_builder_options.ioptions.info_log
          , "TerarkZipTableFactory::NewTableBuilder() returns class: %s\n"
          , typeid(*tb).name());
      return tb;
    }
	}
	nth_new_terark_table_++;
	return new TerarkZipTableBuilder(
			table_options_,
		    table_builder_options.ioptions,
		    table_builder_options.int_tbl_prop_collector_factories,
			column_family_id,
		    file,
		    table_builder_options.column_family_name);
}

std::string TerarkZipTableFactory::GetPrintableTableOptions() const {
  std::string ret;
  ret.reserve(2000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  const auto& to =  table_options_;
#undef AppendF
#define AppendF(...) ret.append(buffer, snprintf(buffer, kBufferSize, ##__VA_ARGS__))
  AppendF("indexNestLevel: %d\n", to.indexNestLevel);
  AppendF("checksumLevel : %d\n", to.checksumLevel);
  AppendF("entropyAlgo   : %d\n", to.entropyAlgo);
  AppendF("terarkZipMinLevel: %d\n", to.terarkZipMinLevel);
  AppendF("useSuffixArrayLocalMatch: %d\n", to.useSuffixArrayLocalMatch);
  AppendF("estimateCompressionRatio: %f\n", to.estimateCompressionRatio);
  AppendF("sampleRatio  : %f\n", to.sampleRatio);
  AppendF("softZipWorkingMemLimit: %8.3f GB\n", to.softZipWorkingMemLimit/1e9);
  AppendF("hardZipWorkingMemLimit: %8.3f GB\n", to.hardZipWorkingMemLimit/1e9);
  ret += "localTempDir : ";
  ret += to.localTempDir;
  ret += "\n";
  return ret;
}

Status
TerarkZipTableFactory::SanitizeOptions(const DBOptions& db_opts,
                       	   	   	   	   const ColumnFamilyOptions& cf_opts)
const {
	if (!IsBytewiseComparator(cf_opts.comparator)) {
		return Status::InvalidArgument("TerarkZipTableFactory::SanitizeOptions()",
				"user comparator must be 'leveldb.BytewiseComparator'");
	}
#if 0
	if (fstring(cf_opts.comparator->Name()).startsWith("rev:")) {
	  return Status::InvalidArgument("TerarkZipTableFactory::SanitizeOptions",
	      "Damn, fuck out the reverse bytewise comparator");
	}
#endif
  auto indexFactory = TerocksIndex::GetFactory(table_options_.indexType);
  if (!indexFactory) {
    std::string msg = "invalid indexType: " + table_options_.indexType;
    return Status::InvalidArgument(msg);
  }
	return Status::OK();
}

} /* namespace rocksdb */
