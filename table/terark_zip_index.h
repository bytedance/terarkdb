#pragma once

#include <terark/fstring.hpp>
#include <terark/histogram.hpp>
#include <terark/entropy/entropy_base.hpp>
#include <terark/valvec.hpp>
#include <terark/util/refcount.hpp>
#include <terark/int_vector.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/util/fstrvec.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <memory>

namespace terark {
class ZReorderMap;
}

namespace rocksdb {

using terark::fstring;
using terark::freq_hist_o1;
using terark::valvec;
using terark::byte_t;
using terark::NativeDataInput;
using terark::InputBuffer;
using terark::fstrvec;
using terark::Uint64Histogram;
using terark::ZReorderMap;
using terark::TerarkContext;
using terark::ContextBuffer;
using std::unique_ptr;

struct TerarkZipTableOptions;
class TempFileDeleteOnClose;
class TerarkKeyReader;
struct ImmutableCFOptions;

class TerarkIndex : boost::noncopyable {
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
    virtual size_t DictRank() const = 0;
    inline bool Valid() const { return size_t(-1) != m_id; }
    inline size_t id() const { return m_id; }
    virtual fstring key() const = 0;
    inline void SetInvalid() { m_id = size_t(-1); }
  };
  struct KeyStat {
    struct DiffItem {
      size_t cur = 0, max = 0, cnt = 0;
    };
    size_t keyCount = 0;
    size_t sumKeyLen = 0;
    size_t minKeyLen = size_t(-1);
    size_t maxKeyLen = 0;
    size_t minPrefixLen = size_t(-1);
    size_t maxPrefixLen = 0;
    size_t sumPrefixLen = 0;
    size_t minSuffixLen = size_t(-1);
    size_t maxSuffixLen = 0;
    size_t entropyLen = 0;
    valvec<byte_t> minKey;
    valvec<byte_t> maxKey;
    valvec<DiffItem> diff;
  };
  struct UintPrefixBuildInfo {
    size_t common_prefix;
    size_t key_length;
    size_t key_count;
    size_t entry_count;
    size_t bit_count0;
    size_t bit_count1;
    uint64_t min_value;
    uint64_t max_value;
    double zip_ratio;
    size_t estimate_size;
    enum UintType{
      fail = 0,
      asc_allone,
      asc_few_zero_3,
      asc_few_zero_4,
      asc_few_zero_5,
      asc_few_zero_6,
      asc_few_zero_7,
      asc_few_zero_8,
      asc_il_256,
      asc_se_512,
      asc_few_one_3,
      asc_few_one_4,
      asc_few_one_5,
      asc_few_one_6,
      asc_few_one_7,
      asc_few_one_8,
      non_desc_il_256,
      non_desc_se_512,
      non_desc_few_one_3,
      non_desc_few_one_4,
      non_desc_few_one_5,
      non_desc_few_one_6,
      non_desc_few_one_7,
      non_desc_few_one_8,
    } type;
  };
  class TerarkIndexDebugBuilder {
    freq_hist_o1 freq;
    KeyStat stat;
    fstrvec data;
    size_t keyCount = 0;
    size_t prevSamePrefix = 0;
  public:

    void Init(size_t count);
    void Add(fstring key);
    TerarkKeyReader* Finish(KeyStat* output);
  };
  class Factory : public terark::RefCounter {
  public:
    virtual ~Factory();
    static TerarkIndex* Build(TerarkKeyReader* keyReader,
                              const TerarkZipTableOptions& tzopt,
                              const KeyStat&, const UintPrefixBuildInfo*);
    static size_t MemSizeForBuild(const KeyStat&);

    virtual unique_ptr<TerarkIndex> LoadMemory(fstring mem) const = 0;
  };
  typedef boost::intrusive_ptr<Factory> FactoryPtr;
  static UintPrefixBuildInfo GetUintPrefixBuildInfo(const TerarkIndex::KeyStat& ks);
  static unique_ptr<TerarkIndex> LoadMemory(fstring mem);
  virtual ~TerarkIndex();
  virtual fstring Name() const = 0;
  virtual void SaveMmap(std::function<void(const void*, size_t)> write) const = 0;
  virtual void
  Reorder(ZReorderMap& newToOld, std::function<void(const void*, size_t)> write, fstring tmpFile) const = 0;
  virtual size_t Find(fstring key, TerarkContext* ctx) const = 0;
  virtual size_t DictRank(fstring key, TerarkContext* ctx) const = 0;
  virtual void MinKey(valvec<byte_t>* key, TerarkContext* ctx) const = 0;
  virtual void MaxKey(valvec<byte_t>* key, TerarkContext* ctx) const = 0;
  virtual size_t NumKeys() const = 0;
  virtual size_t TotalKeySize() const = 0;
  virtual fstring Memory() const = 0;
  virtual valvec<fstring> GetMetaData() const = 0;
  virtual void DetachMetaData(const valvec<fstring>&) = 0;
  virtual const char* Info(char* buffer, size_t size) const = 0;
  virtual Iterator* NewIterator(valvec<byte_t>* buffer = nullptr, TerarkContext* ctx = nullptr) const = 0;
  virtual size_t IteratorSize() const = 0;
  virtual bool NeedsReorder() const = 0;
  virtual void GetOrderMap(terark::UintVecMin0& newToOld) const = 0;
  virtual void BuildCache(double cacheRatio) = 0;
  virtual void DumpKeys(std::function<void(fstring, fstring, fstring)>) const = 0;
};

}

