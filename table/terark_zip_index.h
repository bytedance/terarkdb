#pragma once

#include <terark/fstring.hpp>
#include <terark/histogram.hpp>
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
using terark::valvec;
using terark::byte_t;
using terark::NativeDataInput;
using terark::InputBuffer;
using terark::fstrvec;
using terark::Uint64Histogram;
using terark::ZReorderMap;
using std::unique_ptr;

struct TerarkZipTableOptions;
class TempFileDeleteOnClose;
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
    size_t commonPrefixLen = 0;
    Uint64Histogram prefix;
    Uint64Histogram diff;
    size_t minKeyLen = size_t(-1);
    size_t maxKeyLen = 0;
    size_t minSuffixLen = size_t(-1);
    size_t maxSuffixLen = 0;
    size_t sumKeyLen = 0;
    valvec<byte_t> minKey;
    valvec<byte_t> maxKey;
  };
  class Factory : public terark::RefCounter {
  public:
    virtual ~Factory();
    static TerarkIndex* Build(NativeDataInput<InputBuffer>& tmpKeyFileReader,
                              const TerarkZipTableOptions& tzopt,
                              const KeyStat&,
                              const ImmutableCFOptions* ioption = nullptr);
    static size_t MemSizeForBuild(const KeyStat&);

    virtual unique_ptr<TerarkIndex> LoadMemory(fstring mem) const = 0;
  };
  typedef boost::intrusive_ptr<Factory> FactoryPtr;

  static unique_ptr<TerarkIndex> LoadMemory(fstring mem);
  virtual ~TerarkIndex();
  virtual const char* Name() const = 0;
  virtual void SaveMmap(std::function<void(const void *, size_t)> write) const = 0;
  virtual void Reorder(ZReorderMap& newToOld, std::function<void(const void *, size_t)> write, fstring tmpFile) const = 0;
  virtual size_t Find(fstring key, valvec<byte_t>* ctx) const = 0;
  virtual size_t DictRank(fstring key, valvec<byte_t>* ctx) const = 0;
  virtual size_t NumKeys() const = 0;
  virtual size_t TotalKeySize() const = 0;
  virtual fstring Memory() const = 0;
  virtual Iterator* NewIterator(void* ptr) const = 0;
  virtual size_t IteratorSize() const = 0;
  virtual bool NeedsReorder() const = 0;
  virtual void GetOrderMap(terark::UintVecMin0& newToOld) const = 0;
  virtual void BuildCache(double cacheRatio) = 0;
};

}

