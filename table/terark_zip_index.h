#pragma once

#include <terark/fstring.hpp>
#include <terark/valvec.hpp>
#include <terark/util/refcount.hpp>
#include <terark/int_vector.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/util/fstrvec.hpp>
#include <boost/intrusive_ptr.hpp>
#include <memory>

namespace rocksdb {

using terark::fstring;
using terark::valvec;
using terark::byte_t;
using terark::NativeDataInput;
using terark::InputBuffer;
using terark::fstrvec;
using std::unique_ptr;

struct TerarkZipTableOptions;
class TempFileDeleteOnClose;

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
    size_t minKeyLen = size_t(-1);
    size_t maxKeyLen = 0;
    size_t sumKeyLen = 0;
    size_t numKeys   = 0;
    valvec<byte_t> minKey;
    valvec<byte_t> maxKey;
  };
  class Factory : public terark::RefCounter {
  public:
    virtual ~Factory();
    virtual TerarkIndex* Build(NativeDataInput<InputBuffer>& tmpKeyFileReader,
                               const TerarkZipTableOptions& tzopt,
                               const KeyStat&) const = 0;
    virtual unique_ptr<TerarkIndex> LoadMemory(fstring mem) const = 0;
    virtual unique_ptr<TerarkIndex> LoadFile(fstring fpath) const = 0;
    virtual size_t MemSizeForBuild(const KeyStat&) const = 0;
  };
  typedef boost::intrusive_ptr<Factory> FactoryPtr;
  struct AutoRegisterFactory {
    AutoRegisterFactory(std::initializer_list<const char*> names,
        const char* rtti_name, Factory* factory);
  };
  static const Factory* GetFactory(fstring name);
  static const Factory* SelectFactory(const KeyStat&, fstring name);
  static bool SeekCostEffectiveIndexLen(const KeyStat& ks, size_t& ceLen);
  static unique_ptr<TerarkIndex> LoadFile(fstring fpath);
  static unique_ptr<TerarkIndex> LoadMemory(fstring mem);
  virtual ~TerarkIndex();
  virtual const char* Name() const = 0;
  virtual void SaveMmap(std::function<void(const void *, size_t)> write) const = 0;
  virtual size_t Find(fstring key) const = 0;
  virtual size_t NumKeys() const = 0;
  virtual size_t TotalKeySize() const = 0;
  virtual fstring Memory() const = 0;
  virtual Iterator* NewIterator() const = 0;
  virtual bool NeedsReorder() const = 0;
  virtual void GetOrderMap(terark::UintVecMin0& newToOld) const = 0;
  virtual void BuildCache(double cacheRatio) = 0;
};

#define TerarkIndexRegister(clazz, ...) \
    TerarkIndexRegisterImp(clazz, #clazz, ##__VA_ARGS__)

#define TerarkIndexRegisterNLT(clazzSuffix, ...)          \
    TerarkIndexRegisterImp(TrieDAWG_##clazzSuffix,                      \
        BOOST_STRINGIZE(BOOST_PP_CAT(NestLoudsTrieDAWG_, clazzSuffix)), \
        BOOST_STRINGIZE(clazzSuffix), \
        ##__VA_ARGS__)

#define TerarkIndexRegisterImp(clazz, WireName, ...)             \
	  BOOST_STATIC_ASSERT(sizeof(BOOST_STRINGIZE(clazz)) <= 60);  \
    TerarkIndex::AutoRegisterFactory                            \
    terark_used_static_obj                                      \
    g_AutoRegister_##clazz(                                     \
        {WireName,__VA_ARGS__},                                 \
        typeid(clazz).name(),                                   \
        new clazz::MyFactory()                                  \
    )

}
 
