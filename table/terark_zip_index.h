#pragma once

#include <terark/fstring.hpp>
#include <terark/util/refcount.hpp>
#include <boost/intrusive_ptr.hpp>
#include <memory>

namespace rocksdb {

using terark::fstring;
using std::unique_ptr;

struct TerarkZipTableOptions;
class TempFileDeleteOnClose;

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
    virtual fstring key() const = 0;
    inline void SetInvalid() { m_id = size_t(-1); }
  };
  struct KeyStat {
    size_t commonPrefixLen = 0;
    size_t minKeyLen = 0;
    size_t maxKeyLen = size_t(-1);
    size_t sumKeyLen = size_t(-1);
    size_t numKeys   = size_t(-1);
  };
  class Factory : public terark::RefCounter {
  public:
    virtual ~Factory();
    virtual void Build(TempFileDeleteOnClose& tmpKeyFile,
                       const TerarkZipTableOptions& tzopt,
                       fstring tmpFilePath,
                       const KeyStat&) const = 0;
    virtual unique_ptr<TerocksIndex> LoadMemory(fstring mem) const = 0;
    virtual unique_ptr<TerocksIndex> LoadFile(fstring fpath) const = 0;
    virtual size_t MemSizeForBuild(const KeyStat&) const = 0;
  };
  typedef boost::intrusive_ptr<Factory> FactoryPtr;
  struct AutoRegisterFactory {
    AutoRegisterFactory(std::initializer_list<const char*> names, Factory* factory);
  };
  static const Factory* GetFactory(fstring name);
  static const Factory* SelectFactory(const KeyStat&, fstring name);
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

#define TerocksIndexRegister(clazz, ...) \
    TerocksIndex::AutoRegisterFactory \
    g_AutoRegister_##clazz({#clazz,##__VA_ARGS__}, new clazz::MyFactory())

}
