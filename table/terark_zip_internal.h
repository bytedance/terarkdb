/*
 * terark_zip_internal.h
 *
 *  Created on: 2017-05-02
 *      Author: zhaoming
 */

#pragma once

// project headers
#include "terark_zip_table.h"
// std headers
#include <mutex>
#include <atomic>
// boost headers
#include <boost/intrusive_ptr.hpp>
#include <boost/noncopyable.hpp>
// rocksdb headers
#include <rocksdb/slice.h>
#include <rocksdb/env.h>
#include <rocksdb/table.h>
// terark headers
#include <terark/fstring.hpp>
#include <terark/valvec.hpp>
#include <terark/stdtypes.hpp>
#include <terark/util/profiling.hpp>
#include <terark/zbs/lru_page_cache.hpp>

//#define TERARK_SUPPORT_UINT64_COMPARATOR
//#define DEBUG_TWO_PASS_ITER


#if defined(TerocksPrivateCode)
//# define USE_CRYPTOPP
//# define USE_OPENSSL
#endif // TerocksPrivateCode

#if ROCKSDB_MAJOR * 1000 + ROCKSDB_MINOR >= 5008
  #define TERARK_ROCKSDB_5008(...) __VA_ARGS__
#else
  #define TERARK_ROCKSDB_5008(...)
#endif

#if ROCKSDB_MAJOR * 1000 + ROCKSDB_MINOR >= 5007
  #define TERARK_ROCKSDB_5007(...) __VA_ARGS__
#else
  #define TERARK_ROCKSDB_5007(...)
#endif

void PrintVersion(rocksdb::Logger* info_log);

namespace rocksdb {

using terark::fstring;
using terark::valvec;
using terark::byte_t;
using terark::LruReadonlyCache;


extern terark::profiling g_pf;

extern const uint64_t kTerarkZipTableMagicNumber;

extern const std::string kTerarkZipTableIndexBlock;
extern const std::string kTerarkZipTableValueTypeBlock;
extern const std::string kTerarkZipTableValueDictBlock;
extern const std::string kTerarkZipTableOffsetBlock;
extern const std::string kTerarkZipTableCommonPrefixBlock;
extern const std::string kTerarkEmptyTableKey;
#if defined(TerocksPrivateCode)
extern const std::string kTerarkZipTableExtendedBlock;
#endif // TerocksPrivateCode
extern const std::string kTerarkZipTableBuildTimestamp;
extern const std::string kTerarkZipTableDictInfo;

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

#if defined(TerocksPrivateCode)

struct LicenseInfo {
  struct Head {
    char meta[4] = {'E', 'X', 'T', '.'};
    uint32_t size = sizeof(Head);
    uint64_t sign;
  } head;
  struct SubHead {
    char name[4];
    uint16_t version;
    uint16_t size;
  };
  struct KeyInfo {
    SubHead head = {{ 'k', 'e', 'y', '_' }, 1, sizeof(KeyInfo)};
    uint64_t sign;
    uint64_t id_hash;
    uint64_t date;
    uint64_t duration;
  } *key = nullptr, key_storage;
  struct SstInfo {
    SubHead head = {{ 's', 's', 't', '_' }, 1, sizeof(SstInfo)};
    uint64_t sign;
    uint64_t create_date;
  } *sst = nullptr, sst_storage;

  mutable std::mutex mutex;

  enum Result : uint32_t {
    OK,
    BadStream,
    BadHead,
    BadSign,
    BadVersion,
    BadLicense,
    InternalError,
  };

  LicenseInfo();
  Result load_nolock(const std::string& license_file);
  Result merge(const void* data, size_t size);
  valvec<byte_t> dump() const;
  bool check() const;
  void print_error(const char* file_name, bool startup, rocksdb::Logger* logger) const;
};

#endif // TerocksPrivateCode

struct CollectInfo {
  static const size_t queue_size;
  static const double hard_ratio;

  struct CompressionInfo {
    uint64_t timestamp;
    size_t raw_value;
    size_t zip_value;
    size_t raw_store;
    size_t zip_store;
  };
  std::vector<CompressionInfo> queue;
  size_t raw_value_size = 0;
  size_t zip_value_size = 0;
  size_t raw_store_size = 0;
  size_t zip_store_size = 0;
  std::atomic<float> estimate_compression_ratio;
  mutable std::mutex mutex;

  CollectInfo() : estimate_compression_ratio{0} {}

  void update(uint64_t timestamp
    , size_t raw_value, size_t zip_value
    , size_t raw_store, size_t zip_store);
  static bool hard(size_t raw, size_t zip);
  bool hard() const;
  float estimate(float def_value) const;
};

enum class ZipValueType : unsigned char {
  kZeroSeq = 0,
  kDelete = 1,
  kValue = 2,
  kMulti = 3,
};
//const size_t kZipValueTypeBits = 2;

struct ZipValueMultiValue {
  // use offset[0] as num, and do not store offsets[num]
  // when unzip, reserve num+1 cells, set offsets[0] to 0,
  // and set offsets[num] to length of value pack
  //	uint32_t num;
  uint32_t offsets[1];

  ///@size size include the extra uint32, == encoded_size + 4
  static
    const ZipValueMultiValue* decode(void* data, size_t size, size_t* pNum) {
    // data + 4 is the encoded data
    auto me = (ZipValueMultiValue*)(data);
    size_t num = me->offsets[1];
    assert(num > 0);
    memmove(me->offsets + 1, me->offsets + 2, sizeof(uint32_t)*(num - 1));
    me->offsets[0] = 0;
    me->offsets[num] = size - sizeof(uint32_t)*(num + 1);
    *pNum = num;
    return me;
  }
  static
    const ZipValueMultiValue* decode(valvec<byte_t>& buf, size_t* pNum) {
    return decode(buf.data(), buf.size(), pNum);
  }
  Slice getValueData(size_t nth, size_t num) const {
    assert(nth < num);
    size_t offset0 = offsets[nth + 0];
    size_t offset1 = offsets[nth + 1];
    size_t dlength = offset1 - offset0;
    const char* base = (const char*)(offsets + num + 1);
    return Slice(base + offset0, dlength);
  }
  static size_t calcHeaderSize(size_t n) {
    return sizeof(uint32_t) * (n);
  }
};

struct TerarkZipMultiOffsetInfo {
  struct KeyValueOffset {
    size_t key;
    size_t value;
    size_t type;
    size_t commonPrefix;
  };
  size_t partCount_, prefixLen_;
  valvec<KeyValueOffset> offset_;
  valvec<char> prefixSet_;

  static size_t calc_size(size_t prefixLen, size_t partCount);
  void Init(size_t prefixLen, size_t partCount);
  void set(size_t index
    , fstring prefix
    , size_t key
    , size_t value
    , size_t type
    , size_t commonPrefix);
  valvec<byte_t> dump();
  bool risk_set_memory(const void*, size_t);
  void risk_release_ownership();
};

class TerarkZipTableFactory : public TableFactory, boost::noncopyable {
public:
  explicit
  TerarkZipTableFactory(const TerarkZipTableOptions& tzto,
      std::shared_ptr<class TableFactory> fallback);
  ~TerarkZipTableFactory();

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

  bool IsDeleteRangeSupported() const override { return true; }

  LruReadonlyCache* cache() const { return cache_.get(); }

  Status GetOptionString(std::string* opt_string, const std::string& delimiter)
  const TERARK_ROCKSDB_5008(override);

private:
  TerarkZipTableOptions table_options_;
  std::shared_ptr<class TableFactory> fallback_factory_;
  TableFactory* adaptive_factory_; // just for open table
  boost::intrusive_ptr<LruReadonlyCache> cache_;
  mutable size_t nth_new_terark_table_ = 0;
  mutable size_t nth_new_fallback_table_ = 0;
private:
#if defined(TerocksPrivateCode)
  mutable LicenseInfo license_;
#endif // TerocksPrivateCode
  mutable CollectInfo collect_;
public:
#if defined(TerocksPrivateCode)
  LicenseInfo& GetLicense() const {
    return license_;
  }
#endif // TerocksPrivateCode
  CollectInfo& GetCollect() const {
    return collect_;
  }
};


}  // namespace rocksdb
