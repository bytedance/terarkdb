/*
 * terark_zip_internal.h
 *
 *  Created on: 2017-05-02
 *      Author: zhaoming
 */

#pragma once

#include <atomic>
#include <boost/intrusive_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <mutex>
#include <terark/fstring.hpp>
#include <terark/stdtypes.hpp>
#include <terark/util/profiling.hpp>
#include <terark/valvec.hpp>
#include <terark/zbs/lru_page_cache.hpp>

#include "options/options_helper.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/terark_namespace.h"
#include "table/terark_zip_table.h"

//#define DEBUG_TWO_PASS_ITER

void PrintVersion(TERARKDB_NAMESPACE::Logger* info_log);

namespace TERARKDB_NAMESPACE {

using terark::byte_t;
using terark::fstring;
using terark::LruReadonlyCache;
using terark::valvec;

extern terark::profiling g_pf;

extern const uint64_t kTerarkZipTableMagicNumber;

extern const std::string kTerarkZipTableValueDictBlock;
extern const std::string kTerarkZipTableOffsetBlock;
extern const std::string kTerarkEmptyTableKey;
extern const std::string kTerarkZipTableBuildTimestamp;
extern const std::string kTerarkZipTableDictInfo;
extern const std::string kTerarkZipTableDictSize;
extern const std::string kTerarkZipTableEntropy;

template <class ByteArray>
inline Slice SliceOf(const ByteArray& ba) {
  BOOST_STATIC_ASSERT(sizeof(ba[0] == 1));
  return Slice((const char*)ba.data(), ba.size());
}

inline static fstring fstringOf(const Slice& x) {
  return fstring(x.data(), x.size());
}

template <class ByteArrayView>
inline ByteArrayView SubStr(const ByteArrayView& x, size_t pos) {
  assert(pos <= x.size());
  return ByteArrayView(x.data() + pos, x.size() - pos);
}

bool IsForwardBytewiseComparator(const fstring name);
inline bool IsForwardBytewiseComparator(const Comparator* cmp) {
  return IsForwardBytewiseComparator(cmp->Name());
}
bool IsBackwardBytewiseComparator(const fstring name);
inline bool IsBackwardBytewiseComparator(const Comparator* cmp) {
  return IsBackwardBytewiseComparator(cmp->Name());
}

bool IsBytewiseComparator(const Comparator* cmp);

struct CollectInfo {
  static const size_t queue_size;

  struct CompressionInfo {
    uint64_t timestamp;
    size_t entropy;
    size_t zip_store;
  };
  std::vector<CompressionInfo> queue;
  size_t entropy_size = 0;
  size_t zip_store_size = 0;
  std::atomic<float> estimate_compression_ratio;
  mutable std::mutex mutex;

  CollectInfo() : estimate_compression_ratio{0} {}

  void update(uint64_t timestamp, size_t raw_store, size_t zip_store);
  float estimate() const;
};

enum class ZipValueType : unsigned char {
  kZeroSeq = 0,
  kDelete = 1,
  kValue = 2,
  kMulti = 3,
};
// const size_t kZipValueTypeBits = 2;

struct ZipValueMultiValue {
  // use offset[0] as num, and do not store offsets[num]
  // when unzip, reserve num+1 cells, set offsets[0] to 0,
  // and set offsets[num] to length of value pack
  //	uint32_t num;
  uint32_t offsets[1];

  ///@size size include the extra uint32, == encoded_size + 4
  static const ZipValueMultiValue* decode(void* data, size_t size,
                                          size_t* pNum) {
    // data + 4 is the encoded data
    auto me = (ZipValueMultiValue*)(data);
    size_t num = me->offsets[1];
    assert(num > 0);
    memmove(me->offsets + 1, me->offsets + 2, sizeof(uint32_t) * (num - 1));
    me->offsets[0] = 0;
    me->offsets[num] = uint32_t(size - sizeof(uint32_t) * (num + 1));
    *pNum = num;
    return me;
  }
  Slice getValueData(size_t nth, size_t num) const {
    assert(nth < num);
    size_t offset0 = offsets[nth + 0];
    size_t offset1 = offsets[nth + 1];
    size_t dlength = offset1 - offset0;
    const char* base = (const char*)(offsets + num + 1);
    return Slice(base + offset0, dlength);
  }
  static size_t calcHeaderSize(size_t n) { return sizeof(uint32_t) * (n); }
};

struct TerarkZipMultiOffsetInfo {
  struct KeyValueOffset {
    uint64_t key;
    uint64_t value;
    uint64_t type;
  };
  valvec<KeyValueOffset> offset_;

  static size_t calc_size(size_t partCount);
  void Init(size_t partCount);
  void set(size_t index, size_t key, size_t value, size_t type);
  valvec<byte_t> dump();
  bool risk_set_memory(const void*, size_t);
  void risk_release_ownership();
};

class TerarkZipTableFactory : public TableFactory, boost::noncopyable {
 public:
  explicit TerarkZipTableFactory(const TerarkZipTableOptions& tzto,
                                 std::shared_ptr<class TableFactory> fallback);
  ~TerarkZipTableFactory();

  const char* Name() const override { return "TerarkZipTable"; }

  Status NewTableReader(const TableReaderOptions& table_reader_options,
                        unique_ptr<RandomAccessFileReader>&& file,
                        uint64_t file_size, unique_ptr<TableReader>* table,
                        bool prefetch_index_and_filter_in_cache) const override;

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id, WritableFileWriter* file) const override;

  std::string GetPrintableTableOptions() const override;

  // Sanitizes the specified DB Options.
  Status SanitizeOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;

  void* GetOptions() override { return &table_options_; }

  bool IsDeleteRangeSupported() const override {
    if (table_options_.terarkZipMinLevel != kTerarkZipMinLevelForDisabled) {
      return true;
    }
    assert(fallback_factory_ != nullptr);
    return fallback_factory_->IsDeleteRangeSupported();
  }

  bool IsBuilderNeedSecondPass() const override {
    if (table_options_.terarkZipMinLevel != kTerarkZipMinLevelForDisabled) {
      return true;
    }
    assert(fallback_factory_ != nullptr);
    return fallback_factory_->IsBuilderNeedSecondPass();
  }

  LruReadonlyCache* cache() const { return cache_.get(); }

  Status GetOptionString(std::string* opt_string,
                         const std::string& delimiter) const override;

 private:
  TerarkZipTableOptions table_options_;
  std::shared_ptr<class TableFactory> fallback_factory_;
  TableFactory* adaptive_factory_;  // just for open table
  mutable std::mutex cache_create_mutex_;
  mutable boost::intrusive_ptr<LruReadonlyCache> cache_;
  mutable size_t nth_new_terark_table_ = 0;
  mutable size_t nth_new_fallback_table_ = 0;

 private:
  mutable CollectInfo collect_;

 public:
  CollectInfo& GetCollect() const { return collect_; }
  static std::unordered_map<std::string, OptionTypeInfo>
      terark_zip_table_type_info;
};

static auto& terark_zip_table_type_info =
    TerarkZipTableFactory::terark_zip_table_type_info;

}  // namespace TERARKDB_NAMESPACE
