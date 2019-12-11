/*
 * terark_zip_table.cc
 *
 *  Created on: 2016-08-09
 *      Author: leipeng
 */

// project headers
#include "terark_zip_table.h"
#include "terark_zip_common.h"
#include "terark_zip_internal.h"
#include "terark_zip_table_reader.h"
#include <terark/idx/terark_zip_index.hpp>

// std headers
#include <future>
#include <random>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <util/arena.h> // for #include <sys/mman.h>
#ifdef _MSC_VER
# include <io.h>
#else
# include <sys/types.h>
# include <sys/mman.h>
#endif

// boost headers
#include <boost/predef/other/endian.h>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/transform_width.hpp>

// rocksdb headers
#include <table/meta_blocks.h>

// terark headers
#include <terark/lcast.hpp>
#include <terark/zbs/xxhash_helper.hpp>

static std::once_flag PrintVersionHashInfoFlag;

#ifndef _MSC_VER
const char* git_version_hash_info_core();
const char* git_version_hash_info_fsa();
const char* git_version_hash_info_zbs();
const char* git_version_hash_info_idx();
#endif

void PrintVersionHashInfo(rocksdb::Logger* info_log) {
  std::call_once(PrintVersionHashInfoFlag, [info_log] {
# ifndef _MSC_VER
    INFO(info_log, "core %s", git_version_hash_info_core());
    INFO(info_log, "fsa %s", git_version_hash_info_fsa());
    INFO(info_log, "zbs %s", git_version_hash_info_zbs());
    INFO(info_log, "idx %s", git_version_hash_info_idx());
# endif
  });
}

namespace rocksdb {

terark::profiling g_pf;

const uint64_t kTerarkZipTableMagicNumber = 0x1122334455667788;

const std::string kTerarkZipTableValueDictBlock = "TerarkZipTableValueDictBlock";
const std::string kTerarkZipTableOffsetBlock    = "TerarkZipTableOffsetBlock";
const std::string kTerarkEmptyTableKey          = "ThisIsAnEmptyTable";

using terark::XXHash64;

const std::string kTerarkZipTableBuildTimestamp = "terark.build.timestamp";
const std::string kTerarkZipTableDictInfo       = "terark.build.dict_info";
const std::string kTerarkZipTableDictSize       = "terark.build.dict_size";
const std::string kTerarkZipTableEntropy        = "terark.build.entropy";

const size_t CollectInfo::queue_size = 1024;

void CollectInfo::update(uint64_t timestamp, size_t entropy, size_t zip_store) {
  std::unique_lock<std::mutex> l(mutex);
  entropy_size += entropy;
  zip_store_size += zip_store;
  auto comp = [](const CompressionInfo& l, const CompressionInfo& r) {
    return l.timestamp > r.timestamp;
  };
  queue.emplace_back(CompressionInfo{timestamp, entropy, zip_store});
  std::push_heap(queue.begin(), queue.end(), comp);
  while (queue.size() > queue_size) {
    auto& front = queue.front();
    entropy_size -= front.entropy;
    zip_store_size -= front.zip_store;
    std::pop_heap(queue.begin(), queue.end(), comp);
    queue.pop_back();
  }
  estimate_compression_ratio = float(zip_store_size) / float(entropy_size);
}

float CollectInfo::estimate() const {
  float ret = estimate_compression_ratio;
  return ret ? ret : 1.0f;
}

size_t TerarkZipMultiOffsetInfo::calc_size(size_t partCount) {
  return 8 + partCount * sizeof(KeyValueOffset);
}

void TerarkZipMultiOffsetInfo::Init(size_t partCount) {
  offset_.resize_no_init(partCount);
}

void TerarkZipMultiOffsetInfo::set(size_t i, size_t k, size_t v, size_t t) {
  offset_[i].key = k;
  offset_[i].value = v;
  offset_[i].type = t;
}

valvec<byte_t> TerarkZipMultiOffsetInfo::dump() {
  valvec<byte_t> ret;
  size_t size = calc_size(offset_.size());
  ret.resize_no_init(size);
  size_t offset = 0;
  auto push = [&](const void* d, size_t s) {
    memcpy(ret.data() + offset, d, s);
    offset += s;
  };
  uint64_t partCount = offset_.size();
  push(&partCount, 8);
  push(offset_.data(), offset_.size() * sizeof(KeyValueOffset));
  assert(offset == ret.size());
  return ret;
}

bool TerarkZipMultiOffsetInfo::risk_set_memory(const void* p, size_t s) {
  offset_.clear();
  if (s < 8) {
    return false;
  }
  auto src = (const byte_t*)p;
  uint64_t partCount;
  memcpy(&partCount, src, 8);
  if (s != calc_size(partCount)) {
    return false;
  }
  offset_.risk_set_data((KeyValueOffset*)(src + 8), partCount);
  return true;
}

void TerarkZipMultiOffsetInfo::risk_release_ownership() {
  offset_.risk_release_ownership();
}

TableFactory*
NewTerarkZipTableFactory(const TerarkZipTableOptions& tzto,
                         std::shared_ptr<TableFactory> fallback) {
  TerarkZipTableFactory* factory = new TerarkZipTableFactory(tzto, fallback);
  if (tzto.debugLevel > 0) {
    STD_INFO("NewTerarkZipTableFactory(\n%s)\n",
             factory->GetPrintableTableOptions().c_str()
    );
  }
  return factory;
}

TableFactory*
NewTerarkZipTableFactory(const std::string& options,
                         std::shared_ptr<TableFactory> fallback, Status* s) {
  TerarkZipTableOptions opt;
  *s = opt.Parse(options);
  if (s->ok()) {
    return NewTerarkZipTableFactory(opt, fallback);
  }
  return nullptr;
}

std::shared_ptr<TableFactory>
SingleTerarkZipTableFactory(const TerarkZipTableOptions& tzto,
                            std::shared_ptr<TableFactory> fallback) {
  static std::shared_ptr<TableFactory> factory(
      NewTerarkZipTableFactory(tzto, fallback));
  return factory;
}

bool IsForwardBytewiseComparator(const fstring name) {
  if (name.startsWith("RocksDB_SE_")) {
    return true;
  }
  return name == "leveldb.BytewiseComparator";
}

bool IsBackwardBytewiseComparator(const fstring name) {
  if (name.startsWith("rev:RocksDB_SE_")) {
    return true;
  }
  return name == "rocksdb.ReverseBytewiseComparator";
}

bool IsBytewiseComparator(const Comparator* cmp) {
  const fstring name = cmp->Name();
  if (name.startsWith("RocksDB_SE_")) {
    return true;
  }
  if (name.startsWith("rev:RocksDB_SE_")) {
    // reverse bytewise compare, needs reverse in iterator
    return true;
  }
  return name == "leveldb.BytewiseComparator" ||
         name == "rocksdb.ReverseBytewiseComparator";
}

inline static
size_t GetFixedPrefixLen(const SliceTransform* tr) {
  if (tr == nullptr) {
    return 0;
  }
  fstring trName = tr->Name();
  fstring namePrefix = "rocksdb.FixedPrefix.";
  if (namePrefix != trName.substr(0, namePrefix.size())) {
    return 0;
  }
  return terark::lcast(trName.substr(namePrefix.size()));
}

TerarkZipTableFactory::TerarkZipTableFactory(
    const TerarkZipTableOptions& tzto,
    std::shared_ptr<TableFactory> fallback)
    : table_options_(tzto), fallback_factory_(fallback) {
  adaptive_factory_ = NewAdaptiveTableFactory();
  if (tzto.warmUpIndexOnOpen) {
    // turn off warmUpIndexOnOpen if forceMetaInMemory
    table_options_.warmUpIndexOnOpen = !tzto.forceMetaInMemory;
  }
}

TerarkZipTableFactory::~TerarkZipTableFactory() {
  delete adaptive_factory_;
}

Status
TerarkZipTableFactory::NewTableReader(
    const TableReaderOptions& table_reader_options,
    unique_ptr <RandomAccessFileReader>&& file,
    uint64_t file_size, unique_ptr <TableReader>* table,
    bool prefetch_index_and_filter_in_cache)
const {
  if (table_options_.minPreadLen >= 0 && table_options_.cacheCapacityBytes) {
    if (!cache_) {
      std::lock_guard<std::mutex> lock(cache_create_mutex_);
      if (!cache_) {
        size_t initial_file_num = 2048;
        cache_.reset(LruReadonlyCache::create(
            table_options_.cacheCapacityBytes,
            table_options_.cacheShards,
            initial_file_num,
            table_reader_options.env_options.use_aio_reads));
      }
    }
  }
  PrintVersionHashInfo(table_reader_options.ioptions.info_log);
  auto userCmp = table_reader_options.internal_comparator.user_comparator();
  if (!IsBytewiseComparator(userCmp)) {
    return Status::InvalidArgument("TerarkZipTableFactory::NewTableReader()",
                                   "user comparator must be 'leveldb.BytewiseComparator'");
  }
  Footer footer;
  Status s = ReadFooterFromFile(file.get(), TERARK_ROCKSDB_5007(nullptr,)
                                file_size, &footer);
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
  BlockContents emptyTableBC;
  s = ReadMetaBlockAdapte(file.get(), file_size, kTerarkZipTableMagicNumber, table_reader_options.ioptions,
                          kTerarkEmptyTableKey, &emptyTableBC);
  if (s.ok()) {
    std::unique_ptr<TerarkEmptyTableReader>
      t(new TerarkEmptyTableReader(table_reader_options));
    s = t->Open(file.release(), file_size);
    if (!s.ok()) {
      return s;
    }
    *table = std::move(t);
    return s;
  }
  BlockContents offsetBC;
  s = ReadMetaBlockAdapte(file.get(), file_size, kTerarkZipTableMagicNumber, table_reader_options.ioptions,
                          kTerarkZipTableOffsetBlock, &offsetBC);
  if (s.ok()) {
    TerarkZipMultiOffsetInfo info;
    if (info.risk_set_memory(offsetBC.data.data(), offsetBC.data.size())) {
      if (info.offset_.size() > 1) {
        std::unique_ptr<TerarkZipTableMultiReader>
          t(new TerarkZipTableMultiReader(this, table_reader_options, table_options_));
        s = t->Open(file.release(), file_size);
        if (s.ok()) {
          *table = std::move(t);
        }
      } else {
        std::unique_ptr<TerarkZipTableReader>
          t(new TerarkZipTableReader(this, table_reader_options, table_options_));
        s = t->Open(file.release(), file_size);
        if (s.ok()) {
          *table = std::move(t);
        }
      }
      info.risk_release_ownership();
      return s;
    }
    return Status::InvalidArgument(
      "TerarkZipTableFactory::NewTableReader()", "bad TerarkZipMultiOffsetInfo"
    );
  }
  return Status::InvalidArgument(
    "TerarkZipTableFactory::NewTableReader()", "missing TerarkZipMultiOffsetInfo"
  );
}

// defined in terark_zip_table_builder.cc
extern
TableBuilder*
createTerarkZipTableBuilder(const TerarkZipTableFactory* table_factory,
                            const TerarkZipTableOptions& tzo,
                            const TableBuilderOptions& tbo,
                            uint32_t column_family_id,
                            WritableFileWriter* file,
                            uint32_t key_prefixLen);
extern long long g_lastTime;

TableBuilder*
TerarkZipTableFactory::NewTableBuilder(
  const TableBuilderOptions& table_builder_options,
  uint32_t column_family_id,
  WritableFileWriter* file)
const {
  PrintVersionHashInfo(table_builder_options.ioptions.info_log);
  auto userCmp = table_builder_options.internal_comparator.user_comparator();
  if (!IsBytewiseComparator(userCmp)) {
    THROW_STD(invalid_argument,
              "TerarkZipTableFactory::NewTableBuilder(): "
              "user comparator must be 'leveldb.BytewiseComparator'");
  }
  int curlevel = table_builder_options.level;
  int numlevel = table_builder_options.ioptions.num_levels;
  int minlevel = table_options_.terarkZipMinLevel;
  if (minlevel < 0) {
    minlevel = numlevel - 1;
  }
  uint32_t keyPrefixLen = (uint32_t)GetFixedPrefixLen(table_builder_options.moptions.prefix_extractor.get());
  if (keyPrefixLen != 0) {
    if (table_options_.keyPrefixLen != 0) {
      if (keyPrefixLen != table_options_.keyPrefixLen) {
        WARN(table_builder_options.ioptions.info_log,
             "TerarkZipTableFactory::NewTableBuilder() found non best config , keyPrefixLen = %zd , prefix_extractor = %zd\n",
             table_options_.keyPrefixLen, keyPrefixLen
        );
      }
      keyPrefixLen = std::min<uint32_t>(keyPrefixLen, table_options_.keyPrefixLen);
    }
  } else {
    keyPrefixLen = table_options_.keyPrefixLen;
  }
  if (table_builder_options.sst_purpose != kEssenceSst) {
    keyPrefixLen = 0;
  }
#if 1
  INFO(table_builder_options.ioptions.info_log,
       "nth_newtable{ terark = %3zd fallback = %3zd } curlevel = %d minlevel = %d numlevel = %d fallback = %p\n",
       nth_new_terark_table_, nth_new_fallback_table_, curlevel, minlevel, numlevel, fallback_factory_.get()
  );
#endif
  if (0 == nth_new_terark_table_) {
    g_lastTime = g_pf.now();
  }
  if (fallback_factory_) {
    if (curlevel >= 0 && curlevel < minlevel) {
      nth_new_fallback_table_++;
      TableBuilder* tb = fallback_factory_->NewTableBuilder(table_builder_options,
                                                            column_family_id, file);
      INFO(table_builder_options.ioptions.info_log, "TerarkZipTableFactory::NewTableBuilder() returns class: %s\n",
           ClassName(*tb).c_str());
      return tb;
    }
  }
  nth_new_terark_table_++;

  return createTerarkZipTableBuilder(this, table_options_,
                                     table_builder_options, column_family_id,
                                     file, keyPrefixLen);
}

#define PrintBuf(...) ret.append(buffer, snprintf(buffer, kBufferSize, __VA_ARGS__))

std::string TerarkZipTableFactory::GetPrintableTableOptions() const {
  std::string ret;
  ret.reserve(2000);
  const char* cvb[] = {"false", "true"};
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  const auto& tzto = table_options_;

#define M_String(name)                                       \
  ret.append(#name);                                         \
  ret.append("                         : " + strlen(#name)   \
                                      , 27 - strlen(#name)); \
  ret.append(tzto.name);                                     \
  ret.append("\n")

#define M_NumFmt(name, fmt) \
                       PrintBuf("%-24s : " fmt "\n", #name, tzto.name)
#define M_NumGiB(name) PrintBuf("%-24s : %.3fGiB\n", #name, tzto.name/GiB)
#define M_Boolea(name) PrintBuf("%-24s : %s\n", #name, cvb[!!tzto.name])
#include "terark_zip_table_property_print.h"
  return ret;
}

Status
TerarkZipTableFactory::GetOptionString(std::string* opt_string,
                                       const std::string& delimiter)
const {
  const char* cvb[] = {"false", "true"};
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  const auto& tzto = table_options_;

  std::string& ret = *opt_string;
  ret.resize(0);

#define WriteName(name) ret.append(#name).append("=")

#define M_String(name) WriteName(name).append(tzto.name).append(delimiter)
#define M_NumFmt(name, fmt) \
                       WriteName(name);PrintBuf(fmt, tzto.name); \
                       ret.append(delimiter)
#define M_NumGiB(name) WriteName(name);PrintBuf("%.3fGiB", tzto.name/GiB);\
                       ret.append(delimiter)
#define M_Boolea(name) WriteName(name);PrintBuf("%s", cvb[!!tzto.name]); \
                       ret.append(delimiter)

#include "terark_zip_table_property_print.h"

  return Status::OK();
}

Status
TerarkZipTableFactory::SanitizeOptions(const DBOptions& /*db_opts*/,
                                       const ColumnFamilyOptions& cf_opts)
const {
  auto table_factory = dynamic_cast<TerarkZipTableFactory*>(cf_opts.table_factory.get());
  assert(table_factory);
  auto& tzto = *reinterpret_cast<const TerarkZipTableOptions*>(table_factory->GetOptions());
  try {
    terark::TempFileDeleteOnClose test;
    test.path = tzto.localTempDir + "/Terark-XXXXXX";
    test.open_temp();
    test.writer << "Terark";
    test.complete_write();
  }
  catch (...) {
    std::string msg = "ERROR: bad localTempDir : " + tzto.localTempDir;
    fprintf(stderr, "%s\n", msg.c_str());
    return Status::InvalidArgument("TerarkZipTableFactory::SanitizeOptions()", msg);
  }
  if (!IsBytewiseComparator(cf_opts.comparator)) {
    return Status::InvalidArgument("TerarkZipTableFactory::SanitizeOptions()",
                                   "user comparator must be 'leveldb.BytewiseComparator'");
  }
  return Status::OK();
}

// delimiter must be "\n"
Status TerarkZipTableOptions::Parse(const std::string& opt) {
  const char* beg = opt.c_str();
  const char* end = opt.size() + beg;
  std::unordered_map<std::string, std::string> map;
  while (beg < end) {
    int namelen = -1, val_beg = -1, val_end, eol = -1;
    sscanf(beg, "%*s%n=%n%*s%n\n%n", &namelen, &val_beg, &val_end, &eol);
    if (-1 == eol) {
      return Status::InvalidArgument("TerarkZipTableOptions::Parse", "Missing linefeed char");
    }
    map[std::string(beg, namelen)] = std::string(beg + val_beg, val_end - val_beg);
    beg += eol;
  }
#define M_String(name) { \
    auto iter = map.find(#name); \
    if (map.end() != iter) name = iter->second; \
  }
#define M_NumFmt(name, fmt) { \
    auto iter = map.find(#name); \
    if (map.end() != iter) { \
      if (sscanf(iter->second.c_str(), fmt, &name) != 1) { \
        return Status::InvalidArgument("TerarkZipTableOptions::Parse():", \
                                       "bad " #name); \
      } \
    } \
  }
#define M_NumGiB(name) { \
    auto iter = map.find(#name); \
    if (map.end() != iter) { \
      double dval = strtof(iter->second.c_str(), NULL); \
      name = size_t(dval * GiB); \
    } \
  }
#define M_Boolea(name) { \
    auto iter = map.find(#name); \
    if (map.end() != iter) { \
      if (strcasecmp(iter->second.c_str(), "true") == 0) \
        name = true; \
      else if (strcasecmp(iter->second.c_str(), "false") == 0) \
        name = false; \
      else \
        return Status::InvalidArgument("TerarkZipTableOptions::Parse():", \
                                       "bad " #name); \
    } \
  }
  int entropyAlgo;
  int debugLevel, indexNestScale, indexTempLevel, offsetArrayBlockUnits;
#include "terark_zip_table_property_print.h"

  this->debugLevel = (byte_t)debugLevel;
  this->indexNestScale = (byte_t)indexNestScale;
  this->indexTempLevel = (byte_t)indexTempLevel;
  this->offsetArrayBlockUnits = (uint16_t)offsetArrayBlockUnits;
  this->entropyAlgo = (EntropyAlgo)entropyAlgo;

  return Status::OK();
}

bool TerarkZipTablePrintCacheStat(TableFactory* factory, FILE* fp) {
  auto tztf = dynamic_cast<const TerarkZipTableFactory*>(factory);
  if (tztf) {
    if (tztf->cache()) {
      tztf->cache()->print_stat_cnt(fp);
      return true;
    } else {
      fprintf(fp, "PrintCacheStat: terark user cache == nullptr\n");
    }
  } else {
    fprintf(fp, "PrintCacheStat: factory is not TerarkZipTableFactory\n");
  }
  return false;
}

} /* namespace rocksdb */
