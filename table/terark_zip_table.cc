/*
 * terark_zip_table.cc
 *
 *  Created on: 2016-08-09
 *      Author: leipeng
 */

// project headers
#include "terark_zip_table.h"

#include <terark/idx/terark_zip_index.hpp>

#include "terark_zip_common.h"
#include "terark_zip_internal.h"
#include "terark_zip_table_reader.h"

// std headers
#include <util/arena.h>  // for #include <sys/mman.h>

#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <future>
#include <random>
#ifdef _MSC_VER
#include <io.h>
#else
#include <sys/mman.h>
#include <sys/types.h>
#endif

// boost headers
#include <boost/predef/other/endian.h>

#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/transform_width.hpp>

// rocksdb headers
#include <table/meta_blocks.h>

// terark headers
#include <terark/lcast.hpp>
#include <terark/util/tmpfile.hpp>
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
#ifndef _MSC_VER
    INFO(info_log, "core %s", git_version_hash_info_core());
    INFO(info_log, "fsa %s", git_version_hash_info_fsa());
    INFO(info_log, "zbs %s", git_version_hash_info_zbs());
    INFO(info_log, "idx %s", git_version_hash_info_idx());
#endif
  });
}

namespace rocksdb {

terark::profiling g_pf;

const uint64_t kTerarkZipTableMagicNumber = 0x1122334455667788;

const std::string kTerarkZipTableValueDictBlock =
    "TerarkZipTableValueDictBlock";
const std::string kTerarkZipTableOffsetBlock = "TerarkZipTableOffsetBlock";
const std::string kTerarkEmptyTableKey = "ThisIsAnEmptyTable";

using terark::XXHash64;

const std::string kTerarkZipTableBuildTimestamp = "terark.build.timestamp";
const std::string kTerarkZipTableDictInfo = "terark.build.dict_info";
const std::string kTerarkZipTableDictSize = "terark.build.dict_size";
const std::string kTerarkZipTableEntropy = "terark.build.entropy";

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

TableFactory* NewTerarkZipTableFactory(const TerarkZipTableOptions& tzto,
                                       std::shared_ptr<TableFactory> fallback) {
  TerarkZipTableFactory* factory = new TerarkZipTableFactory(tzto, fallback);
  if (tzto.debugLevel > 0) {
    STD_INFO("NewTerarkZipTableFactory(\n%s)\n",
             factory->GetPrintableTableOptions().c_str());
  }
  return factory;
}

TableFactory* NewTerarkZipTableFactory(Slice options,
                                       std::shared_ptr<TableFactory> fallback,
                                       Status* s) {
  TerarkZipTableOptions opt;
  *s = opt.Parse(options);
  if (s->ok()) {
    return NewTerarkZipTableFactory(opt, fallback);
  }
  return nullptr;
}

std::shared_ptr<TableFactory> SingleTerarkZipTableFactory(
    const TerarkZipTableOptions& tzto, std::shared_ptr<TableFactory> fallback) {
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

inline static size_t GetFixedPrefixLen(const SliceTransform* tr) {
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
    const TerarkZipTableOptions& tzto, std::shared_ptr<TableFactory> fallback)
    : table_options_(tzto), fallback_factory_(fallback) {
  adaptive_factory_ = NewAdaptiveTableFactory();
  if (tzto.warmUpIndexOnOpen) {
    // turn off warmUpIndexOnOpen if forceMetaInMemory
    table_options_.warmUpIndexOnOpen = !tzto.forceMetaInMemory;
  }
}

TerarkZipTableFactory::~TerarkZipTableFactory() { delete adaptive_factory_; }

Status TerarkZipTableFactory::NewTableReader(
    const TableReaderOptions& table_reader_options,
    unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    unique_ptr<TableReader>* table,
    bool prefetch_index_and_filter_in_cache) const {
  if (table_options_.minPreadLen >= 0 && table_options_.cacheCapacityBytes) {
    if (!cache_) {
      std::lock_guard<std::mutex> lock(cache_create_mutex_);
      if (!cache_) {
        size_t initial_file_num = 2048;
        cache_.reset(LruReadonlyCache::create(
            table_options_.cacheCapacityBytes, table_options_.cacheShards,
            initial_file_num, table_reader_options.env_options.use_aio_reads));
      }
    }
  }
  PrintVersionHashInfo(table_reader_options.ioptions.info_log);
  auto userCmp = table_reader_options.internal_comparator.user_comparator();
  if (!IsBytewiseComparator(userCmp)) {
    return Status::InvalidArgument(
        "TerarkZipTableFactory::NewTableReader()",
        "user comparator must be 'leveldb.BytewiseComparator'");
  }
  Footer footer;
  Status s = ReadFooterFromFile(
      file.get(), TERARK_ROCKSDB_5007(nullptr, ) file_size, &footer);
  if (!s.ok()) {
    return s;
  }
  if (footer.table_magic_number() != kTerarkZipTableMagicNumber) {
    if (adaptive_factory_) {
      // just for open table
      return adaptive_factory_->NewTableReader(
          table_reader_options, std::move(file), file_size, table,
          prefetch_index_and_filter_in_cache);
    }
    if (fallback_factory_) {
      return fallback_factory_->NewTableReader(
          table_reader_options, std::move(file), file_size, table,
          prefetch_index_and_filter_in_cache);
    }
    return Status::InvalidArgument(
        "TerarkZipTableFactory::NewTableReader()",
        "fallback_factory is null and magic_number is not kTerarkZipTable");
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
  s = ReadMetaBlockAdapte(file.get(), file_size, kTerarkZipTableMagicNumber,
                          table_reader_options.ioptions, kTerarkEmptyTableKey,
                          &emptyTableBC);
  if (s.ok()) {
    std::unique_ptr<TerarkEmptyTableReader> t(
        new TerarkEmptyTableReader(table_reader_options));
    s = t->Open(file.release(), file_size);
    if (!s.ok()) {
      return s;
    }
    *table = std::move(t);
    return s;
  }
  BlockContents offsetBC;
  s = ReadMetaBlockAdapte(file.get(), file_size, kTerarkZipTableMagicNumber,
                          table_reader_options.ioptions,
                          kTerarkZipTableOffsetBlock, &offsetBC);
  if (s.ok()) {
    TerarkZipMultiOffsetInfo info;
    if (info.risk_set_memory(offsetBC.data.data(), offsetBC.data.size())) {
      if (info.offset_.size() > 1) {
        std::unique_ptr<TerarkZipTableMultiReader> t(
            new TerarkZipTableMultiReader(this, table_reader_options,
                                          table_options_));
        s = t->Open(file.release(), file_size);
        if (s.ok()) {
          *table = std::move(t);
        }
      } else {
        std::unique_ptr<TerarkZipTableReader> t(new TerarkZipTableReader(
            this, table_reader_options, table_options_));
        s = t->Open(file.release(), file_size);
        if (s.ok()) {
          *table = std::move(t);
        }
      }
      info.risk_release_ownership();
      return s;
    }
    return Status::InvalidArgument("TerarkZipTableFactory::NewTableReader()",
                                   "bad TerarkZipMultiOffsetInfo");
  }
  return Status::InvalidArgument("TerarkZipTableFactory::NewTableReader()",
                                 "missing TerarkZipMultiOffsetInfo");
}

// defined in terark_zip_table_builder.cc
extern TableBuilder* createTerarkZipTableBuilder(
    const TerarkZipTableFactory* table_factory,
    const TerarkZipTableOptions& tzo, const TableBuilderOptions& tbo,
    uint32_t column_family_id, WritableFileWriter* file,
    uint32_t key_prefixLen);
extern long long g_lastTime;

TableBuilder* TerarkZipTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options, uint32_t column_family_id,
    WritableFileWriter* file) const {
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
  uint32_t keyPrefixLen = (uint32_t)GetFixedPrefixLen(
      table_builder_options.moptions.prefix_extractor.get());
  if (keyPrefixLen != 0) {
    if (table_options_.keyPrefixLen != 0) {
      if (keyPrefixLen != table_options_.keyPrefixLen) {
        WARN(table_builder_options.ioptions.info_log,
             "TerarkZipTableFactory::NewTableBuilder() found non best config , "
             "keyPrefixLen = %zd , prefix_extractor = %zd\n",
             table_options_.keyPrefixLen, keyPrefixLen);
      }
      keyPrefixLen =
          std::min<uint32_t>(keyPrefixLen, table_options_.keyPrefixLen);
    }
  } else {
    keyPrefixLen = table_options_.keyPrefixLen;
  }
  if (table_builder_options.sst_purpose != kEssenceSst) {
    keyPrefixLen = 0;
  }
#if 1
  INFO(table_builder_options.ioptions.info_log,
       "nth_newtable{ terark = %3zd fallback = %3zd } curlevel = %d minlevel = "
       "%d numlevel = %d fallback = %p\n",
       nth_new_terark_table_, nth_new_fallback_table_, curlevel, minlevel,
       numlevel, fallback_factory_.get());
#endif
  if (0 == nth_new_terark_table_) {
    g_lastTime = g_pf.now();
  }
  if (fallback_factory_) {
    if (curlevel >= 0 && curlevel < minlevel) {
      nth_new_fallback_table_++;
      TableBuilder* tb = fallback_factory_->NewTableBuilder(
          table_builder_options, column_family_id, file);
      INFO(table_builder_options.ioptions.info_log,
           "TerarkZipTableFactory::NewTableBuilder() returns class: %s\n",
           ClassName(*tb).c_str());
      return tb;
    }
  }
  nth_new_terark_table_++;

  return createTerarkZipTableBuilder(this, table_options_,
                                     table_builder_options, column_family_id,
                                     file, keyPrefixLen);
}

#define PrintBuf(...) \
  ret.append(buffer, snprintf(buffer, kBufferSize, __VA_ARGS__))

std::string TerarkZipTableFactory::GetPrintableTableOptions() const {
  std::string ret;
  ret.reserve(2000);
  const char* cvb[] = {"false", "true"};
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  const auto& tzto = table_options_;

#define M_String(name)                                      \
  ret.append(#name);                                        \
  ret.append("                         : " + strlen(#name), \
             27 - strlen(#name));                           \
  ret.append(tzto.name);                                    \
  ret.append("\n")

#define M_NumFmt(name, fmt) PrintBuf("%-24s : " fmt "\n", #name, tzto.name)
#define M_NumGiB(name) PrintBuf("%-24s : %.3fGiB\n", #name, tzto.name / GiB)
#define M_Boolea(name) PrintBuf("%-24s : %s\n", #name, cvb[!!tzto.name])
#include "terark_zip_table_property_print.h"
  return ret;
}

namespace {
bool SerializeSingleTerarkZipTableOption(
    std::string* opt_string, const TerarkZipTableOptions& tzto_options,
    const std::string& name, const std::string& delimiter) {
  auto iter = terark_zip_table_type_info.find(name);
  if (iter == terark_zip_table_type_info.end()) {
    return false;
  }
  auto& opt_info = iter->second;
  const char* opt_address =
      reinterpret_cast<const char*>(&tzto_options) + opt_info.offset;
  std::string value;
  bool result = SerializeSingleOptionHelper(opt_address, opt_info.type, &value);
  if (result) {
    *opt_string = name + "=" + value + delimiter;
  }
  return result;
}
}  // namespace

Status TerarkZipTableFactory::GetOptionString(
    std::string* opt_string, const std::string& delimiter) const {
  assert(opt_string);
  opt_string->clear();
  for (auto iter = terark_zip_table_type_info.begin();
       iter != terark_zip_table_type_info.end(); ++iter) {
    if (iter->second.verification == OptionVerificationType::kDeprecated) {
      // If the option is no longer used in rocksdb and marked as deprecated,
      // we skip it in the serialization.
      continue;
    }
    std::string single_output;
    bool result = SerializeSingleTerarkZipTableOption(
        &single_output, table_options_, iter->first, delimiter);
    assert(result);
    if (result) {
      opt_string->append(single_output);
    }
  }
  return Status::OK();
}

Status TerarkZipTableFactory::SanitizeOptions(
    const DBOptions& /*db_opts*/, const ColumnFamilyOptions& cf_opts) const {
  auto table_factory =
      dynamic_cast<TerarkZipTableFactory*>(cf_opts.table_factory.get());
  assert(table_factory);
  auto& tzto = *reinterpret_cast<const TerarkZipTableOptions*>(
      table_factory->GetOptions());
  try {
    terark::TempFileDeleteOnClose test;
    test.path = tzto.localTempDir + "/Terark-XXXXXX";
    test.open_temp();
    test.writer << "Terark";
    test.complete_write();
  } catch (...) {
    std::string msg = "ERROR: bad localTempDir : " + tzto.localTempDir;
    fprintf(stderr, "%s\n", msg.c_str());
    return Status::InvalidArgument("TerarkZipTableFactory::SanitizeOptions()",
                                   msg);
  }
  if (!IsBytewiseComparator(cf_opts.comparator)) {
    return Status::InvalidArgument(
        "TerarkZipTableFactory::SanitizeOptions()",
        "user comparator must be 'leveldb.BytewiseComparator'");
  }
  return Status::OK();
}

std::unordered_map<std::string, OptionTypeInfo>
    TerarkZipTableFactory::terark_zip_table_type_info = {
        {"indexNestLevel",
         {offsetof(struct TerarkZipTableOptions, indexNestLevel),
          OptionType::kInt, OptionVerificationType::kNormal, false, 0}},
        {"checksumLevel",
         {offsetof(struct TerarkZipTableOptions, checksumLevel),
          OptionType::kInt, OptionVerificationType::kNormal, false, 0}},
        {"checksumSmallValSize",
         {offsetof(struct TerarkZipTableOptions, checksumSmallValSize),
          OptionType::kInt, OptionVerificationType::kNormal, false, 0}},
        {"entropyAlgo",
         {offsetof(struct TerarkZipTableOptions, entropyAlgo),
          OptionType::kEntropyAlgo, OptionVerificationType::kNormal, false, 0}},
        {"terarkZipMinLevel",
         {offsetof(struct TerarkZipTableOptions, terarkZipMinLevel),
          OptionType::kInt, OptionVerificationType::kNormal, false, 0}},
        {"debugLevel",
         {offsetof(struct TerarkZipTableOptions, debugLevel), OptionType::kUInt,
          OptionVerificationType::kNormal, false, 0}},
        {"indexNestScale",
         {offsetof(struct TerarkZipTableOptions, indexNestScale),
          OptionType::kUInt, OptionVerificationType::kNormal, false, 0}},
        {"indexTempLevel",
         {offsetof(struct TerarkZipTableOptions, indexTempLevel),
          OptionType::kInt, OptionVerificationType::kNormal, false, 0}},
        {"enableCompressionProbe",
         {offsetof(struct TerarkZipTableOptions, enableCompressionProbe),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"useSuffixArrayLocalMatch",
         {offsetof(struct TerarkZipTableOptions, useSuffixArrayLocalMatch),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"warmUpIndexOnOpen",
         {offsetof(struct TerarkZipTableOptions, warmUpIndexOnOpen),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"warmUpValueOnOpen",
         {offsetof(struct TerarkZipTableOptions, warmUpValueOnOpen),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"disableSecondPassIter",
         {offsetof(struct TerarkZipTableOptions, disableSecondPassIter),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"disableCompressDict",
         {offsetof(struct TerarkZipTableOptions, disableCompressDict),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"optimizeCpuL3Cache",
         {offsetof(struct TerarkZipTableOptions, optimizeCpuL3Cache),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"forceMetaInMemory",
         {offsetof(struct TerarkZipTableOptions, forceMetaInMemory),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"enableEntropyStore",
         {offsetof(struct TerarkZipTableOptions, enableEntropyStore),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"cbtHashBits",
         {offsetof(struct TerarkZipTableOptions, cbtHashBits),
          OptionType::kUInt, OptionVerificationType::kNormal, false, 0}},
        {"offsetArrayBlockUnits",
         {offsetof(struct TerarkZipTableOptions, offsetArrayBlockUnits),
          OptionType::kUInt, OptionVerificationType::kNormal, false, 0}},
        {"sampleRatio",
         {offsetof(struct TerarkZipTableOptions, sampleRatio),
          OptionType::kDouble, OptionVerificationType::kNormal, false, 0}},
        {"indexCacheRatio",
         {offsetof(struct TerarkZipTableOptions, indexCacheRatio),
          OptionType::kDouble, OptionVerificationType::kNormal, false, 0}},
        {"localTempDir",
         {offsetof(struct TerarkZipTableOptions, localTempDir),
          OptionType::kString, OptionVerificationType::kNormal, false, 0}},
        {"indexType",
         {offsetof(struct TerarkZipTableOptions, indexType),
          OptionType::kString, OptionVerificationType::kNormal, false, 0}},
        {"softZipWorkingMemLimit",
         {offsetof(struct TerarkZipTableOptions, softZipWorkingMemLimit),
          OptionType::kUInt64T, OptionVerificationType::kNormal, false, 0}},
        {"hardZipWorkingMemLimit",
         {offsetof(struct TerarkZipTableOptions, hardZipWorkingMemLimit),
          OptionType::kUInt64T, OptionVerificationType::kNormal, false, 0}},
        {"smallTaskMemory",
         {offsetof(struct TerarkZipTableOptions, smallTaskMemory),
          OptionType::kUInt64T, OptionVerificationType::kNormal, false, 0}},
        {"minDictZipValueSize",
         {offsetof(struct TerarkZipTableOptions, minDictZipValueSize),
          OptionType::kUInt32T, OptionVerificationType::kNormal, false, 0}},
        {"keyPrefixLen",
         {offsetof(struct TerarkZipTableOptions, keyPrefixLen),
          OptionType::kUInt32T, OptionVerificationType::kNormal, false, 0}},
        {"singleIndexMinSize",
         {offsetof(struct TerarkZipTableOptions, singleIndexMinSize),
          OptionType::kUInt64T, OptionVerificationType::kNormal, false, 0}},
        {"singleIndexMaxSize",
         {offsetof(struct TerarkZipTableOptions, singleIndexMaxSize),
          OptionType::kUInt64T, OptionVerificationType::kNormal, false, 0}},
        {"minPreadLen",
         {offsetof(struct TerarkZipTableOptions, minPreadLen), OptionType::kInt,
          OptionVerificationType::kNormal, false, 0}},
        {"cacheShards",
         {offsetof(struct TerarkZipTableOptions, cacheShards), OptionType::kInt,
          OptionVerificationType::kNormal, false, 0}},
        {"cacheCapacityBytes",
         {offsetof(struct TerarkZipTableOptions, cacheCapacityBytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal, false, 0}},
        {"cbtEntryPerTrie",
         {offsetof(struct TerarkZipTableOptions, cbtEntryPerTrie),
          OptionType::kUInt32T, OptionVerificationType::kNormal, false, 0}},
        {"cbtMinKeySize",
         {offsetof(struct TerarkZipTableOptions, cbtMinKeySize),
          OptionType::kUInt32T, OptionVerificationType::kNormal, false, 0}},
        {"cbtMinKeyRatio",
         {offsetof(struct TerarkZipTableOptions, cbtMinKeyRatio),
          OptionType::kDouble, OptionVerificationType::kNormal, false, 0}},
};

// delimiter must be "\n"
Status TerarkZipTableOptions::Parse(Slice opt) {
  const char* beg = opt.data();
  const char* end = opt.size() + beg;
  std::unordered_map<std::string, std::string> map;
  while (beg < end) {
    const char* eq = (char*)memchr(beg, '=', end - beg);
    if (eq) {
      const char* eol = std::find(eq + 1, end, '\n');
      fstring name(beg, eq);
      fstring value(eq + 1, eol);
      name.trim();
      value.trim();
      map[name.str()] = value.str();
      beg = eol + 1;
    } else {
      // return Status::InvalidArgument("TerarkZipTableOptions::Parse", "Missing
      // linefeed char");
      Slice line(beg, end - beg);
      fprintf(stderr,
              "ERROR: TerarkZipTableOptions::Parse(): opt=\n%.*s\n---Missing "
              "\\n\nremaining = %.*s\n",
              DOT_STAR_S(opt), DOT_STAR_S(line));
      break;
    }
  }
#define M_String(name)                          \
  {                                             \
    auto iter = map.find(#name);                \
    if (map.end() != iter) name = iter->second; \
  }
#define M_NumFmt(name, fmt)                                               \
  {                                                                       \
    auto iter = map.find(#name);                                          \
    if (map.end() != iter) {                                              \
      if (sscanf(iter->second.c_str(), fmt, &name) != 1) {                \
        return Status::InvalidArgument("TerarkZipTableOptions::Parse():", \
                                       "bad " #name);                     \
      }                                                                   \
    }                                                                     \
  }
#define M_NumGiB(name)                                  \
  {                                                     \
    auto iter = map.find(#name);                        \
    if (map.end() != iter) {                            \
      double dval = strtof(iter->second.c_str(), NULL); \
      name = size_t(dval * GiB);                        \
    }                                                   \
  }
#define M_Boolea(name)                                                    \
  {                                                                       \
    auto iter = map.find(#name);                                          \
    if (map.end() != iter) {                                              \
      if (strcasecmp(iter->second.c_str(), "true") == 0)                  \
        name = true;                                                      \
      else if (strcasecmp(iter->second.c_str(), "false") == 0)            \
        name = false;                                                     \
      else                                                                \
        return Status::InvalidArgument("TerarkZipTableOptions::Parse():", \
                                       "bad " #name);                     \
    }                                                                     \
  }
  int entropyAlgo, cbtHashBits;
  int debugLevel, indexNestScale, indexTempLevel, offsetArrayBlockUnits;
#include "terark_zip_table_property_print.h"

  this->debugLevel = (byte_t)debugLevel;
  this->indexNestScale = (byte_t)indexNestScale;
  this->indexTempLevel = (byte_t)indexTempLevel;
  this->cbtHashBits = (byte_t)cbtHashBits;
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

TERARK_FACTORY_REGISTER_EX(TerarkZipTableFactory, "TerarkZipTable",
                           ([](const std::string& options, Status* s) {
                             return NewTerarkZipTableFactory(options, nullptr,
                                                             s);
                           }));

} /* namespace rocksdb */
