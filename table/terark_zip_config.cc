#include "terark_zip_table.h"
#include "terark_zip_common.h"
#include <terark/hash_strmap.hpp>
#include <terark/util/throw.hpp>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#ifdef _MSC_VER
# include <Windows.h>
# define strcasecmp _stricmp
#else
# include <unistd.h>
#endif
#include <mutex>

namespace terark {
  void DictZipBlobStore_setZipThreads(int zipThreads);
}

namespace rocksdb {

static
int ComputeFileSizeMultiplier(double diskLimit, double minVal, int levels) {
  if (diskLimit > 0) {
    double maxSST = diskLimit / 6.0;
    double maxMul = maxSST / minVal;
    double oneMul = pow(maxMul, 1.0/(levels-1));
    if (oneMul > 1.0)
      return (int)oneMul;
    else
      return 1;
  }
  else {
    return 5;
  }
}

void TerarkZipAutoConfigForBulkLoad(struct TerarkZipTableOptions& tzo,
                                    struct DBOptions& dbo,
                                    struct ColumnFamilyOptions& cfo,
                                    size_t cpuNum,
                                    size_t memBytesLimit,
                                    size_t diskBytesLimit)
{
  using namespace std; // max, min
  int iCpuNum = int(cpuNum);
  if (cpuNum > 0) {
    terark::DictZipBlobStore_setZipThreads(max(iCpuNum-1, 0));
  }
  if (0 == memBytesLimit) {
#ifdef _MSC_VER
    MEMORYSTATUSEX statex;
    statex.dwLength = sizeof(statex);
    GlobalMemoryStatusEx(&statex);
    memBytesLimit = statex.ullTotalPhys;
#else
    size_t page_num  = sysconf(_SC_PHYS_PAGES);
    size_t page_size = sysconf(_SC_PAGE_SIZE);
    memBytesLimit = page_num * page_size;
#endif
  }
  tzo.softZipWorkingMemLimit = memBytesLimit * 7 / 8;
  tzo.hardZipWorkingMemLimit = tzo.softZipWorkingMemLimit;
  tzo.smallTaskMemory = memBytesLimit / 16;
  tzo.indexNestLevel = 2;

  cfo.write_buffer_size = tzo.smallTaskMemory;
  cfo.num_levels = 5;
  cfo.max_write_buffer_number = 6;
  cfo.min_write_buffer_number_to_merge = 1;
  cfo.target_file_size_base = cfo.write_buffer_size;
  cfo.target_file_size_multiplier = ComputeFileSizeMultiplier(
      diskBytesLimit, cfo.target_file_size_base, cfo.num_levels);
  cfo.compaction_style = rocksdb::kCompactionStyleUniversal;
  cfo.compaction_options_universal.allow_trivial_move = true;

  cfo.max_compaction_bytes = (static_cast<uint64_t>(1) << 60);
  cfo.disable_auto_compactions = true;
  cfo.level0_file_num_compaction_trigger = (1<<30);
  cfo.level0_slowdown_writes_trigger = (1<<30);
  cfo.level0_stop_writes_trigger = (1<<30);
  cfo.soft_pending_compaction_bytes_limit = 0;
  cfo.hard_pending_compaction_bytes_limit = 0;

  dbo.create_if_missing = true;
  dbo.allow_concurrent_memtable_write = false;
  dbo.allow_mmap_reads = true;
  dbo.max_background_flushes = 2;
  dbo.max_subcompactions = 1; // no sub compactions

  dbo.env->SetBackgroundThreads(max(1,min(4,iCpuNum/2)), rocksdb::Env::HIGH);
}

void TerarkZipAutoConfigForOnlineDB(struct TerarkZipTableOptions& tzo,
                                    struct DBOptions& dbo,
                                    struct ColumnFamilyOptions& cfo,
                                    size_t cpuNum,
                                    size_t memBytesLimit,
                                    size_t diskBytesLimit)
{
  using namespace std; // max, min
  int iCpuNum = int(cpuNum);
  if (cpuNum > 0) {
    terark::DictZipBlobStore_setZipThreads((iCpuNum * 3 + 1) / 5);
  }
  if (0 == memBytesLimit) {
#ifdef _MSC_VER
    MEMORYSTATUSEX statex;
    statex.dwLength = sizeof(statex);
    GlobalMemoryStatusEx(&statex);
    memBytesLimit = statex.ullTotalPhys;
#else
    size_t page_num  = sysconf(_SC_PHYS_PAGES);
    size_t page_size = sysconf(_SC_PAGE_SIZE);
    memBytesLimit = page_num * page_size;
#endif
  }
  tzo.softZipWorkingMemLimit = memBytesLimit * 1 / 8;
  tzo.hardZipWorkingMemLimit = tzo.softZipWorkingMemLimit * 2;
  tzo.smallTaskMemory = memBytesLimit / 64;

  cfo.write_buffer_size = tzo.smallTaskMemory;
  cfo.num_levels = 5;
  cfo.max_write_buffer_number = 5;
  cfo.target_file_size_base = cfo.write_buffer_size;
  cfo.target_file_size_multiplier = ComputeFileSizeMultiplier(
      diskBytesLimit, cfo.target_file_size_base, cfo.num_levels);
  cfo.compaction_style = rocksdb::kCompactionStyleUniversal;
  cfo.compaction_options_universal.allow_trivial_move = true;

  dbo.create_if_missing = true;
  dbo.allow_mmap_reads = true;
  dbo.max_background_flushes = 2;
  dbo.max_subcompactions = 1; // no sub compactions
  dbo.base_background_compactions = 3;
  dbo.max_background_compactions = 5;

  dbo.env->SetBackgroundThreads(max(1,min(3,iCpuNum*3/8)), rocksdb::Env::LOW);
  dbo.env->SetBackgroundThreads(max(1,min(2,iCpuNum*2/8)), rocksdb::Env::HIGH);
}

bool TerarkZipConfigFromEnv(DBOptions& dbo, ColumnFamilyOptions& cfo) {
  if (TerarkZipCFOptionsFromEnv(cfo)) {
    TerarkZipDBOptionsFromEnv(dbo);
    return true;
  } else {
    return false;
  }
}

bool TerarkZipCFOptionsFromEnv(ColumnFamilyOptions& cfo) {
  const char* localTempDir = getenv("TerarkZipTable_localTempDir");
  if (!localTempDir) {
    STD_INFO("TerarkZipConfigFromEnv(dbo, cfo) failed because env TerarkZipTable_localTempDir is not defined\n");
    return false;
  }
  if (!*localTempDir) {
      THROW_STD(invalid_argument,
        "If env TerarkZipTable_localTempDir is defined, it must not be empty");
  }
  struct TerarkZipTableOptions tzo;
  tzo.localTempDir = localTempDir;
  if (const char* algo = getenv("TerarkZipTable_entropyAlgo")) {
    if (strcasecmp(algo, "NoEntropy") == 0) {
      tzo.entropyAlgo = tzo.kNoEntropy;
    } else if (strcasecmp(algo, "FSE") == 0) {
      tzo.entropyAlgo = tzo.kFSE;
    } else if (strcasecmp(algo, "huf") == 0) {
      tzo.entropyAlgo = tzo.kHuffman;
    } else if (strcasecmp(algo, "huff") == 0) {
      tzo.entropyAlgo = tzo.kHuffman;
    } else if (strcasecmp(algo, "huffman") == 0) {
      tzo.entropyAlgo = tzo.kHuffman;
    } else {
      tzo.entropyAlgo = tzo.kNoEntropy;
      STD_WARN(
        "bad env TerarkZipTable_entropyAlgo=%s, must be one of {NoEntropy, FSE, huf}, reset to default 'NoEntropy'\n"
        , algo);
    }
  }
  if (const char* env = getenv("TerarkZipTable_indexType")) {
    tzo.indexType = env;
  }

#define MyGetInt(obj, name, Default) \
    obj.name = (int)terark::getEnvLong("TerarkZipTable_" #name, Default)
#define MyGetBool(obj, name, Default) \
    obj.name = terark::getEnvBool("TerarkZipTable_" #name, Default)
#define MyGetDouble(obj, name, Default) \
    obj.name = terark::getEnvDouble("TerarkZipTable_" #name, Default)
#define MyGetXiB(obj, name) \
  if (const char* env = getenv("TerarkZipTable_" #name))\
    obj.name = terark::ParseSizeXiB(env)

  MyGetInt   (tzo, checksumLevel           , 3    );
  MyGetInt   (tzo, indexNestLevel          , 3    );
  MyGetInt   (tzo, terarkZipMinLevel       , 0    );
  MyGetInt   (tzo, debugLevel              , 0    );
  MyGetInt   (tzo, offsetArrayBlockUnits   , 0    );
  if (true
      && 0   != tzo.offsetArrayBlockUnits
      && 64  != tzo.offsetArrayBlockUnits
      && 128 != tzo.offsetArrayBlockUnits
  ){
    STD_WARN(
      "TerarkZipConfigFromEnv: bad offsetArrayBlockUnits = %d, must be one of {0,64,128}, reset to 128\n"
      , tzo.offsetArrayBlockUnits
    );
    tzo.offsetArrayBlockUnits = 128;
  }

  MyGetBool  (tzo, useSuffixArrayLocalMatch, false);
  MyGetBool  (tzo, warmUpIndexOnOpen       , true );
  MyGetBool  (tzo, warmUpValueOnOpen       , false);
  MyGetBool  (tzo, disableSecondPassIter   , false);

  MyGetDouble(tzo, estimateCompressionRatio, 0.20 );
  MyGetDouble(tzo, sampleRatio             , 0.03 );
  MyGetDouble(tzo, indexCacheRatio         , 0.00 );

  MyGetXiB(tzo, softZipWorkingMemLimit);
  MyGetXiB(tzo, hardZipWorkingMemLimit);
  MyGetXiB(tzo, smallTaskMemory);

#if defined(TerocksPrivateCode)
  MyGetInt(tzo, minDictZipValueSize, 50);
#endif // TerocksPrivateCode

  cfo.table_factory.reset(NewTerarkZipTableFactory(tzo, NewAdaptiveTableFactory()));
  const char* compaction_style = "Universal";
  if (const char* env = getenv("TerarkZipTable_compaction_style")) {
    compaction_style = env;
  }
  if (strcasecmp(compaction_style, "Level") == 0) {
    cfo.compaction_style = kCompactionStyleLevel;
  } else {
    if (strcasecmp(compaction_style, "Universal") != 0) {
      STD_WARN(
        "bad env TerarkZipTable_compaction_style=%s, use default 'universal'",
        compaction_style);
    }
    cfo.compaction_style = kCompactionStyleUniversal;
    cfo.compaction_options_universal.allow_trivial_move = true;
  }
  cfo.write_buffer_size     = uint64_t(1) << 30; // 1G
  cfo.target_file_size_base = uint64_t(1) << 30; // 1G
  MyGetXiB(cfo, write_buffer_size);
  MyGetXiB(cfo, target_file_size_base);
  MyGetInt(cfo, max_write_buffer_number    , 5);
  MyGetInt(cfo, target_file_size_multiplier, 5);
  MyGetInt(cfo, num_levels                 , 5);

  if (tzo.debugLevel) {
    STD_INFO("TerarkZipConfigFromEnv(dbo, cfo) successed\n");
  }
  return true;
}

void TerarkZipDBOptionsFromEnv(DBOptions& dbo) {
  MyGetInt(dbo, base_background_compactions, 3);
  MyGetInt(dbo,  max_background_compactions, 5);
  MyGetInt(dbo,  max_background_flushes    , 2);
  MyGetInt(dbo,  max_subcompactions        , 1);

  dbo.env->SetBackgroundThreads(dbo.max_background_compactions, rocksdb::Env::LOW);
  dbo.env->SetBackgroundThreads(dbo.max_background_flushes    , rocksdb::Env::HIGH);
  dbo.allow_mmap_reads = true;
}

bool TerarkZipIsBlackListCF(const std::string& cfname) {
  static std::mutex  mtx;
  static size_t  isInitialized = false;
  static terark::hash_strmap<>  blackList;
  if (!isInitialized) {
    std::lock_guard<std::mutex> lock(mtx);
    if (!isInitialized) {
      if (const char* env = getenv("TerarkZipTable_blackListColumnFamily")) {
        valvec<fstring> names;
        fstring(env).split(',', &names);
        for (auto nm : names)
          blackList.insert_i(nm);
      }
      isInitialized = true;
    }
  }
  return blackList.exists(cfname);
}

template<class T>
T& auto_const_cast(const T& x) {
  return const_cast<T&>(x);
}

void
TerarkZipMultiCFOptionsFromEnv(const DBOptions& db_options,
      const std::vector<ColumnFamilyDescriptor>& cfvec) {
  size_t numIsBlackList = 0;
  for (auto& cf : auto_const_cast(cfvec)) {
    if (TerarkZipIsBlackListCF(cf.name)) {
      numIsBlackList++;
    } else {
      TerarkZipCFOptionsFromEnv(cf.options);
    }
  }
  if (numIsBlackList < cfvec.size()) {
    TerarkZipDBOptionsFromEnv(auto_const_cast(db_options));
  }
}

}
