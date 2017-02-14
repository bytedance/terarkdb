#include "terark_zip_table.h"
#include "terark_zip_common.h"
#include <terark/fstring.hpp>
#include <terark/util/throw.hpp>
#include <terark/num_to_str.hpp>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/env.h>
#include <unistd.h>

namespace rocksdb {

void TerarkZipAutoConfigForBulkLoad(struct TerarkZipTableOptions& tzo,
                                    struct DBOptions& dbo,
                                    struct ColumnFamilyOptions& cfo,
                                    size_t memBytesLimit)
{
  if (0 == memBytesLimit) {
    size_t page_num  = sysconf(_SC_PHYS_PAGES);
    size_t page_size = sysconf(_SC_PAGE_SIZE);
    memBytesLimit = page_num * page_size;
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
  cfo.target_file_size_multiplier = 5;
  cfo.compaction_style = rocksdb::kCompactionStyleUniversal;
  cfo.compaction_options_universal.allow_trivial_move = true;

  cfo.max_compaction_bytes = (static_cast<uint64_t>(1) << 60);
  cfo.disable_auto_compactions = true;
  dbo.disableDataSync = true;
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

  dbo.env->SetBackgroundThreads(4, rocksdb::Env::HIGH);
}

void TerarkZipAutoConfigForOnlineDB(struct TerarkZipTableOptions& tzo,
                                    struct DBOptions& dbo,
                                    struct ColumnFamilyOptions& cfo,
                                    size_t memBytesLimit)
{
  if (0 == memBytesLimit) {
    size_t page_num  = sysconf(_SC_PHYS_PAGES);
    size_t page_size = sysconf(_SC_PAGE_SIZE);
    memBytesLimit = page_num * page_size;
  }
  tzo.softZipWorkingMemLimit = memBytesLimit * 1 / 8;
  tzo.hardZipWorkingMemLimit = tzo.softZipWorkingMemLimit * 2;
  tzo.smallTaskMemory = memBytesLimit / 64;

  cfo.write_buffer_size = tzo.smallTaskMemory;
  cfo.num_levels = 5;
  cfo.max_write_buffer_number = 5;
  cfo.target_file_size_base = cfo.write_buffer_size;
  cfo.target_file_size_multiplier = 5;
  cfo.compaction_style = rocksdb::kCompactionStyleUniversal;
  cfo.compaction_options_universal.allow_trivial_move = true;

  dbo.create_if_missing = true;
  dbo.allow_mmap_reads = true;
  dbo.max_background_flushes = 2;
  dbo.max_subcompactions = 1; // no sub compactions
  dbo.base_background_compactions = 3;
  dbo.max_background_compactions = 5;

  dbo.env->SetBackgroundThreads(3, rocksdb::Env::LOW);
  dbo.env->SetBackgroundThreads(2, rocksdb::Env::HIGH);
}

void TerarkZipConfigFromEnv(DBOptions& dbo, ColumnFamilyOptions& cfo) {
  struct TerarkZipTableOptions tzo;
  if (const char* env = getenv("TerarkZipTable_localTempDir")) {
    tzo.localTempDir = env;
  }
  else {
    THROW_STD(invalid_argument,
        "env TerarkZipTable_localTempDir must be defined");
  }
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
  dbo.allow_mmap_reads = true;

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
  MyGetBool  (tzo, useSuffixArrayLocalMatch, true );
  MyGetBool  (tzo, warmUpIndexOnOpen       , true );
  MyGetBool  (tzo, warmUpValueOnOpen       , false);

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
  MyGetInt(cfo, write_buffer_size          , 1ull << 30);
  MyGetInt(cfo, max_write_buffer_number    , 5         );
  MyGetInt(cfo, target_file_size_base      , 1ull << 30);
  MyGetInt(cfo, target_file_size_multiplier, 5         );
  MyGetInt(cfo, num_levels                 , 5         );
  MyGetInt(dbo, base_background_compactions, 3         );
  MyGetInt(dbo,  max_background_compactions, 5         );
  MyGetInt(dbo,  max_background_flushes    , 2         );
  MyGetInt(dbo,  max_subcompactions        , 1         );

  dbo.env->SetBackgroundThreads(dbo.max_background_compactions, rocksdb::Env::LOW);
  dbo.env->SetBackgroundThreads(dbo.max_background_flushes    , rocksdb::Env::HIGH);
}

}
