#include "terark_zip_table.h"
#include <rocksdb/options.h>
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
  dbo.base_background_compactions = 2;
  dbo.max_background_compactions = 3;

  dbo.env->SetBackgroundThreads(3, rocksdb::Env::LOW);
  dbo.env->SetBackgroundThreads(2, rocksdb::Env::HIGH);
}

}
