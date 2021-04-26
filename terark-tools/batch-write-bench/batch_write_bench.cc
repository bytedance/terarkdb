// Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License.
//
// This bench tool is used for batch writing bench.
//
// guokuankuan@bytedance.com
//
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/iostats_context.h>
#include <rocksdb/lazy_buffer.h>
#include <rocksdb/options.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/perf_level.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/slice.h>
#include <rocksdb/sst_file_manager.h>
#include <rocksdb/table.h>
#include <table/terark_zip_table.h>
#include <util/gflags_compat.h>

#include <cstdio>
#include <memory>
#include <random>
#include <string>
#include <vector>

DEFINE_string(db_path, "", "data dir");
DEFINE_uint64(gb_per_thread, 1, "data size in GB");
DEFINE_uint64(threads, 1, "thread count");
DEFINE_uint64(batch_size, 64, "batch size");
DEFINE_uint64(value_size, 16384, "batch size");
DEFINE_int32(perf_level, TERARKDB_NAMESPACE::PerfLevel::kDisable,
             "Level of perf collection");

void init_db_options(
    TERARKDB_NAMESPACE::DBOptions& db_options_,  // NOLINT
    const std::string& work_dir_,
    std::shared_ptr<TERARKDB_NAMESPACE::SstFileManager> sst_file_manager_,
    std::shared_ptr<TERARKDB_NAMESPACE::RateLimiter> rate_limiter_) {
  db_options_.create_if_missing = true;
  db_options_.create_missing_column_families = true;

  db_options_.bytes_per_sync = 32768;
  db_options_.wal_bytes_per_sync = 32768;
  db_options_.max_background_flushes = 8;
  db_options_.base_background_compactions = 4;
  db_options_.max_background_compactions = 10;
  db_options_.max_background_garbage_collections = 4;

  // db_options_.max_background_jobs = 12;
  db_options_.max_open_files = -1;
  db_options_.allow_mmap_reads = true;
  db_options_.delayed_write_rate = 200ULL << 20;

  db_options_.avoid_unnecessary_blocking_io = true;
  rate_limiter_.reset(
      TERARKDB_NAMESPACE::NewGenericRateLimiter(200ULL << 20, 1000));
  db_options_.rate_limiter = rate_limiter_;
  sst_file_manager_.reset(TERARKDB_NAMESPACE::NewSstFileManager(
      TERARKDB_NAMESPACE::Env::Default(), db_options_.info_log, std::string(),
      200ULL << 20, true, nullptr, 1, 32 << 20));
  db_options_.sst_file_manager = sst_file_manager_;

  db_options_.max_wal_size = 512ULL << 20;
  db_options_.max_total_wal_size = 1024ULL << 20;
#ifdef WITH_TERARK_ZIP
  TERARKDB_NAMESPACE::TerarkZipDeleteTempFiles(work_dir_);  // call once
#endif
  assert(db_options_.env == TERARKDB_NAMESPACE::Env::Default());
  std::once_flag ENV_INIT_FLAG;
  std::call_once(ENV_INIT_FLAG, [] {
    auto env = TERARKDB_NAMESPACE::Env::Default();
    int num_db_instance = 1;
    double reserve_factor = 0.3;
    // compaction线程配置
    int num_low_pri =
        static_cast<int>((reserve_factor * num_db_instance + 1) * 20);
    // flush线程配置
    int num_high_pri =
        static_cast<int>((reserve_factor * num_db_instance + 1) * 6);
    env->IncBackgroundThreadsIfNeeded(num_low_pri,
                                      TERARKDB_NAMESPACE::Env::Priority::LOW);
    env->IncBackgroundThreadsIfNeeded(num_high_pri,
                                      TERARKDB_NAMESPACE::Env::Priority::HIGH);
  });
}

void init_cf_options(
    std::vector<TERARKDB_NAMESPACE::ColumnFamilyOptions>& cf_options,  // NOLINT
    const std::string& work_dir_) {
  cf_options.resize(1);

  std::shared_ptr<TERARKDB_NAMESPACE::TableFactory> table_factory;

  TERARKDB_NAMESPACE::BlockBasedTableOptions table_options;
  table_options.block_cache =
      TERARKDB_NAMESPACE::NewLRUCache(128ULL << 30, 8, false);
  table_options.filter_policy.reset(
      TERARKDB_NAMESPACE::NewBloomFilterPolicy(10, false));
  table_options.block_size = 8ULL << 10;
  table_options.cache_index_and_filter_blocks = true;
  table_factory.reset(NewBlockBasedTableFactory(table_options));
#ifdef WITH_TERARK_ZIP
  TERARKDB_NAMESPACE::TerarkZipTableOptions tzto{};
  tzto.localTempDir = work_dir_;
  tzto.indexNestLevel = 3;
  tzto.checksumLevel = 2;
  tzto.entropyAlgo = TERARKDB_NAMESPACE::TerarkZipTableOptions::kNoEntropy;
  tzto.terarkZipMinLevel = 0;
  tzto.debugLevel = 2;
  tzto.indexNestScale = 8;
  tzto.enableCompressionProbe = true;
  tzto.useSuffixArrayLocalMatch = false;
  tzto.warmUpIndexOnOpen = true;
  tzto.warmUpValueOnOpen = false;
  tzto.disableSecondPassIter = false;
  tzto.enableEntropyStore = false;
  tzto.indexTempLevel = 0;
  tzto.offsetArrayBlockUnits = 128;
  tzto.sampleRatio = 0.01;
  tzto.indexType = "Mixed_XL_256_32_FL";
  tzto.softZipWorkingMemLimit = 8ull << 30;
  tzto.hardZipWorkingMemLimit = 16ull << 30;
  tzto.smallTaskMemory = 1200 << 20;     // 1.2G
  tzto.minDictZipValueSize = (1 << 26);  // 64M
  tzto.indexCacheRatio = 0.001;
  tzto.singleIndexMinSize = 8ULL << 20;
  tzto.singleIndexMaxSize = 0x1E0000000;  // 7.5G
  tzto.minPreadLen = 0;
  tzto.cacheShards = 17;        // to reduce lock competition
  tzto.cacheCapacityBytes = 0;  // non-zero implies direct io read
  tzto.disableCompressDict = false;
  tzto.optimizeCpuL3Cache = true;
  tzto.forceMetaInMemory = false;

  table_factory.reset(
      TERARKDB_NAMESPACE::NewTerarkZipTableFactory(tzto, table_factory));
#endif
  auto page_cf_option = TERARKDB_NAMESPACE::ColumnFamilyOptions();
  page_cf_option.write_buffer_size = 256ULL << 20;
  page_cf_option.max_write_buffer_number = 100;
  page_cf_option.target_file_size_base = 128ULL << 20;
  page_cf_option.max_bytes_for_level_base =
      page_cf_option.target_file_size_base * 4;
  page_cf_option.table_factory = table_factory;
  page_cf_option.compaction_style = TERARKDB_NAMESPACE::kCompactionStyleLevel;
  page_cf_option.num_levels = 6;
  page_cf_option.compaction_options_universal.allow_trivial_move = true;
  page_cf_option.level_compaction_dynamic_level_bytes = true;
  page_cf_option.compression =
      TERARKDB_NAMESPACE::CompressionType::kNoCompression;
  page_cf_option.enable_lazy_compaction = true;
  page_cf_option.level0_file_num_compaction_trigger = 4;
  page_cf_option.level0_slowdown_writes_trigger = 1000;
  page_cf_option.level0_stop_writes_trigger = 1000;
  page_cf_option.soft_pending_compaction_bytes_limit = 1ULL << 60;
  page_cf_option.hard_pending_compaction_bytes_limit = 1ULL << 60;
  page_cf_option.blob_size = 32;
  page_cf_option.blob_gc_ratio = 0.1;
  page_cf_option.max_subcompactions = 6;
  page_cf_option.optimize_filters_for_hits = true;
  page_cf_option.optimize_range_deletion = true;

  cf_options[0] = page_cf_option;
}

void batch_write(TERARKDB_NAMESPACE::DB* db, int record_bytes, int batch_size,
                 size_t total_bytes) {
  int loops = total_bytes / (record_bytes * batch_size);
  printf("total write loops: %d, batch = %d * %d KB, total bytes(MB) : %zd\n",
         loops, batch_size, record_bytes >> 10, total_bytes >> 20);

  SetPerfLevel(static_cast<TERARKDB_NAMESPACE::PerfLevel>(FLAGS_perf_level));
  TERARKDB_NAMESPACE::get_perf_context()->EnablePerLevelPerfContext();

  std::random_device device;
  std::mt19937 generator(device());
  std::uniform_int_distribution<int> dist(0, 25);

  for (int loop = 0; loop < loops; ++loop) {
    TERARKDB_NAMESPACE::WriteBatch batch;
    for (size_t idx = 0; idx < batch_size; ++idx) {
      char key[16];
      char value[16 << 10];
      for (auto i = 0; i < 16; ++i) {
        key[i] = 'a' + dist(generator);
      }
      for (auto i = 0; i < FLAGS_value_size; ++i) {
        value[i] = 'a' + dist(generator);
      }

      batch.Put(TERARKDB_NAMESPACE::Slice(key, 16),
                TERARKDB_NAMESPACE::Slice(value, FLAGS_value_size));
    }

    TERARKDB_NAMESPACE::WriteOptions woptions =
        TERARKDB_NAMESPACE::WriteOptions();
    woptions.sync = true;

    TERARKDB_NAMESPACE::get_perf_context()->Reset();
    TERARKDB_NAMESPACE::get_iostats_context()->Reset();
    auto now = std::chrono::high_resolution_clock::now();
    auto s = db->Write(woptions, &batch);
    if (std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - now)
            .count() > 1000) {
      printf("PerfContext: %s\n",
             TERARKDB_NAMESPACE::get_perf_context()->ToString(true).c_str());
      printf("IOContext: %s\n",
             TERARKDB_NAMESPACE::get_iostats_context()->ToString(true).c_str());
    }
    if (!s.ok()) {
      printf("write batch failed, code = %d, msg = %s\n", s.code(),
             s.getState());
      return;
    }

    if (loop % 100 == 0) {
      printf("Finished %d loops\n", loop);
    }
  }
}

void print_help() {
  printf(
      "usage: ./batch_write_bench --db_path=$PWD/data --gb_per_thread=1 "
      "--threads=1 --batch_size=64 --value_size=16384 --perf_level=0\n");
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_db_path == "") {
    print_help();
    exit(0);
  }

  printf("db path: %s\n", FLAGS_db_path.data());
  printf("gb_per_thread: %zd GB\n", FLAGS_gb_per_thread);
  printf("threads: %zd \n", FLAGS_threads);
  printf("batch_size: %zd \n", FLAGS_batch_size);
  printf("value_size: %zd \n", FLAGS_value_size);
  printf("perf_level: %d \n", FLAGS_perf_level);

  std::string work_dir = FLAGS_db_path;

  TERARKDB_NAMESPACE::DB* db;
  TERARKDB_NAMESPACE::DBOptions db_options;

  std::vector<TERARKDB_NAMESPACE::ColumnFamilyOptions> cf_options;
  std::vector<TERARKDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  std::vector<TERARKDB_NAMESPACE::ColumnFamilyDescriptor> column_families;
  std::vector<std::string> cf_names = {
      TERARKDB_NAMESPACE::kDefaultColumnFamilyName};

  std::shared_ptr<TERARKDB_NAMESPACE::SstFileManager> sst_file_manager;
  std::shared_ptr<TERARKDB_NAMESPACE::RateLimiter> rate_limiter;

  init_db_options(db_options, work_dir, sst_file_manager, rate_limiter);
  init_cf_options(cf_options, work_dir);

  column_families.resize(1);
  for (auto i = 0; i < cf_options.size(); ++i) {
    column_families[i] =
        TERARKDB_NAMESPACE::ColumnFamilyDescriptor(cf_names[i], cf_options[i]);
  }

  cf_handles.resize(1);
  auto s = TERARKDB_NAMESPACE::DB::Open(db_options, work_dir, column_families,
                                        &cf_handles, &db);
  if (!s.ok()) {
    printf("Open db failed, code = %d, msg = %s\n", s.code(), s.getState());
    exit(0);
  }

  printf("start writing...\n");

  std::vector<std::thread> threads;
  for (int i = 0; i < FLAGS_threads; ++i) {
    threads.emplace_back([&]() {
      batch_write(db, 16 + FLAGS_value_size, 64, FLAGS_gb_per_thread << 30);
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  for (auto i = 0; i < cf_options.size(); ++i) {
    delete cf_handles[i];
  }
  delete db;
  return 0;
}
