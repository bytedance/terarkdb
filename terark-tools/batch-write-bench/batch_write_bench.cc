//
// This bench tool is used for batch writing bench.
//
// guokuankuan@bytedance.com
//
#include <cstdio>
#include <memory>
#include <vector>
#include <string>
#include <random>

#include <util/gflags_compat.h>

#include <rocksdb/lazy_buffer.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>
#include <rocksdb/options.h>
#include <table/terark_zip_table.h>
#include <rocksdb/sst_file_manager.h>


void init_db_options(rocksdb::DBOptions& db_options_, // NOLINT
                     const std::string& work_dir_,
                     std::shared_ptr<rocksdb::SstFileManager> sst_file_manager_,
                     std::shared_ptr<rocksdb::RateLimiter> rate_limiter_) {
  db_options_.create_if_missing = true;
  db_options_.create_missing_column_families = true;

  db_options_.bytes_per_sync = 65536;
  db_options_.wal_bytes_per_sync = 65536;
  db_options_.max_background_flushes = 4;
  db_options_.base_background_compactions = 4;
  db_options_.max_background_compactions = 10;
  db_options_.max_background_garbage_collections = 4;

  // db_options_.max_background_jobs = 12;
  db_options_.max_open_files = -1;
  db_options_.allow_mmap_reads = true;
  db_options_.delayed_write_rate = 200ULL<<20;
  db_options_.avoid_unnecessary_blocking_io = true;

  rate_limiter_.reset(rocksdb::NewGenericRateLimiter(400ULL<<20, 1000 /* refill_period_us */));
  db_options_.rate_limiter = rate_limiter_;

  sst_file_manager_.reset(rocksdb::NewSstFileManager(rocksdb::Env::Default(), db_options_.info_log,
                                                     std::string() /* trash_dir */, 400ULL<<20,
                                                     true /* delete_existing_trash */, nullptr /* status */,
                                                     1 /* max_trash_db_ratio */,
                                                     32 << 20 /* bytes_max_delete_chunk */));
  db_options_.sst_file_manager = sst_file_manager_;

  db_options_.max_wal_size = uint64_t(256ULL<<21);
  db_options_.max_total_wal_size = 256ULL<<22;

  rocksdb::TerarkZipDeleteTempFiles(work_dir_);  // call once
  assert(db_options_.env == rocksdb::Env::Default());
  std::once_flag ENV_INIT_FLAG;
  std::call_once(ENV_INIT_FLAG, [] {
    auto env = rocksdb::Env::Default();
    int num_db_instance = 1;
    double reserve_factor = 0.3;
    // compaction线程配置
    int num_low_pri = static_cast<int>((reserve_factor * num_db_instance + 1) * 20);
    // flush线程配置
    int num_high_pri = static_cast<int>((reserve_factor * num_db_instance + 1) * 6);
    env->IncBackgroundThreadsIfNeeded(num_low_pri, rocksdb::Env::Priority::LOW);
    env->IncBackgroundThreadsIfNeeded(num_high_pri, rocksdb::Env::Priority::HIGH);
  });
}


void init_cf_options(std::vector<rocksdb::ColumnFamilyOptions>& cf_options, // NOLINT
                     const std::string& work_dir_) {
  cf_options.resize(1);

  std::shared_ptr<rocksdb::TableFactory> table_factory;

  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_cache = rocksdb::NewLRUCache(128ULL<<30, 8, false);
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  table_options.block_size = 8ULL << 10;
  table_options.cache_index_and_filter_blocks = true;
  table_factory.reset(NewBlockBasedTableFactory(table_options));

  rocksdb::TerarkZipTableOptions tzto{};
  tzto.localTempDir = work_dir_;
  tzto.indexNestLevel = 3;
  tzto.checksumLevel = 2;
  tzto.entropyAlgo = rocksdb::TerarkZipTableOptions::kNoEntropy;
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

  table_factory.reset(rocksdb::NewTerarkZipTableFactory(tzto, table_factory));

  auto page_cf_option = rocksdb::ColumnFamilyOptions();
  page_cf_option.write_buffer_size = 256ULL<<20;
  page_cf_option.max_write_buffer_number = 10;
  page_cf_option.target_file_size_base = 128ULL << 20;
  page_cf_option.max_bytes_for_level_base = page_cf_option.target_file_size_base * 4;
  page_cf_option.table_factory = table_factory;
  page_cf_option.compaction_style = rocksdb::kCompactionStyleLevel;
  page_cf_option.num_levels = 6;
  page_cf_option.compaction_options_universal.allow_trivial_move = true;
  page_cf_option.level_compaction_dynamic_level_bytes = true;
  page_cf_option.compression = rocksdb::CompressionType::kNoCompression;
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

  cf_options[0] = page_cf_option;
}


void batch_write(rocksdb::DB* db, int record_bytes, int batch_size, size_t total_bytes) {
  int loops = total_bytes / (record_bytes * batch_size);
  printf("total write loops: %d, batch = %d * %d KB, total bytes(MB) : %zd\n", 
                          loops, batch_size, record_bytes>>10, total_bytes>>20);

  std::random_device device;
  std::mt19937 generator(device());
  std::uniform_int_distribution<int> dist(0, 25);

  for (int loop = 0; loop < loops; ++loop) {
    rocksdb::WriteBatch batch;
    for (size_t idx = 0; idx < batch_size; ++idx) {
      char key[16];
      char value[16<<10];
      for (auto i = 0; i < 16; ++i) {
        key[i] = 'a' + dist(generator);
      }
      for (auto i = 0; i < 16<<10; ++i) {
        value[i] = 'a' + dist(generator);
      }

      batch.Put(rocksdb::Slice(key, 16), rocksdb::Slice(value, 16<<10));
    }

    rocksdb::WriteOptions woptions = rocksdb::WriteOptions();
    // woptions.sync = 
    auto s = db->Write(woptions, &batch);
    if (!s.ok()) {
      printf("write batch failed, code = %d, msg = %s\n", s.code(), s.getState());
      return;
    }

    if (loop % 100 == 0) {
      printf("Finished %d loops\n", loop);
    }
  }
}

DEFINE_string(db_path, "", "data dir");
DEFINE_uint64(data_size_gb, 1, "data size in GB");

void print_help() {
  printf("usage: ./batch_write_bench --db_path=$PWD/data --data_size_gb=10 \n");
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_db_path == "") {
    print_help();
    exit(0);
  }

  printf("db path: %s\n", FLAGS_db_path.data());
  printf("data size: %zd GB\n", FLAGS_data_size_gb);

  std::string work_dir = FLAGS_db_path;

  rocksdb::DB* db;
  rocksdb::DBOptions db_options;

  std::vector<rocksdb::ColumnFamilyOptions> cf_options;
  std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  std::vector<std::string> cf_names = {rocksdb::kDefaultColumnFamilyName};

  std::shared_ptr<rocksdb::SstFileManager> sst_file_manager;
  std::shared_ptr<rocksdb::RateLimiter> rate_limiter;

  init_db_options(db_options, work_dir, sst_file_manager, rate_limiter); 
  init_cf_options(cf_options, work_dir);

  column_families.resize(1);
  for (auto i = 0; i < cf_options.size(); ++i) {
    column_families[i] = rocksdb::ColumnFamilyDescriptor(cf_names[i], cf_options[i]);
  }

  cf_handles.resize(1);
  auto s = rocksdb::DB::Open(db_options, work_dir, column_families,  &cf_handles, &db);
  if (!s.ok()) {
    printf("Open db failed, code = %d, msg = %s\n", s.code(), s.getState());
    exit(0);
  }

  printf("start writing...\n");

  batch_write(db, 16<<10, 64, FLAGS_data_size_gb<<30);

  for (auto i = 0; i < cf_options.size(); ++i) {
    delete cf_handles[i];
  }
  delete db;
  return 0;
}
