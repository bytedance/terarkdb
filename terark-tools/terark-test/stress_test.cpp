#pragma warning(disable : 4996)

#include <cctype>
#include <chrono>
#include <cinttypes>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include <boost/fiber/future.hpp>
#include <db/memtable.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/experimental.h>
#include <rocksdb/memtablerep.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <table/get_context.h>
#include <table/iterator_wrapper.h>
#include <table/table_builder.h>
#include <table/table_reader.h>
#include <terark/fsa/cspptrie.inl>
#include <terark/io/FileStream.hpp>
#include <terark/lcast.hpp>
#include <terark/mempool_lock_none.hpp>
#include <terark/rank_select.hpp>
#include <terark/stdtypes.hpp>
#include <terark/util/mmap.hpp>
#include <terark/zbs/dict_zip_blob_store.hpp>
#include <terark/zbs/sufarr_inducedsort.h>
#include <terark/zbs/zip_reorder_map.hpp>
#include <util/coding.h>
#include <util/filename.h>
#include <utilities/merge_operators/string_append/stringappend.h>
#include <utilities/merge_operators/string_append/stringappend2.h>
//#include "split_iterator.h"

#define TEST_TERARK 1

#if TEST_TERARK
#include <table/terark_zip_common.h>
#include <table/terark_zip_table.h>
#include <terark/idx/terark_zip_index.hpp>
#else
namespace rocksdb {
struct TerarkZipTableOptions {};
} // namespace rocksdb
#endif

size_t file_size_base = 64ull << 20;
size_t blob_size = 128;
size_t value_avg_size = 1024;
size_t rand_key_times = 500;

#define ITER_TEST 0
#define GET_TEST 0
#define RANGE_DEL 0
#define COMPACTION 0
#define ASYNC_TEST 1
#define READ_ONLY 0
#define WORKER_TEST 0

#define READ_ONLY_TEST_KEY "FF7EAC449F56EB1E9A9A0D43195"
#define READ_ONLY_TEST_SEQ 12983622

class ComparatorRename : public rocksdb::Comparator {
public:
  virtual const char *Name() const override { return n; }

  virtual int Compare(const rocksdb::Slice &a,
                      const rocksdb::Slice &b) const override {
    return c->Compare(a, b);
  }

  virtual bool Equal(const rocksdb::Slice &a,
                     const rocksdb::Slice &b) const override {
    return c->Equal(a, b);
  }
  virtual void
  FindShortestSeparator(std::string *start,
                        const rocksdb::Slice &limit) const override {
    c->FindShortestSeparator(start, limit);
  }

  virtual void FindShortSuccessor(std::string *key) const override {
    c->FindShortSuccessor(key);
  }

  const char *n;
  const rocksdb::Comparator *c;

  ComparatorRename(const char *_n, const rocksdb::Comparator *_c)
      : n(_n), c(_c) {}
};

uint64_t get_snapshot_seqno(const rocksdb::Snapshot *s) {
  return ((const uint64_t *)s)[1];
};
void set_snapshot_seqno(const rocksdb::Snapshot *s, uint64_t seqno) {
  ((uint64_t *)s)[1] = seqno;
};

#if WORKER_TEST
#include <rocksdb/compaction_dispatcher.h>

class AsyncCompactionDispatcher : public rocksdb::RemoteCompactionDispatcher {
public:
  class AsyncWorker : public rocksdb::RemoteCompactionDispatcher::Worker {
  public:
    AsyncWorker(const rocksdb::Options &options, std::string working_dir,
                uint64_t seed)
        : rocksdb::RemoteCompactionDispatcher::Worker(
              rocksdb::EnvOptions(options), options.env),
          working_dir_(working_dir), seed_(seed) {}
    virtual std::string GenerateOutputFileName(size_t file_index) {
      uint64_t name_seed = seed_++;
      char name[40];
      snprintf(name, sizeof name, "/Terark-%08" PRIx64 ".sst", name_seed);
      return working_dir_ + name;
    }

  private:
    std::string working_dir_;
    uint64_t seed_;
  };

  AsyncCompactionDispatcher(const std::string &self,
                            const std::string &working_dir)
      : self_(self), working_dir_(working_dir) {}

  virtual std::future<std::string> DoCompaction(const std::string &data) {
    uint64_t seed = seed_.fetch_add(65536);
    return std::async([this, data, seed]() -> std::string {
      // popen("self working_dir seed" << data) >> result
      char buffer[64];
      snprintf(buffer, sizeof buffer, "/Terark-%08" PRIx64 ".param", seed);
      std::string param = working_dir_ + buffer;

      std::fstream(param, std::ios::out) << data;
      std::string cmd = "bash start.sh " + self_ + " " + working_dir_ + " " +
                        terark::lcast(seed) + " < " + param;
      FILE *p = popen(cmd.c_str(), "r");
      std::string result;
      char *ln = NULL;
      size_t len = 0;
      while (getline(&ln, &len, p) != -1) {
        result.append(ln);
      }
      free(ln);
      pclose(p);
      ::remove(param.c_str());
      size_t s = result.find('{');
      size_t e = result.rfind('}');
      result = result.substr(s, e - s + 1);
      return result;
    });
  }
  std::string self_;
  std::string working_dir_;
  std::atomic_uint64_t seed_ = {};
};

#endif
// class TestMergeOperator : public rocksdb::StringAppendOperator {
class TestMergeOperator : public rocksdb::StringAppendTESTOperator {
public:
  // TestMergeOperator(char delim_char) :
  // rocksdb::StringAppendOperator(delim_char) {}
  TestMergeOperator(char delim_char)
      : rocksdb::StringAppendTESTOperator(delim_char) {}

  virtual rocksdb::Status Serialize(std::string * /*bytes*/) const override {
    return rocksdb::Status::OK();
  }
  virtual rocksdb::Status
  Deserialize(const rocksdb::Slice & /*bytes*/) override {
    return rocksdb::Status::OK();
  }
};

class TestCompactionFilter : public rocksdb::CompactionFilter {
  bool Filter(int /*level*/, const rocksdb::Slice & /*key*/,
              const rocksdb::Slice & /*existing_value*/,
              std::string * /*new_value*/,
              bool * /*value_changed*/) const override {
    return false;
  }
  const char *Name() const override { return "TestCompactionFilter"; }
};

namespace rocksdb {
MemTableRepFactory *NewTRBTreeRepFactory();
}

#if __APPLE__
#define DebugBreak __builtin_debugtrap
#endif

#undef assert
#define assert(exp)                                                            \
  do {                                                                         \
    if (!(exp))                                                                \
      DebugBreak();                                                            \
  } while (false)

template <class T, class F> bool IsSame(std::vector<T> &arr, F &&f) {
  for (size_t i = 1; i < arr.size(); ++i) {
    if (!f(arr[i - 1], arr[i])) {
      return false;
    }
  }
  return true;
}
template <class T, class F> bool IsAny(std::vector<T> &arr, F &&f) {
  for (auto &t : arr) {
    if (f(t)) {
      return true;
    }
  }
  return false;
}
template <class T, class F> bool IsAll(std::vector<T> &arr, F &&f) {
  for (auto &t : arr) {
    if (!f(t)) {
      return false;
    }
  }
  return true;
}

template <class T, class V, class F>
bool AllSame(std::vector<T> &left, std::vector<V> &right, F &&f) {
  if (left.size() != right.size())
    return false;
  for (size_t i = 0; i < left.size(); ++i) {
    if (!f(left[i], right[i])) {
      return false;
    }
  }
  return true;
}

std::string get_key(size_t i) {
  char buffer[32];
  snprintf(buffer, sizeof buffer, "%012zd", i);
  return buffer;
}

std::string get_rnd_key(size_t r) {
  std::mt19937_64 mt(r);
  char buffer[65];
  snprintf(buffer + 0, 17, "%016llX", mt());
  snprintf(buffer + 16, 17, "%016llX", mt());
  snprintf(buffer + 32, 17, "%016llX", mt());
  snprintf(buffer + 48, 17, "%016llX", mt());
  // uint64_t v = mt();
  // memcpy(buffer + 8, &v, sizeof v);
  return std::string(buffer,
                     buffer + std::uniform_int_distribution<size_t>(8, 64)(mt));
}
std::string get_value(size_t i) {
  static std::string str = [] {
    std::string s =
        "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890";
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    std::shuffle(s.begin(), s.end(), std::mt19937_64());
    return s;
  }();
  size_t pos = (i * 4999) % str.size();
  size_t size = std::min(str.size() - pos, (i * 13) % (value_avg_size * 2));
  std::string value = get_key(i);
  value.append("#");
  value.append(str.data() + pos, str.data() + pos + size);
  value.append("#");
  return value;
}

void get_options(int argc, const char *argv[], rocksdb::Options &options,
                 rocksdb::BlockBasedTableOptions &bbto,
                 rocksdb::TerarkZipTableOptions &tzto) {

  options.atomic_flush = false;
  options.allow_mmap_reads = true;
  options.max_open_files = 8192;
  options.allow_fallocate = true;
  options.writable_file_max_buffer_size = 1048576;
  options.allow_mmap_writes = false;
  options.allow_concurrent_memtable_write = true;
  options.use_direct_reads = false;
  options.max_background_garbage_collections = 32;
  options.db_write_buffer_size = 0;
  options.WAL_size_limit_MB = 0;
  options.use_aio_reads = false;
  options.max_background_jobs = 32;
  options.max_task_per_thread = 1;
  options.WAL_ttl_seconds = 0;
  options.enable_thread_tracking = true;
  options.error_if_exists = false;
  options.is_fd_close_on_exec = true;
  options.recycle_log_file_num = 0;
  options.max_manifest_file_size = 1073741824;
  options.skip_log_error_on_recovery = false;
  options.skip_stats_update_on_db_open = false;
  options.max_total_wal_size = 0;
  options.new_table_reader_for_compaction_inputs = true;
  options.manual_wal_flush = true;
  options.compaction_readahead_size = 8388608;
  options.random_access_max_buffer_size = 1048576;
  options.create_missing_column_families = false;
  options.wal_bytes_per_sync = 4194304;
  options.use_adaptive_mutex = false;
  options.use_direct_io_for_flush_and_compaction = false;
  options.max_background_compactions = 64;
  options.advise_random_on_open = true;
  options.base_background_compactions = 4;
  options.max_background_flushes = 32;
  options.two_write_queues = true;
  options.table_cache_numshardbits = 6;
  options.keep_log_file_num = 1000;
  options.write_thread_slow_yield_usec = 3;
  options.stats_dump_period_sec = 600;
  options.avoid_flush_during_recovery = false;
  options.log_file_time_to_roll = 0;
  options.delayed_write_rate = 209715200;
  options.manifest_preallocation_size = 4194304;
  options.paranoid_checks = true;
  options.max_log_file_size = 0;
  options.allow_2pc = true;
  options.max_subcompactions = 16;
  options.create_if_missing = true;
  options.enable_pipelined_write = false;
  options.bytes_per_sync = 4194304;
  options.max_manifest_edit_count = 4096;
  options.fail_if_options_file_error = false;
  options.use_fsync = false;
  options.wal_recovery_mode =
      rocksdb::WALRecoveryMode::kTolerateCorruptedTailRecords;
  options.delete_obsolete_files_period_micros = 21600000000;
  options.enable_write_thread_adaptive_yield = false;
  options.avoid_flush_during_shutdown = false;
  options.write_thread_max_yield_usec = 100;
  options.info_log_level = rocksdb::INFO_LEVEL;
  options.max_file_opening_threads = 16;
  options.dump_malloc_stats = false;
  options.allow_mmap_populate = false;
  options.allow_ingest_behind = false;
  options.access_hint_on_compaction_start =
      rocksdb::Options::AccessHint::NORMAL;
  options.preserve_deletes = false;

  // bbto.no_block_cache = true;
  bbto.pin_top_level_index_and_filter = true;
  bbto.pin_l0_filter_and_index_blocks_in_cache = true;
  bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
  bbto.block_cache = rocksdb::NewLRUCache(64ULL << 30, 6, false);

  options.compaction_pri = rocksdb::kMinOverlappingRatio;
  options.bottommost_compression = rocksdb::kZSTD;
  options.bottommost_compression = rocksdb::kZSTD;

  static ComparatorRename c{"RocksDB_SE_v3.10", rocksdb::BytewiseComparator()};
  static ComparatorRename rc{"rev:RocksDB_SE_v3.10",
                             rocksdb::ReverseBytewiseComparator()};
  static TestCompactionFilter filter;

  // options.comparator = &c;
  options.compaction_filter = &filter;

  options.merge_operator.reset(new TestMergeOperator(','));
  options.memtable_insert_with_hint_prefix_extractor = nullptr;
  options.target_file_size_base = file_size_base;
  options.max_sequential_skip_in_iterations = 8;
  options.max_bytes_for_level_base = file_size_base * 4;
  options.bloom_locality = 0;
  options.write_buffer_size = file_size_base;
  options.memtable_huge_page_size = 0;
  options.max_successive_merges = 0;
  options.arena_block_size = file_size_base / 8;
  options.target_file_size_multiplier = 1;
  options.num_levels = 5;
  options.min_write_buffer_number_to_merge = 1;
  options.max_write_buffer_number_to_maintain = 16;
  options.max_write_buffer_number = 16;
  options.compaction_filter = nullptr;
  options.max_compaction_bytes = file_size_base * 2;
  options.memtable_prefix_bloom_size_ratio = 0.000000;
  options.hard_pending_compaction_bytes_limit = 274877906944;
  options.prefix_extractor = nullptr;
  options.force_consistency_checks = false;
  options.paranoid_file_checks = false;
  options.max_bytes_for_level_multiplier = 2.000000;
  options.optimize_filters_for_hits = false;
  options.level_compaction_dynamic_level_bytes = true;
  options.inplace_update_num_locks = 10000;
  options.inplace_update_support = false;
  options.blob_gc_ratio = 0.05;
  options.blob_size = blob_size;
  options.ttl = 0;
  options.soft_pending_compaction_bytes_limit = 68719476736;
  options.enable_lazy_compaction = true;
  options.disable_auto_compactions = false;
  options.report_bg_io_stats = false;
  options.compaction_options_universal.allow_trivial_move = true;

  options.level0_file_num_compaction_trigger = 4;
  options.level0_slowdown_writes_trigger = 40;
  options.level0_stop_writes_trigger = 100;

  options.max_manifest_file_size = 1ull << 30;
  options.max_manifest_edit_count = 8192;

  options.force_consistency_checks = true;
  options.max_file_opening_threads = 8;
  // options.delayed_write_rate = 32ull << 20;
  // options.rate_limiter.reset(
  //    rocksdb::NewGenericRateLimiter(128ull << 20, 1000));

#if WORKER_TEST
  options.compaction_dispatcher.reset(
      new AsyncCompactionDispatcher(argv[0], argv[1]));
#endif

#if TEST_TERARK
  tzto.localTempDir = argv[1];
  tzto.indexNestLevel = 3;
  tzto.checksumLevel = 2;
  tzto.entropyAlgo = rocksdb::TerarkZipTableOptions::kHuffman;
  // tzto.entropyAlgo = rocksdb::TerarkZipTableOptions::kNoEntropy;
  tzto.terarkZipMinLevel = 0;
  tzto.debugLevel = 0;
  tzto.indexNestScale = 8;
  tzto.enableCompressionProbe = true;
  tzto.useSuffixArrayLocalMatch = false;
  tzto.warmUpIndexOnOpen = false;
  tzto.warmUpValueOnOpen = false;
  tzto.disableSecondPassIter = false;
  tzto.indexTempLevel = 0;
  tzto.offsetArrayBlockUnits = 128;
  tzto.sampleRatio = 0.03;
  tzto.indexType = "Mixed_XL_256_32_FL";
  tzto.softZipWorkingMemLimit = 128ull << 30;
  tzto.hardZipWorkingMemLimit = 256ull << 30;
  tzto.smallTaskMemory = 1200 << 20; // 1.2G
  tzto.minDictZipValueSize = 15;
  // tzto.keyPrefixLen = 1; // for IndexID
  tzto.indexCacheRatio = 0.001;
  tzto.singleIndexMinSize = 8ULL << 20;
  tzto.singleIndexMaxSize = 0x1E0000000; // 7.5G
  // tzto.singleIndexMinSize = 768ULL << 10;
  // tzto.singleIndexMaxSize = 2ULL << 29;
  tzto.minPreadLen = -1;
  tzto.cacheShards = 17;       // to reduce lock competition
  tzto.cacheCapacityBytes = 0; // non-zero implies direct io read
  tzto.disableCompressDict = false;
  tzto.optimizeCpuL3Cache = false;
  tzto.forceMetaInMemory = false;
#endif
}

struct SortedRunGroup {
  size_t start;
  size_t count;
  double ratio;
};

double GenSortedRunGroup(const std::vector<double> &sr, size_t group,
                         std::vector<SortedRunGroup> *output_group) {
  auto Q = [](std::vector<double>::const_iterator b,
              std::vector<double>::const_iterator e, size_t g) {
    double S = std::accumulate(b, e, 0.0);
    // sum of [q, q^2, q^3, ... , q^n]
    auto F = [](double q, size_t n) {
      return (std::pow(q, n + 1) - q) / (q - 1);
    };
    // let S = ∑q^i, i in <1..n>, seek q
    double q = std::pow(S, 1.0 / g);
    if (S <= g + 1) {
      q = 1;
    } else {
      // Newton-Raphson method
      for (size_t c = 0; c < 8; ++c) {
        double Fp = q, q_k = q;
        for (size_t k = 2; k <= g; ++k) {
          Fp += k * (q_k *= q);
        }
        q -= (F(q, g) - S) / Fp;
      }
    }
    return q;
  };
  auto &o = *output_group;
  o.resize(group);
  double ret_q = Q(sr.begin(), sr.end(), group);
  size_t sr_size = sr.size();
  size_t g = group;
  double q = ret_q;
  for (size_t i = g - 1; q > 1 && i > 0; --i) {
    size_t e = g - i;
    double new_q = Q(sr.begin(), sr.begin() + sr_size - e, g - e);
    if (new_q < q) {
      for (size_t j = i; j < g; ++j) {
        size_t start = j + sr_size - g;
        o[j].ratio = sr[start];
        o[j].count = 1;
        o[j].start = start;
      }
      sr_size -= e;
      g -= e;
      q = new_q;
    }
  }
  // Standard Deviation pattern matching
  double sr_acc = sr[sr_size - 1];
  double q_acc = std::pow(q, g);
  int q_i = int(g) - 1;
  o[q_i].ratio = sr_acc;
  o[0].start = 0;
  for (int i = int(sr_size) - 2; i >= 0; --i) {
    double new_acc = sr_acc + sr[i];
    if ((i < q_i || sr_acc > q_acc ||
         std::abs(new_acc - q_acc) > std::abs(sr_acc - q_acc)) &&
        q_i > 0) {
      o[q_i].start = i + 1;
      q_acc += std::pow(q, q_i--);
      o[q_i].ratio = 0;
    }
    sr_acc = new_acc;
    o[q_i].ratio += sr[i];
  }
  for (size_t i = 1; i < g; ++i) {
    o[i - 1].count = o[i].start - o[i - 1].start;
  }
  o[g - 1].count = sr_size - o[g - 1].start;
  return ret_q;
}

void test_g() {
  using namespace rocksdb;
  std::vector<SortedRunGroup> o;
  std::vector<double> sr;
  std::mt19937_64 mt;

  sr = {1600, 16, 100, 1, 200, 2, 400, 4, 8000, 80};
  // GenSortedRunGroup(sr, sr.size() - 3, &o);

  std::cout.setf(std::ios::right);
  std::cout << std::setprecision(0);
  std::cout << std::setiosflags(std::ios::fixed);
  for (size_t j = 0; j < 100000000; ++j) {
    double r = GenSortedRunGroup(sr, sr.size() - 1, &o);
    for (auto i : o) {
      std::cout.width(9);
      std::cout << r;
      std::cout << i.ratio;
    }
    std::cout << std::endl;
    sr.resize(o.size() + 1);
    sr[0] = std::uniform_real_distribution<double>(0.5, 1.2)(mt);
    for (size_t i = 0; i < o.size(); ++i) {
      sr[i + 1] = o[i].ratio;
    }
  }

  while (true) {
    size_t c = std::uniform_int_distribution<size_t>(1, 20)(mt);
    sr.resize(c);
    for (double &v : sr) {
      v = std::uniform_real_distribution<double>(0.1, 4)(mt);
    }
    size_t p = std::uniform_int_distribution<size_t>(1, c)(mt);
    GenSortedRunGroup(sr, p, &o);
    printf("");
  }
}

int main(int argc, const char *argv[], const char *env[]) {
  using namespace rocksdb;
#define ASSERT_OK(a) assert((a).ok())
#define ASSERT_NOK(a) assert(!(a).ok())
#define ASSERT_EQ(a, b) assert((a) == (b))
#define ASSERT_TRUE(a) assert(a)
#define ASSERT_FALSE(a) assert(!(a))

  struct MallocCustomizeBuffer {
    struct Handle {
      void *ptr;
      size_t size;
      Handle() : ptr(nullptr), size(0) {}
      ~Handle() {
        if (ptr != nullptr) {
          ::free(ptr);
        }
      }
    };
    static void *uninitialized_resize(void *handle, size_t size) {
      auto mem = reinterpret_cast<Handle *>(handle);
      if (size > 0) {
        auto new_ptr = realloc(mem->ptr, size);
        if (new_ptr == nullptr) {
          return nullptr;
        }
        mem->ptr = new_ptr;
      }
      mem->size = size;
      return mem->ptr;
    }
    Handle handle;
    LazyBufferCustomizeBuffer get() { return {&handle, &uninitialized_resize}; }
  };

  struct StringCustomizeBuffer {
    using Handle = std::string;
    static void *uninitialized_resize(void *handle, size_t size) {
      auto string_ptr = reinterpret_cast<Handle *>(handle);
      try {
        string_ptr->resize(size);
      } catch (...) {
        return nullptr;
      }
      return (void *)string_ptr->data();
    };
    Handle handle;
    LazyBufferCustomizeBuffer get() { return {&handle, &uninitialized_resize}; }
  };

  std::string string;
  LazyBuffer buffer(&string);
  buffer.trans_to_string()->assign("abc");
  std::move(buffer).dump(&string);

  fprintf(stderr, "OK\n");
  // }

  //{
  //  terark::FileStream f;
  //  std::string name = terark::fstring(argv[1]) + "/test_file";
  //  f.open(name, "w");
  //  for (size_t i = 0; i < 1000; ++i) {
  //    f.ensureWrite(&i, 8);
  //  }
  //  f.close();
  //  terark::MmapWholeFile mmap(name);
  //  f.open(name, "a");
  //  for (size_t i = 0; i < 1000; ++i) {
  //    f.ensureWrite(&i, 8);
  //  }
  //  f.close();
  //}

  // test_g();
  // terark::DictZipBlobStore::Options op;
  // op.checksumLevel = 3;
  // op.compressGlobalDict = true;
  // op.embeddedDict = true;
  // op.offsetArrayBlockUnits = 128;
  // auto zb = terark::DictZipBlobStore::createZipBuilder(op);
  // zb->addSample("F**k");
  // zb->finishSample();
  // zb->prepareDict();
  // zb->prepare(26039, R"(C:\osc\rocksdb_test\testdb\test.zbs)");
  // for (size_t i = 0; i < 26039; ++i) {
  //  zb->addRecord("");
  //}
  // zb->finish(terark::DictZipBlobStore::ZipBuilder::FinishFreeDict);
  // delete zb;
  // auto bs =
  // terark::BlobStore::load_from_mmap(R"(C:\osc\rocksdb_test\testdb\test.zbs)",
  // false); auto r1 = bs->get_record(1); delete bs; for (; ; ) {
  //    terark::SortedUintVec suv;
  //    std::unique_ptr<terark::SortedUintVec::Builder>
  //        //builder(terark::SortedUintVec::createBuilder(true, 128,
  //        R"(/tmp/test.zo)"));
  //        builder(terark::SortedUintVec::createBuilder(true, 128));
  //    size_t l = rand() % 32768;
  //    printf("%zd - ", l);
  //    for (size_t j = 0; j < l; ++j) {
  //        builder->push_back(0);
  //    }
  //    //size_t ms = builder->finish(nullptr).mem_size;
  //    size_t ms = builder->finish(&suv).mem_size;
  //    for (size_t j = 0; j < l; ++j) {
  //        if (suv[j] != 0) {
  //            std::abort();
  //        }
  //    }
  //    printf("%zd\n", ms);
  //    fflush(stdout);
  //}

  if (argc < 2) {
    return -1;
  }
  auto statistics = rocksdb::CreateDBStatistics();
  std::mt19937_64 mt;
  rocksdb::Options options;
  rocksdb::BlockBasedTableOptions bbto;
  rocksdb::TerarkZipTableOptions tzto;
  get_options(argc, argv, options, bbto, tzto);
  options.statistics = statistics;

  if (argc == 3) {
#if WORKER_TEST
    AsyncCompactionDispatcher::AsyncWorker worker(options, argv[1],
                                                  terark::lcast(argv[2]));
    worker.RegistComparator(options.comparator);
    worker.RegistTableFactory(
        "TerarkZipTable",
        [&](std::shared_ptr<rocksdb::TableFactory> *ptr, const std::string &) {
          ptr->reset(rocksdb::NewTerarkZipTableFactory(tzto, nullptr));
          return rocksdb::Status::OK();
        });
    worker.RegistMergeOperator(
        [](std::shared_ptr<rocksdb::MergeOperator> *ptr) {
          ptr->reset(new TestMergeOperator(','));
          return rocksdb::Status::OK();
        });

    std::string input;
    std::getline(std::cin, input);
    // std::getline(std::fstream("data/Terark-00000000.param", std::ios::in),
    // input);
    printf("%s\n", worker.DoCompaction(input).c_str());
#endif
    return 0;
  }

#if TEST_TERARK
  rocksdb::TerarkZipDeleteTempFiles(tzto.localTempDir);
#endif

#if TEST_TERARK
  if (0) {
    for (int i = 3; i < argc; ++i) {
      terark::FileStream index_file(argv[i], "rb");
      size_t index_size = index_file.fsize();
      unsigned char *data = new unsigned char[index_size];
      index_file.ensureRead(data, index_size);
      auto index =
          terark::TerarkIndex::LoadMemory(terark::fstring(data, index_size));
      std::unique_ptr<terark::TerarkIndex::Iterator> iter(index->NewIterator());
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        printf("%s\n", iter->key().str().c_str());
      }
      iter.reset();
      index->DumpKeys(
          [](terark::fstring c, terark::fstring p, terark::fstring s) {
            for (auto v : c) {
              printf("%02x", (unsigned char)v);
            }
            printf(" ");
            for (auto v : p) {
              printf("%02x", (unsigned char)v);
            }
            printf(" ");
            for (auto v : s) {
              printf("%02x", (unsigned char)v);
            }
            printf("\n");
          });
      index.reset();
      delete[] data;
    }
  }
  if (0) {
    terark::fstring base_name = "/Users/zhaoming/Documents/Work/Terark-RztCiT";
    terark::FileStream index_file(base_name + ".index", "rb");
    size_t index_size = index_file.fsize();
    unsigned char *data = new unsigned char[index_size];
    index_file.ensureRead(data, index_size);
    auto index =
        terark::TerarkIndex::LoadMemory(terark::fstring(data, index_size));
    index->DumpKeys(
        [](terark::fstring c, terark::fstring p, terark::fstring s) {
          for (auto v : c) {
            printf("%02x", (unsigned char)v);
          }
          printf(" ");
          for (auto v : p) {
            printf("%02x", (unsigned char)v);
          }
          printf(" ");
          for (auto v : s) {
            printf("%02x", (unsigned char)v);
          }
          printf("\n");
        });
    assert(index->NeedsReorder());
    terark::UintVecMin0 newToOld(index->NumKeys(), index->NumKeys() - 1);
    index->GetOrderMap(newToOld);
    terark::ZReorderMap::Builder builder(index->NumKeys(), 1,
                                         base_name + ".reorder", "wb");
    for (size_t n = 0, c = index->NumKeys(); n < c; ++n) {
      builder.push_back(newToOld[n]);
    }
    builder.finish();
    terark::ZReorderMap reorder(base_name + ".reorder");
    terark::valvec<terark::byte_t> output;
    std::string reorder_tmp = base_name + ".reorder-tmp";
    index->Reorder(
        reorder,
        [&output](const void *d, size_t l) {
          output.append((terark::byte_t *)d, l);
        },
        reorder_tmp);
    size_t new_index_size = output.size();
    delete[] data;
  }
  if (0) {
    terark::fstring base_name = "/Users/zhaoming/Documents/Work/Terark-RztCiT";
    terark::FileStream index_file(base_name + ".index", "rb");
    size_t index_size = index_file.fsize();
    unsigned char *data = new unsigned char[index_size];
    index_file.ensureRead(data, index_size);
    auto index =
        terark::TerarkIndex::LoadMemory(terark::fstring(data, index_size));
    index->DumpKeys(
        [](terark::fstring c, terark::fstring p, terark::fstring s) {
          for (auto v : c) {
            printf("%02x", (unsigned char)v);
          }
          printf(" ");
          for (auto v : p) {
            printf("%02x", (unsigned char)v);
          }
          printf(" ");
          for (auto v : s) {
            printf("%02x", (unsigned char)v);
          }
          printf("\n");
        });
    assert(index->NeedsReorder());
    terark::UintVecMin0 newToOld(index->NumKeys(), index->NumKeys() - 1);
    index->GetOrderMap(newToOld);
    terark::ZReorderMap::Builder builder(index->NumKeys(), 1,
                                         base_name + ".reorder", "wb");
    for (size_t n = 0, c = index->NumKeys(); n < c; ++n) {
      builder.push_back(newToOld[n]);
    }
    builder.finish();
    terark::ZReorderMap reorder(base_name + ".reorder");
    terark::valvec<terark::byte_t> output;
    std::string reorder_tmp = base_name + ".reorder-tmp";
    index->Reorder(
        reorder,
        [&output](const void *d, size_t l) {
          output.append((terark::byte_t *)d, l);
        },
        reorder_tmp);
    size_t new_index_size = output.size();
    delete[] data;
  }

//  if (0) {
//    terark::fstring dict_file_name =
//    "/Users/zhaoming/Downloads/QQ_V6.5.3.dmg"; terark::FileStream
//    dict_file(dict_file_name, "rb"); int dict_size = (int)dict_file.fsize();
//    unsigned char* dict_data = new unsigned char[dict_size];
//    dict_file.ensureRead(dict_data, dict_size);
//    int* sa = new int[dict_size];
//    sufarr_inducedsort(dict_data, sa, dict_size);
//    terark::TerarkIndex::TerarkIndexDebugBuilder tidb;
//    tidb.Init(dict_size);
//    size_t lastSamePrefix = 0;
//    terark::fstring last = terark::fstring(dict_data + sa[0], dict_size -
//    sa[0]); for (size_t i = 1; i < dict_size; ++i) {
//      terark::fstring cur = terark::fstring(dict_data + sa[i], dict_size -
//      sa[i]); size_t samePrefix = commonPrefixLen(cur, last); size_t
//      maxSamePrefix = std::max(samePrefix, lastSamePrefix) + 1; lastSamePrefix
//      = samePrefix; tidb.Add(last.substr(0, std::min(maxSamePrefix,
//      last.size()))); last = cur;
//    }
//    tidb.Add(last.substr(0, std::min(lastSamePrefix + 1, last.size())));
//    delete[] sa;
//    delete[] dict_data;
//    terark::TerarkIndex::KeyStat ks;
//    std::unique_ptr<terark::TerarkKeyReader> reader(tidb.Finish(&ks));
//    auto index = terark::TerarkIndex::Factory::Build(reader.get(), tzto, ks,
//    nullptr); terark::valvec<terark::byte_t> data;
//    index->SaveMmap([&data](const void* d, size_t l){
//      data.append((terark::byte_t*)d, l);
//    });
//    delete index;
//    index = terark::TerarkIndex::LoadMemory(data).release();
//    char b[512];
//    printf("%s\n", index->Info(b, sizeof b));
//    delete index;
//  }
#endif

  std::vector<rocksdb::ColumnFamilyDescriptor> cfDescriptors;
  // options.disable_auto_compactions = true;
  {
    ;
    options.compaction_style = rocksdb::kCompactionStyleLevel;
    options.write_buffer_size = file_size_base;
    options.enable_lazy_compaction = false;
    options.blob_gc_ratio = 0.1;
#if TEST_TERARK
    // options.memtable_factory.reset(rocksdb::NewPatriciaTrieRepFactory());
    // options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));
    options.table_factory.reset(
        rocksdb::NewTerarkZipTableFactory(tzto, options.table_factory));
#else
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));
#endif
    cfDescriptors.emplace_back(rocksdb::kDefaultColumnFamilyName, options);

    options.compaction_style = rocksdb::kCompactionStyleUniversal;
    options.write_buffer_size = size_t(file_size_base * 1.1);
    options.enable_lazy_compaction = true;
#if WORKER_TEST
    options.compaction_dispatcher.reset();
#endif
    // rocksdb::PlainTableOptions pto;
    // pto.hash_table_ratio = 0;
    // options.table_factory.reset(rocksdb::NewPlainTableFactory(pto));
    // options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));
    cfDescriptors.emplace_back("universal", options);
    // options.comparator = &rc;
    // cfDescriptors.emplace_back("rev:cf1", options);

    options.compaction_style = rocksdb::kCompactionStyleLevel;
    options.write_buffer_size = size_t(file_size_base / 1.1);
    options.enable_lazy_compaction = true;
#if WORKER_TEST
    options.compaction_dispatcher.reset();
#endif
    // rocksdb::PlainTableOptions pto;
    // pto.hash_table_ratio = 0;
    // options.table_factory.reset(rocksdb::NewPlainTableFactory(pto));
    // options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));
    cfDescriptors.emplace_back("level", options);
    // options.comparator = &rc;
    // cfDescriptors.emplace_back("rev:cf1", options);
  }

  // options.table_properties_collector_factories.emplace_back(new
  // MongoRocksOplogPropertiesCollectorFactory);

  options.env->SetBackgroundThreads(options.max_background_compactions,
                                    rocksdb::Env::LOW);
  options.env->SetBackgroundThreads(options.max_background_flushes,
                                    rocksdb::Env::HIGH);

  options.create_if_missing = true;

  rocksdb::DBOptions dbo = options;
  dbo.create_missing_column_families = true;
#if ASYNC_TEST
  dbo.use_aio_reads = true;
#endif
  // dbo.max_log_file_size = 2ull << 20;
  rocksdb::Status s;
  std::vector<rocksdb::ColumnFamilyHandle *> hs;

  if (0) {
    rocksdb::EnvOptions eo(options);
    rocksdb::InternalKeyComparator ikc(options.comparator);
    rocksdb::ImmutableDBOptions idbo(options);
    rocksdb::ImmutableCFOptions ioptions(idbo, cfDescriptors[0].options);
    rocksdb::TableReaderOptions tro(ioptions, nullptr, eo, ikc);
    rocksdb::MutableCFOptions mutable_cf_options(cfDescriptors[0].options);
#if 0
    const char *dump =
        R"(
DEBUG: 1st pass => '00000001746573742E7231' seq:7, type:1 / 000100000000000001000000000000000101
DEBUG: 1st pass => '000000020000000000000100' seq:5, type:1 / 000601000D000000000000000000000000
DEBUG: 1st pass => '000000020000000000000101' seq:6, type:1 / 000602000D000000000000000000000000
DEBUG: 1st pass => '0000000300000000' seq:2, type:1 / 000100000000
DEBUG: 1st pass => '0000000300000001' seq:1, type:1 / 000100000000
DEBUG: 1st pass => '00000007' seq:4, type:1 / 000100000101
)";

    std::vector<std::unique_ptr<rocksdb::IntTblPropCollectorFactory>>
        int_tbl_prop_collector_factories;
    std::string sst_file_path = std::string(argv[1]) + "/temp.sst";

    rocksdb::TableBuilderOptions table_builder_options(
        ioptions, mutable_cf_options, ikc, &int_tbl_prop_collector_factories,
        rocksdb::kNoCompression, {}, nullptr, true,
#if TEST_TERARK
        false,
#endif
        cfDescriptors[0].name, -1, 0);

    std::unique_ptr<rocksdb::WritableFile> sst_file;
    options.env->NewWritableFile(sst_file_path, &sst_file, eo);
    std::unique_ptr<rocksdb::WritableFileWriter> file_writter(
        new rocksdb::WritableFileWriter(std::move(sst_file), sst_file_path, eo,
                                        nullptr, idbo.listeners));
    std::unique_ptr<rocksdb::TableBuilder> builder(
        ioptions.table_factory->NewTableBuilder(table_builder_options, 0,
                                                file_writter.get()));
    rocksdb::InternalKey ik;
    for (auto str : make_split(string_ref<>(dump), '\n')) {
      auto sr = string_ref<>(str);
      if (sr.empty()) {
        continue;
      }
      auto user_key = make_split(sr, '\'')[1];
      auto sequence = make_split_any_of(sr, ",:")[2].to_value<uint64_t>();
          auto type = (rocksdb::ValueType)make_split_any_of(sr, ":")[9].to_value<uint8_t>(); auto value = make_split(sr, ' ')[8];
          std::string key_buffer, value_buffer;
          key_buffer.resize(user_key.size() / 2);
          for (size_t i = 0; i < user_key.size(); i += 2) {
            char x[3] = {user_key[i], user_key[i + 1], '\0'};
            key_buffer[i / 2] = std::stoi(x, nullptr, 16);
          }
          value_buffer.resize(value.size() / 2);
          for (size_t i = 0; i < value.size(); i += 2) {
            char x[3] = {value[i], value[i + 1], '\0'};
            value_buffer[i / 2] = std::stoi(x, nullptr, 16);
          }
          ik.Set(key_buffer, sequence, type);
          builder->Add(ik.Encode(), rocksdb::LazySlice(value_buffer));
          // builder->Add(ik.Encode(), value_buffer);
    }
    s = builder->Finish(nullptr);
    // s = builder->Finish();
    builder.reset();
    file_writter.reset();
#endif

    auto proc = [&](std::string file_name) {
      std::cout << "# open file " + file_name + " ...\n";
      std::unique_ptr<rocksdb::RandomAccessFile> file;
      uint64_t file_size = terark::FileStream(file_name, "rb").fsize();
      ioptions.env->NewRandomAccessFile(file_name, &file, eo);
      std::unique_ptr<rocksdb::RandomAccessFileReader> file_reader(
          new rocksdb::RandomAccessFileReader(std::move(file), file_name,
                                              options.env));
      std::unique_ptr<rocksdb::TableReader> reader;
      auto s = ioptions.table_factory->NewTableReader(
          tro, std::move(file_reader), file_size, &reader, false);
      auto it = reader->NewIterator(rocksdb::ReadOptions(), nullptr);
      rocksdb::FileMetaData m;
      m.fd.smallest_seqno = size_t(-1);
      m.fd.largest_seqno = 0;
      rocksdb::ParsedInternalKey pik;
      std::string save;

      auto tp = reader->GetTableProperties();

      // for (it->SeekToFirst(); it->Valid(); it->Next()) {
      //  auto value = it->value();
      //  s = value.fetch();
      //  assert(s.ok());
      //  rocksdb::MapSstElement e;
      //  assert(e.Decode(it->key(), value.slice()));
      //  printf("S: %s\nL: %s\n", e.smallest_key.ToString(true).c_str(),
      //         e.largest_key.ToString(true).c_str());
      //  for (auto &d : e.link) {
      //    printf("  F: %zd C: %zd\n", d.file_number, d.size);
      //  }
      //}

      // rocksdb::InternalKey ikey;
      // ikey.Set("FFF9900000000", 0, rocksdb::kTypeValue);
      // size_t as = reader->ApproximateOffsetOf(ikey.Encode());
      // ikey.Set("FFFFF", 0, rocksdb::kTypeValue);
      // size_t ae = reader->ApproximateOffsetOf(ikey.Encode());

      auto cmp = [c = options.comparator](const std::string &a,
                                          const std::string &b) {
        int r =
            c->Compare(rocksdb::ExtractUserKey(a), rocksdb::ExtractUserKey(b));
        if (r == 0) {
          const uint64_t anum = rocksdb::DecodeFixed64(a.data() + a.size() - 8);
          const uint64_t bnum = rocksdb::DecodeFixed64(b.data() + b.size() - 8);
          if (anum > bnum) {
            r = -1;
          } else if (anum < bnum) {
            r = +1;
          }
        }
        return r < 0;
      };
      std::set<std::string, decltype(cmp)> data(cmp);
      // rocksdb::InternalKey seek_key;
      // seek_key.Set(rocksdb::Slice("\0\0\0\1", 4), 0,
      // rocksdb::kValueTypeForSeek); for (it->Seek(seek_key.Encode());
      // it->Valid(); it->Next()) {
      //  auto key = it->key();
      //  auto val = it->value();
      //  if (key == val) {
      //    printf("");
      //  }
      //}
      //{
      //  std::string
      //  err_key("\0\0\x03,\0\0\0\0\0\x85\xe0P\x10\xcf\xb5\xfc,\0\0\0",
      //  20);
      //
      //  rocksdb::LazyBuffer buffer;
      //  rocksdb::GetContext ctx(options.comparator,
      //  options.merge_operator.get(), nullptr, nullptr,
      //  rocksdb::GetContext::kNotFound,
      //                          rocksdb::ExtractUserKey(err_key),
      //                          &buffer, nullptr, nullptr, nullptr,
      //                          nullptr, nullptr, nullptr, nullptr);
      //  s = reader->Get(rocksdb::ReadOptions(), err_key, &ctx, nullptr,
      //  true);
      //
      //}
      // it->SeekToLast();
      // std::string ikey = it->key().ToString();
      // rocksdb::MapSstElement e;
      // e.Decode(ikey, it->value().slice());
      // it->SeekForPrev(ikey);
      // size_t i = 0;
      // for (it->SeekToFirst(); it->Valid(); it->Next()) {
      //  rocksdb::LazyBuffer buffer;
      //  rocksdb::GetContext ctx(options.comparator,
      //  options.merge_operator.get(), nullptr, nullptr,
      //  rocksdb::GetContext::kNotFound,
      //                          rocksdb::ExtractUserKey(it->key()),
      //                          &buffer, nullptr, nullptr, nullptr,
      //                          nullptr, nullptr, nullptr, nullptr);
      //  s = reader->Get(rocksdb::ReadOptions(), it->key(), &ctx,
      //  nullptr, true); assert(s.ok()); assert(ctx.State() ==
      //  rocksdb::GetContext::kFound); assert(buffer.fetch().ok()); auto
      //  value = it->value(); assert(value.fetch().ok());
      //  assert(buffer.slice() == value.slice());
      //  ++i;
      //}
      // for (it->SeekToFirst(); it->Valid(); it->Next()) {
      //  auto value = it->value();
      //  s = value.fetch();
      //  assert(s.ok());
      //  rocksdb::ParsedInternalKey ikey;
      //  rocksdb::ParseInternalKey(it->key(), &ikey);
      //
      //  printf("DEBUG: 1st pass => %s / %s \n",
      //          ikey.DebugString(true).c_str(),
      //          value.ToString(true).c_str());
      //  data.emplace(it->key().ToString());
      //}
      // auto test = [it, &data](const std::string &k){
      //  auto find = data.lower_bound(k);
      //  it->Seek(k);
      //  if (find == data.end()) {
      //    assert(!it->Valid());
      //  } else {
      //    assert(it->key() == *find);
      //  }
      //};
      // for (auto k : data) {
      //  assert(k.size() >= 8);
      //  for (size_t i = 1; i + 8 <= k.size(); ++i) {
      //    std::string k2 = k.substr(0, i) + k.substr(k.size() - 8, 8);
      //    test(k2);
      //    k2.end()[-9] += 1;
      //    test(k2);
      //    k2.end()[-9] -= 2;
      //    test(k2);
      //  }
      //  test(k.substr(0, k.size() - 8) + '\0' + k.substr(k.size() - 8,
      //  8)); k.end()[-1] += 1; test(k); k.end()[-1] -= 2; test(k);
      //}
      delete it;
    };
    // proc("023366.sst");
    // proc("023372.sst");
    // proc(std::string(argv[1]) + "/000973.sst");
    std::vector<std::thread> tv;
    for (size_t i = 3; i < argc; ++i) {
      // proc(argv[i]);
      tv.emplace_back(proc, argv[i]);
    }
    for (auto &t : tv) {
      t.join();
    }
  }

  // rocksdb::DestroyDB(argv[1], options);
  // rocksdb::TransactionDBOptions tdbo;
  // tdbo.write_policy = rocksdb::WRITE_COMMITTED;
  // rocksdb::TransactionDB *db;
  rocksdb::DB *db;
#if READ_ONLY
  // s = rocksdb::TransactionDB::Open(dbo, tdbo, argv[1], cfDescriptors, &hs,
  // &db);
  s = rocksdb::DB::OpenForReadOnly(dbo, argv[1], cfDescriptors, &hs, &db);
#else
  // s = rocksdb::TransactionDB::Open(dbo, tdbo, argv[1], cfDescriptors, &hs,
  // &db);
  dbo.create_if_missing = true;
  s = rocksdb::DB::Open(dbo, argv[1], cfDescriptors, &hs, &db);
#endif
  if (!s.ok()) {
    fprintf(stderr, "%s\n", s.getState());
    return -1;
  }

  // std::this_thread::sleep_for(std::chrono::hours(24));

  rocksdb::WriteOptions wo;
  rocksdb::FlushOptions fo;
  rocksdb::CompactRangeOptions co;

  // std::random_device rd;
  // mt.seed(rd());
  uint64_t stop = 1000000000;

  //  char key_buf[] = {
  //  "2A66EE89FB12FEB7B41A6A9E0D9E399459E36813D1CB1E2A45B721466A89FBB8" };
  //  std::string vvv;
  //  s = db->Get(ro, h0, key_buf, &vvv);

  // if (0) {
  //  std::string file_name = "/Users/zhaoming/Documents/Work/000030.sst";
  //  rocksdb::ImmutableCFOptions icfo(rocksdb::ImmutableDBOptions(options),
  //                                   cfDescriptors[0].options);
  //  rocksdb::EnvOptions eo(options);
  //  rocksdb::InternalKeyComparator ikc(options.comparator);
  //  rocksdb::TableReaderOptions tro(icfo, nullptr, eo, ikc);
  //  std::unique_ptr<rocksdb::RandomAccessFile> file;
  //  uint64_t file_size = terark::FileStream(file_name, "rb").fsize();
  //  icfo.env->NewRandomAccessFile(file_name, &file, eo);
  //  std::unique_ptr<rocksdb::RandomAccessFileReader> file_reader(
  //      new rocksdb::RandomAccessFileReader(std::move(file), file_name,
  //                                          options.env));
  //  std::unique_ptr<rocksdb::TableReader> reader;
  //  auto s = icfo.table_factory->NewTableReader(tro, std::move(file_reader),
  //                                              file_size, &reader, false);
  //  auto it = reader->NewIterator(rocksdb::ReadOptions(), nullptr);
  //
  //  rocksdb::ReadOptions ro;
  //  rocksdb::WriteBatch b;
  //  size_t c = 0;
  //  uint64_t number = VALUE_OF_BYTE_SWAP_IF_LITTLE_ENDIAN(
  //      rocksdb::TableFileNameToNumber(file_name));
  //  std::string key_buffer((char *)&number, sizeof number);
  //  std::string value_buffer;
  //  for (it->SeekToFirst(); it->Valid(); it->Next()) {
  //    auto k = it->key();
  //    key_buffer.resize(sizeof number);
  //    key_buffer.append(k.data(), k.size());
  //    auto value = it->value();
  //    s = value.fetch();
  //    assert(s.ok());
  //    b.Put(h0, key_buffer, value.slice());
  //    b.Put(h1, key_buffer, value.slice());
  //    if (++c > 100) {
  //      s = db->Write(wo, b.GetWriteBatch());
  //      b.Clear();
  //      c = 0;
  //    }
  //  }
  //  if (c > 0) {
  //    s = db->Write(wo, b.GetWriteBatch());
  //    b.Clear();
  //  }
  //  db->Flush(fo, h0);
  //  db->Flush(fo, h1);
  //  for (it->SeekToFirst(); it->Valid(); it->Next()) {
  //    auto value = it->value();
  //    // s = value.inplace_decode();
  //    // assert(s.ok());
  //    key_buffer.resize(sizeof number);
  //    key_buffer.append(it->key().data(), it->key().size());
  //    s0 = db->Get(ro, h0, key_buffer, &value_buffer);
  //    if (s0.IsNotFound() || value_buffer != value.slice()) {
  //      std::cerr << "# mismatch data in file " + file_name + " ...\n";
  //    }
  //    s1 = db->Get(ro, h1, key_buffer, &value_buffer);
  //    if (s1.IsNotFound() || value_buffer != value.slice()) {
  //      std::cerr << "# mismatch data in file " + file_name + " ...\n";
  //    }
  //  }
  //}

  //  for (count = 1000000; count < 1000000000; ++count)
  //  {
  //    std::uniform_int_distribution<uint64_t> uid(10000, count);
  //    key = count % 2 ? get_rnd_key(uid(mt)) : get_key(uid(mt));
  //    s0 = db->Get(ro, h0, key, &value_out0);
  //    s1 = db->Get(ro, h1, key, &value_out1);
  //    if (s0.IsNotFound()) {
  //      assert(s1.IsNotFound());
  //    }
  //    else {
  //      assert(!s1.IsNotFound());
  //      assert(value_out0 == value_out1);
  //    }
  //    iter0->Seek(key);
  //    iter1->Seek(key);
  //    if (iter0->Valid()) {
  //      assert(iter0->key() == iter1->key());
  //      assert(iter0->value() == iter1->value());
  //    }
  //    else {
  //      assert(!iter1->Valid());
  //    }
  //  }
  //  return -1;
  // auto start = rocksdb::Slice(key_buf + 8, 4);
  // std::string limit_storage = start.ToString();
  //++limit_storage[3];
  // auto limit = rocksdb::Slice(limit_storage);
  // rocksdb::RangePtr r(&start, &limit, true, false);
  // rocksdb::DeleteFilesInRanges(db, h0, &r, 1);

  // iter0->Seek(rocksdb::Slice(key_buf + 8, 4));
  // auto k = iter0->key();
  // key = k.ToString();
  // for (iter0->SeekToFirst(); iter0->Valid(); iter0->Next()) {
  //  k = iter0->key();
  //  key = k.ToString();
  //  assert(!k.starts_with(rocksdb::Slice(key_buf + 8, 4)));
  //}
  // for (iter0->SeekToLast(); iter0->Valid(); iter0->Prev()) {
  //  k = iter0->key();
  //  key = k.ToString();
  //  assert(!k.starts_with(rocksdb::Slice(key_buf + 8, 4)));
  //}

  std::vector<std::shared_ptr<const rocksdb::Snapshot>> snapshot;
  std::mutex snapshot_mutex;

  co.exclusive_manual_compaction = false;
  // std::this_thread::sleep_for(std::chrono::hours(24));
  // db->CompactRange(co, h0, nullptr, nullptr);
  // db->CompactRange(co, h1, nullptr, nullptr);
  // std::uniform_int_distribution<uint64_t> uid(0, stop);

  struct ReadContext {
    rocksdb::ReadOptions ro;
    uint64_t seqno;
#if ITER_TEST
    std::vector<std::unique_ptr<rocksdb::Iterator>> iter;
#endif
    size_t count = 0;
    std::string key;
    std::vector<rocksdb::Status> ss;
    std::vector<rocksdb::Slice> keys;
    std::vector<std::string> values;
#if ASYNC_TEST
    std::vector<
        boost::fibers::future<std::tuple<Status, std::string, std::string>>>
        futures;
    std::vector<std::string> async_values;
    std::vector<Status> async_status;
#endif
  };
  std::atomic<bool> has_error{false};

  auto check_assert = [db, &hs, &has_error](ReadContext *ctx, bool assert_value,
                                            const char *err) {
    auto &ro = ctx->ro;
    auto &seqno = ctx->seqno;
#if ITER_TEST
    auto &iter = ctx->iter;
#endif
#if ASYNC_TEST
    auto &futures = ctx->futures;
    auto &async_values = ctx->async_values;
    auto &async_status = ctx->async_status;
#endif
    auto &count = ctx->count;
    auto &key = ctx->key;
    auto &ss = ctx->ss;
    auto &keys = ctx->keys;
    auto &values = ctx->values;
    if (!assert_value) {
      fprintf(stderr, "count = %zd, seqno = %" PRIu64 ", key = %s, err = %s \n",
              count, seqno, key.c_str(), err);
      fprintf(stderr, "Get:\n");
      for (size_t i = 0; i < hs.size(); ++i) {
        fprintf(stderr, "s%zd = %s, v%zd = %s\n", i,
                ss.size() > i ? ss[i].ToString().c_str() : "null", i,
                values.size() > i ? values[i].c_str() : "null");
      }
#if ITER_TEST
      fprintf(stderr, "Iter:\n");
      for (size_t i = 0; i < hs.size(); ++i) {
        fprintf(stderr, "s%zd = %s, k%zd = %s, v%zd = %s\n", i,
                iter.size() > i ? iter[i]->status().ToString().c_str() : "null",
                i,
                iter.size() > i && iter[i]->Valid()
                    ? iter[i]->key().ToString().c_str()
                    : "Invalid",
                i,
                iter.size() > i && iter[i]->Valid()
                    ? iter[i]->value().ToString().c_str()
                    : "Invalid");
      }
#endif

#if ASYNC_TEST
      fprintf(stderr, "Async:\n");
      for (size_t i = 0; i < hs.size(); ++i) {
        const auto &tmp_tuple = futures[i].get();
        fprintf(stderr, "Get : s%zd = %s, k%zd = %s, v%zd = %s\n", i,
                ss.size() > i ? ss[i].ToString().c_str() : "null", i,
                key.c_str(), i, values.size() > i ? values[i].c_str() : "null");
        fprintf(stderr, "GetFuture : s%zd = %s, k%zd = %s, v%zd = %s\n", i,
                std::get<0>(tmp_tuple).ToString().c_str(), i,
                std::get<1>(tmp_tuple).c_str(), i,
                std::get<2>(tmp_tuple).c_str());
        fprintf(stderr, "GetAsync : s%zd = %s, k%zd = %s, v%zd = %s\n", i,
                async_status.size() > i ? async_status[i].ToString().c_str()
                                        : "null",
                i, key.c_str(), i,
                async_values.size() > i ? async_values[i].c_str() : "null");
      }
#endif

      has_error = true;
    }
    assert(assert_value);
    for (; !assert_value;) {
#if GET_TEST
      for (auto &k : keys) {
        k = key;
      }
      ss = db->MultiGet(ro, hs, keys, &values);
      if (IsAny(ss, [](auto &s) { return s.IsNotFound(); })) {
        assert(IsAll(ss, [](auto &s) { return s.IsNotFound(); }));
      } else {
        assert(IsAny(ss, [](auto &s) { return s.ok(); }));
        assert(IsSame(values, [](auto &l, auto &r) { return l == r; }));
      }
#endif
#if ITER_TEST
      for (auto &it : iter) {
        it->Seek(key);
      }
      if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
        assert(IsAll(iter, [](auto &it) { return !it->Valid(); }));
        assert(IsAll(iter, [](auto &it) { return it->status().ok(); }));
      } else {
        assert(IsSame(iter,
                      [](auto &l, auto &r) { return l->key() == r->key(); }));
        assert(IsSame(
            iter, [](auto &l, auto &r) { return l->value() == r->value(); }));

        for (auto &it : iter) {
          it->Next();
        }
        if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
          assert(IsAll(iter, [](auto &it) { return !it->Valid(); }));
          assert(IsAll(iter, [](auto &it) { return it->status().ok(); }));
        } else {
          assert(IsSame(iter,
                        [](auto &l, auto &r) { return l->key() == r->key(); }));
          assert(IsSame(
              iter, [](auto &l, auto &r) { return l->value() == r->value(); }));

          for (auto &it : iter) {
            it->Prev();
          }
          if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
            assert(IsAll(iter, [](auto &it) { return !it->Valid(); }));
            assert(IsAll(iter, [](auto &it) { return it->status().ok(); }));
          } else {
            assert(IsSame(
                iter, [](auto &l, auto &r) { return l->key() == r->key(); }));
            assert(IsSame(iter, [](auto &l, auto &r) {
              return l->value() == r->value();
            }));
          }
        }
      }
      for (auto &it : iter) {
        it->Seek(key);
      }
      if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
        assert(IsAll(iter, [](auto &it) { return !it->Valid(); }));
        assert(IsAll(iter, [](auto &it) { return it->status().ok(); }));
      } else {
        assert(IsSame(iter,
                      [](auto &l, auto &r) { return l->key() == r->key(); }));
        assert(IsSame(
            iter, [](auto &l, auto &r) { return l->value() == r->value(); }));

        for (auto &it : iter) {
          it->Prev();
        }
        if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
          assert(IsAll(iter, [](auto &it) { return !it->Valid(); }));
          assert(IsAll(iter, [](auto &it) { return it->status().ok(); }));
        } else {
          assert(IsSame(iter,
                        [](auto &l, auto &r) { return l->key() == r->key(); }));
          assert(IsSame(
              iter, [](auto &l, auto &r) { return l->value() == r->value(); }));

          for (auto &it : iter) {
            it->Next();
          }
          if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
            assert(IsAll(iter, [](auto &it) { return !it->Valid(); }));
            assert(IsAll(iter, [](auto &it) { return it->status().ok(); }));
          } else {
            assert(IsSame(
                iter, [](auto &l, auto &r) { return l->key() == r->key(); }));
            assert(IsSame(iter, [](auto &l, auto &r) {
              return l->value() == r->value();
            }));
          }
        }
      }
      for (auto &it : iter) {
        it->SeekForPrev(key);
      }
      if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
        assert(IsAll(iter, [](auto &it) { return !it->Valid(); }));
        assert(IsAll(iter, [](auto &it) { return it->status().ok(); }));
      } else {
        assert(IsSame(iter,
                      [](auto &l, auto &r) { return l->key() == r->key(); }));
        assert(IsSame(
            iter, [](auto &l, auto &r) { return l->value() == r->value(); }));

        for (auto &it : iter) {
          it->Prev();
        }
        if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
          assert(IsAll(iter, [](auto &it) { return !it->Valid(); }));
          assert(IsAll(iter, [](auto &it) { return it->status().ok(); }));
        } else {
          assert(IsSame(iter,
                        [](auto &l, auto &r) { return l->key() == r->key(); }));
          assert(IsSame(
              iter, [](auto &l, auto &r) { return l->value() == r->value(); }));

          for (auto &it : iter) {
            it->Next();
          }
          if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
            assert(IsAll(iter, [](auto &it) { return !it->Valid(); }));
            assert(IsAll(iter, [](auto &it) { return it->status().ok(); }));
          } else {
            assert(IsSame(
                iter, [](auto &l, auto &r) { return l->key() == r->key(); }));
            assert(IsSame(iter, [](auto &l, auto &r) {
              return l->value() == r->value();
            }));
          }
        }
      }
      for (auto &it : iter) {
        it->SeekForPrev(key);
      }
      if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
        assert(IsAll(iter, [](auto &it) { return !it->Valid(); }));
        assert(IsAll(iter, [](auto &it) { return it->status().ok(); }));
      } else {
        assert(IsSame(iter,
                      [](auto &l, auto &r) { return l->key() == r->key(); }));
        assert(IsSame(
            iter, [](auto &l, auto &r) { return l->value() == r->value(); }));

        for (auto &it : iter) {
          it->Next();
        }
        if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
          assert(IsAll(iter, [](auto &it) { return !it->Valid(); }));
          assert(IsAll(iter, [](auto &it) { return it->status().ok(); }));
        } else {
          assert(IsSame(iter,
                        [](auto &l, auto &r) { return l->key() == r->key(); }));
          assert(IsSame(
              iter, [](auto &l, auto &r) { return l->value() == r->value(); }));

          for (auto &it : iter) {
            it->Prev();
          }
          if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
            assert(IsAll(iter, [](auto &it) { return !it->Valid(); }));
            assert(IsAll(iter, [](auto &it) { return it->status().ok(); }));
          } else {
            assert(IsSame(
                iter, [](auto &l, auto &r) { return l->key() == r->key(); }));
            assert(IsSame(iter, [](auto &l, auto &r) {
              return l->value() == r->value();
            }));
          }
        }
      }
#endif
    }
  };

#if READ_ONLY
  {
    ReadContext ctx;
    ctx.ro.snapshot = db->GetSnapshot();
    ctx.key = READ_ONLY_TEST_KEY;
    // set_snapshot_seqno(ctx.ro.snapshot, ctx.seqno = READ_ONLY_TEST_SEQ);
#if ITER_TEST
    ctx.iter.resize(hs.size());
    for (size_t i = 0; i < hs.size(); ++i) {
      ctx.iter[i] =
          std::unique_ptr<rocksdb::Iterator>(db->NewIterator(ctx.ro, hs[i]));
    }
#endif
    ctx.ss.resize(hs.size());
    ctx.keys.resize(hs.size());
    ctx.values.resize(hs.size());
    check_assert(&ctx, false, "TEST");
    db->ReleaseSnapshot(ctx.ro.snapshot);
  }
#endif
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  auto time = std::chrono::system_clock::now() + std::chrono::minutes(1);

  std::atomic<uint64_t> atomic_count{1};
  //{
  //  auto snapshot = db->GetSnapshot();
  //  atomic_count.store(get_snapshot_seqno(snapshot) + 1);
  //  db->ReleaseSnapshot(snapshot);
  //}

  auto get_snapshot =
      [&snapshot, &snapshot_mutex](
          std::mt19937_64 &mt) -> std::shared_ptr<const rocksdb::Snapshot> {
    snapshot_mutex.lock();
#if ASYNC_TEST
    size_t si = mt() % snapshot.size();
#else
    size_t si = mt() % (snapshot.size() + 1);
#endif
    auto ret = si == snapshot.size() ? nullptr : snapshot[si];
    snapshot_mutex.unlock();
    return ret;
  };

  auto read_func = [db, &hs, &has_error, &atomic_count, &options, get_snapshot,
                    check_assert](int seed) {
    ReadContext ctx;
    uint64_t iter_seqno = uint64_t(-1);
    auto &ro = ctx.ro;
    auto &seqno = ctx.seqno;
    seqno = uint64_t(-1);
#if ITER_TEST
    auto &iter = ctx.iter;
    std::vector<rocksdb::Iterator *> iter_for_new;
    if (!db->NewIterators(ro, hs, &iter_for_new).ok()) {
      fprintf(stderr, "NewIterator fail !\n");
      return;
    }
    for (auto it : iter_for_new) {
      iter.emplace_back(it);
    }
    iter_for_new.clear();
    iter_seqno = uint64_t(-1);
#endif
    auto &count = ctx.count;
    auto &key = ctx.key;
    key.resize(hs.size());
    auto &ss = ctx.ss;
    ss.resize(hs.size());
    auto &keys = ctx.keys;
    keys.resize(hs.size());
    auto &values = ctx.values;
    values.resize(hs.size());
    auto &futures = ctx.futures;
    futures.resize(hs.size());
    auto &async_values = ctx.async_values;
    async_values.resize(hs.size());
    auto &async_status = ctx.async_status;
    async_status.resize(hs.size());

    std::mt19937_64 mt(seed);
    for (count = atomic_count;; count = atomic_count) {
      std::uniform_int_distribution<uint64_t> uid(0, count);
#if ITER_TEST
      if (count % 103 == 0) {
        auto snapshot = get_snapshot(mt);
        ro.snapshot = snapshot.get();
        if (ro.snapshot == nullptr) {
          iter_seqno = uint64_t(-1);
        } else {
          iter_seqno = get_snapshot_seqno(ro.snapshot);
        }
        if (!db->NewIterators(ro, hs, &iter_for_new).ok()) {
          fprintf(stderr, "NewIterator fail !\n");
          return;
        }
        iter.clear();
        for (auto it : iter_for_new) {
          iter.emplace_back(it);
        }
        iter_for_new.clear();
        ro.snapshot = nullptr;
      }
#endif
      if (count % 17 == 0) {
        size_t r0 = uid(mt);
        size_t r1 = uid(mt);
        auto key0 = get_rnd_key(r0);
        auto key1 = get_rnd_key(r1);
        auto key2 = get_key(r0);
        auto key3 = get_key(r1);
        if (options.comparator->Compare(key0, key1) > 0) {
          std::swap(key0, key1);
        }
        if (options.comparator->Compare(key2, key3) > 0) {
          std::swap(key2, key3);
        }
        rocksdb::Range range[2];
        range[0].start = key0;
        range[0].limit = key1;
        range[1].start = key2;
        range[1].limit = key3;
        uint64_t size[2];
        for (auto &h : hs) {
          db->GetApproximateSizes(h, range, 2, size);
        }
      }
#if GET_TEST
      if (count % 5 == 0) {
        auto snapshot = get_snapshot(mt);
        for (int i = 0; i < 2; ++i) {
          ro.snapshot = snapshot.get();
          if (ro.snapshot == nullptr) {
            seqno = uint64_t(-1);
          } else {
            seqno = get_snapshot_seqno(ro.snapshot);
          }
          key = i % 2 ? get_rnd_key(uid(mt)) : get_key(uid(mt));
          for (auto &k : keys) {
            k = key;
          }
          ss = db->MultiGet(ro, hs, keys, &values);
          if (IsAny(ss, [](auto &s) { return s.IsNotFound(); })) {
            check_assert(&ctx,
                         IsAll(ss, [](auto &s) { return s.IsNotFound(); }),
                         "MultiGet Status");
          } else {
            check_assert(&ctx, IsAny(ss, [](auto &s) { return s.ok(); }),
                         "MultiGet Status");
            check_assert(
                &ctx, IsSame(values, [](auto &l, auto &r) { return l == r; }),
                "MultiGet Value");
          }
          ro.snapshot = nullptr;
        }
      }
#endif
#if ASYNC_TEST
      if (count % 13 == 0) {
        auto snapshot = get_snapshot(mt);
        for (int i = 0; i < 4; ++i) {
          values.clear();
          async_values.clear();
          futures.clear();
          ro.snapshot = snapshot.get();
          if (ro.snapshot == nullptr) {
            seqno = uint64_t(-1);
          } else {
            seqno = get_snapshot_seqno(ro.snapshot);
          }
          key = i % 2 ? get_rnd_key(uid(mt)) : get_key(uid(mt));
          for (auto &k : keys) {
            k = key;
          }
          for (size_t i = 0; i < hs.size(); ++i) {
            ss[i] = db->Get(ro, hs[i], keys[i], &values[i]);
            db->GetAsync(ro, hs[i], keys[i].ToString(), &async_values[i],
                         [&async_status, &i](Status &&s, std::string &&key,
                                             std::string *value) {
                           async_status[i] = s;
                         });
            futures[i] = db->GetFuture(ro, hs[i], std::move(key));
          }
          db->WaitAsync();
          check_assert(&ctx,
                       AllSame(values, async_values,
                               [](auto &l, auto &r) { return l == r; }),
                       "Get vs GetAsync");
          check_assert(
              &ctx,
              AllSame(values, futures,
                      [](auto &l, auto &r) {
                        const std::tuple<Status, std::string, std::string>
                            &tmp_tuple = r.get();
                        return l == std::get<2>(tmp_tuple);
                      }),
              "Get vs GetFuture");
        }
      }
#endif
#if ITER_TEST
      if (count % 29 == 0) {
        seqno = iter_seqno;
        key = count % 2 ? get_rnd_key(uid(mt)) : get_key(uid(mt));

        for (auto &it : iter) {
          it->Seek(key);
        }
        if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
          check_assert(&ctx, IsAll(iter, [](auto &it) { return !it->Valid(); }),
                       "Iter Seek Valid");
          check_assert(&ctx,
                       IsAll(iter, [](auto &it) { return it->status().ok(); }),
                       "Iter Seek Status");
        } else {
          check_assert(
              &ctx,
              IsSame(iter,
                     [](auto &l, auto &r) { return l->key() == r->key(); }),
              "Iter Seek Key");
          check_assert(
              &ctx,
              IsSame(iter,
                     [](auto &l, auto &r) { return l->value() == r->value(); }),
              "Iter Seek Value");

          for (auto &it : iter) {
            it->Next();
          }
          if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
            check_assert(&ctx,
                         IsAll(iter, [](auto &it) { return !it->Valid(); }),
                         "Iter Seek Next Valid");
            check_assert(
                &ctx, IsAll(iter, [](auto &it) { return it->status().ok(); }),
                "Iter Seek Next Status");
          } else {
            check_assert(
                &ctx,
                IsSame(iter,
                       [](auto &l, auto &r) { return l->key() == r->key(); }),
                "Iter Seek Next Key");
            check_assert(
                &ctx,
                IsSame(iter, [](auto &l,
                                auto &r) { return l->value() == r->value(); }),
                "Iter Seek Next Value");

            for (auto &it : iter) {
              it->Prev();
            }
            if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
              check_assert(&ctx,
                           IsAll(iter, [](auto &it) { return !it->Valid(); }),
                           "Iter Seek Next Prev Valid");
              check_assert(
                  &ctx, IsAll(iter, [](auto &it) { return it->status().ok(); }),
                  "Iter Seek Next Prev Status");
            } else {
              check_assert(
                  &ctx,
                  IsSame(iter,
                         [](auto &l, auto &r) { return l->key() == r->key(); }),
                  "Iter Seek Next Prev Key");
              check_assert(&ctx,
                           IsSame(iter,
                                  [](auto &l, auto &r) {
                                    return l->value() == r->value();
                                  }),
                           "Iter Seek Next Prev Value");
            }
          }
        }

        for (auto &it : iter) {
          it->SeekForPrev(key);
        }
        if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
          check_assert(&ctx, IsAll(iter, [](auto &it) { return !it->Valid(); }),
                       "Iter SeekForPrev Valid");
          check_assert(&ctx,
                       IsAll(iter, [](auto &it) { return it->status().ok(); }),
                       "Iter SeekForPrev Status");
        } else {
          check_assert(
              &ctx,
              IsSame(iter,
                     [](auto &l, auto &r) { return l->key() == r->key(); }),
              "Iter SeekForPrev Key");
          check_assert(
              &ctx,
              IsSame(iter,
                     [](auto &l, auto &r) { return l->value() == r->value(); }),
              "Iter SeekForPrev Value");

          for (auto &it : iter) {
            it->Prev();
          }
          if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
            check_assert(&ctx,
                         IsAll(iter, [](auto &it) { return !it->Valid(); }),
                         "Iter SeekForPrev Prev Valid");
            check_assert(
                &ctx, IsAll(iter, [](auto &it) { return it->status().ok(); }),
                "Iter SeekForPrev Prev Status");
          } else {
            check_assert(
                &ctx,
                IsSame(iter,
                       [](auto &l, auto &r) { return l->key() == r->key(); }),
                "Iter SeekForPrev Prev Key");
            check_assert(
                &ctx,
                IsSame(iter, [](auto &l,
                                auto &r) { return l->value() == r->value(); }),
                "Iter SeekForPrev Prev Value");

            for (auto &it : iter) {
              it->Next();
            }
            if (IsAny(iter, [](auto &it) { return !it->Valid(); })) {
              check_assert(&ctx,
                           IsAll(iter, [](auto &it) { return !it->Valid(); }),
                           "Iter SeekForPrev Prev Next Valid");
              check_assert(
                  &ctx, IsAll(iter, [](auto &it) { return it->status().ok(); }),
                  "Iter SeekForPrev Prev Next Status");
            } else {
              check_assert(
                  &ctx,
                  IsSame(iter,
                         [](auto &l, auto &r) { return l->key() == r->key(); }),
                  "Iter SeekForPrev Prev Next Key");
              check_assert(&ctx,
                           IsSame(iter,
                                  [](auto &l, auto &r) {
                                    return l->value() == r->value();
                                  }),
                           "Iter SeekForPrev Prev Next Value");
            }
          }
        }
      }
#endif
    }
  };
  auto write_func = [db, &hs, &atomic_count, &has_error, &wo, &co, &snapshot,
                     &snapshot_mutex, &options](int seed) {
    rocksdb::WriteBatchWithIndex b(
        rocksdb::BytewiseComparator(), 0, false, 0,
#if TEST_TERARK
        rocksdb::patricia_WriteBatchEntryIndexFactory()
#else
        nullptr
#endif
    );
    std::string key, value;
    std::mt19937_64 mt(seed);

    auto cmp = [c = options.comparator](const std::string &a,
                                        const std::string &b) {
      return c->Compare(a, b) < 0;
    };
    std::set<std::string, decltype(cmp)> rand_key_0(cmp);
    std::set<std::string, decltype(cmp)> rand_key_1(cmp);
    for (uint64_t count = atomic_count;; count = ++atomic_count) {
      if (has_error) {
        std::this_thread::sleep_for(std::chrono::hours(24 * 265));
      }
      // if (count % 100000 == 0) {
      //  fprintf(stderr, "\n\n%s\n\n", statistics->ToString().c_str());
      //}
      std::uniform_int_distribution<uint64_t> uid(0, count);
      auto get_ran_range_pair = [&](int mode) {
        size_t i = mt() % (rand_key_times - 1);
        std::string k1, k2;
        if (mode == 0) {
          while (rand_key_0.size() < rand_key_times) {
            rand_key_0.emplace(get_key(uid(mt)));
          }
          auto it = std::next(rand_key_0.begin(), i);
          k1 = *it;
          it = rand_key_0.erase(it);
          k2 = *it;
          rand_key_0.erase(it);
        } else {
          while (rand_key_1.size() < rand_key_times) {
            rand_key_1.emplace(get_rnd_key(uid(mt)));
          }
          auto it = std::next(rand_key_1.begin(), i);
          k1 = *it;
          it = rand_key_1.erase(it);
          k2 = *it;
          rand_key_1.erase(it);
        }
        return std::make_pair(std::move(k1), std::move(k2));
      };
#if COMPACTION
      if (mt() % (1ull << 20) == 0) {
        auto keys = get_ran_range_pair(mt() & 1);
        rocksdb::Slice slice0 = keys.first, slice1 = keys.second;
        db->CompactRange(co, hs[mt() % hs.size()], &slice0, &slice1);
      }
#endif
      value = get_value(count);
      size_t r = uid(mt);
      key = get_key(r);
      if (count % 2 == 0) {
        for (auto &h : hs) {
          b.Put(h, key, value);
        }
      } else {
        for (auto &h : hs) {
          b.Merge(h, key, value);
        }
      }
      key = get_rnd_key(r);
      if (count % 3 == 0) {
        for (auto &h : hs) {
          b.Put(h, key, value);
        }
      } else {
        for (auto &h : hs) {
          b.Merge(h, key, value);
        }
      }
      if (count % 11 == 0) {
        key = get_rnd_key(uid(mt));
        for (auto &h : hs) {
          b.Delete(h, key);
        }
      }
      if (count % 13 == 0) {
        key = get_key(uid(mt));
        for (auto &h : hs) {
          b.Delete(h, key);
        }
      }
#if RANGE_DEL
      // if (count % 400003 == 0) {
      if (count % 70003 == 0) {
        auto keys = get_ran_range_pair(0);
        rocksdb::Slice slice0 = keys.first, slice1 = keys.second;
        fprintf(stderr, "RangeDel [%s, %s)\n", keys.first.c_str(),
                keys.second.c_str());
        for (auto &h : hs) {
          b.DeleteRange(h, slice0, slice1);
        }
      }
      // if (count % 500017 == 0) {
      if (count % 90017 == 0) {
        auto keys = get_ran_range_pair(1);
        rocksdb::Slice slice0 = keys.first, slice1 = keys.second;
        fprintf(stderr, "RangeDel [%s, %s)\n", keys.first.c_str(),
                keys.second.c_str());
        for (auto &h : hs) {
          b.DeleteRange(h, slice0, slice1);
        }
      }
#endif
      if (std::uniform_int_distribution<uint64_t>(0, 1 << 10)(mt) == 0) {
        auto s = db->Write(wo, b.GetWriteBatch());
        if (!s.ok()) {
          printf("%s\n", s.getState());
          break;
        }
        b.Clear();
        if (snapshot_mutex.try_lock()) {
          auto del_snapshot = [db](const rocksdb::Snapshot *s) {
            db->ReleaseSnapshot(s);
          };
          if (snapshot.size() < 2) {
            snapshot.emplace_back(db->GetSnapshot(), del_snapshot);
          } else if (snapshot.size() > 50 || (mt() % 4) == 0) {
            auto i = mt() % snapshot.size();
            snapshot.erase(snapshot.begin() + i);
          } else {
            snapshot.emplace_back(db->GetSnapshot(), del_snapshot);
          }
          snapshot_mutex.unlock();
        }
      }
    }
  };

  // auto verify_func = [db, &hs, &has_error, &atomic_count, &options,
  //                    get_snapshot, check_assert] {
  //  rocksdb::ReadOptions ro;
  //  // ro.snapshot = db->GetSnapshot();
  //  // set_snapshot_seqno(ro.snapshot, 6845218240);
  //  std::unique_ptr<rocksdb::Iterator> iter0(db->NewIterator(ro, h0));
  //  std::unique_ptr<rocksdb::Iterator> iter1(db->NewIterator(ro, h1));
  //  size_t i = 0;
  //  size_t notify = 100000;
  //  size_t notify_i = notify;
  //  std::string value;
  //  rocksdb::LazyBuffer buffer;
  //  std::vector<rocksdb::Slice> key_list;
  //  std::vector<std::string> value_list;
  //  std::vector<rocksdb::Status> status_list;
  //  for (iter1->SeekToFirst(); iter1->Valid(); iter1->Next()) {
  //    // for
  //    //
  //    (iter1->Seek("3B2219C48449BD3A98FFF6E00C23092D1272DD2A85ADBE06484BFF709F94103");
  //    // iter1->Valid(); iter1->Next()) {
  //    buffer.reset(&value);
  //    auto s = db->Get(ro, h0, iter1->key(), &buffer);
  //    if (s.ok()) {
  //      s = std::move(buffer).dump(&value);
  //    }
  //    if (!s.ok() || value != iter1->value()) {
  //      iter0->Seek(iter1->key());
  //      if (!iter0->Valid()) {
  //        fprintf(stderr, "n = %zd key = %s iter seek invalid = %s\n", i,
  //                iter1->key().ToString().c_str(),
  //                iter0->status().ToString().c_str());
  //      } else if (iter0->key() != iter1->key()) {
  //        fprintf(stderr, "n = %zd key = %s iter key mismatch\n", i,
  //                iter1->key().ToString().c_str());
  //        fprintf(stderr, " key = %s\n", iter0->key().ToString().c_str());
  //      } else if (iter0->value() != iter1->value()) {
  //        fprintf(stderr, "n = %zd key = %s iter value mismatch\n", i,
  //                iter1->key().ToString().c_str());
  //        fprintf(stderr, " value = %s\n", iter0->value().ToString().c_str());
  //        fprintf(stderr, " value = %s\n", iter1->value().ToString().c_str());
  //      }
  //      fprintf(stderr, "n = %zd key = %s get error = %s\n", i,
  //              iter1->key().ToString().c_str(), s.ToString().c_str());
  //      if (s.ok()) {
  //        fprintf(stderr, " value = %s\n", value.c_str());
  //        fprintf(stderr, " value = %s\n", iter1->value().ToString().c_str());
  //      }
  //      notify_i = i + notify;
  //    } else {
  //      s = db->MultiGet(ro, {h0}, {iter1->key()}, &value_list).front();
  //      if (!s.ok() || value_list.front() != iter1->value()) {
  //        fprintf(stderr, "n = %zd key = %s multi get error = %s\n", i,
  //                iter1->key().ToString().c_str(), s.ToString().c_str());
  //        if (s.ok()) {
  //          fprintf(stderr, " value = %s\n", value_list.front().c_str());
  //          fprintf(stderr, " value = %s\n",
  //          iter1->value().ToString().c_str());
  //        }
  //        notify_i = i + notify;
  //      }
  //    }
  //    if (++i >= notify_i) {
  //      fprintf(stderr, "n = %zd key = %s\n", i,
  //              iter1->key().ToString().c_str());
  //      notify_i = i + notify;
  //    }
  //  }
  //  if (!iter1->status().ok()) {
  //    fprintf(stderr, "iter err = %s\n", iter1->status().ToString().c_str());
  //  }
  //};
  auto iterate_func = [db, &hs, &has_error, &atomic_count, &options,
                       get_snapshot,
                       check_assert](rocksdb::ColumnFamilyHandle *cfh) {
    rocksdb::ReadOptions ro;
    std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator(ro, cfh));
    size_t i = 0;
    size_t notify = 100000;
    size_t notify_i = notify;
    std::string value;
    rocksdb::LazyBuffer buffer;
    std::vector<rocksdb::Slice> key_list;
    std::vector<std::string> value_list;
    std::vector<rocksdb::Status> status_list;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      buffer.reset(&value);
      auto s = db->Get(ro, cfh, iter->key(), &buffer);
      if (s.ok()) {
        s = std::move(buffer).dump(&value);
      }
      if (!s.ok()) {
        fprintf(stderr, "n = %zd key = %s get error = %s\n", i,
                iter->key().ToString(true).c_str(), s.ToString().c_str());
        notify_i = i + notify;
      }
      if (++i >= notify_i) {
        fprintf(stderr, "n = %zd key = %s\n", i,
                iter->key().ToString(true).c_str());
        notify_i = i + notify;
      }
    }
    if (iter->status().ok()) {
      fprintf(stderr, "n = %zd finish !\n", i);
    } else {
      fprintf(stderr, "iter err = %s\n", iter->status().ToString().c_str());
    }
  };
  // std::this_thread::sleep_for(std::chrono::hours(24));
#if READ_ONLY
  // verify_func();
  exit(0);
#endif
  // for (auto& h : hs) { db->Put(wo, h, "0", "0"); }
  // const rocksdb::Snapshot* pin_range_deletions = db->GetSnapshot();
  std::vector<std::thread> thread_vec;
  for (int j = 0; j < 32; ++j) {
    thread_vec.emplace_back(read_func, j);
  }
  for (int j = 0; j < 16; ++j) {
    thread_vec.emplace_back(write_func, j);
  }
  for (auto &t : thread_vec) {
    t.join();
  }
  std::this_thread::sleep_for(std::chrono::hours(24));

  // db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);

  snapshot.clear();
  // db->ReleaseSnapshot(pin_range_deletions);
  for (auto &h : hs) {
    db->DestroyColumnFamilyHandle(h);
  }
  delete db;

  return 0;
}
