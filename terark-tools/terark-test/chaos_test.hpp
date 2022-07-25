#include <sys/epoll.h>

#include <cctype>
#include <chrono>
#include <cinttypes>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <memory>
#include <thread>
#include <utility>
#include <vector>
#define GUJIA_HAS_EPOLL
namespace gujia {
typedef struct epoll_event Event;
}

#include <cache/lru_cache.h>
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
#include <util/coding.h>
#include <util/filename.h>
#include <util/gflags_compat.h>
#include <utilities/merge_operators/string_append/stringappend.h>
#include <utilities/merge_operators/string_append/stringappend2.h>

#ifdef WITH_BOOSTLIB
#include <boost/fiber/future.hpp>
#endif

#ifdef WITH_TERARK_ZIP
#include <table/terark_zip_common.h>
#include <table/terark_zip_table.h>
#include <terark/zbs/sufarr_inducedsort.h>

#include <terark/fsa/cspptrie.inl>
#include <terark/idx/terark_zip_index.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/lcast.hpp>
#include <terark/mempool_lock_none.hpp>
#include <terark/rank_select.hpp>
#include <terark/stdtypes.hpp>
#include <terark/util/mmap.hpp>
#include <terark/zbs/dict_zip_blob_store.hpp>
#include <terark/zbs/zip_reorder_map.hpp>
#endif

// worker test
#include <rocksdb/compaction_dispatcher.h>

#include "db/column_family.h"
#include "db/compaction_job.h"
#include "db/db_impl.h"
#include "db/error_handler.h"
#include "db/memtable.h"

DEFINE_bool(get, true, "test Get");
DEFINE_bool(iter, false, "test Iterator");
DEFINE_bool(async, false, "test GetFuture and GetAsync");
DEFINE_bool(worker, false, "test Worker Compaction");
DEFINE_bool(readonly, false, "test ReadOnly Open");
DEFINE_bool(terark, false, "test Terark Table");
DEFINE_bool(compactrange, false, "test CompactRange");
DEFINE_bool(rangedel, false, "test RangeDel");
DEFINE_bool(hash, false, "test Hash(key) after Compaction");
DEFINE_int32(write_thread, 2, "the number of write thread.");
DEFINE_int32(read_thread, 20, "the number of read thread.");
DEFINE_int32(cf_num, 1, "the number of column family");
DEFINE_int32(value_avg_size, 2048, "the average size of value");
DEFINE_int32(key_avg_size, 64, "the average size of key");
DEFINE_int32(force_evict, 2,
             "force evict level, 0 kSkipForceEvict; 1 kForceEvictIfOpen, 2 "
             "kAlwaysForceEvict");

const size_t file_size_base = 64ull << 20;
const size_t blob_size = 2048;
const size_t key_mode_nums = 2;
const size_t value_avg_size = FLAGS_value_avg_size;
const size_t key_avg_size = FLAGS_key_avg_size;
const size_t rand_key_times = 500;
const char *READ_ONLY_TEST_KEY = "FF7EAC449F56EB1E9A9A0D43195";
const size_t READ_ONLY_TEST_SEQ = 12983622;
std::string MinKey;
std::string MaxKey;

static thread_local std::mt19937_64 mt;
std::hash<std::string> h1;
std::string h(TERARKDB_NAMESPACE::Slice key) {
  return std::to_string(h1(key.ToString()));
}
enum {
  TestIter = 1ULL << 0,
  TestTerark = 1ULL << 1,
  TestGet = 1ULL << 2,
  TestRangeDel = 1ULL << 3,
  TestCompaction = 1ULL << 4,
  TestAsync = 1ULL << 5,
  TestWorker = 1ULL << 6,
  ReadOnly = 1ULL << 7,
  TestHash = 1ULL << 8,
};
class ComparatorRename : public TERARKDB_NAMESPACE::Comparator {
 public:
  virtual const char *Name() const override { return n; }

  virtual int Compare(const TERARKDB_NAMESPACE::Slice &a,
                      const TERARKDB_NAMESPACE::Slice &b) const override {
    return c->Compare(a, b);
  }

  virtual bool Equal(const TERARKDB_NAMESPACE::Slice &a,
                     const TERARKDB_NAMESPACE::Slice &b) const override {
    return c->Equal(a, b);
  }
  virtual void FindShortestSeparator(
      std::string *start,
      const TERARKDB_NAMESPACE::Slice &limit) const override {
    c->FindShortestSeparator(start, limit);
  }

  virtual void FindShortSuccessor(std::string *key) const override {
    c->FindShortSuccessor(key);
  }

  const char *n;
  const TERARKDB_NAMESPACE::Comparator *c;

  ComparatorRename(const char *_n, const TERARKDB_NAMESPACE::Comparator *_c)
      : n(_n), c(_c) {}
};

class TestCompactionFilter : public TERARKDB_NAMESPACE::CompactionFilter {
  bool Filter(int /*level*/, const TERARKDB_NAMESPACE::Slice &key,
              const TERARKDB_NAMESPACE::Slice &existing_value,
              std::string *new_value, bool *value_changed) const override {
    assert(!existing_value.empty());
    // filter random
    // std::uniform_int_distribution<size_t> dis(0, 100);
    // if (dis(mt) < 5) return true;
    auto value = existing_value.ToString();
    size_t pos = value.rfind("#");
    if (value.size() % 3 == 0) {
      *value_changed = true;
      new_value->assign(value.data(), value.size());
    }
    assert(pos != std::string::npos);
    auto sub_str = value.substr(pos + 1);
    assert(h(key) == sub_str);
    return false;
  }
  const char *Name() const override { return "TestCompactionFilter"; }
};

class TestMergeOperator : public TERARKDB_NAMESPACE::StringAppendTESTOperator {
 public:
  // TestMergeOperator(char delim_char) :
  // TERARKDB_NAMESPACE::StringAppendOperator(delim_char) {}
  TestMergeOperator(char delim_char)
      : TERARKDB_NAMESPACE::StringAppendTESTOperator(delim_char) {}

  virtual TERARKDB_NAMESPACE::Status Serialize(
      std::string * /*bytes*/) const override {
    return TERARKDB_NAMESPACE::Status::OK();
  }
  virtual TERARKDB_NAMESPACE::Status Deserialize(
      const TERARKDB_NAMESPACE::Slice & /*bytes*/) override {
    return TERARKDB_NAMESPACE::Status::OK();
  }
};

class AsyncCompactionDispatcher
    : public TERARKDB_NAMESPACE::RemoteCompactionDispatcher {
 public:
  class AsyncWorker
      : public TERARKDB_NAMESPACE::RemoteCompactionDispatcher::Worker {
   public:
    AsyncWorker(const TERARKDB_NAMESPACE::Options &options)
        : TERARKDB_NAMESPACE::RemoteCompactionDispatcher::Worker(
              TERARKDB_NAMESPACE::EnvOptions(options), options.env) {}
    virtual std::string GenerateOutputFileName(size_t file_index) {
      return "worker";
    }
  };

  AsyncCompactionDispatcher(TERARKDB_NAMESPACE::Options options)
      : options_(options) {}
  virtual std::future<std::string> DoCompaction(
      const std::string data) override {
    AsyncWorker worker(options_);
    worker.DoCompaction(data);
    return std::async([]() -> std::string { return "test"; });
  }
  TERARKDB_NAMESPACE::Options options_;
};

struct ReadContext {
  TERARKDB_NAMESPACE::ReadOptions ro;
  uint64_t seqno;
  std::vector<std::unique_ptr<TERARKDB_NAMESPACE::Iterator>> iter;
  size_t count = 0;
  std::string key;
  std::vector<TERARKDB_NAMESPACE::Status> ss;
  std::vector<TERARKDB_NAMESPACE::Slice> keys;
  std::vector<std::string> values;
#ifdef WITH_BOOSTLIB
  std::vector<boost::fibers::future<
      std::tuple<TERARKDB_NAMESPACE::Status, std::string, std::string>>>
      futures;
#else
  std::vector<std::future<
      std::tuple<TERARKDB_NAMESPACE::Status, std::string, std::string>>>
      futures;
#endif
  std::vector<std::string> async_values;
  std::vector<TERARKDB_NAMESPACE::Status> async_status;
  std::vector<std::string> multi_values;
};

template <class T, class F>
bool IsSame(std::vector<T> &arr, F &&f) {
  for (size_t i = 1; i < arr.size(); ++i) {
    if (!f(arr[i - 1], arr[i])) {
      return false;
    }
  }
  return true;
}
template <class T, class F>
bool IsAny(std::vector<T> &arr, F &&f) {
  for (auto &t : arr) {
    if (f(t)) {
      return true;
    }
  }
  return false;
}
template <class T, class F>
bool IsAll(std::vector<T> &arr, F &&f) {
  for (auto &t : arr) {
    if (!f(t)) {
      return false;
    }
  }
  return true;
}

template <class T, class V, class F>
bool AllSame(std::vector<T> &left, std::vector<V> &right, F &&f) {
  if (left.size() != right.size()) return false;
  for (size_t i = 0; i < left.size(); ++i) {
    if (!f(left[i], right[i])) {
      return false;
    }
  }
  return true;
}

std::string get_seq_key(size_t i) {
  char buffer[32];
  snprintf(buffer, sizeof buffer, "%012zd", i);
  return buffer;
}

std::string get_rnd_key(size_t r) {
  std::mt19937_64 mt(r);
  char buffer[65];
  snprintf(buffer + 0, 17, "%016lX", mt());
  snprintf(buffer + 16, 17, "%016lX", mt());
  snprintf(buffer + 32, 17, "%016lX", mt());
  snprintf(buffer + 48, 17, "%016lX", mt());
  // uint64_t v = mt();
  // memcpy(buffer + 8, &v, sizeof v);
  return std::string(buffer,
                     buffer + std::uniform_int_distribution<size_t>(8, 64)(mt));
}

std::string gen_key(size_t mode) {
  std::string key;
  switch (mode % key_mode_nums) {
    case 0:
      key = get_seq_key(mode);
      break;
    case 1:
      key = get_rnd_key(mode);
    default:
      break;
  }
  MinKey = std::min(MinKey, key);
  MaxKey = std::max(MaxKey, key);
  return key;
}

std::string get_value(size_t i, std::string &key) {
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
  std::string value = gen_key(i);
  value.append("#");
  value.append(str.data() + pos, str.data() + pos + size);
  value.append("#");
  value.append(h(TERARKDB_NAMESPACE::Slice(key)));
  return value;
}

uint64_t get_snapshot_seqno(const TERARKDB_NAMESPACE::Snapshot *s) {
  return ((const uint64_t *)s)[1];
};

void set_snapshot_seqno(const TERARKDB_NAMESPACE::Snapshot *s, uint64_t seqno) {
  ((uint64_t *)s)[1] = seqno;
};
