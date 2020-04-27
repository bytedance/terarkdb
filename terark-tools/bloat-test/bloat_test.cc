#include <string>
#include <atomic>

#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "bloat_test.h"
#include "options/db_options.h"
#include "rocksdb/table.h"
#include "util/hash.h"

namespace rocksdb {

void RandomString(std::string& key, size_t length)
{
    auto randchar = []() -> char
    {
        const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[ rand() % max_index ];
    };

    key.resize(length);
    std::generate_n(key.begin(), length, randchar);
}

class BloatTest
{
public:
  std::string config_file_ = "";

  DB *db_;
  std::string db_name_ = "";
  std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_; // size = 1

  DB *db_ss_;
  std::string db_ss_name_ = "";
  std::vector<rocksdb::ColumnFamilyHandle *> cf_ss_handles_; // size = 1

  DB *db_ms_;
  std::string db_ms_name_ = "";
  std::vector<rocksdb::ColumnFamilyHandle *> cf_ms_handles_; // size = 64

  DBOptions db_options_;
  ColumnFamilyDescriptor cf_desc_;

  BloatTest (const std::string &conf_path, const std::string &&name) :
    config_file_(conf_path),
    db_name_(name),
    db_ss_name_(name+"ss"),
    db_ms_name_(name+"ms") {

    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;

    rocksdb::Status s = rocksdb::LoadOptionsFromFile(
        config_file_,
        Env::Default(), // ?
        &db_options_,
        &cf_descs,
        false); // ignore_unknown_options
    if (!s.ok()) {
      fprintf(stderr, "Load Option Error! %s\n", s.getState());
      assert(false);
    }

    // get options from file
    assert(cf_descs.size() == 1);
    cf_desc_ = cf_descs[0];
    db_options_.create_if_missing = true;
    db_options_.create_missing_column_families = true;

    OpenDB();
  };

  ~BloatTest () {};

  void OpenDB() {
    rocksdb::Status s;

    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;

    // create single cf non-kv-split DB
    cf_descs.push_back(cf_desc_);
    s = DB::Open(db_options_, db_name_, cf_descs, &cf_handles_, &db_);
    if (!s.ok()) {
      fprintf(stderr, "Open Error! %s\n", s.getState());
      assert(false);
    }
    assert(cf_handles_.size() == 1);

    // create single cf kv-split DB
    cf_descs[0].options.blob_size = 2048; // 2k to turn on kv split
    s = DB::Open(db_options_, db_ss_name_, cf_descs, &cf_ss_handles_, &db_ss_);
    if (!s.ok()) {
      fprintf(stderr, "Open Error! %s\n", s.getState());
      assert(false);
    }
    assert(cf_ss_handles_.size() == 1);

    // create multi cf kv-split DB
    auto cfo = cf_descs.back().options;
    for (int i = 1; i < 64; ++i) {
      cf_descs.push_back(ColumnFamilyDescriptor("mycf" + std::to_string(i), cfo));
    }
    s = DB::Open(db_options_, db_ms_name_, cf_descs, &cf_ms_handles_, &db_ms_);
    if (!s.ok()) {
      fprintf(stderr, "Open Error! %s\n", s.getState());
      assert(false);
    }
    assert(cf_ms_handles_.size() == 64);
  }

  void WriteFunc(int seed) {

    std::string key, value;
    std::atomic<uint64_t> atomic_count{1};
    for (uint64_t count = atomic_count;; count = ++atomic_count) {

      RandomString(key, 40 + rand() % (10*2));
      RandomString(value, 40000 + rand() % (7104*2) );
      fprintf(stderr, "key size: %u, value size: %u \n", key.size(), value.size());

      rocksdb::Status s;
      // only put
      s = db_->Put(WriteOptions(), cf_handles_[0], Slice(key), Slice(value));
      if (!s.ok()) {
        fprintf(stderr, "Open Error! %s\n", s.getState());
        assert(false);
      }

      s = db_ss_->Put(WriteOptions(), cf_ss_handles_[0], Slice(key), Slice(value));
      if (!s.ok()) {
        fprintf(stderr, "Open Error! %s\n", s.getState());
        assert(false);
      }

      Slice slice_key(key);
      s = db_ms_->Put(WriteOptions(), cf_ms_handles_[GetSliceHash(slice_key)%64], slice_key, Slice(value));
      if (!s.ok()) {
        fprintf(stderr, "Open Error! %s\n", s.getState());
        assert(false);
      }
    }
  }
private:
  /* data */
};

} // namespace rocksdb

int main(int argc, char *argv[])
{
  rocksdb::BloatTest t("./db.ini", "/data02/lymtestdata/bloatdb");

  std::vector<std::thread> thread_vec;
  for (int j = 0; j < 8; ++j) {
    thread_vec.emplace_back(&rocksdb::BloatTest::WriteFunc, std::ref(t), j);
  }
  for (auto &t : thread_vec) {
    t.join();
  }

  return 0;
}

