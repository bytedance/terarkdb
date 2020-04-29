#include <string>
#include <atomic>
#include <random>
#include <chrono>

#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "bloat_test.h"
#include "options/db_options.h"
#include "rocksdb/table.h"
#include "util/hash.h"

namespace rocksdb {

uint64_t gene_seed() {
  std::random_device rd;
  return rd();
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

  BloatTest (const std::string &conf_path,
      const std::string &&name,
      const std::string &&ss_name,
      const std::string &&ms_name) :
    config_file_(conf_path),
    db_name_(name),
    db_ss_name_(ss_name),
    db_ms_name_(ms_name) {

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

  void WriteFunc() {

    auto fill_kv = [&](uint64_t seed, std::string& k, std::string& v) {
        std::mt19937_64 mt(seed);
        auto uid = std::uniform_int_distribution<char>(0, 255);
        auto klen = std::uniform_int_distribution<size_t>(50 - 10, 50 + 10);
        auto vlen = std::uniform_int_distribution<size_t>(46000 - 20000, 46000 + 20000);
        k.resize(klen(mt));
        v.resize(vlen(mt));
        std::generate_n(k.begin(), k.size(), std::bind(uid, std::ref(mt)));
        std::generate_n(v.begin(), v.size(), std::bind(uid, std::ref(mt)));
    };
    auto key_seed = std::bind(
        std::uniform_int_distribution<uint64_t>(0, 12000000),
        std::mt19937_64(gene_seed()));

    std::string key, value;
    for (;;) {

      fill_kv(key_seed(), key, value);
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
  rocksdb::BloatTest t("./db.ini",
      "/data00/bt/sn-bloatdb",
      "/data01/bt/ss-bloatdb",
      "/data04/bt/ms-bloatdb");

  uint32_t thread_num  = 2;
  std::vector<std::thread> thread_vec;
  for (int j = 0; j < thread_num; ++j) {
    thread_vec.emplace_back(&rocksdb::BloatTest::WriteFunc, std::ref(t));
  }
  for (auto &t : thread_vec) {
    t.join();
  }

  return 0;
}

