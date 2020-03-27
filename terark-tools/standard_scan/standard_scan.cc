#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>

#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

using namespace rocksdb;

class Stats {
 public:
  Stats(bool empty) : empty_(empty) {}
  // not used yet/
  std::unordered_map<std::string, int> total;
  
  std::string key_prefix;
  int count = 0;

 public:
  void add(const char *key, int sz) {
    assert(sz >= 4);
    std::string prefix = std::string(key, 4);
    // std::cout << prefix << " " << ToHex(key, sz) << std::endl;
    if (count == 0) {
      key_prefix = prefix;
      count = 1;
    } else if (key_prefix == prefix) {
      ++count;
    } else {
      printPrefixStats();
      key_prefix = prefix;
      count = 1;
    }
  }

  void printPrefixStats() {
    if (empty_) {
      std::cout << "Empty Value, Key Prefix: " << ToHex(key_prefix.c_str(), 4)
                << ", Count: " << count << std::endl;
    } else {
      std::cout << "Non-Empty Value, Key Prefix: " << ToHex(key_prefix.c_str(), 4)
                << ", Count: " << count << std::endl;
    }
  }

  inline std::string ToHex(const char *data_, int size_) {
    std::string result;
    static const char hextab[] = "0123456789ABCDEF";
    if (size_) {
      result.resize(2 * size_);
      auto beg = &result[0];
      for (size_t i = 0; i < size_; ++i) {
        unsigned char c = data_[i];
        beg[i * 2 + 0] = hextab[c >> 4];
        beg[i * 2 + 1] = hextab[c & 0xf];
      }
    }
    return result;
  }

 private:
  bool empty_ = false;
};

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
  virtual void FindShortestSeparator(
      std::string *start, const rocksdb::Slice &limit) const override {
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

int main(int argc, const char **argv) {
  if (argc != 2) {
    printf("usage: ./standard_scan [db_dir]\n");
    return 1;
  }
  setenv("TerarkZipTable_localTempDir", "./", true);
  setenv("TerarkConfigString", "TerarkZipTable_checksumLevel=0", true);

  std::string db_dir = argv[1];
  printf("open database: %s\n", db_dir.c_str());

  ComparatorRename cmp{"RocksDB_SE_v3.10", rocksdb::BytewiseComparator()};
  DB *db;
  Options options;
  options.comparator = &cmp;
  options.create_if_missing = false;
  // open DB
  Status s = DB::OpenForReadOnly(options, db_dir, &db);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    return 0;
  }

  rocksdb::ReadOptions readOptions;
  readOptions.fill_cache = false;
  readOptions.verify_checksums = false;
  auto it = db->NewIterator(readOptions);
  printf("seek to first...\n");
  it->SeekToFirst();

  //   std::cout << it->status().ToString() << std::endl;
  Stats empty_stats(true);
  Stats non_empty_stats(false);

  int i = 0;
  printf("start scan...\n");
  while (it->Valid()) {
    if (it->value().empty()) {
      empty_stats.add(it->key().data(), it->key().size());
    } else {
      non_empty_stats.add(it->key().data(), it->key().size());
    }
    it->Next();

    if (i == 10) break;

    if (i++ % (1 << 20) == 0) {
      printf("parsed %d records\n", i);
    }
  }

  if (empty_stats.count > 0) empty_stats.printPrefixStats();
  if (non_empty_stats.count > 0) non_empty_stats.printPrefixStats();

  delete db;
  return 0;
}