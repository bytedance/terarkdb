#include <chrono>
#include <cstdlib>
#include <string>
#include <utility>
#include <vector>

#include "trace.hpp"
#include "utilities/trace/file_trace_reader_writer.h"

using namespace rocksdb;

namespace terark {

const std::string TraceTest::generate_text(size_t bytes) {
  std::string ret;
  ret.resize(bytes);
  for (int i = 0; i < bytes; ++i) {
    ret[i] = 'a' + (rand() % 26);
  }
  return ret;
}

void TraceTest::traceSeek() {
  Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;

  Env* env = rocksdb::Env::Default();
  EnvOptions env_options(options);

  std::string trace_path = "./rocksdb_trace_test";
  std::unique_ptr<TraceWriter> trace_writer;
  DB* db = nullptr;
  std::string db_name = "./rocksdb";
  /*Create the trace file writer*/
  NewFileTraceWriter(env, env_options, trace_path, &trace_writer);

  DB::Open(options, db_name, &db);

  std::vector<std::pair<std::string, std::string>> checks;

  // prepare data, about 3GB
  uint32_t byte_cnt = 0;
  for (int i = 0; i < 1024 * 1024; ++i) {
    int r = rand() % 1000;
    Slice key(std::to_string(r) + "_" + std::to_string(i));
    auto text = generate_text(3 * 1024);
    Slice value(text);
    db->Put(rocksdb::WriteOptions(), key, value);

    // print progress
    if (i % 10000 == 0) {
      std::cout << i << " pairs" << std::endl;
    }
    // record some data for further seeking
    if (i % 100000 == 0) {
      checks.emplace_back(key.data(), value.data());
      std::cout << "key = " << checks.back().first.data() << std::endl;
    }
    byte_cnt += key.size();
    byte_cnt += value.size();
  }

  std::cout << "total data write : " << byte_cnt / (1024 * 1024) << " MB"
            << std::endl;
  std::cout << "check count: " << checks.size() << std::endl;
  std::cout << "flush memtables" << std::endl;

  // flush
  auto flsuh_opt = rocksdb::FlushOptions();
  int us = 0;
  auto t1 = std::chrono::high_resolution_clock::now();
  flsuh_opt.wait = true;
  flsuh_opt.allow_write_stall = true;
  db->Flush(flsuh_opt);
  auto t2 = std::chrono::high_resolution_clock::now();
  us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
  std::cout << "flush memtable, time cost: " << us << " us" << std::endl;

  // check sample print
  std::cout << "kv sample, checks.size = " << checks.size() << std::endl;
  for (auto& check : checks) {
    std::cout << "key = " << check.first.data() << std::endl;
    std::cout << "value size = " << check.second.size() << std::endl;
  }

  /*Start tracing*/
  TraceOptions trace_opt;
  db->StartTrace(trace_opt, std::move(trace_writer));

  std::cout << "create iterator..." << std::endl;
  t1 = std::chrono::high_resolution_clock::now();
  auto it = db->NewIterator(rocksdb::ReadOptions());
  t2 = std::chrono::high_resolution_clock::now();
  us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();

  std::cout << "iterator creation finished, time cost: " << us << " us"
            << std::endl;

  uint64_t lat = 0;
  std::cout << "-------------------" << std::endl;
  for (int i = 0; i < 100; ++i) {
    for (auto& check : checks) {
      // std::cout << "seek(" << check.first.data() << ")... ";
      t1 = std::chrono::high_resolution_clock::now();
      it->Seek(check.first);
      t2 = std::chrono::high_resolution_clock::now();
      us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1)
               .count();
      // std::cout << us << " us" << std::endl;
      lat += us;
    }
  }
  std::cout << "1000 seeks, average cost: " << lat << " us" << std::endl;

  /*End tracing*/
  db->EndTrace();
}
}  // namespace terark

void PrintHelp() {
  std::cout << "Usage:" << std::endl;
  std::cout << "\t./trace seek" << std::endl;
}

int main(const int argc, const char** argv) {
  if (argc < 2) {
    PrintHelp();
    return 1;
  }
  terark::TraceTest trace;
  // enable terarkdb by default
  setenv("TerarkZipTable_localTempDir", "./", true);
  if (memcmp(argv[1], "seek", 4) == 0) {
    trace.traceSeek();
  } else {
    std::cout << "Unsupported Operation!" << std::endl;
    PrintHelp();
    return 10;
  }
  return 0;
}
