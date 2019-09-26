//
// Created by leipeng on 2019-09-26.
//

#include <rocksdb/db.h>
#include <iostream>
#include <sstream>
#include <thread>
#include <terark/util/linebuf.hpp>

class MyWorker : public rocksdb::RemoteCompactionDispatcher::Worker {
  std::string GenerateOutputFileName(size_t file_index) override {
    // make a file name
    std::ostringstream oss;
    oss << "Worker-" << std::this_thread::get_id() << "-" << file_index;
    return oss.str();
  }

 public:
  using rocksdb::RemoteCompactionDispatcher::Worker::Worker;
};

int main() {
  rocksdb::EnvOptions env_options;
  MyWorker worker(env_options, rocksdb::Env::Default());

  //worker.RegistComparator(const Comparator*);
  //worker.RegistPrefixExtractor(std::shared_ptr<const SliceTransform>);
  //worker.RegistTableFactory(const char* Name, CreateTableFactoryCallback);
  //worker.RegistMergeOperator(CreateMergeOperatorCallback);
  //worker.RegistCompactionFilter(const CompactionFilter*);
  //worker.RegistCompactionFilterFactory(
  //    std::shared_ptr<CompactionFilterFactory>);
  //worker.RegistTablePropertiesCollectorFactory(
  //    std::shared_ptr<TablePropertiesCollectorFactory>);

  terark::LineBuf buf;
  buf.read_all(stdin);
  std::cout << worker.DoCompaction(std::string(buf.p, buf.n));
  return 0;
}

// a shell script calling this program:
// ----------------------------------------------
// env TerarkZipTable_localTempDir=/tmp remote_compaction_worker_101
// ----------------------------------------------
