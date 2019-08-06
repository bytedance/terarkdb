#include <rocksdb/db.h>
#include <stdio.h>
#include <getopt.h>
#include <terark/thread/pipeline.hpp>
#include <terark/util/linebuf.hpp>

static void usage(const char* prog) {
  fprintf(stderr, R"EOS(usage: %s\n"

  -p parallel_type
     parallel_type should be "fiber" or "thread"

  -d queue_depth

  -c concurrency
     number of thread or fiber
)EOS"
, prog);
}

struct KVTask : terark::PipelineTask {
    std::string key;
    std::string value;
    rocksdb::Status status;
    KVTask(const char* k, size_t n) : key(k, n) {}
};

int main(int argc, char* argv[]) {
  int queue_depth = 32;
  int concurrency = 32;
  int log_level = 0;
  const char* parallel_type = "fiber";
  for (int opt=0; -1 != opt && '?' != opt;) {
    opt = getopt(argc, argv, "c:d:p:l:");
    switch (opt) {
      default:
        usage(argv[0]);
        return 1;
      case -1:
        goto GetoptDone;
      case 'c':
        concurrency = atoi(optarg);
        break;
      case 'd':
        queue_depth = atoi(optarg);
        break;
      case 'l':
        log_level = atoi(optarg);
        break;
      case 'p':
        parallel_type = optarg;
        break;
    }
  }
GetoptDone:
  if (optind >= argc) {
    usage(argv[0]);
    return 1;
  }
  rocksdb::DB* db = nullptr;
  rocksdb::Options opt;
  std::string path = argv[optind];
  rocksdb::Status s = rocksdb::DB::Open(opt, path, &db);
  if (!s.ok()) {
    fprintf(stderr, "ERROR: Open(%s) = %s\n", path.c_str(), s.ToString().c_str());
    return 1;
  }
  using namespace terark;
  PipelineProcessor pipeline;
  pipeline.setFiberMode(strcmp(parallel_type, "fiber") == 0);
  pipeline.setQueueSize(queue_depth);
  pipeline.setLogLevel(log_level);
  pipeline | std::make_pair(concurrency, [db](PipelineTask* ptask) {
    KVTask* task = static_cast<KVTask*>(ptask);
    rocksdb::ReadOptions rdopt;
    task->status = db->Get(rdopt, task->key, &task->value);
  });
  pipeline | std::make_pair(0, [](PipelineTask* ptask) {
    KVTask* task = static_cast<KVTask*>(ptask);
    if (task->status.ok()) {
      printf("OK\t%s\n", task->value.c_str());
    }
    else {
      printf("%s\t%s\n", task->status.ToString().c_str(), task->value.c_str());
    }
  });
  pipeline.compile();
  LineBuf line;
  while (line.getline(stdin) > 0) {
    line.chomp();
    auto t = new KVTask(line.p, line.n);
    pipeline.enqueue(t);
  }
  delete db;
  return 0;
}