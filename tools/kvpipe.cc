#include <rocksdb/db.h>
#include <stdio.h>
#include <getopt.h>
#include <terark/thread/pipeline.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/profiling.hpp>

static void usage(const char* prog) {
  fprintf(stderr, R"EOS(usage: %s

  -d queue_depth

  -c concurrency
     format is thread_num:fiber_num

)EOS"
, prog);
}

struct KVTask : terark::PipelineTask {
    std::string key;
    std::string value;
    rocksdb::Status status;
    KVTask(const char* k, size_t n) : key(k, n) {}
};

inline void chomp(std::string& s) {
  while (!s.empty()) {
    const char c = s.back();
    if ('\n' == c || '\r' == c) {
      s.pop_back();
    } else {
      break;
    }
  }
}

int main(int argc, char* argv[]) {
  int queue_depth = 32;
  int nthr = 1;
  int nfib = 32;
  int log_level = 0;
  size_t bench_report = 0;
  size_t cnt1 = 0;
  bool quite = false;
  for (int opt=0; -1 != opt && '?' != opt;) {
    opt = getopt(argc, argv, "b:c:d:l:q");
    switch (opt) {
      default:
        usage(argv[0]);
        return 1;
      case -1:
        goto GetoptDone;
      case 'b':
        bench_report = atoi(optarg);
        break;
      case 'c':
        if (':' == optarg[0]) {
          nthr = 0;
          nfib = atoi(optarg+1);
        }
        else {
          char* endp = NULL;
          nthr = strtol(optarg, &endp, 10);
          if (':' == endp[0]) {
            nfib = atoi(endp+1);
          } else {
            nfib = 0; // thread mode
          }
        }
        break;
      case 'd':
        queue_depth = atoi(optarg);
        break;
      case 'l':
        log_level = atoi(optarg);
        break;
      case 'q':
        quite = true;
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
  opt.use_aio_reads = true;
  opt.use_direct_reads = true;
  std::string path = argv[optind];
  rocksdb::Status s = rocksdb::DB::OpenForReadOnly(opt, path, &db);
  if (!s.ok()) {
    fprintf(stderr, "ERROR: Open(%s) = %s\n", path.c_str(), s.ToString().c_str());
    return 1;
  }
  using namespace terark;
  profiling pf;
  PipelineProcessor pipeline;
  typedef PipelineProcessor::EUType EUT;
  EUT euType;
  if (0 == nfib)
    euType = EUT::thread;
  else if (0 == nthr)
    euType = EUT::fiber;
  else
    euType = EUT::mixed;

  pipeline.setEUType(euType);
  pipeline.setQueueSize(queue_depth);
  pipeline.setLogLevel(log_level);
  pipeline | std::make_tuple(nthr, nfib, [db](PipelineTask* ptask) {
    KVTask* task = static_cast<KVTask*>(ptask);
    rocksdb::ReadOptions rdopt;
    task->status = db->Get(rdopt, task->key, &task->value);
    chomp(task->value);
  });
  auto t0 = pf.now();
  pipeline | std::make_pair(0, [&,quite,bench_report](PipelineTask* ptask) {
    if (bench_report) {
      if (++cnt1 == bench_report) {
        auto t1 = pf.now();
        fprintf(stderr, "%s(%d:%d) qps = %f M/sec\n",
                pipeline.euTypeName(), nthr, nfib, cnt1/pf.uf(t0,t1));
        t0 = t1;
        cnt1 = 0;
      }
    }
    if (quite) {
      return; // do nothing
    }
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
  pipeline.wait();
  delete db;
  return 0;
}