#include <rocksdb/db.h>
#include <stdio.h>
#include <getopt.h>
#include <terark/thread/pipeline.hpp>
#include <terark/fstring.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/profiling.hpp>

static void usage(const char* prog) {
  fprintf(stderr, R"EOS(usage: %s

  -a use_aio(default true)

  -b bench_report(default 0)

  -d use_direct_io(default true)

  -p pipeline_queue_size(default 32)

  -c concurrency
     format is thread_num:fiber_num, default is 1:32

  -C CacheSize
     for RocksDB BlockBasedTable

  -l log_level(default 3)

  -o max_file_opening_threads(default is 16)

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
  int queue_size = 32;
  int nthr = 1;
  int nfib = 32;
  int log_level = 0;
  size_t bench_report = 0;
  size_t cnt1 = 0, cnt2 = 0;
  rocksdb::Options opt;
  opt.use_aio_reads = true;
  opt.use_direct_reads = true;
  bool quite = false;
  for (int gopt=0; -1 != gopt && '?' != gopt;) {
    gopt = getopt(argc, argv, "a:b:c:C:d:p:D:l:o:q");
    switch (gopt) {
      default:
        usage(argv[0]);
        return 1;
      case -1:
        goto GetoptDone;
      case 'a':
        opt.use_aio_reads = atoi(optarg) != 0;
        break;
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
          nthr = (int)strtol(optarg, &endp, 10);
          if (':' == endp[0]) {
            nfib = atoi(endp+1);
          } else {
            nfib = 0; // thread mode
          }
        }
        break;
      case 'C':
	opt.OptimizeForPointLookup(terark::ParseSizeXiB(optarg)>>20);
        break;
      case 'd':
        opt.use_direct_reads = atoi(optarg) != 0;
        break;
      case 'p':
        queue_size = atoi(optarg);
        break;
      case 'l':
        log_level = atoi(optarg);
        break;
      case 'o':
        opt.max_file_opening_threads = atoi(optarg);
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

  int ncon = EUT::fiber == euType ? nfib : nthr;

  pipeline.setEUType(euType);
  pipeline.setQueueSize(queue_size);
  pipeline.setLogLevel(log_level);
  valvec<std::unique_ptr<PipelineTask> > taskpool(4096);
  std::mutex taskpool_mutex;
  pipeline.setDestroyTask([&](PipelineTask* task) {
    taskpool_mutex.lock();
    taskpool.emplace_back(task);
    taskpool_mutex.unlock();
  });
  pipeline | std::make_tuple(ncon, nfib, [db](PipelineTask* ptask) {
    KVTask* task = static_cast<KVTask*>(ptask);
    rocksdb::ReadOptions rdopt;
    task->status = db->Get(rdopt, task->key, &task->value);
    chomp(task->value);
  });
  long long start, t0;
  pipeline | std::make_pair(0, [&,quite,bench_report](PipelineTask* ptask) {
    if (bench_report) {
      if (++cnt1 == bench_report) {
        cnt2 += cnt1;
        auto t1 = pf.now();
        fprintf(stderr, "%s(%d:%d) qps.recent = %f M/sec, qps.long = %f M/sec\n",
                pipeline.euTypeName(), nthr, nfib, cnt1/pf.uf(t0,t1),
                cnt2/pf.uf(start,t1));
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
  start = t0 = pf.now();
  LineBuf line;
  while (line.getline(stdin) > 0) {
    line.chomp();
    taskpool_mutex.lock();
    KVTask* t = nullptr;
    if (!taskpool.empty()) {
      t = static_cast<KVTask*>(taskpool.pop_val().release());
    }
    taskpool_mutex.unlock();
    if (!t) {
      t = new KVTask(line.p, line.n);
    } else {
      t->key.assign(line.p, line.n);
    }
    pipeline.enqueue(t);
  }
  pipeline.stop();
  pipeline.wait();
  delete db;
  return 0;
}
