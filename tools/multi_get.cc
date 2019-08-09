//
// Created by leipeng on 2019-08-09.
//

#include <rocksdb/db.h>
#include <stdio.h>
#include <getopt.h>
#include <terark/util/linebuf.hpp>
#include <terark/util/profiling.hpp>


static void usage(const char* prog) {
  fprintf(stderr, R"EOS(usage: %s

  -m number per multi_get

  -f use_fiber for multi_get

  -q
     be quite

)EOS"
, prog);
}

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
  size_t mget_num = 128;
  size_t bench_report = 0;
  size_t cnt1 = 0;

  rocksdb::Options dopt;
  rocksdb::ReadOptions ropt;
  dopt.use_aio_reads = true;
  dopt.use_direct_reads = true;
  ropt.use_fiber = true;
  bool quite = false;
  for (int opt=0; -1 != opt && '?' != opt;) {
    opt = getopt(argc, argv, "a:b:d:f:m:q");
    switch (opt) {
      default:
        usage(argv[0]);
        return 1;
      case -1:
        goto GetoptDone;
      case 'a':
        dopt.use_aio_reads = atoi(optarg) != 0;
        break;
      case 'b':
        bench_report = atoi(optarg);
        break;
      case 'd':
        dopt.use_direct_reads = atoi(optarg) != 0;
        break;
      case 'f':
        ropt.use_fiber = atoi(optarg) != 0;
        break;
      case 'm':
        mget_num = atoi(optarg);
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
  rocksdb::Status s = rocksdb::DB::OpenForReadOnly(dopt, path, &db);
  if (!s.ok()) {
    fprintf(stderr, "ERROR: Open(%s) = %s\n", path.c_str(), s.ToString().c_str());
    return 1;
  }
  using namespace terark;
  profiling pf;
  std::vector<rocksdb::Slice> keys;
  std::vector<std::string> values;
  std::string keystore;
  LineBuf line;
  long long t0 = pf.now();
  do {
    keys.resize(0);
    for (size_t i = 0; i < mget_num && line.getline(stdin) > 0; ++i) {
      line.chomp();
      keystore.append(line.p, line.n);
      keys.emplace_back(NULL, line.n);
    }
    const char* p = keystore.size();
    for (size_t i = 0; i < keys.size(); ++i) {
      keys[i].data_ = p;
      p += keys[i].size();
    }
    auto sv = db->MultiGet(ropt, keys, &values);
    if (bench_report) {
      cnt1 += mget_num;
      if (cnt1 >= bench_report) {
        auto t1 = pf.now();
        fprintf(stderr,
                "mget(use_fiber=%d,direct_io=%d,aio=%d) qps = %f M/sec\n",
                ropt.use_fiber,
                dopt.use_aio_reads,
                dopt.use_aio_reads,
                cnt1/pf.uf(t0,t1));
        t0 = t1;
        cnt1 = 0;
      }
    }
    if (!quite) {
      for (size_t i = 0; i < keys.size(); ++i) {
        if (sv[i].ok()) {
            chomp(values[i]);
            printf("OK\t%s\n", values[i].c_str());
        } else {
            printf("%s\t%s\n", sv[i].ToString().c_str(), values[i].c_str());
        }
      }
    }
  } while (keys.size() == mget_num);

  delete db;
  return 0;
}
