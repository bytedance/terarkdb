#include "ioprof.h"

#include <sys/types.h>
#include <unistd.h>
#include <time.h>
#include <assert.h>

// Author: Haiping Zhao (ByteDance)
// Date: July 30, 2020

using namespace std;
using namespace IOProfiler;

///////////////////////////////////////////////////////////////////////////////////////////////////

Data Data::operator-(const Data &another) const {
  Data data = *this;
  data.rchar -= another.rchar;
  data.wchar -= another.wchar;
  data.syscr -= another.syscr;
  data.syscw -= another.syscw;
  data.read_bytes -= another.read_bytes;
  data.write_bytes -= another.write_bytes;
  data.cancelled_write_bytes -= another.cancelled_write_bytes;
  data.walltime -= another.walltime;
  data.error = (data.error || another.error);
  return data;
}

///////////////////////////////////////////////////////////////////////////////////////////////////

string read_file(const char *filename) {
  string content;
  FILE *f = fopen(filename, "r");
  if (f) {
    char buf[1024];
    size_t len = fread(buf, 1, sizeof(buf), f);
    while (len > 0) {
      content += string(buf, len);
      len = fread(buf, 1, sizeof(buf), f);
    }
    fclose(f);
  }
  return content;
}

bool decode_file(const string &content, Data &data) {
  struct {
    const char *name;
    int64 *field;
  } mappings[] = {
    { "rchar", &data.rchar },
    { "wchar", &data.wchar },
    { "syscr", &data.syscr },
    { "syscw", &data.syscw },
    { "read_bytes", &data.read_bytes },
    { "write_bytes", &data.write_bytes },
    { "cancelled_write_bytes", &data.cancelled_write_bytes },
  };
  size_t pos = 0;
  for (auto &mapping : mappings) {
    pos = content.find(mapping.name, pos);
    if (pos == string::npos) return false;
    pos = content.find(": ", pos);
    if (pos == string::npos) return false;
    auto end = content.find('\n', pos);
    if (end == string::npos || end - pos <= 2) return false;
    *mapping.field = atoi(content.substr(pos + 2, end - pos - 2).c_str());
  }
  return true;
}

Data LocalDiskCollector::Collect() {
  Data data{0};

  // getting disk io stats
  int64 pid = (int64)getpid();
  char filename[256];
  snprintf(filename, sizeof(filename), "/proc/%lld/io", pid);
  string content = read_file(filename);

  // getting wall time
  struct timespec tp;
  int res = clock_gettime(CLOCK_MONOTONIC, &tp);
  data.walltime = tp.tv_sec * 1000 * 1000 * 1000 + tp.tv_nsec;

  data.error = (res || content.empty() || !decode_file(content, data));
  return data;
}

void ConsoleReporter::Report(const std::string &fullname, const Data &data) {
  char buf[1024];
  if (!data.error) {
    snprintf(buf, sizeof(buf),
	     "%s\n"
	     "\trchar = %lld\n"
	     "\twchar = %lld\n"
	     "\tsyscr = %lld\n"
	     "\tsyscw = %lld\n"
	     "\tread_bytes = %lld\n"
	     "\twrite_bytes = %lld\n"
	     "\tcancelled_write_bytes = %lld\n"
	     "\twalltime = %lldns\n",
	     fullname.c_str(),
	     data.rchar, data.wchar, data.syscr, data.syscw, data.read_bytes, data.write_bytes,
	     data.cancelled_write_bytes, data.walltime);
    printf("\e[0;32m%s\e[m\n", buf);
  } else {
    snprintf(buf, sizeof(buf), "%s\n", fullname.c_str());
    printf("\e[0;31m%s\e[m\n", buf);
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

Profiler &Profiler::TheOne() { 
  static Profiler thread_local s_profiler;
  return s_profiler;
}

Profiler::Profiler() :
  m_collector(make_shared<LocalDiskCollector>()),
  m_reporter(make_shared<ConsoleReporter>()) {
}

Profiler::~Profiler() {
}

void Profiler::Initialize(const std::string &name,
			  const std::shared_ptr<Collector> &collector,
			  const std::shared_ptr<Reporter> &reporter) {
  assert(collector);
  m_name = name;
  m_collector = collector;
  m_reporter = reporter;
}

void Profiler::PushScope(const std::string &name) {
  m_stack.push_back(name);
}

void Profiler::PopScope() {
  m_stack.pop_back();
}

std::string Profiler::GetScope() const {
  return m_stack.empty() ? m_name : m_stack.back();
}

///////////////////////////////////////////////////////////////////////////////////////////////////

Scope::Scope(const std::string &name) :
  m_name(name) {
  auto &profiler = Profiler::TheOne();

  profiler.PushScope(name);
  m_start = profiler.GetCollector()->Collect();
}

void Scope::End() {
  auto &profiler = Profiler::TheOne();
  auto final = profiler.GetCollector()->Collect();
  auto delta = final - m_start;

  profiler.PopScope();
  auto fullname = m_name;
  auto scope = profiler.GetScope();
  if (!scope.empty()) {
    fullname += " <= " + scope;
  }

  profiler.GetReporter()->Report(fullname, delta);
}
