#include "terark_zip_table.h"
#include "terark_zip_common.h"
#include <terark/io/byte_swap.hpp>
#include <terark/util/throw.hpp>
#include <stdlib.h>
#include <ctime>
#ifdef _MSC_VER
# include <io.h>
#else
# include <sys/types.h>
# include <sys/stat.h>
# include <fcntl.h>
# include <cxxabi.h>
#endif

namespace rocksdb {

const char* StrDateTimeNow() {
  static thread_local char buf[64];
  time_t rawtime;
  time(&rawtime);
  struct tm* timeinfo = localtime(&rawtime);
  strftime(buf, sizeof(buf), "%F %T", timeinfo);
  return buf;
}

std::string demangle(const char* name) {
#ifdef _MSC_VER
  return name;
#else
  int status = -4; // some arbitrary value to eliminate the compiler warning
  terark::AutoFree<char> res(abi::__cxa_demangle(name, NULL, NULL, &status));
  return (status == 0) ? res.p : name;
#endif
}

void AutoDeleteFile::Delete() {
  if (!fpath.empty()) {
    ::remove(fpath.c_str());
    fpath.clear();
  }
}
AutoDeleteFile::~AutoDeleteFile() {
  if (!fpath.empty()) {
    ::remove(fpath.c_str());
  }
}

TempFileDeleteOnClose::~TempFileDeleteOnClose() {
  if (fp)
    this->close();
}

/// this->path is temporary filename template such as: /some/dir/tmpXXXXXX
void TempFileDeleteOnClose::open_temp() {
  if (!terark::fstring(path).endsWith("XXXXXX")) {
    THROW_STD(invalid_argument,
              "ERROR: path = \"%s\", must ends with \"XXXXXX\"", path.c_str());
  }
#if _MSC_VER
  if (int err = _mktemp_s(&path[0], path.size() + 1)) {
    THROW_STD(invalid_argument, "ERROR: _mktemp_s(%s) = %s"
        , path.c_str(), strerror(err));
  }
  this->open();
#else
  int fd = mkstemp(&path[0]);
  if (fd < 0) {
    int err = errno;
    THROW_STD(invalid_argument, "ERROR: mkstemp(%s) = %s", path.c_str(), strerror(err));
  }
  this->dopen(fd);
#endif
}
void TempFileDeleteOnClose::open() {
  fp.open(path.c_str(), "wb+");
  fp.disbuf();
  writer.attach(&fp);
}
void TempFileDeleteOnClose::dopen(int fd) {
  fp.dopen(fd, "wb+");
  fp.disbuf();
  writer.attach(&fp);
}
void TempFileDeleteOnClose::close() {
  assert(nullptr != fp);
  writer.resetbuf();
  fp.close();
  ::remove(path.c_str());
}
void TempFileDeleteOnClose::complete_write() {
  writer.flush_buffer();
  fp.rewind();
}

} // namespace rocksdb

