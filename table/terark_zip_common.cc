#include "table/terark_zip_common.h"

#include <stdlib.h>

#include <ctime>
#include <terark/idx/terark_zip_index.hpp>
#include <terark/io/byte_swap.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/throw.hpp>

#include "table/terark_zip_table.h"

#ifdef _MSC_VER
#include <io.h>
#else
#include <cxxabi.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
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
  int status = -4;  // some arbitrary value to eliminate the compiler warning
  terark::AutoFree<char> res(abi::__cxa_demangle(name, NULL, NULL, &status));
  return (status == 0) ? res.p : name;
#endif
}

}  // namespace rocksdb
