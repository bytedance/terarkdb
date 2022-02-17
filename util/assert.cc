#include "rocksdb/assert.h"

#include <features.h>

#include <iostream>

void terark_assert_fail(const char *assertion, const char *file,
                        unsigned int line, const char *function) __THROW {
  std::string err_msg = std::string("Assertion fails:") + assertion + ", at " +
                        file + ":" + std::to_string(line) + ":" + function;
  std::cerr << err_msg << std::endl;
  std::abort();
}
