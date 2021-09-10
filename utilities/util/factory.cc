// Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License.

#include "rocksdb/terark_namespace.h"

#ifndef WITH_TERARK_ZIP
#include "utilities/util/factory.h"
//#include <terark/hash_strmap.hpp>
// #include <terark/gold_hash_map.hpp>

namespace terark {
///@note on principle, the factory itself is stateless, but its product
/// can has states, sometimes we need factory of factory, in this case,
/// just let the factory being factoryable:
/// class SomeFactory : public Factoyable<SomeFactory> {
///    ...
/// };

bool getEnvBool(const char* envName, bool Default) {
  if (const char* env = getenv(envName)) {
    if (isdigit(env[0])) {
      return atoi(env) != 0;
    }
#if defined(_MSC_VER)
#define strcasecmp stricmp
#endif
    if (strcasecmp(env, "true") == 0) return true;
    if (strcasecmp(env, "false") == 0) return false;
    fprintf(stderr,
            "WARN: terark::getEnvBool(\"%s\") = \"%s\" is invalid, treat as "
            "false\n",
            envName, env);
  }
  return Default;
}

long getEnvLong(const char* envName, long Default) {
  if (const char* env = getenv(envName)) {
    int base = 0;  // env can be oct, dec, hex
    return strtol(env, NULL, base);
  }
  return Default;
}

double getEnvDouble(const char* envName, double Default) {
  if (const char* env = getenv(envName)) {
    return strtof(env, NULL);
  }
  return Default;
}

}  // namespace terark

#endif