//
// Created by zhaoming.274 on 2022/4/15.
//

#include <mutex>

#include "rocksdb/terark_namespace.h"
#include "util/sync_point.h"

namespace TERARKDB_NAMESPACE {

#define TERARKDB_RESERVE_PREFIX "$TerarkDB:"
const char* kDebugMessageReservePrefix = TERARKDB_RESERVE_PREFIX;

const char* kDebugMessageInstallSuccessfully =
    TERARKDB_RESERVE_PREFIX "InstallSuccessfully";
const char* kDebugMessageInstallFailed =
    TERARKDB_RESERVE_PREFIX "InstallFailed";
const char* kDebugMessageSkipCoreDump = TERARKDB_RESERVE_PREFIX "SkipCoreDump";

#undef TERARKDB_RESERVE_PREFIX

#if TERARKDB_DEBUG_LEVEL != 0

void DebugCallbackAbort(DebugCallbackType type, const char* msg) {
  const char* type_msg[] = {"Message", "CoreDump", "Restart", "Unavailable"};
  fprintf(stderr, "Debug: %s:%s\n", type_msg[type], msg);
  if (type != kDebugMessage) {
    std::abort();
  }
}

static void (*g_debug_callback)(DebugCallbackType type,
                                const char* msg) = &DebugCallbackAbort;
static std::mutex g_debug_term_mutex;

void InvokeDebugCallback(DebugCallbackType type, const char* msg) {
  if (type == kDebugMessage) {
    g_debug_callback(kDebugMessage, msg);
    return;
  }
  if (g_debug_term_mutex.try_lock()) {
    g_debug_callback(type, msg);
  } else {
    g_debug_callback(kDebugMessage, kDebugMessageSkipCoreDump);
  }
  if (type != kDebugCoreDump) {
    TEST_SYNC_POINT_CALLBACK("InvokeDebugCallback:Lock", &g_debug_term_mutex);
    g_debug_term_mutex.lock();
  }
}
#else

void InvokeDebugCallback(DebugCallbackType type, const char* msg) {}

#endif

bool TerarkDBInstallDebugCallback(void (*debug_callback)(DebugCallbackType type,
                                                         const char* msg)) {
#if TERARKDB_DEBUG_LEVEL == 0
  if (debug_callback != nullptr) {
    debug_callback(kDebugMessage, kDebugMessageInstallFailed);
  }
  return false;
#else
  if (debug_callback == nullptr) {
    g_debug_callback = &DebugCallbackAbort;
  } else {
    g_debug_callback = debug_callback;
    g_debug_callback(kDebugMessage, kDebugMessageInstallSuccessfully);
  }
  return true;
#endif
}

}  // namespace TERARKDB_NAMESPACE