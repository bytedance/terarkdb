//
// Created by zhaoming.274 on 2022/3/18.
//

namespace TERARKDB_NAMESPACE {

enum DebugCallbackType {
  kDebugMessage = 0,      // output some message
  kDebugCoreDump = 1,     // dump core file for debugging
  kDebugRestart = 2,      // restart process
  kDebugUnavailable = 3,  // on-call engine team
};

// Debug callback message reserve prefix
// All notify msg starts-with kDebugMessageReservePrefix
extern const char* kDebugMessageReservePrefix;

// Debug callback setup successfully
extern const char* kDebugMessageInstallSuccessfully;
// Debug callback setup successfully
extern const char* kDebugMessageInstallFailed;
// Skipped a core dump request
extern const char* kDebugMessageSkipCoreDump;

void InvokeDebugCallback(DebugCallbackType type, const char* msg);

#if TERARKDB_DEBUG_LEVEL == 0

#define terarkdb_assert(exp) (void)0

#else

#define TERARKDB_STRINGIZE_DETAIL(x) #x
#define TERARKDB_STRINGIZE(x) TERARKDB_STRINGIZE_DETAIL(x)
#define terarkdb_assert(exp)                          \
  ((exp) ? (void)0                                    \
         : ::TERARKDB_NAMESPACE::InvokeDebugCallback( \
               kDebugUnavailable,                     \
               "[" __FILE__ "(" TERARKDB_STRINGIZE(__LINE__) ")]:" #exp))
#endif

// Only worked with build type Debug or RelWithDebInfo Disabled on Release
bool TerarkDBInstallDebugCallback(void (*debug_callback)(DebugCallbackType type,
                                                         const char* msg));

}  // namespace TERARKDB_NAMESPACE
