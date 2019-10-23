#pragma once

#include <terark/io/DataIO.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/idx/terark_zip_index.hpp>

namespace rocksdb {

#if (defined(IOS_CROSS_COMPILE) || defined(__DARWIN_C_LEVEL)) && 0
# define MY_THREAD_LOCAL(Type, Var)  Type Var
//#elif defined(_WIN32)
//  #define MY_THREAD_LOCAL(Type, Var)  static __declspec(thread) Type Var
#else
# define MY_THREAD_LOCAL(Type, Var)  static thread_local Type Var
#endif

#if defined(IOS_CROSS_COMPILE) || defined(__DARWIN_C_LEVEL)
# define MY_THREAD_STATIC_LOCAL(Type, Var)  static Type Var
//# elif defined(_WIN32)
//  # define MY_THREAD_LOCAL(Type, Var)  static __declspec(thread) Type Var
#else
# define MY_THREAD_STATIC_LOCAL(Type, Var)  static thread_local Type Var
#endif

#define STD_INFO(format, ...) fprintf(stderr, "%s INFO: " format, StrDateTimeNow(), ##__VA_ARGS__)
#define STD_WARN(format, ...) fprintf(stderr, "%s WARN: " format, StrDateTimeNow(), ##__VA_ARGS__)

#undef INFO
#undef WARN
#if defined(NDEBUG) || 1
# define INFO(logger, format, ...) Info(logger, format, ##__VA_ARGS__)
# define WARN(logger, format, ...) Warn(logger, format, ##__VA_ARGS__)
# define WARN_EXCEPT(logger, format, ...) \
    WARN(logger, format, ##__VA_ARGS__); \
    LogFlush(logger); \
    STD_WARN(format, ##__VA_ARGS__)
#else
# define INFO(logger, format, ...) STD_INFO(format, ##__VA_ARGS__)
# define WARN(logger, format, ...) STD_WARN(format, ##__VA_ARGS__)
# define WARN_EXCEPT WARN
#endif

using std::string;
using std::unique_ptr;

using terark::byte_t;
using terark::fstring;
using terark::valvec;
using terark::valvec_no_init;
using terark::valvec_reserve;

using terark::FileStream;
using terark::InputBuffer;
using terark::OutputBuffer;
using terark::LittleEndianDataInput;
using terark::LittleEndianDataOutput;

template<class T>
inline unique_ptr<T> UniquePtrOf(T* p) { return unique_ptr<T>(p); }

template<class T>
inline void correct_minmax(T& minVal, T& maxVal) {
  if (maxVal < minVal) {
    using namespace std;
    swap(maxVal, minVal);
  }
}

template<class T>
T abs_diff(const T& x, const T& y) {
  if (x < y)
    return y - x;
  else
    return x - y;
}

const char* StrDateTimeNow();
std::string demangle(const char* name);

template<class T>
inline std::string ClassName() {
  return demangle(typeid(T).name());
}
template<class T>
inline std::string ClassName(const T& x) {
  return demangle(typeid(x).name());
}


} // namespace rocksdb
