#pragma once

#include <terark/io/DataIO.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>

namespace rocksdb {

#if defined(IOS_CROSS_COMPILE) || defined(__DARWIN_C_LEVEL)
  #define MY_THREAD_LOCAL(Type, Var)  Type Var
//#elif defined(_WIN32)
//  #define MY_THREAD_LOCAL(Type, Var)  static __declspec(thread) Type Var
#else
  #define MY_THREAD_LOCAL(Type, Var)  static thread_local Type Var
#endif

#define STD_INFO(format, ...) fprintf(stderr, "%s INFO: " format, StrDateTimeNow(), ##__VA_ARGS__)
#define STD_WARN(format, ...) fprintf(stderr, "%s WARN: " format, StrDateTimeNow(), ##__VA_ARGS__)

#undef INFO
#undef WARN
#if defined(NDEBUG) || 1
# define INFO(logger, format, ...) Info(logger, format, ##__VA_ARGS__)
# define WARN(logger, format, ...) Warn(logger, format, ##__VA_ARGS__)
#else
# define INFO(logger, format, ...) STD_INFO(format, ##__VA_ARGS__)
# define WARN(logger, format, ...) STD_WARN(format, ##__VA_ARGS__)
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

uint64_t ReadUint64(const byte_t* beg, size_t len);
uint64_t ReadUint64(const byte_t* beg, const byte_t* end);

inline
uint64_t ReadUint64Aligned(const byte_t* beg, size_t len) {
  assert(8 == len); (void)len;
#if BOOST_ENDIAN_LITTLE_BYTE
  return terark::byte_swap(*(const uint64_t*)beg);
#else
  return *(const uint64_t*)beg;
#endif
}

uint64_t ReadUint64Aligned(const byte_t* beg, const byte_t* end);
void AssignUint64(byte_t* beg, byte_t* end, uint64_t value);

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

class AutoDeleteFile {
public:
  std::string fpath;
  operator fstring() const { return fpath; }
  void Delete();
  ~AutoDeleteFile();
};

class TempFileDeleteOnClose {
public:
  std::string path;
  FileStream  fp;
  NativeDataOutput<OutputBuffer> writer;
  ~TempFileDeleteOnClose();
  void open_temp();
  void open();
  void dopen(int fd);
  void close();
  void complete_write();
};

} // namespace rocksdb
