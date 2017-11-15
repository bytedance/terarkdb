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

#if defined(TerocksPrivateCode)

inline uint64_t ReadBigEndianUint64(const byte_t* beg, size_t len) {
  union {
    byte_t bytes[8];
    uint64_t value;
  } c;
  c.value = 0;  // this is fix for gcc-4.8 union init bug
  memcpy(c.bytes + (8 - len), beg, len);
  return VALUE_OF_BYTE_SWAP_IF_LITTLE_ENDIAN(c.value);
}

inline uint64_t ReadBigEndianUint64(const byte_t* beg, const byte_t* end) {
  assert(end - beg <= 8);
  return ReadBigEndianUint64(beg, end-beg);
}

inline
uint64_t ReadBigEndianUint64Aligned(const byte_t* beg, size_t len) {
  assert(8 == len); (void)len;
  return VALUE_OF_BYTE_SWAP_IF_LITTLE_ENDIAN(*(const uint64_t*)beg);
}
inline
uint64_t ReadBigEndianUint64Aligned(const byte_t* beg, const byte_t* end) {
  assert(end - beg == 8); (void)end;
  return VALUE_OF_BYTE_SWAP_IF_LITTLE_ENDIAN(*(const uint64_t*)beg);
}

inline void SaveAsBigEndianUint64(byte_t* beg, size_t len, uint64_t value) {
  assert(len <= 8);
  union {
    byte_t bytes[8];
    uint64_t value;
  } c;
  c.value = VALUE_OF_BYTE_SWAP_IF_LITTLE_ENDIAN(value);
  memcpy(beg, c.bytes + (8 - len), len);
}

inline void SaveAsBigEndianUint64(byte_t* beg, byte_t* end, uint64_t value) {
  assert(end - beg <= 8);
  SaveAsBigEndianUint64(beg, end-beg, value);
}

#endif // TerocksPrivateCode

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
