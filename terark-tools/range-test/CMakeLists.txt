CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(range_test)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -DROCKSDB_PLATFORM_POSIX -D_DEBUG=1 -DDebugBreak=__builtin_trap -O0 -g3 -march=haswell -fsanitize=address")

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")

add_executable(range_test range_test.cc)

target_link_libraries(range_test terarkdb gtest)