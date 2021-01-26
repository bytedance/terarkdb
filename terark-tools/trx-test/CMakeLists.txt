Cmake_minimum_required(VERSION 2.8)
project(trx_test)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -DROCKSDB_PLATFORM_POSIX -D_DEBUG=1 -DDebugBreak=__builtin_trap -O0 -g3 -march=haswell")

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS}")

add_executable(trx_test trx_test.cc)

target_link_libraries(trx_test terarkdb gtest)
