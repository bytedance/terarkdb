Cmake_minimum_required(VERSION 2.8)
project(bloat_test)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -DROCKSDB_PLATFORM_POSIX -D_DEBUG=1 -DDebugBreak=__builtin_trap -O0 -g3 -march=haswell -fsanitize=address -Wfatal-errors")

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -flto -fsanitize=address")
set(TERARKDB_CORE ../../terark-core/build/Linux-x86_64-g++-6.3-bmi2-1)

include_directories(../../terark-core/boost-include)
include_directories(../../include)
include_directories(../../third-party/gtest-1.7.0/fused-src/gtest)
include_directories(../../)
include_directories(../../terark-core/src)

link_directories(../../)
link_directories(../../..)
link_directories(../../terark-core/boost-include/stage/lib)

add_executable(bloat_test bloat_test.cc)
target_link_libraries(
        bloat_test
        rocksdb_debug
        gomp
        stdc++
        pthread
        snappy
        zstd
        lz4
        z
        aio
        rt
        dl
)
