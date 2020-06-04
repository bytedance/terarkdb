Cmake_minimum_required(VERSION 2.8)
project(rocksdb_test)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -DROCKSDB_PLATFORM_POSIX -DOS_MACOSX -DDebugBreak=__builtin_debugtrap -O3 -g3")

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -DROCKSDB_PLATFORM_POSIX -D_DEBUG=1 -DDebugBreak=__builtin_trap -O2 -g3 -march=haswell -fsanitize=address")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -DROCKSDB_PLATFORM_POSIX -D_DEBUG=1 -DDebugBreak=__builtin_trap -O0 -g3 -march=haswell -fsanitize=address")

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -DROCKSDB_PLATFORM_POSIX -D_DEBUG=1 -DDebugBreak=__builtin_trap -O0 -g3 -Wframe-larger-than=4096 -Wstack-usage=4096")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -DROCKSDB_PLATFORM_POSIX -D_DEBUG=1 -DDebugBreak=__builtin_trap -O0 -g3")
#set(TERARKDB_CORE ../terark-zip-rocksdb/build/Darwin-x86_64-clang-10.0-bmi2-0)
set(TERARKDB_CORE ../../terark-core/build/Linux-x86_64-g++-6.3-bmi2-1)
include_directories(../../terark-core/boost-include)
include_directories(../../include)
include_directories(../../)
include_directories(../../terark-core/src)
link_directories(../../output/lib_static)
#link_directories(${TERARKDB_CORE}/lib_static)

#set(SOURCE_FILES delete_range_test.cpp)
add_executable(rocksdb_test chaos_test.cc)

target_link_libraries(
        rocksdb_test
        rocksdb_debug
        gomp
        stdc++
        pthread
        snappy
        bz2
        lz4
        z
	aio
	rt
        dl
)
