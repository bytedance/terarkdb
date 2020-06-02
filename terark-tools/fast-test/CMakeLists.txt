Cmake_minimum_required(VERSION 2.8)
project(fast_test)



#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -DROCKSDB_PLATFORM_POSIX -DDebugBreak=__builtin_debugtrap -O3 -g3")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -DROCKSDB_PLATFORM_POSIX -D_DEBUG=1 -DDebugBreak=__builtin_trap -O2 -g3")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -DROCKSDB_PLATFORM_POSIX -D_DEBUG=1 -DDebugBreak=__builtin_trap -O1 -g3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -DROCKSDB_PLATFORM_POSIX -D_DEBUG=1 -DDebugBreak=__builtin_trap -O0 -g3")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -DROCKSDB_PLATFORM_POSIX -D_DEBUG=1 -DDebugBreak=__builtin_trap -O0 -g3")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -flto -fsanitize=address")

include_directories(../..)
include_directories(../../include)
include_directories(../../terark-core/src)
include_directories(../../terark-core/boost-include)

link_directories(../..)

add_executable(fast_test test.cpp)

target_link_libraries(
        fast_test
        rocksdb_debug.a
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
