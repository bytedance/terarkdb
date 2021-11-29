Cmake_minimum_required(VERSION 3.0)
project(rocksdb_test)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -DROCKSDB_PLATFORM_POSIX -DOS_MACOSX -DDebugBreak=__builtin_debugtrap -O3 -g3")

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -DROCKSDB_PLATFORM_POSIX -D_DEBUG=1 -DDebugBreak=__builtin_trap -O2 -g3 -march=haswell -fsanitize=address")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -DROCKSDB_PLATFORM_POSIX -D_DEBUG=1 -DDebugBreak=__builtin_trap -O0 -g3 -march=haswell")
set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DROCKSDB_PLATFORM_POSIX -DDebugBreak=__builtin_trap")

#set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")

add_executable(rocksdb_test chaos_test.cc)

target_link_libraries(rocksdb_test terarkdb gtest)
