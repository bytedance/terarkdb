CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(map_sst_performance_test)




set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -DROCKSDB_PLATFORM_POSIX -D_DEBUG=1 -DDebugBreak=__builtin_trap -O0 -g3 -march=haswell -fsanitize=address")

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")

add_executable(map_test map_test.cc)

target_link_libraries(map_test terarkdb gtest)