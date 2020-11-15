CMAKE_MINIMUM_REQUIRED(VERSION 3.1)
PROJECT(batch_write_bench)

SET(CMAKE_CXX_STANDARD 14)
MESSAGE("[terark-tools] Build batch_write_bench...")
ADD_EXECUTABLE(batch_write_bench batch_write_bench.cc)
TARGET_LINK_LIBRARIES(batch_write_bench terarkdb)
