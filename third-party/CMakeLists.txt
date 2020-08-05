cmake_minimum_required(VERSION 3.6)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")

PROJECT(terarkdb-third-party)
#SET(CMAKE_VERBOSE_MAKEFILE ON)

INCLUDE(ExternalProject)

# Snappy
IF(WITH_SNAPPY)
  FIND_PACKAGE(snappy QUIET)
  IF (NOT snappy_FOUND)
    MESSAGE("[terarkdb/third-party] snappy not found, build target")
    ExternalProject_Add(snappy-project
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/snappy
        CONFIGURE_COMMAND mkdir -p build
        BUILD_IN_SOURCE 1
        BUILD_COMMAND cd build && cmake ../ -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR} -DSNAPPY_BUILD_TESTS=OFF -DHAVE_LIBLZO2=OFF && make -j $(nproc)
        INSTALL_COMMAND cd build && make install && cd ../ && rm -rf build)
    ADD_LIBRARY(snappy STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(snappy snappy-project)
    SET_TARGET_PROPERTIES(snappy PROPERTIES
        IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libsnappy.a
        INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/include)
  ELSE()
    MESSAGE("[terarkdb/third-party] snappy exist, not build")
  ENDIF()
ENDIF()

# Metrics
ExternalProject_Add(metrics2-project
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/metrics2-cmake
    CONFIGURE_COMMAND mkdir -p cmake-build && mkdir -p ${CMAKE_BINARY_DIR}/lib
    BUILD_IN_SOURCE 1
    BUILD_COMMAND cd cmake-build && cmake ../ && make -j $(nproc)
    # metrics2 doesn't have a install instruction, so we have to manully install it
    INSTALL_COMMAND cp cmake-build/libmetrics2.a ${CMAKE_BINARY_DIR}/lib/)
ADD_LIBRARY(metrics2 STATIC IMPORTED GLOBAL)
ADD_DEPENDENCIES(metrics2 metrics2-project)
SET_TARGET_PROPERTIES(metrics2 PROPERTIES
    IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libmetrics2.a
    INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/metrics2/)

# Zlib
ExternalProject_Add(zlib-project
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/zlib
    CONFIGURE_COMMAND mkdir -p build
    BUILD_IN_SOURCE 1
    BUILD_COMMAND cd build && cmake ../ -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/zlib && make
    # zlib compile will produce some dirty files, we need to clean them.
    INSTALL_COMMAND cd build && make install && cd ../ && rm -rf build zconf.h.included && git checkout . )
ADD_LIBRARY(zlib STATIC IMPORTED GLOBAL)
ADD_DEPENDENCIES(zlib zlib-project)
SET_TARGET_PROPERTIES(zlib PROPERTIES
    IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/zlib/lib/libz.a
    INCLUDE_DIRECTORIES ${CMAKE_CURRENT_BINARY_DIR}/zlib/include)

# Gflags
IF(WITH_GFLAGS)
  IF(NOT TARGET gflags)
    FIND_PACKAGE(gflags QUIET)
  ELSE()
	MESSAGE("[terarkdb/third-party] target gflags existed")
  ENDIF()

  IF(NOT TARGET gflags AND NOT gflags_FOUND)
    MESSAGE("[terarkdb/third-party] gflags not find, build target")
    ExternalProject_Add(gflags-project
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/gflags
        CONFIGURE_COMMAND mkdir -p build-cmake
        BUILD_IN_SOURCE 1
        BUILD_COMMAND cd build-cmake && cmake ../ -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/gflags && make
        INSTALL_COMMAND cd build-cmake && make install)
    ADD_LIBRARY(gflags STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(gflags gflags-project)
    SET_TARGET_PROPERTIES(gflags PROPERTIES
        IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/gflags/lib/libgflags.a
        INCLUDE_DIRECTORIES ${CMAKE_CURRENT_BINARY_DIR}/gflags/include)
  ELSE()
    MESSAGE("[terarkdb/third-party] gflags exist, skip build")
  ENDIF()
ENDIF()

# ZSTD
ExternalProject_Add(zstd-project
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/zstd
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE 1
    BUILD_COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/zstd && make "CXXFLAGS=-fPIC -O2" "CFLAGS=-fPIC -O2"
    INSTALL_COMMAND prefix=${CMAKE_CURRENT_BINARY_DIR}/zstd make install)
ADD_LIBRARY(zstd STATIC IMPORTED GLOBAL)
ADD_DEPENDENCIES(zstd zstd-project)
SET_TARGET_PROPERTIES(zstd PROPERTIES
    IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/zstd/lib/libzstd.a
    INCLUDE_DIRECTORIES ${CMAKE_CURRENT_BINARY_DIR}/zstd/include
    POSITION_INDEPENDENT_CODE ON)

# LZ4
ExternalProject_Add(lz4-project
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/lz4
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE 1
    BUILD_COMMAND make "CXXFLAGS=-fPIC -O2" "CFLAGS=-fPIC -O2"
    INSTALL_COMMAND prefix=${CMAKE_CURRENT_BINARY_DIR}/lz4 make install)
ADD_LIBRARY(lz4 STATIC IMPORTED GLOBAL)
ADD_DEPENDENCIES(lz4 lz4-project)
SET_TARGET_PROPERTIES(lz4 PROPERTIES
    IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/lz4/lib/liblz4.a
    INCLUDE_DIRECTORIES ${CMAKE_CURRENT_BINARY_DIR}/lz4/include)
