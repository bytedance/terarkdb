cmake_minimum_required(VERSION 3.6)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c++14")

PROJECT(terarkdb-third-party)
#SET(CMAKE_VERBOSE_MAKEFILE ON)

# Snappy
SET(SNAPPY_BUILD_TESTS OFF CACHE BOOL "disable build test cache" FORCE)
SET(HAVE_LIBLZO2 OFF CACHE BOOL "disable liblzo2" FORCE)
ADD_SUBDIRECTORY(snappy)
SET_PROPERTY(TARGET snappy PROPERTY POSITION_INDEPENDENT_CODE ON)

INCLUDE(ExternalProject)

# Metrics
ExternalProject_Add(metrics2-project
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/metrics2-cmake
    CONFIGURE_COMMAND mkdir -p build && mkdir -p ${CMAKE_CURRENT_BINARY_DIR}/metrics2
    BUILD_IN_SOURCE 1
    BUILD_COMMAND cd build && cmake ../ && make -j $(nproc)
    # metrics2 doesn't have a install instruction, so we have to manully install it
    INSTALL_COMMAND cp build/libmetrics2.a ${CMAKE_CURRENT_BINARY_DIR}/metrics2/ && rm -rf ${CMAKE_CURRENT_SOURCE_DIR}/metrics2-cmake/build)
ADD_LIBRARY(metrics2 STATIC IMPORTED GLOBAL)
ADD_DEPENDENCIES(metrics2 metrics2-project)
SET_TARGET_PROPERTIES(metrics2 PROPERTIES
    IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/metrics2/libmetrics2.a
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
  FIND_PACKAGE(gflags QUIET)
  IF(NOT gflags_FOUND)
    MESSAGE("[terarkdb/third-party] gflags not find, building....")
    ExternalProject_Add(gflags-project
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/gflags
        CONFIGURE_COMMAND mkdir -p build
        BUILD_IN_SOURCE 1
        BUILD_COMMAND cd build && cmake ../ -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/gflags && make
        INSTALL_COMMAND cd build && make install)
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
