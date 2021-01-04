cmake_minimum_required(VERSION 3.6)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")

PROJECT(terarkdb-third-party)
#SET(CMAKE_VERBOSE_MAKEFILE ON)

INCLUDE(ExternalProject)

# Gtest
IF(WITH_TESTS OR WITH_TOOLS)
  SET(GTEST_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/gtest-1.8.1/fused-src)
  if(TARGET gtest)
    MESSAGE("[terarkdb/third-party] gtest target provided, skip local build") 
    include_directories(${GTEST_SRC_DIR})
  else()
    MESSAGE("[terarkdb/third-party] gtest target not exist, let's build it!")
    ADD_SUBDIRECTORY(${GTEST_SRC_DIR}/gtest)
  endif()
ENDIF()

# Snappy
IF(WITH_SNAPPY)
  IF(TARGET snappy)
    MESSAGE("[terarkdb/third-party] snappy target exist, re-use it!")
  ELSE()
    MESSAGE("[terarkdb/third-party] snappy target not exist, let's build it!")
    ExternalProject_Add(snappy-project
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/snappy
        CONFIGURE_COMMAND rm -rf build && mkdir -p build
        BUILD_IN_SOURCE 1
        BUILD_COMMAND cd build && cmake ../ -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR} -DCMAKE_INSTALL_LIBDIR=${CMAKE_BINARY_DIR}/lib -DCMAKE_BUILD_TYPE=Release -DSNAPPY_BUILD_TESTS=OFF -DHAVE_LIBLZO2=OFF && make -j $(nproc)
        INSTALL_COMMAND cd build && ls -l && make install && cd ../ && rm -rf build)
    ADD_LIBRARY(snappy STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(snappy snappy-project)
    SET_TARGET_PROPERTIES(snappy PROPERTIES
        IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libsnappy.a
        INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/include)
  ENDIF()
ENDIF()

# Bytedance Internal Metrics Library
IF(WITH_BYTEDANCE_METRICS)	
  IF(TARGET metrics2)
    MESSAGE("[terarkdb/third-party] bytedance_metrics2 target exist, re-use it!")	
  ELSE()	
    MESSAGE("[terarkdb/third-party] bytedance_metrics2 target not exist, let's build it!")	
    ExternalProject_Add(metrics2-project	
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/metrics2-cmake	
        CONFIGURE_COMMAND rm -rf cmake-build && mkdir -p cmake-build && mkdir -p ${CMAKE_BINARY_DIR}/lib	
        BUILD_IN_SOURCE 1	
        BUILD_COMMAND cd cmake-build && cmake ../ -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} && make -j $(nproc)
        # metrics2 doesn't have a install instruction, so we have to manully install it	
        INSTALL_COMMAND cp cmake-build/libmetrics2.a ${CMAKE_BINARY_DIR}/lib/)	
    ADD_LIBRARY(metrics2 STATIC IMPORTED GLOBAL)	
    ADD_DEPENDENCIES(metrics2 metrics2-project)	
    SET_TARGET_PROPERTIES(metrics2 PROPERTIES	
        IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libmetrics2.a	
        INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/metrics2-cmake/)	
  ENDIF()	
ENDIF()

# Zlib
IF(WITH_ZLIB)
  IF(TARGET zlib)
    MESSAGE("[terarkdb/third-party] zlib target existed, re-use it!")
  ELSE()
    MESSAGE("[terarkdb/third-party] zlib target not exist, let's build it!")
    ExternalProject_Add(zlib-project
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/zlib
        CONFIGURE_COMMAND rm -rf build && mkdir -p build
        BUILD_IN_SOURCE 1
        BUILD_COMMAND cd build && cmake ../ -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR} -DCMAKE_BUILD_TYPE=Release && make
        # zlib compile will produce some dirty files, we need to clean them.
        INSTALL_COMMAND cd build && make install && cd ../ && rm -rf build zconf.h.included && git checkout . )
    ADD_LIBRARY(zlib STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(zlib zlib-project)
    SET_TARGET_PROPERTIES(zlib PROPERTIES
      IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libz.a
      INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/include)
  ENDIF()
ENDIF()

# Gflags
IF(WITH_GFLAGS)
  IF(TARGET gflags)
    MESSAGE("[terarkdb/third-party] gflags target existed, re-use it!")
  ELSE()
    MESSAGE("[terarkdb/third-party] gflags target not exist, let's build it!")
    ExternalProject_Add(gflags-project
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/gflags
        CONFIGURE_COMMAND rm -rf build-cmake && mkdir -p build-cmake
        BUILD_IN_SOURCE 1
        BUILD_COMMAND cd build-cmake && cmake ../ -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR} -DCMAKE_BUILD_TYPE=Release && make -j $(nproc)
        INSTALL_COMMAND cd build-cmake && make install)
    ADD_LIBRARY(gflags STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(gflags gflags-project)
    SET_TARGET_PROPERTIES(gflags PROPERTIES
      IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libgflags.a
      INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/include)
  ENDIF() # end target
ENDIF() # end with gflags

# ZSTD
IF(WITH_ZSTD)
  IF(TARGET zstd)
    MESSAGE("[terarkdb/third-party] zstd target existed, re-use it!")
  ELSE()
    MESSAGE("[terarkdb/third-party] zstd target not exist, let's build it!")
    ExternalProject_Add(zstd-project
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/zstd
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/zstd && make "CXXFLAGS=-fPIC -O2" "CFLAGS=-fPIC -O2" -j $(nproc)
        INSTALL_COMMAND PREFIX=${CMAKE_BINARY_DIR} make install)
    ADD_LIBRARY(zstd STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(zstd zstd-project)
    SET_TARGET_PROPERTIES(zstd PROPERTIES
        IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libzstd.a
        INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/include
        POSITION_INDEPENDENT_CODE ON)
  ENDIF()
ENDIF()

# LZ4
IF(WITH_LZ4)
  IF(TARGET lz4)
    MESSAGE("[terarkdb/third-party] lz4 target exist, re-use it!")
  ELSE()
    MESSAGE("[terarkdb/third-party] lz4 target not exist, let's build it!")
    ExternalProject_Add(lz4-project
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/lz4
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND make "CXXFLAGS=-fPIC -O2" "CFLAGS=-fPIC -O2" -j $(nproc)
        INSTALL_COMMAND prefix=${CMAKE_BINARY_DIR} make install)
    ADD_LIBRARY(lz4 STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(lz4 lz4-project)
    SET_TARGET_PROPERTIES(lz4 PROPERTIES
        IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/liblz4.a
        INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/include)
  ENDIF()
ENDIF()

# bzip2
IF(WITH_BZ2)
  IF(TARGET bzip2)
    MESSAGE("[terarkdb/third-party] bzip2 target exist, re-use it!")
  ELSE()
    MESSAGE("[terarkdb/third-party] bzip2 target not exist, let's build it!")
    ExternalProject_Add(bzip2-project
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/bzip2
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND make "CXXFLAGS=-fPIC -O2" "CFLAGS=-fPIC -O2" -j $(nproc)
        INSTALL_COMMAND make install PREFIX=${CMAKE_BINARY_DIR} && make clean)
    ADD_LIBRARY(bzip2 STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(bzip2 bzip2-project)
    SET_TARGET_PROPERTIES(bzip2 PROPERTIES
        IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libbz2.a
        INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/include)
  ENDIF()
ENDIF()
 
# Jemalloc
IF(WITH_JEMALLOC)
  if(TARGET jemalloc)
    MESSAGE("[terarkdb/third-party] jemalloc target exist, re-use it!")
  else()
    MESSAGE("[terarkdb/third-party] jemalloc target not exist, let's build it!")
    ExternalProject_Add(jemalloc-project
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/jemalloc
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND bash autogen.sh && "CFLAGS=-fPIC" "CXXFLAGS=-fPIC" "LDFLAGS=-fPIC" ./configure --prefix=${CMAKE_BINARY_DIR} --enable-prof && make -j $(nproc)
        INSTALL_COMMAND make install PREFIX=${CMAKE_BINARY_DIR} && make clean)
    ADD_LIBRARY(jemalloc STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(jemalloc jemalloc-project)
    SET_TARGET_PROPERTIES(jemalloc PROPERTIES
      IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libjemalloc.a
      INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/include)
  endif()
ENDIF()

# terark-zip (for TerarkZipTable)
IF(WITH_TERARK_ZIP)
  add_subdirectory(terark-zip)
ENDIF()
