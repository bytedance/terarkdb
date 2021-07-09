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
        CONFIGURE_COMMAND cmake ${CMAKE_CURRENT_SOURCE_DIR}/snappy -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR} -DCMAKE_INSTALL_LIBDIR=${CMAKE_BINARY_DIR}/lib -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS=-fPIC -DCMAKE_CXX_FLAGS=-fPIC -DSNAPPY_BUILD_TESTS=OFF -DHAVE_LIBLZO2=OFF -DCMAKE_VERBOSE_MAKEFILE=${CMAKE_VERBOSE_MAKEFILE}
        BUILD_IN_SOURCE 0
        BUILD_COMMAND $(MAKE)
        INSTALL_COMMAND $(MAKE) install
    )
    ADD_LIBRARY(snappy STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(snappy snappy-project)
    SET_TARGET_PROPERTIES(snappy PROPERTIES
        IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libsnappy.a
        INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/include
        POSITION_INDEPENDENT_CODE ON
    )
  ENDIF()
ENDIF()

# Bytedance Internal Metrics Library
# You can either add metrics2 target or specify a BYTEDANCE_METRICS_PATH
IF(WITH_BYTEDANCE_METRICS)
  IF(TARGET metrics2)
    MESSAGE("[terarkdb/third-party] bytedance_metrics2 target exist, re-use it!")
  ELSE()
    MESSAGE("[terarkdb/third-party] bytedance_metrics2 target not exist, let's build it!")
    ExternalProject_Add(metrics2-project
        SOURCE_DIR ${BYTEDANCE_METRICS_PATH}
        CONFIGURE_COMMAND cmake ${BYTEDANCE_METRICS_PATH} -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS=-fPIC -DCMAKE_CXX_FLAGS=-fPIC -DCMAKE_VERBOSE_MAKEFILE=${CMAKE_VERBOSE_MAKEFILE}
        BUILD_IN_SOURCE 0
        BUILD_COMMAND $(MAKE)
        # metrics2 doesn't have a install instruction, so we have to manully install it
        INSTALL_COMMAND cp libmetrics2.a ${CMAKE_BINARY_DIR}/lib
    )
    ADD_LIBRARY(metrics2 STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(metrics2 metrics2-project)
    SET_TARGET_PROPERTIES(metrics2 PROPERTIES
        IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libmetrics2.a
        INCLUDE_DIRECTORIES ${BYTEDANCE_METRICS_PATH}
        POSITION_INDEPENDENT_CODE ON
    )
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
        CONFIGURE_COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/zlib/configure --prefix=${CMAKE_BINARY_DIR} --static
        BUILD_IN_SOURCE 0
        BUILD_COMMAND $(MAKE) "CFLAGS=-fPIC -O2"
        INSTALL_COMMAND $(MAKE) install
    )
    ADD_LIBRARY(zlib STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(zlib zlib-project)
    SET_TARGET_PROPERTIES(zlib PROPERTIES
        IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libz.a
        INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/include
        POSITION_INDEPENDENT_CODE ON
    )
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
        CONFIGURE_COMMAND cmake ${CMAKE_CURRENT_SOURCE_DIR}/gflags -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR} -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS=-fPIC -DCMAKE_CXX_FLAGS=-fPIC -DCMAKE_VERBOSE_MAKEFILE=${CMAKE_VERBOSE_MAKEFILE}
        BUILD_IN_SOURCE 0
        BUILD_COMMAND $(MAKE)
        INSTALL_COMMAND $(MAKE) install
    )
    ADD_LIBRARY(gflags STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(gflags gflags-project)
    SET_TARGET_PROPERTIES(gflags PROPERTIES
      IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libgflags.a
      INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/include
      POSITION_INDEPENDENT_CODE ON
    )
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
        CONFIGURE_COMMAND cp -rT ${CMAKE_CURRENT_SOURCE_DIR}/zstd .
        BUILD_IN_SOURCE 0
        BUILD_COMMAND $(MAKE) "CXXFLAGS=-fPIC -O2" "CFLAGS=-fPIC -O2"
        INSTALL_COMMAND PREFIX=${CMAKE_BINARY_DIR} $(MAKE) install
    )
    ADD_LIBRARY(zstd STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(zstd zstd-project)
    SET_TARGET_PROPERTIES(zstd PROPERTIES
        IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libzstd.a
        INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/include
        POSITION_INDEPENDENT_CODE ON
    )
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
        CONFIGURE_COMMAND cp -rT ${CMAKE_CURRENT_SOURCE_DIR}/lz4 .
        BUILD_IN_SOURCE 0
        BUILD_COMMAND $(MAKE) "CXXFLAGS=-fPIC -O2" "CFLAGS=-fPIC -O2"
        INSTALL_COMMAND prefix=${CMAKE_BINARY_DIR} $(MAKE) install)
    ADD_LIBRARY(lz4 STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(lz4 lz4-project)
    SET_TARGET_PROPERTIES(lz4 PROPERTIES
        IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/liblz4.a
        INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/include
        POSITION_INDEPENDENT_CODE ON
    )
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
        CONFIGURE_COMMAND cp -rT ${CMAKE_CURRENT_SOURCE_DIR}/bzip2 .
        BUILD_IN_SOURCE 0
        BUILD_COMMAND $(MAKE) "CXXFLAGS=-fPIC -O2" "CFLAGS=-fPIC -O2"
        INSTALL_COMMAND $(MAKE) install PREFIX=${CMAKE_BINARY_DIR})
    ADD_LIBRARY(bzip2 STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(bzip2 bzip2-project)
    SET_TARGET_PROPERTIES(bzip2 PROPERTIES
        IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libbz2.a
        INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/include
        POSITION_INDEPENDENT_CODE ON
    )
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
        CONFIGURE_COMMAND cp -rT ${CMAKE_CURRENT_SOURCE_DIR}/jemalloc . && bash autogen.sh && "CFLAGS=-fPIC" "CXXFLAGS=-fPIC" "LDFLAGS=-fPIC" ./configure --prefix=${CMAKE_BINARY_DIR} --enable-prof
        BUILD_IN_SOURCE 0
        BUILD_COMMAND $(MAKE)
        INSTALL_COMMAND $(MAKE) install PREFIX=${CMAKE_BINARY_DIR}
    )
    ADD_LIBRARY(jemalloc STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(jemalloc jemalloc-project)
    SET_TARGET_PROPERTIES(jemalloc PROPERTIES
        IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libjemalloc.a
        INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/include
        POSITION_INDEPENDENT_CODE ON
    )
  endif()
ENDIF()

# terark-zip (for TerarkZipTable)
IF(WITH_TERARK_ZIP)
  SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
  add_subdirectory(terark-zip)
ENDIF()
