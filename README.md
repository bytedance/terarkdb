# TerarkDB

# 1. CMake Build

## Method 1: As a CMake subdirectory (Recommend)

1) Clone

```
cd {YOUR_PROJECT_DIR}
git submodule add git@code.byted.org:storage/terarkdb.git && cd terarkdb &&  git submodule update --init --recursive
```

2) Edit your CMakeLists.txt

```
add_subdirectory(terarkdb)
target_link_libraries({YOUR_TARGET} terarkdb)
```
3) Important Default Options

- CMAKE_BUILD_TYPE: Debug
- WITH_JEMALLOC: ON
- WITH_TESTS: OFF

It SHOULD works now, if it doesn't please contact us.


### Notes
- TerarkDB is built with zstd, lz4, snappy, zlib, gtest, boost by default, if you need these libraries, you can remove them from your higher level application.


## Method 2: Manully import to your existing project

1) clone & build

```
git clone git@code.byted.org:storage/terarkdb.git && cd terarkdb && git submodule update --init --recursive
./build.sh
```

2) linking

Directory:

```
  terarkdb/
        \___ output/
                \_____ include/
                \_____ lib/
                         \___ libterarkdb.a
                         \___ libzstd.a
                         \___ ...
```

We didn't archieve all static libraries together yet, so you have to pack all libraries to your target:

```
-Wl,-Bstatic \
-lterarkdb -lbz2 -ljemalloc -llz4 -lmetrics2 -lsnappy -lz -lterark-zip-r -lzstd \
-lboost_context -lboost_fiber -lboost_filesystem -lboost_system \
-Wl,-Bdynamic -pthread -lgomp
```
















# 2. Makefile build (deprecated)

### Note: If you can use CMake build or encounter any problem, try build.sh directly.

1. checkout the latest tag version
2. `./build.sh` will produces release and debug version in the same time
3. find static / dynamic libraries inside output directory

the output directory looks like this:

```
output
    \__ include
    \__ lib_static
        \__ librocksdb.a
        \__ ...
```

**IMPORTANT NOTICE**

- The static library `librocksdb.a` already includes `zstd`, `boost-filesystem` and `boost-fiber`
- Link library requrements (please link all required libraries manually):
 - `-lzstd` (embedded, no need to link again)
 - `-llz4` (optional)
 - `-lz` (optional)
 - `-lsnappy` (required)
 - `-lgomp` (required)
 - `-laio` (required)
 - `-lrt` (required)

If you are using boost outside terarkdb, you may want to link terarkdb's dynamic library since terarkdb itself is also using boost 1.70 and we've changed some of its code for better performance.

TerarkDB's dynamic library do not export its boost's symbol, so you are safe to use boost outside.


## 2.1. Usage
If you want to use it as original rocksdb, you can just include librocksdb and the headers.

If you want to enable TerarkDB, then you have to pass these environment variables before you start you application, e.g.


```
env TerarkZipTable_localTempDir=$PWD/terark-tempdir ./app_executable
```


## 2.2. Valgrind
If you want to use `Valgrind`, please build terarkdb this way:

```
USE_VALGRIND=1 ./build.sh
```

And add a new env when you start your application:

```
env Terark_hasValgrind=1 ./application
```

## 2.3. Non-Portable Build
If your compiler enviornment is same as your prodution environment, you can use `PORTABLE_BUILD=0 ./build.sh`


## 2.4. Flink
TerarkDB now support flink on Linux x86-64.

Usage:

Download latest terarkdb jar and replace your frocksdb jar, then start your application with this two env variables:

```
# temp dir for terarkdb, slower device is OK
TerarkZipTable_localTempDir=

# terark control parameters
TerarkConfigString=TerarkConfigString='TerarkZipTable_compaction_style=Level;DictZipBlobStore_zipThreads=0;TerarkZipTable_max_subcompactions=4;TerarkZipTable_max_background_flushes=6;TerarkZipTable_max_background_compactions=4;TerarkZipTable_max_background_garbage_collections=3;TerarkZipTable_level0_file_num_compaction_trigger=4;TerarkZipTable_level0_slowdown_writes_trigger=20;TerarkZipTable_level0_stop_writes_trigger=36;TerarkZipTable_max_compaction_bytes=256M;TerarkZipTable_max_write_buffer_number=8;TerarkZipTable_target_file_size_base=64M;TerarkZipTable_blob_size=64'
```
