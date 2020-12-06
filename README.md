# What is TerarkDB
TerarkDB is a better RocksDB replacement in terms of tail latency and write throughput, compression etc. In most cases you can migirate your existing RocksDB instance to TerarkDB without any
drawbacks.

**NOTES**
- TerarkDB was only tested under Linux platform
- Existing data can be migirated from RocksDB directly to TerarkDB, but cannot the other way around

# 1. Build TerarkDB

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
-Wl,-Bdynamic -pthread -lgomp -lrt -ldl -laio
```


# 2. Usage
## 2.1. BlockBasedTable
TODO

## 2.2. TerarkZipTable
TODO


# 3. Real-world Performance Improvement
TerarkDB has been deployed in lots of applications in Bytedance, in most cases TerarkDB can help to reduce overall latency and improve throughput tremendously.

TODO



# 4. License
TerarkDB is licensed under GPLv2 (Inherited from original [RocksDB](https://github.com/facebook/rocksdb))
