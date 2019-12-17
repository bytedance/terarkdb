# TerarkDB

## Dependencies
- terark-core
  - Terark's core algorithms, including CO-Index and PA-Zip etc.

## Documentation
[Documentation](https://bytedance.feishu.cn/space/doc/doccnPkcQEZ10MmaIKZTow#)


## Build

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


## Usage
If you want to use it as original rocksdb, you can just include librocksdb and the headers.

If you want to enable TerarkDB, then you have to pass these environment variables before you start you application, e.g.


```
env TerarkZipTable_localTempDir=$PWD/terark-tempdir ./app_executable
```


## Valgrind
If you want to use `Valgrind`, please build terarkdb this way:

```
USE_VALGRIND=1 ./build.sh
```

And add a new env when you start your application:

```
env Terark_hasValgrind=1 ./application
```


## CHANGELOG
2019-12-16
- Use new Compaction Style = Level
- Fix few issues
- Enable commit in the middle by default

2019-12-09
- Fix TerarkDB iterator.seek_prev bug, may cause data corruption.

2019-12-04
- Improvement: remove useless compaction

2019-12-03
- Add metrics for grafana on storage engine level

2019-12-02
- Update TerarkDB for better compaction

2019-11-28
- Fix TerarkDB bug: sstable map may lose some data

2019-11-08
- Add KV Seperation for LSM, for better compaction performance
- Bug fix

2019-11-05
- Fix rocksdb's super version bug, which may cause merge operator gets wrong value.
- Fix terarkdb's kv seperation bug
- Add more test cases in terark-core

2019-09-26
- Fix LocalTempDir is not set correctly

2019-09-12
- Update TerarkDB, refine build script

2019-08-29
- 解决 MEMTABLE FLUSH CRASH 问题，这是由一个手误引入的 BUG

2019-08-28
- Fix Bug: 解决 TerarkDB OOM 的问题

2019-08-21
- add co-routina by using boost fiber

