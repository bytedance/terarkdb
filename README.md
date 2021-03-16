## ZenFS Overview

ZenFS is a simple file system that utilizes RockDBs FileSystem interface to place files into zones on a raw zoned block device. By separating files into zones and utilizing the write life time hints to co-locate data of similar life times, the system write amplification is greatly reduced(compared to conventional block devices) while keeping the ZenFS capacity overhead at a very reasonable level.

ZenFS is designed to work with host-managed zoned spinning disks as well as NVME SSDs with Zoned Namespaces.

Some of the ideas and concepts in ZenFS are based on earlier work done by Abutalib Aghayev and Marc Acosta.

### Dependencies

ZenFS depends on[ libzbd ](https://github.com/westerndigitalcorporation/libzbd) and Linux kernel 5.4 or later to perform zone management operations.

## Architecture overview
![zenfs stack](https://user-images.githubusercontent.com/447288/84152469-fa3d6300-aa64-11ea-87c4-8a6653bb9d22.png)

ZenFS implements the FileSystem API, and stores all data files on to a raw zoned block device. Log and lock files are stored on the default file system under a configurable directory. Zone management is done through libzbd and zenfs io is done through normal pread/pwrite calls.

Optimizing the IO path is on the TODO list.

## Example usage

This example issues 100 million random inserts followed by as many overwrites on a 100G memory backed zoned null block device. Target file sizes are set up to align with zone size.
Please report any issues.


```
make db_bench zenfs

sudo su

./setup_zone_nullblk.sh

DEV=nullb1
ZONE_CAP_SECS=$(blkzone report -c 5 /dev/$FS_PATH | grep -oP '(?<=cap )[0-9xa-f]+' | head -1)
FUZZ=5
ZONE_CAP=$((ZONE_CAP_SECS * 512))
BASE_FZ=$(($ZONE_CAP  * (100 - $FUZZ) / 100))
WB_SIZE=$(($BASE_FZ * 2))

TARGET_FZ_BASE=$WB_SIZE
TARGET_FILE_SIZE_MULTIPLIER=2
MAX_BYTES_FOR_LEVEL_BASE=$((2 * $TARGET_FZ_BASE))

# We need the deadline io scheduler to gurantee write ordering
echo deadline > /sys/class/block/$DEV/queue/scheduler

./zenfs mkfs --zbd=$DEV --aux_path=/tmp/zenfs_$DEV --finish_threshold=$FUZZ --force

./db_bench --fs_uri=zenfs://$DEV --key_size=16 --value_size=800 --target_file_size_base=$TARGET_FZ_BASE --write_buffer_size=$WB_SIZE --max_bytes_for_level_base=$MAX_BYTES_FOR_LEVEL_BASE --max_bytes_for_level_multiplier=4 --use_direct_io_for_flush_and_compaction --max_background_jobs=$(nproc) --num=100000000 --benchmarks=fillrandom,overwrite
```

This graph below shows the capacity usage over time.
As ZenFS does not do any garbage collection the write amplification is 1.

![zenfs_capacity](https://user-images.githubusercontent.com/447288/84157574-2c51c380-aa6b-11ea-89e2-def4a4d658af.png)


## File system implementation

Files are mapped into into a set of extents:

* Extents are block-aligned, continious regions on the block device
* Extents do not span across zones
* A zone may contain more than one extent
* Extents from different files may share zones

### Reclaim 
ZenFS is exceptionally lazy at current state of implementation and does not do any garbage collection whatsoever. As files gets deleted, the used capacity zone counters drops and when
it reaches zero, a zone can be reset and reused.

###  Metadata 
Metadata is stored in a rolling log in the first zones of the block device.

Each valid meta data zone contains:

* A superblock with the current sequence number and global file system metadata
* At least one snapshot of all files in the file system
    
**The metadata format is currently experimental.** More extensive testing is needed and support for differential updates is planned to be implemented before bumping up the version to 1.0.


 ## RocksDB: A Persistent Key-Value Store for Flash and RAM Storage
 
 [![CircleCI Status](https://circleci.com/gh/facebook/rocksdb.svg?style=svg)](https://circleci.com/gh/facebook/rocksdb)

# 0. What is TerarkDB
TerarkDB is a RocksDB replacement with optimized tail latency, throughput and compression etc. In most cases you can migirate your existing RocksDB instance to TerarkDB without any
drawbacks.

- [All-in-one Docs](https://bytedance.feishu.cn/docs/doccnZmYFqHBm06BbvYgjsHHcKc#)
- [Slack Channel](https://join.slack.com/t/terarkdb/shared_invite/zt-k67ibxwn-PMn~x4yHqV0YD30FqNjqsQ)

**NOTES**
- TerarkDB was only tested and production ready under Linux platform
- Language bindings except C/C++ are not fully tested yet.
- Existing data can be migirated from RocksDB directly to TerarkDB, but cannot migrate back to RocksDB.
- TerarkDB was forked from RocksDB v5.18.3.


## Performance Overview
- RocksDB v6.12
- Server
  - Intel(R) Xeon(R) Gold 5218 CPU @ 2.30GHz (2 Sockets, 32 cores 64 threads)
  - 376 GB DRAM
  - NVMe TLC SSD (3.5 TB)
- Bench Tools & Workloads
  - use `db_bench`
  - 10 client threads, 20GB requests per thread
  - key = 24 bytes, value = 2000 bytes
  - `heavy_write` means 90% write operations
  - `heavy_read` means 90% read operations


![](docs/images/compare_rocksdb.png)


# 1. Use TerarkDB

## Prerequisite
If you enabled TerarkZipTable support (`-DWITH_TERARK_ZIP=ON`), you should install `libaio` before compile TerarkDB:

`sudo apt-get install libaio-dev`

If this is your first time using TerarkDB, we recommend you to use without TerarkZipTable by changing `-DWITH_TERARK_ZIP` to `OFF` in `build.sh`.

## Method 1: Use CMake subdirectory (Recommend)

1) Clone

```
cd {YOUR_PROJECT_DIR}
git submodule add https://github.com/bytedance/terarkdb.git

cd terarkdb && git submodule update --init --recursive
```

2) Edit your Top Project's CMakeLists.txt

```
add_subdirectory(terarkdb)
target_link_libraries({YOUR_TARGET} terarkdb)
```

3) Important Default Options

- CMAKE_BUILD_TYPE: RelWithDebInfo
- WITH_JEMALLOC: ON
  - Use Jemalloc or Not (If you are using a different malloc library, change to OFF)
- WITH_TESTS: OFF
  - Build test cases
- WITH_TOOLS: OFF
  - Build with TerarkDB tools (e.g. db_bench, ldb etc)
- WITH_TERARK_ZIP: OFF
  - Build with TerarkZipTable


### Notes
- TerarkDB is built with zstd, lz4, snappy, zlib, gtest, boost by default, if you need these libraries, you can remove them from your higher level application.


## Method 2: Link as static library

1) clone & build

```
git clone https://github.com/bytedance/terarkdb.git

cd terarkdb && git submodule update --init --recursive

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
-lterarkdb -lbz2 -ljemalloc -llz4 -lsnappy -lz -lzstd \
-Wl,-Bdynamic -pthread -lgomp -lrt -ldl -laio
```


# 2. Usage
## 2.1. BlockBasedTable
```c++
#include <cassert>
#include "rocksdb/db.h"

rocksdb::DB* db;
rocksdb::Options options;

// Your options here
options.create_if_missing = true;
options.wal_bytes_per_sync = 32768;
options.bytes_per_sync = 32768;

// Open DB
auto status = rocksdb::DB::Open(options, "/tmp/testdb", &db);

// Operations
std::string value;
auto s = db->Put(rocksdb::WriteOptions(), "key1", "value1");
s = db->Get(rocksdb::ReadOptions(), "key1", &value);
assert(s.ok());
assert("value1" == value);

s = db->Delete(rocksdb::WriteOptions(), "key1");
assert(s.ok());
```

Or manually set table format and table options:

```c++
#include <cassert>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

rocksdb::DB* db;
rocksdb::Options options;

// Your db options here
options.create_if_missing = true;
options.wal_bytes_per_sync = 32768;
options.bytes_per_sync = 32768;

// Manually specify target table and table options
rocksdb::BlockBasedTableOptions table_options;
table_options.block_cache =
    rocksdb::NewLRUCache(32ULL << 30, 8, false);
table_options.block_size = 8ULL << 10;
options.table_factory = std::shared_ptr<rocksdb::TableFactory>
                          (NewBlockBasedTableFactory(table_options));

// Open DB
auto status = rocksdb::DB::Open(options, "/tmp/testdb2", &db);

// Operations
std::string value;
auto s = db->Put(rocksdb::WriteOptions(), "key1", "value1");
s = db->Get(rocksdb::ReadOptions(), "key1", &value);
assert(s.ok());
assert("value1" == value);

s = db->Delete(rocksdb::WriteOptions(), "key1");
assert(s.ok());
```

## 2.2. TerarkZipTable
```c++
#include <cassert>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/terark_zip_table.h"

rocksdb::DB* db;
rocksdb::Options options;

// Your db options here
options.create_if_missing = true;
options.wal_bytes_per_sync = 32768;
options.bytes_per_sync = 32768;

// TerarkZipTable need a `fallback` options because you can indicate which LSM level you want to start using TerarkZipTable
// For example, by setting tzt_options.terarkZipMinLevel = 2, TerarkDB will use your fallback Table on level 0 and 1.
std::shared_ptr<rocksdb::TableFactory> table_factory;
rocksdb::BlockBasedTableOptions blockbased_options;
blockbased_options.block_size = 8ULL << 10;
table_factory.reset(NewBlockBasedTableFactory(blockbased_options));

rocksdb::TerarkZipTableOptions tzt_options;
// TerarkZipTable requires a temp directory other than data directory, a slow device is acceptable
tzt_options.localTempDir = "/tmp";
tzt_options.indexNestLevel = 3;
tzt_options.sampleRatio = 0.01;
tzt_options.terarkZipMinLevel = 2; // Start using TerarkZipTable from level 2

table_factory.reset(
    rocksdb::NewTerarkZipTableFactory(tzt_options, table_factory));

options.table_factory = table_factory;

// Open DB
auto status = rocksdb::DB::Open(options, "/tmp/testdb2", &db);

// Operations
std::string value;
auto s = db->Put(rocksdb::WriteOptions(), "key1", "value1");
s = db->Get(rocksdb::ReadOptions(), "key1", &value);
assert(s.ok());
assert("value1" == value);

s = db->Delete(rocksdb::WriteOptions(), "key1");
assert(s.ok());
```


# 3. Real-world Performance Improvement
TerarkDB has been deployed in lots of applications in Bytedance, in most cases TerarkDB can help to reduce latency spike and improve throughput tremendously.

### Disk Write
![](docs/images/disk_write.png)

### Get Latency (us)
![](docs/images/get_latency.png)


# 4. Contributing
- TerarkDB uses Github issues and pull requests to manage features and bug fixes.
- All PRs are welcome including code formating and refactoring.


# 5. License
- Apache 2.0

# 6. Users

Please let us know if you are using TerarkDB, thanks! (guokuankuan@bytedance.com)

- ByteDance (core online services)
