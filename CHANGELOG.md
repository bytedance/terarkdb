## CHANGELOG

### 2020-02-08
- v1.4.0
- Release Note
  - [Feature] Better TTL Support
    - Support `TtlExtractor`
    - add cf opt `ttl_gc_ratio` & `ttl_max_scan_gap`
  - [Feature] CompactionFilter adds `is_bottommost_level`
  - [Feature] Add `Comparator::IsAlias` for compatibility
  - [Feature] Add `DiagnosableLRUCache` for debugging
  - [Stats] Disable LazyCompaction by default
  - [Bug Fix] Fix DeleteFile multiple deletion bug
  - [Bug Fix] Fix TransactionDB Open infinity loop bug
  - [Bug Fix] Fix manual compaction bug when enable LazyCompaction
  - [Bug Fix] Fix Patricia Trie memtable crash when doing cocurrent writes
  - [Bug Fix] Fix exclusive manual compaction wait gc bug

### 2020-12-11
- v1.3.2
- Release Note
  - [Stats] Release TerarkDB as a public project
  - [Stats] Code refine & docs improvement etc.

### 2020-09-18
- v1.2.12
- Release Note
  - [Feature] Add `DBOptions::max_wal_size` for WAL size control
  - [Feature] Add DisableTruncate/EnableTruncate API for SstFileManager
    - Allow users disablng `DeleteScheduler`'s `Truncate` function
  - [Bug Fix] Fix the calculation of used space in `SstFileManager`(not accurate) after enabling KV separetion
  - [Bug Fix] Fix memory usage problem uder multi-CF

### 2020-08-28
- v1.2.11
- Release Note
  - [Bug Fix] Large WAL deletion triggers latency spike due to IO content
  - [Bug Fix] Move lots of code outside db mutex lock for better performance when SST file number is huge.
  - [Bug Fix] DumpStats cost too much time inside mutex when there are too many CFs

### 2020-08-07
- v1.2.10
- Release Note：
  - [Bug Fix] WAL may corruput when enabling prepare_log_writer_num 
  - [Bug Fix] WalManager::GetUpdatesSince
  - [Bug Fix] Dirty Read problem after enabling `MemTable InplaceUpdate`

### 2020-07-24
- v1.2.9
- Release Note：
  - [Performance] Optimize comparator performance in SkipList MemTable
  - [Performance] Reduce memory copies in lots of place, reduce value reading cost
  - [Feature] Add a new `PatriciaTrie MemTable` for better performance
  - [Feature] Add `avoid_unnecessary_blocking_io`, reduce tail lantency
  - [Feature] Refactor ValueMetaExtractor, allow users to extract `Key` and `CF_ID`
  - [Build] Change to CMake build
  - [Stats] Add Avg and Max latency for Metrics module
  - [Bug Fix] Reduce mutex cost in `VersionBuilder` which triggers latency spike
  - [Bug Fix] Deletion of `Iterators` trigger latency spike in some cases
  - [Bug Fix] Mutex costs too much during background directory scanning
  - [Bug Fix] Bad pointer ReadOptions::tailing true BUG
  - [Bug Fix] Lifcycle management of `Promise` may crash application in some cases
  - [Bug Fix] WAL creation in background may trigger write stop
  - [Bug Fix] Empty Value reading may fail in some cases

### 2020-06-04
- 版本号：v1.2.8
- 发版说明：
  - 计划内例行发版
- 功能变更：
  - 预创建指定个 WAL 文件，将创建 WAL 文件的逻辑从锁内移动到后台线程，降低写入长尾
  - 让描述 LSM 树拓补关系的 SST 文件常驻内存，降低读取长尾
  - 增加 WriteBufferFlushPri 选项，对多 CF 支持更r友好
- 修复问题：
  - 修复 LOG 中输出的 Flush 信息未包含 Key Value 分离 Blob 信息的问题
  - 修复 Feature 冲突导致数据集损坏的问题，不能同时启用下面的 Feature
    - TerarkZipTable 开启 SecondPassIter 支持
    - 开启 Key Value 分离
    - 在 CompactionFilter 中改变了 Value 或 MergeOperator 结果不稳定

- 版本号：v1.2.7
- 日期：2020-05-15
- 发版说明：
  - 计划内例行发版
- 功能变更：
  - 新增 ValueMetaExtractor 用来获取 Value 的元数据
    - CompactionFilter::FilterV2 接口增加 Value 元数据参数
    - 解决 Key Value 分离后 CompactionFilter 成本过高的问题
  - GetPropertiesOfTablesInRange 增加 include_blob 参数
- 修复问题：
  - 修复 BlockBasedTable 工作在 mmap 模式下有潜在的 OOM Kill 风险的问题

- 版本号：v1.2.6
- 日期：2020-04-29
- 发版说明：
  - 计划内例行发版
- 功能变更：
  - Key Value 分离实现优化，更早的分离大 Value，降低 GC 压力
  - TerarkZipTable 配置深度集成，支持所有原版 RocksDB 的配置方式
  - LazyLevelCompaction 增大 SST 中 Tombstone 的下推权重，更快回收存储空间
- 修复问题：
  - 修复使用 TerarkZipTable 可能导致 OOM Kill 的问题
  - 修复开启 Key Value 分离之后的一系列问题：
    - 修复返回用户的 Properties 未包含对应 Blob SST 的问题
    - 修复估算数据信息未包含 Blob SST 的问题
    - 修复 LSM 树过小导致空间回收不及时的问题
    - 修复大量删除数据有风险反复触发无效 GC 的问题
  - 修复继承原版 RocksDB 数据后开启 Key Value 分离后空间泄露的问题
  - 修复 Metrics 汇报长尾错误的问题
- 已知问题：
  - BlockBasedTable 工作在 mmap 模式下有潜在的 OOM Kill 风险

- 版本号：v1.2.5
- 日期：2020-04-10
- 发版说明：
  - 计划内例行发版
- 功能变更：
  - 增加 pin_table_properties_in_reader 选项，降低 TableProperties 内存占用
  - 增加 max_task_per_thread 选项，支持单线程运行多个 Compaction/GC 任务
  - 支持内嵌 jemalloc，make 参数增加 USE_JEMALLOC=1 启用
  - ZipOffsetBlobStore 新增 ZSTD 算法支持
- 修复问题：
  - 修复从 RocksDB 迁移，若使用了 RangeDeletion，有可能丢失部分数据的问题
  - 修复 Ingest SST 读取数据错误的问题
    - https://github.com/facebook/rocksdb/issues/6666
  - 修复 Lazy Level Compaction 在数据量很少时出现"松树"形 LSM 树的问题
  - 修复后台 Domain Socket 退出问题导致 DB 关闭卡住的问题
  - 修复后台 GC 任务导致排他性手动 Compact 被阻塞的问题
  - 修复 AdaptiveTableFactory 输出配置被截断的问题

- 版本号：v1.2.4
- 日期：2020-03-26
- 发版说明：
  - 计划内例行发版
- 功能变更：
  - 丰富 Key Value 分离相关配置
- 修复问题：
  - 修复 checksum level 为 2 时启动过慢的问题
  - 修复 ColumnFamily 数据删空后 CompactionPicker 内存越界的问题
  - 修复 CompactionFilter 非幂等时，有概率残留部分被过滤掉的 Key 的问题
    - 开启 KV 分离，或使用 BlockBasedTable 不受影响
- 已知问题：
  - 从 RocksDB 迁移，若使用了 RangeDeletion，有可能丢失部分数据
    - 未使用 RangeDeletion 则不受影响
    - 实现方式有差异，未做完兼容。整库 Compact 消除 RangeDeletion 后迁移即可
  - 极低概率后台 Compact 触发异常导致 Write 失败，如果出现请保留现场，联系@zhaoming.274

- 版本号：v1.2.3
- 日期：2020-03-05
- 发版说明：
  - 计划内例行发版
- 功能变更：
  - 增加 Flink 相关适配
  - Metrics 汇报写入流量
- 修复问题：
  - 修复 Metrics 占用资源过高的问题 
  - 修复 PORTABLE 构建时 CRC32 不使用硬件加速的问题
  - 修复开启 ASAN 时 NestLoudsTrie 构建触发警告的问题
- 已知问题：
  - 重启或长时间运行，会出现若干 hugepage warning
  - 极低概率后台 Compact 触发异常导致 Write 失败，如果出现请保留现场，联系@zhaoming.274

- 版本号：v1.2.2
- 日期：2020-02-13
- 发版说明：
  - 计划内例行发版
- 功能变更：
  - 无
- 修复问题：
  - 修复 MapSstIterator 触发断言的问题
  - 修复 DictZipBlobStore 校验级别为 2 时，写入空数据导致宕机的问题
  - 核心库为 MixedLenBlobStore 增加更严格的校验
- 已知问题：
  - 重启或长时间运行，会出现若干 hugepage warning
  - 极低概率后台 Compact 触发异常导致 Write 失败，如果出现请保留现场，联系@zhaoming.274

- 版本号：v1.2.1
- 日期：2020-02-06
- 发版说明：
  - 本版本为预览版发布，旨在进行第一次正式发布演练，请给位同学严格执行测试流程
  - 下一次版本发布会在一周之后正式进行
- 功能变更：
  - 实现 KeyValue 分离存储
  - 移除 BlobDB 的支持
  - 增加 LazyBuffer 系列接口
  - 移除 PinnableSlice 系列接口
- 修复问题：
  - 修复 NestLoudsTrie 的 Rank 接口错误
  - 修复 SubCompaction 导致 CPU 资源使用过度的问题
  - 将 `VersionBuilder::SaveTo` 逻辑移出锁外
  - 修复手动 Compact 导致宕机的问题
  - 修复 BlockBasedTable 与 RangeDeletion 同时使用导致内存泄露的问题
  - 修复信息上报 API 的数据错误
- 已知问题：
  - 重启或长时间运行，会出现若干 hugepage warning
  - 使用了 PinnableSlice、MergeOperator、CompactionFilter 的接口可能需要少许代码修改完成迁移
    - 后续版本提供有少许性能损失的平滑迁移
  - 因为不再支持 BlobDB，自此版本发布后，所有使用了 BlobDB 的实例无法迁移至此版本

2020-01-07
- Fix fake assertion of universal compaction picker
- Refine lazy level compaction pick size
- Refine database statistics report relevance

2020-01-05
- Remove terark index build pipeline
- Fix PatriciaTrieMemTable bugs

2019-12-30
- Refactor terark env logic

2019-12-29
- Level based lazy compaction dev completed
- Add Redis-protocol OPS Console

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
