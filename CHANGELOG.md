
## CHANGELOG

- 版本号：v1.2.4
- 日期：2020-03-26
- 发版说明：
  - 计划内例行发版
- 功能变更：
  - 丰富 Key Value 分离相关配置
- 修复问题：
  - 修复 checksum level 为 2 时启动过慢的问题
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

