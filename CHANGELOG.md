
## CHANGELOG

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

