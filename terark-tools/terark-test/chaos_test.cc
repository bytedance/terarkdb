#include "chaos_test.hpp"

namespace rocksdb {

class ChaosTest {
 public:
  DB *db;
  std::string dbname_;
  Options options;
  EnvOptions env_options_;
  ImmutableDBOptions db_options_;
  BlockBasedTableOptions bbto;
  TerarkZipTableOptions tzto;
  std::vector<rocksdb::ColumnFamilyDescriptor> cfDescriptors;
  std::vector<rocksdb::ColumnFamilyHandle *> hs;
  std::atomic<bool> shutting_down_;
  SequenceNumber preserve_deletes_seqnum_;
  WriteOptions wo;
  FlushOptions fo;
  CompactRangeOptions co;
  std::vector<std::shared_ptr<const Snapshot>> snapshot;
  std::atomic<bool> has_error{false};
  std::mutex snapshot_mutex;
  uint16_t flags_;
  DBOptions dbo;
  std::atomic<uint64_t> atomic_count{1};
  Status s;
  InstrumentedMutex mutex_;
  ErrorHandler error_handler_;
  bool exit_;

  ChaosTest(uint16_t flags)
      : dbname_("chaosdb"),
        db_options_(),
        shutting_down_(false),
        preserve_deletes_seqnum_(0),
        error_handler_(nullptr, db_options_, &mutex_),
        exit_(false) {
    flags_ = flags;
  }

  ~ChaosTest() {}

  void DumpCacheStatistics() {
    auto dcache = dynamic_cast<DiagnosableLRUCache *>(bbto.block_cache.get());
    if (dcache == nullptr) {
      return;
    }
    while (true) {
      auto info = dcache->DumpLRUCacheStatistics();
      std::cout << info << std::endl;
      using namespace std::chrono;
      std::this_thread::sleep_for(60s);
    }
  }

  void set_options() {
    options.atomic_flush = false;
    options.allow_mmap_reads = false;
    options.max_open_files = 8192;
    options.allow_fallocate = true;
    options.writable_file_max_buffer_size = 1048576;
    options.allow_mmap_writes = false;
    options.allow_concurrent_memtable_write = true;
    options.use_direct_reads = true;
    options.max_background_garbage_collections = 8;
    options.WAL_size_limit_MB = 0;
    options.use_aio_reads = true;
    options.max_background_jobs = 32;
    options.WAL_ttl_seconds = 0;
    options.enable_thread_tracking = true;
    options.error_if_exists = false;
    options.is_fd_close_on_exec = true;
    options.recycle_log_file_num = 0;
    options.prepare_log_writer_num = 0;
    options.max_manifest_file_size = 1073741824;
    options.skip_log_error_on_recovery = false;
    options.skip_stats_update_on_db_open = false;
    options.max_total_wal_size = 0;
    options.new_table_reader_for_compaction_inputs = true;
    options.manual_wal_flush = true;
    options.compaction_readahead_size = 8388608;
    options.random_access_max_buffer_size = 1048576;
    options.create_missing_column_families = false;
    options.wal_bytes_per_sync = 4194304;
    options.use_adaptive_mutex = false;
    options.use_direct_io_for_flush_and_compaction = false;
    options.max_background_compactions = 16;
    options.advise_random_on_open = true;
    options.base_background_compactions = 4;
    options.max_background_flushes = 32;
    options.two_write_queues = true;
    options.table_cache_numshardbits = 6;
    options.keep_log_file_num = 1000;
    options.write_thread_slow_yield_usec = 3;
    options.stats_dump_period_sec = 600;
    options.avoid_flush_during_recovery = false;
    options.log_file_time_to_roll = 0;
    options.delayed_write_rate = 209715200;
    options.manifest_preallocation_size = 4194304;
    options.paranoid_checks = true;
    options.max_log_file_size = 0;
    options.allow_2pc = true;
    options.max_subcompactions = 16;
    options.create_if_missing = true;
    options.enable_pipelined_write = false;
    options.bytes_per_sync = 4194304;
    options.max_manifest_edit_count = 4096;
    options.fail_if_options_file_error = false;
    options.use_fsync = false;
    options.wal_recovery_mode =
        rocksdb::WALRecoveryMode::kTolerateCorruptedTailRecords;
    options.delete_obsolete_files_period_micros = 21600000000;
    options.enable_write_thread_adaptive_yield = false;
    options.avoid_flush_during_shutdown = false;
    options.write_thread_max_yield_usec = 100;
    options.info_log_level = rocksdb::INFO_LEVEL;
    options.max_file_opening_threads = 16;
    options.dump_malloc_stats = false;
    options.allow_mmap_populate = false;
    options.allow_ingest_behind = false;
    options.access_hint_on_compaction_start = Options::AccessHint::NORMAL;
    options.preserve_deletes = false;
    options.env->SetBackgroundThreads(options.max_background_compactions,
                                      rocksdb::Env::LOW);
    options.env->SetBackgroundThreads(options.max_background_flushes,
                                      rocksdb::Env::HIGH);
    bbto.pin_top_level_index_and_filter = true;
    bbto.pin_l0_filter_and_index_blocks_in_cache = true;
    bbto.filter_policy.reset(NewBloomFilterPolicy(10, true));
    // bbto.block_cache = NewLRUCache(4ULL << 30, 6, false);
    bbto.block_cache =
        NewDiagnosableLRUCache(4ULL << 30, 3, false, 0.2, nullptr, 10);

    options.compaction_pri = kMinOverlappingRatio;
    options.compression = kZSTD;
    options.bottommost_compression = kZSTD;

    static TestCompactionFilter filter;

    options.compaction_filter = &filter;

    options.merge_operator.reset(new TestMergeOperator(','));
    options.memtable_insert_with_hint_prefix_extractor = nullptr;
    options.target_file_size_base = file_size_base;
    options.max_sequential_skip_in_iterations = 8;
    options.max_bytes_for_level_base = file_size_base * 4;
    options.bloom_locality = 0;
    options.write_buffer_size = file_size_base;
    options.memtable_huge_page_size = 0;
    options.max_successive_merges = 0;
    options.arena_block_size = file_size_base / 8;
    options.target_file_size_multiplier = 1;
    options.num_levels = 5;
    options.min_write_buffer_number_to_merge = 1;
    options.max_write_buffer_number_to_maintain = 16;
    options.max_write_buffer_number = 8;
    options.max_compaction_bytes = file_size_base * 2;
    options.memtable_prefix_bloom_size_ratio = 0.000000;
    options.hard_pending_compaction_bytes_limit = 274877906944;
    options.prefix_extractor = nullptr;
    options.force_consistency_checks = false;
    options.paranoid_file_checks = false;
    options.max_bytes_for_level_multiplier = 2.000000;
    options.optimize_filters_for_hits = false;
    options.level_compaction_dynamic_level_bytes = true;
    options.inplace_update_num_locks = 10000;
    options.inplace_update_support = false;
    options.blob_gc_ratio = 0.05;
    options.blob_size = blob_size;
    options.ttl = 0;
    options.soft_pending_compaction_bytes_limit = 68719476736;
    options.enable_lazy_compaction = true;
    options.disable_auto_compactions = false;
    options.report_bg_io_stats = false;
    options.compaction_options_universal.allow_trivial_move = true;

    options.level0_file_num_compaction_trigger = 4;
    options.level0_slowdown_writes_trigger = 40;
    options.level0_stop_writes_trigger = 100;

    options.max_manifest_file_size = 1ull << 30;
    options.max_manifest_edit_count = 8192;

    options.force_consistency_checks = true;
    options.max_file_opening_threads = 8;
    options.compaction_style = rocksdb::kCompactionStyleLevel;
    options.blob_gc_ratio = 0.1;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.use_aio_reads = (flags_ & TestAsync) ? true : false;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));
#ifdef WITH_TERARK_ZIP
    if (flags_ & TestTerark) {
      tzto.localTempDir = dbname_;
      tzto.indexNestLevel = 3;
      tzto.checksumLevel = 2;
      tzto.entropyAlgo = rocksdb::TerarkZipTableOptions::kHuffman;
      // tzto.entropyAlgo = rocksdb::TerarkZipTableOptions::kNoEntropy;
      tzto.terarkZipMinLevel = 0;
      tzto.debugLevel = 0;
      tzto.indexNestScale = 8;
      tzto.enableCompressionProbe = true;
      tzto.useSuffixArrayLocalMatch = false;
      tzto.warmUpIndexOnOpen = false;
      tzto.warmUpValueOnOpen = false;
      tzto.disableSecondPassIter = false;
      tzto.indexTempLevel = 0;
      tzto.offsetArrayBlockUnits = 128;
      tzto.sampleRatio = 0.03;
      tzto.indexType = "Mixed_XL_256_32_FL";
      tzto.softZipWorkingMemLimit = 128ull << 30;
      tzto.hardZipWorkingMemLimit = 256ull << 30;
      tzto.smallTaskMemory = 1200 << 20;  // 1.2G
      tzto.minDictZipValueSize = 32;
      tzto.keyPrefixLen = 0;  // for IndexID
      tzto.indexCacheRatio = 0.001;
      tzto.singleIndexMinSize = 8ULL << 20;
      tzto.singleIndexMaxSize = 0x1E0000000;  // 7.5G
      tzto.singleIndexMinSize = 8ULL << 20;
      tzto.singleIndexMaxSize = 64ULL << 20;
      tzto.minPreadLen = 0;
      tzto.cacheShards = 257;                 // to reduce lock competition
      tzto.cacheCapacityBytes = 16ULL << 30;  // non-zero implies direct io read
      tzto.disableCompressDict = false;
      tzto.optimizeCpuL3Cache = false;
      tzto.forceMetaInMemory = false;
      options.table_factory.reset(
          rocksdb::NewTerarkZipTableFactory(tzto, options.table_factory));
    }
#endif
  }

  std::pair<std::string, std::string> get_ran_range_pair(
      int mode, std::mt19937_64 mt,
      std::uniform_int_distribution<uint64_t> uid) {
    size_t i = mt() % (rand_key_times - 1);
    std::string k1, k2;
    auto cmp = [c = options.comparator](const std::string &a,
                                        const std::string &b) {
      return c->Compare(a, b) < 0;
    };
    std::set<std::string, decltype(cmp)> rand_key_0(cmp);
    std::set<std::string, decltype(cmp)> rand_key_1(cmp);
    if (mode == 0) {
      while (rand_key_0.size() < rand_key_times) {
        rand_key_0.emplace(gen_key(uid(mt)));
      }
      auto it = std::next(rand_key_0.begin(), i);
      k1 = *it;
      it = rand_key_0.erase(it);
      k2 = *it;
      rand_key_0.erase(it);
    } else {
      while (rand_key_1.size() < rand_key_times) {
        rand_key_1.emplace(gen_key(uid(mt)));
      }
      auto it = std::next(rand_key_1.begin(), i);
      k1 = *it;
      it = rand_key_1.erase(it);
      k2 = *it;
      rand_key_1.erase(it);
    }
    return std::make_pair(std::move(k1), std::move(k2));
  }

  std::shared_ptr<const Snapshot> get_snapshot(std::mt19937_64 &mt) {
    snapshot_mutex.lock();
    size_t si;
    si = mt() % (snapshot.size() + 1);
    auto ret = si == snapshot.size() ? nullptr : snapshot[si];
    snapshot_mutex.unlock();
    return ret;
  }

  void RunWorkerCompaction() {
    ColumnFamilyData *cfd =
        static_cast<ColumnFamilyHandleImpl *>(hs[hs.size() - 1])->cfd();
    VersionStorageInfo *vstorage = cfd->current()->storage_info();
    const std::vector<SequenceNumber> &snapshots = {};
    SequenceNumber earliest_write_conflict_snapshot = kMaxSequenceNumber;

    DBImpl *dbfull = reinterpret_cast<DBImpl *>(db);
    CompactionJobStats compaction_job_stats_;

    int start_level = 0;
    for (; start_level < vstorage->num_levels() &&
           vstorage->NumLevelFiles(start_level) == 0;
         start_level++) {
    }
    if (start_level == vstorage->num_levels()) {
      return;
    }
    std::vector<CompactionInputFiles> inputs(vstorage->num_levels() -
                                             start_level);
    for (int level = start_level; level < vstorage->num_levels(); level++) {
      inputs[level - start_level].level = level;
      auto &files = inputs[level - start_level].files;
      for (FileMetaData *f : vstorage->LevelFiles(level)) {
        files.push_back(f);
      }
    }
    CompactionParams params(vstorage, *cfd->ioptions(),
                            *cfd->GetLatestMutableCFOptions());
    params.inputs = std::move(inputs);
    params.output_level = 1;
    params.target_file_size = 1024 * 1024;
    params.max_compaction_bytes = 10 * 1024 * 1024;
    params.compression_opts = cfd->ioptions()->compression_opts;
    params.manual_compaction = true;

    Compaction compaction(std::move(params));
    compaction.SetInputVersion(cfd->current());
    LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, db_options_.info_log.get());
    mutex_.Lock();
    EventLogger event_logger(db_options_.info_log.get());
    SnapshotChecker *snapshot_checker = nullptr;
    std::shared_ptr<Cache> table_cache(dbfull->TEST_table_cache());
    CompactionJob compaction_job(0, &compaction, db_options_, env_options_,
                                 cfd->current()->version_set(), &shutting_down_,
                                 preserve_deletes_seqnum_, &log_buffer, nullptr,
                                 nullptr, nullptr, &mutex_, &error_handler_,
                                 snapshots, earliest_write_conflict_snapshot,
                                 snapshot_checker, nullptr, &event_logger,
                                 false, false, dbname_, &compaction_job_stats_);
    int sub_compaction_used =
        compaction_job.Prepare(0 /* sub_compaction_slots */);
    s = compaction_job.Run();
    compaction_job.Install(*cfd->GetLatestMutableCFOptions());
    if (!s.ok()) {
      fprintf(stderr, "Worker Compaction Error! %s\n", s.getState());
      assert(false);
    }
  }

  void WriteFunc(int seed) {
    if (flags_ & ReadOnly) return;
#ifdef WITH_TERARK_ZIP
    auto index_factory =
        (flags_ & TestTerark) ? patricia_WriteBatchEntryIndexFactory() :
#else
    auto index_factory =
#endif
                              nullptr;

    WriteBatchWithIndex b(rocksdb::BytewiseComparator(), 0, false, 0,
                          index_factory);
    std::string key, value;
    std::mt19937_64 mt(seed);
    for (uint64_t count = atomic_count;; count = ++atomic_count) {
      if (has_error) {
        std::this_thread::sleep_for(std::chrono::hours(24 * 265));
      }
      std::uniform_int_distribution<uint64_t> uid(0, count);
      if (flags_ & TestCompaction) {
        if (mt() % (1ull << 20) == 0) {
          auto keys = get_ran_range_pair(mt() & 1, mt, uid);
          rocksdb::Slice slice0 = keys.first, slice1 = keys.second;
          db->CompactRange(co, hs[mt() % hs.size()], &slice0, &slice1);
        }
      }
      if (flags_ & TestWorker) {
        if (mt() % (1ull << 15) == 0) {
          RunWorkerCompaction();
        }
      }
      size_t r = uid(mt);
      key = gen_key(r);
      value = get_value(count, key);
      if (count % 2 == 0) {
        for (auto &h : hs) {
          b.Put(h, key, value);
        }
      } else {
        for (auto &h : hs) {
          b.Merge(h, key, value);
        }
      }
      key = gen_key(r);
      if (count % 3 == 0) {
        for (auto &h : hs) {
          b.Put(h, key, value);
        }
      } else {
        for (auto &h : hs) {
          b.Merge(h, key, value);
        }
      }
      if (count % 11 == 0) {
        key = gen_key(uid(mt));
        for (auto &h : hs) {
          b.Delete(h, key);
        }
      }
      if (count % 13 == 0) {
        key = gen_key(uid(mt));
        for (auto &h : hs) {
          b.Delete(h, key);
        }
      }
      if (flags_ & TestRangeDel) {
        if (count % 70003 == 0) {
          auto keys = get_ran_range_pair(0, mt, uid);
          rocksdb::Slice slice0 = keys.first, slice1 = keys.second;
          fprintf(stderr, "RangeDel [%s, %s)\n", keys.first.c_str(),
                  keys.second.c_str());
          for (auto &h : hs) {
            b.DeleteRange(h, slice0, slice1);
          }
        }
        if (count % 90017 == 0) {
          auto keys = get_ran_range_pair(1, mt, uid);
          rocksdb::Slice slice0 = keys.first, slice1 = keys.second;
          fprintf(stderr, "RangeDel [%s, %s)\n", keys.first.c_str(),
                  keys.second.c_str());
          for (auto &h : hs) {
            b.DeleteRange(h, slice0, slice1);
          }
        }
        if (count % 50019 == 0) {
          fprintf(stderr, "RangeDel [MinKey, MaxKey+1), [%s, %s)\n",
                  MinKey.c_str(), MaxKey.c_str());
          for (auto &h : hs) {
            b.DeleteRange(h, MinKey, MaxKey + "1");
          }
        }
      }
      if (std::uniform_int_distribution<uint64_t>(0, 1 << 10)(mt) == 0) {
        auto s = db->Write(wo, b.GetWriteBatch());
        if (!s.ok()) {
          printf("%s\n", s.getState());
          break;
        }
        b.Clear();
        if (snapshot_mutex.try_lock()) {
          auto del_snapshot = [&](const rocksdb::Snapshot *s) {
            db->ReleaseSnapshot(s);
          };
          if (snapshot.size() < 2) {
            snapshot.emplace_back(db->GetSnapshot(), del_snapshot);
          } else if (snapshot.size() > 50 || (mt() % 4) == 0) {
            auto i = mt() % snapshot.size();
            snapshot.erase(snapshot.begin() + i);
          } else {
            snapshot.emplace_back(db->GetSnapshot(), del_snapshot);
          }
          snapshot_mutex.unlock();
        }
      }
    }
  }

  void IterTest(ReadContext &ctx, std::mt19937_64 mt,
                std::uniform_int_distribution<uint64_t> uid) {
    uint64_t iter_seqno = uint64_t(-1);
    ctx.seqno = uint64_t(-1);
    std::vector<rocksdb::Iterator *> iter_for_new;
    auto snapshot = get_snapshot(mt);
    ctx.ro.snapshot = snapshot.get();
    if (ctx.ro.snapshot == nullptr) {
      iter_seqno = uint64_t(-1);
    } else {
      iter_seqno = get_snapshot_seqno(ctx.ro.snapshot);
    }
    if (!db->NewIterators(ctx.ro, hs, &iter_for_new).ok()) {
      fprintf(stderr, "NewIterator fail !\n");
      return;
    }
    ctx.iter.clear();
    for (auto it : iter_for_new) {
      ctx.iter.emplace_back(it);
    }
    iter_for_new.clear();
    ctx.ro.snapshot = nullptr;
    ctx.seqno = iter_seqno;
    ctx.key = gen_key(uid(mt));
    for (auto &it : ctx.iter) {
      it->Seek(ctx.key);
    }
    if (IsAny(ctx.iter, [](auto &it) { return !it->Valid(); })) {
      CheckAssert(ctx, IsAll(ctx.iter, [](auto &it) { return !it->Valid(); }),
                  "Iter Seek Valid");
      CheckAssert(ctx,
                  IsAll(ctx.iter, [](auto &it) { return it->status().ok(); }),
                  "Iter Seek Status");
    } else {
      CheckAssert(ctx,
                  IsSame(ctx.iter,
                         [](auto &l, auto &r) { return l->key() == r->key(); }),
                  "Iter Seek Key");
      CheckAssert(
          ctx,
          IsSame(ctx.iter,
                 [](auto &l, auto &r) { return l->value() == r->value(); }),
          "Iter Seek Value");

      for (auto &it : ctx.iter) {
        it->Next();
      }
      if (IsAny(ctx.iter, [](auto &it) { return !it->Valid(); })) {
        CheckAssert(ctx, IsAll(ctx.iter, [](auto &it) { return !it->Valid(); }),
                    "Iter Seek Next Valid");
        CheckAssert(ctx,
                    IsAll(ctx.iter, [](auto &it) { return it->status().ok(); }),
                    "Iter Seek Next Status");
      } else {
        CheckAssert(
            ctx,
            IsSame(ctx.iter,
                   [](auto &l, auto &r) { return l->key() == r->key(); }),
            "Iter Seek Next Key");
        CheckAssert(
            ctx,
            IsSame(ctx.iter,
                   [](auto &l, auto &r) { return l->value() == r->value(); }),
            "Iter Seek Next Value");
      }

      for (auto &it : ctx.iter) {
        it->Prev();
      }
      if (IsAny(ctx.iter, [](auto &it) { return !it->Valid(); })) {
        CheckAssert(ctx, IsAll(ctx.iter, [](auto &it) { return !it->Valid(); }),
                    "Iter Seek Next Prev Valid");
        CheckAssert(ctx,
                    IsAll(ctx.iter, [](auto &it) { return it->status().ok(); }),
                    "Iter Seek Next Prev Status");
      } else {
        CheckAssert(
            ctx,
            IsSame(ctx.iter,
                   [](auto &l, auto &r) { return l->key() == r->key(); }),
            "Iter Seek Next Prev Key");
        CheckAssert(
            ctx,
            IsSame(ctx.iter,
                   [](auto &l, auto &r) { return l->value() == r->value(); }),
            "Iter Seek Next Prev Value");
      }
    }

    for (auto &it : ctx.iter) {
      it->SeekForPrev(ctx.key);
    }
    if (IsAny(ctx.iter, [](auto &it) { return !it->Valid(); })) {
      CheckAssert(ctx, IsAll(ctx.iter, [](auto &it) { return !it->Valid(); }),
                  "Iter SeekForPrev Valid");
      CheckAssert(ctx,
                  IsAll(ctx.iter, [](auto &it) { return it->status().ok(); }),
                  "Iter SeekForPrev Status");
    } else {
      CheckAssert(ctx,
                  IsSame(ctx.iter,
                         [](auto &l, auto &r) { return l->key() == r->key(); }),
                  "Iter SeekForPrev Key");
      CheckAssert(
          ctx,
          IsSame(ctx.iter,
                 [](auto &l, auto &r) { return l->value() == r->value(); }),
          "Iter SeekForPrev Value");

      for (auto &it : ctx.iter) {
        it->Prev();
      }
      if (IsAny(ctx.iter, [](auto &it) { return !it->Valid(); })) {
        CheckAssert(ctx, IsAll(ctx.iter, [](auto &it) { return !it->Valid(); }),
                    "Iter SeekForPrev Prev Valid");
        CheckAssert(ctx,
                    IsAll(ctx.iter, [](auto &it) { return it->status().ok(); }),
                    "Iter SeekForPrev Prev Status");
      } else {
        CheckAssert(
            ctx,
            IsSame(ctx.iter,
                   [](auto &l, auto &r) { return l->key() == r->key(); }),
            "Iter SeekForPrev Prev Key");
        CheckAssert(
            ctx,
            IsSame(ctx.iter,
                   [](auto &l, auto &r) { return l->value() == r->value(); }),
            "Iter SeekForPrev Prev Value");

        for (auto &it : ctx.iter) {
          it->Next();
        }
        if (IsAny(ctx.iter, [](auto &it) { return !it->Valid(); })) {
          CheckAssert(ctx,
                      IsAll(ctx.iter, [](auto &it) { return !it->Valid(); }),
                      "Iter SeekForPrev Prev Next Valid");
          CheckAssert(
              ctx, IsAll(ctx.iter, [](auto &it) { return it->status().ok(); }),
              "Iter SeekForPrev Prev Next Status");
        } else {
          CheckAssert(
              ctx,
              IsSame(ctx.iter,
                     [](auto &l, auto &r) { return l->key() == r->key(); }),
              "Iter SeekForPrev Prev Next Key");
          CheckAssert(
              ctx,
              IsSame(ctx.iter,
                     [](auto &l, auto &r) { return l->value() == r->value(); }),
              "Iter SeekForPrev Prev Next Value");
        }
      }
    }
  }

  void ApproximateSizes(std::mt19937_64 mt,
                        std::uniform_int_distribution<uint64_t> uid) {
    size_t r0 = uid(mt);
    size_t r1 = uid(mt);
    auto key0 = gen_key(r0);
    auto key1 = gen_key(r1);
    auto key2 = gen_key(r0);
    auto key3 = gen_key(r1);
    if (options.comparator->Compare(key0, key1) > 0) {
      std::swap(key0, key1);
    }
    if (options.comparator->Compare(key2, key3) > 0) {
      std::swap(key2, key3);
    }
    rocksdb::Range range[2];
    range[0].start = key0;
    range[0].limit = key1;
    range[1].start = key2;
    range[1].limit = key3;
    uint64_t size[2];
    for (auto &h : hs) {
      db->GetApproximateSizes(h, range, 2, size);
    }
  }

  void GetTest(ReadContext &ctx, std::mt19937_64 mt,
               std::uniform_int_distribution<uint64_t> uid) {
    auto snapshot = get_snapshot(mt);
    for (int i = 0; i < 2; ++i) {
      ctx.ro.snapshot = snapshot.get();
      if (ctx.ro.snapshot == nullptr) {
        ctx.seqno = uint64_t(-1);
      } else {
        ctx.seqno = get_snapshot_seqno(ctx.ro.snapshot);
      }
      ctx.key = gen_key(uid(mt));
      for (auto &k : ctx.keys) {
        k = ctx.key;
      }
      ctx.ss = db->MultiGet(ctx.ro, hs, ctx.keys, &ctx.values);
      if (IsAny(ctx.ss, [](auto &s) { return s.IsNotFound(); })) {
        CheckAssert(ctx, IsAll(ctx.ss, [](auto &s) { return s.IsNotFound(); }),
                    "MultiGet Status");
      } else {
        CheckAssert(ctx, IsAny(ctx.ss, [](auto &s) { return s.ok(); }),
                    "MultiGet Status");
        CheckAssert(ctx,
                    IsSame(ctx.values, [](auto &l, auto &r) { return l == r; }),
                    "MultiGet Value");
      }
      ctx.ro.snapshot = nullptr;
    }
  }

  void AsyncTest(ReadContext &ctx, std::mt19937_64 &mt,
                 std::uniform_int_distribution<uint64_t> &uid) {
    auto snapshot = get_snapshot(mt);
    for (int i = 0; i < 4; ++i) {
      ctx.values.clear();
      ctx.async_values.clear();
#if defined(TERARKDB_WITH_AIO_FUTURE)
      ctx.futures.clear();
#endif
      ctx.async_status.clear();
      ctx.multi_values.clear();
      ctx.ss.clear();
      ctx.ro.snapshot = snapshot.get();
      if (ctx.ro.snapshot == nullptr) {
        break;
      } else {
        ctx.seqno = get_snapshot_seqno(ctx.ro.snapshot);
      }
      ctx.key = gen_key(uid(mt));
      for (auto &k : ctx.keys) {
        k = ctx.key;
      }
#ifdef BOOSTLIB
      for (size_t i = 0; i < hs.size(); ++i) {
        ctx.ss[i] = db->Get(ctx.ro, hs[i], ctx.keys[i], &(ctx.values[i]));
        db->GetAsync(
            ctx.ro, hs[i], ctx.keys[i].ToString(), &(ctx.async_values[i]),
            [&ctx, &i](Status &&s, std::string &&key, std::string *value) {
              (ctx.async_status)[i] = s;
            });
#if defined(TERARKDB_WITH_AIO_FUTURE)
        ctx.futures[i] = db->GetFuture(ctx.ro, hs[i], ctx.key);
#endif
      }
      db->WaitAsync();
      if (IsAny(ctx.ss, [](auto &s) { return s.IsNotFound(); })) {
        CheckAssert(ctx, IsAll(ctx.ss, [](auto &s) { return s.IsNotFound(); }),
                    "Get Status");
        CheckAssert(
            ctx,
            IsAll(ctx.async_status, [](auto &s) { return s.IsNotFound(); }),
            "GetAsync Status");
#if defined(TERARKDB_WITH_AIO_FUTURE)
        CheckAssert(
            ctx,
            IsAll(ctx.futures,
                  [](auto &fu) { return std::get<0>(fu.get()).IsNotFound(); }),
            "GetFuture Status");
#endif
      } else {
        CheckAssert(ctx,
                    AllSame(ctx.values, ctx.async_values,
                            [](auto &l, auto &r) { return l == r; }),
                    "Get vs GetAsync");
#if defined(TERARKDB_WITH_AIO_FUTURE)

        CheckAssert(ctx,
                    AllSame(ctx.values, ctx.futures,
                            [](auto &l, auto &r) {
                              const std::tuple<Status, std::string, std::string>
                                  &tmp_tuple = r.get();
                              return l == std::get<2>(tmp_tuple);
                            }),
                    "Get vs GetFuture");
#endif
      }
#endif  // BOOSTLIB
    }
  }

  void ReadOnlyTest(ReadContext &ctx) {
    ctx.ro.snapshot = db->GetSnapshot();
    ctx.key = READ_ONLY_TEST_KEY;
    // set_snapshot_seqno(ctx.ro.snapshot, ctx.seqno = READ_ONLY_TEST_SEQ);
    if (flags_ & TestIter) {
      ctx.iter.resize(hs.size());
      for (size_t i = 0; i < hs.size(); ++i) {
        ctx.iter[i] =
            std::unique_ptr<rocksdb::Iterator>(db->NewIterator(ctx.ro, hs[i]));
      }
    }
    ctx.ss.resize(hs.size());
    ctx.keys.resize(hs.size());
    ctx.values.resize(hs.size());
    CheckAssert(ctx, false, "TEST");
    db->ReleaseSnapshot(ctx.ro.snapshot);
  }

  void HashTest(ReadContext &ctx, std::mt19937_64 &mt,
                std::uniform_int_distribution<uint64_t> &uid) {
    auto snapshot = get_snapshot(mt);
    std::vector<rocksdb::Iterator *> iter_for_new;
    ctx.ro.snapshot = snapshot.get();
    if (ctx.ro.snapshot == nullptr) {
      ctx.seqno = uint64_t(-1);
    } else {
      ctx.seqno = get_snapshot_seqno(ctx.ro.snapshot);
    }
    for (size_t i = 0; i < hs.size(); ++i) {
      ctx.ss[i] = db->Get(ctx.ro, hs[i], ctx.keys[i], &(ctx.values[i]));
      if (ctx.ss[i].ok()) {
        size_t pos = ctx.values[i].rfind("#");
        assert(pos != std::string::npos);
        auto sub_str = ctx.values[i].substr(pos + 1);
        assert(h(ctx.key) == sub_str);
      }
    }
  }

  bool HaveLevelFiles(int level) {
    std::vector<LiveFileMetaData> metadata;
    db->GetLiveFilesMetaData(&metadata);
    for (auto &lfmd : metadata) {
      if (lfmd.level >= 2) return true;
    }
    return false;
  }

  void SetExit() { exit_ = true; }

  void Close() {
    for (auto h : hs) {
      if (h) {
        db->DestroyColumnFamilyHandle(h);
      }
    }
    hs.clear();
    cfDescriptors.clear();
    snapshot.clear();
    delete db;
    db = nullptr;
  }

  void Open(int cf_num) {
    set_options();
    exit_ = false;
    for (int i = 0; i < cf_num; ++i) {
      options.compaction_style = rocksdb::kCompactionStyleLevel;
      options.write_buffer_size = size_t(file_size_base * 1.2);
      options.enable_lazy_compaction = true;
      cfDescriptors.emplace_back(rocksdb::kDefaultColumnFamilyName, options);
      options.compaction_style = rocksdb::kCompactionStyleUniversal;
      options.write_buffer_size = size_t(file_size_base * 1.1);
      options.enable_lazy_compaction = true;
      cfDescriptors.emplace_back("universal" + std::to_string(i), options);
      options.compaction_style = rocksdb::kCompactionStyleLevel;
      options.write_buffer_size = size_t(file_size_base / 1.1);
      options.enable_lazy_compaction = true;
      //bbto.block_cache = NewLRUCache(4ULL << 30, 6, false);
      options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));
      cfDescriptors.emplace_back("level" + std::to_string(i), options);
    }
    if (flags_ & TestWorker) {
      options.compaction_dispatcher.reset(
          new AsyncCompactionDispatcher(options));
      options.enable_lazy_compaction = false;
      cfDescriptors.emplace_back("async", options);
    }
    dbo = options;
    if (flags_ & ReadOnly) {
      s = DB::OpenForReadOnly(dbo, dbname_, cfDescriptors, &hs, &db);
      if (!s.ok()) {
        printf("ReadOnly Open Error! %s\n", s.getState());
        assert(false);
      }
    } else {
      s = DB::Open(dbo, dbname_, cfDescriptors, &hs, &db);
      if (!s.ok()) {
        printf("Open Error! %s\n", s.getState());
        assert(false);
      }
    }
  }

  void ReadFunc(int seed) {
    ReadContext ctx;
    ctx.keys.resize(hs.size());
    ctx.values.resize(hs.size());
#if defined(TERARKDB_WITH_AIO_FUTURE)
    ctx.futures.resize(hs.size());
#endif
    ctx.ss.resize(hs.size());
    ctx.async_values.resize(hs.size());
    ctx.multi_values.resize(hs.size());
    ctx.async_status.resize(hs.size() + 1);
    uint64_t iter_seqno = uint64_t(-1);
    ctx.seqno = uint64_t(-1);
    std::mt19937_64 mt(seed);
    for (ctx.count = atomic_count;; ctx.count = atomic_count) {
      std::uniform_int_distribution<uint64_t> uid(0, ctx.count);
      if (flags_ & ReadOnly) {
        ReadOnlyTest(ctx);
        return;
      }
      if ((flags_ & TestIter) && (ctx.count % 103 == 0)) {
        IterTest(ctx, mt, uid);
      }
      if (ctx.count % 17 == 0) {
        ApproximateSizes(mt, uid);
      }
      if ((flags_ & TestGet) && (ctx.count % 5 == 0)) {
        GetTest(ctx, mt, uid);
      }
      if ((flags_ & TestAsync) && (ctx.count % 13 == 0)) {
        AsyncTest(ctx, mt, uid);
      }
      if ((flags_ & TestHash) && (ctx.count % 2 == 0)) {
        HashTest(ctx, mt, uid);
      }
    }
  }

  void CheckAssert(ReadContext &ctx, bool assert_value, const char *err) {
    if (!assert_value) {
      fprintf(stderr, "count = %zd, seqno = %" PRIu64 ", key = %s, err = %s \n",
              ctx.count, ctx.seqno, ctx.key.c_str(), err);
      fprintf(stderr, "Get:\n");
      for (size_t i = 0; i < hs.size(); ++i) {
        fprintf(stderr, "s%zd = %s, v%zd = %s\n", i,
                ctx.ss.size() > i ? ctx.ss[i].ToString().c_str() : "null", i,
                ctx.values.size() > i ? ctx.values[i].c_str() : "null");
      }
      if (flags_ & TestIter) {
        fprintf(stderr, "Iter:\n");
        for (size_t i = 0; i < hs.size(); ++i) {
          fprintf(stderr, "s%zd = %s, k%zd = %s, v%zd = %s\n", i,
                  ctx.iter.size() > i ? ctx.iter[i]->status().ToString().c_str()
                                      : "null",
                  i,
                  ctx.iter.size() > i && ctx.iter[i]->Valid()
                      ? ctx.iter[i]->key().ToString().c_str()
                      : "Invalid",
                  i,
                  ctx.iter.size() > i && ctx.iter[i]->Valid()
                      ? ctx.iter[i]->value().ToString().c_str()
                      : "Invalid");
        }
      }

      if (flags_ & TestAsync) {
        fprintf(stderr, "Async:\n");
        for (size_t i = 0; i < hs.size(); ++i) {
          const auto &tmp_tuple = ctx.futures[i].get();
          fprintf(stderr, "Get : s%zd = %s, k%zd = %s, v%zd = %s\n", i,
                  ctx.ss.size() > i ? ctx.ss[i].ToString().c_str() : "null", i,
                  ctx.key.c_str(), i,
                  ctx.values.size() > i ? ctx.values[i].c_str() : "null");
#if defined(TERARKDB_WITH_AIO_FUTURE)
          const auto &tmp_tuple = ctx.futures[i].get();
          fprintf(stderr, "GetFuture : s%zd = %s, k%zd = %s, v%zd = %s\n", i,
                  std::get<0>(tmp_tuple).ToString().c_str(), i,
                  std::get<1>(tmp_tuple).c_str(), i,
                  std::get<2>(tmp_tuple).c_str());
          fprintf(stderr, "GetAsync : s%zd = %s, k%zd = %s, v%zd = %s\n", i,
                  ctx.async_status[i].ToString().c_str(), i, ctx.key.c_str(), i,
                  ctx.async_values[i].c_str());
#endif
        }
      }
      has_error = true;
      assert(assert_value);
    }
  }
};  // ChaosTest
}  // namespace rocksdb

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  uint16_t flags = 0;
  if (FLAGS_get) {
    flags |= TestGet;
    fprintf(stderr, "Test Get\n");
  }

  if (FLAGS_iter) {
    flags |= TestIter;
    fprintf(stderr, "Test Iterator\n");
  }

  if (FLAGS_async) {
    flags |= TestAsync;
    fprintf(stderr, "Test GetAsync and GetFuture\n");
  }

  if (FLAGS_worker) {
    flags |= TestWorker;
    fprintf(stderr, "Test Worker Compaction\n");
  }

  if (FLAGS_hash) {
    flags |= TestHash;
    fprintf(stderr, "Test Hash(key) after Compaction\n");
  }

  if (FLAGS_readonly) {
    flags |= ReadOnly;
    fprintf(stderr, "Test ReadOnly Open\n");
  }

  if (FLAGS_compactrange) {
    flags |= TestCompaction;
    fprintf(stderr, "Test CompactRange\n");
  }

  if (FLAGS_terark) {
    flags |= TestTerark;
    fprintf(stderr, "Test Terark Table\n");
  }

  if (FLAGS_rangedel) {
    flags |= TestRangeDel;
    fprintf(stderr, "Test RangeDel\n");
  }
  int write_thread = FLAGS_write_thread;
  int read_thread = FLAGS_read_thread;
  int cf_num = FLAGS_cf_num;
  std::vector<std::thread> thread_vec;
  rocksdb::ChaosTest test(flags);
  test.Open(cf_num);
  for (int j = 0; j < read_thread; ++j) {
    thread_vec.emplace_back([&test, j] { test.ReadFunc(j); });
  }
  for (int j = 0; j < write_thread; ++j) {
    thread_vec.emplace_back([&test, j] { test.WriteFunc(j); });
  }
  thread_vec.emplace_back([&test]() { test.DumpCacheStatistics(); });
  for (auto &t : thread_vec) {
    t.join();
  }
  test.Close();
  return 0;
}
