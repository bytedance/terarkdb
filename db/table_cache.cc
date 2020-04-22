//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/dbformat.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/version_edit.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/statistics.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "table/two_level_iterator.h"
#include "util/c_style_callback.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"

namespace rocksdb {

namespace {

template <class T>
static void DeleteEntry(const Slice& /*key*/, void* value) {
  T* typed_value = reinterpret_cast<T*>(value);
  delete typed_value;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

static void DeleteTableReader(void* arg1, void* arg2) {
  TableReader* table_reader = reinterpret_cast<TableReader*>(arg1);
  Statistics* stats = reinterpret_cast<Statistics*>(arg2);
  RecordTick(stats, NO_FILE_CLOSES);
  delete table_reader;
}

static Slice GetSliceForFileNumber(const uint64_t* file_number) {
  return Slice(reinterpret_cast<const char*>(file_number),
               sizeof(*file_number));
}

// Store params for create depend table iterator in future
class LazyCreateIterator : public Snapshot {
  TableCache* table_cache_;
  ReadOptions options_;  // deep copy
  SequenceNumber snapshot_;
  const EnvOptions& env_options_;
  const InternalKeyComparator& icomparator_;
  RangeDelAggregator* range_del_agg_;
  const SliceTransform* prefix_extractor_;
  bool for_compaction_;
  bool skip_filters_;
  int level_;

 public:
  LazyCreateIterator(TableCache* table_cache, const ReadOptions& options,
                     const EnvOptions& env_options,
                     const InternalKeyComparator& icomparator,
                     RangeDelAggregator* range_del_agg,
                     const SliceTransform* prefix_extractor,
                     bool for_compaction, bool skip_filters,
                     bool ignore_range_deletions, int level)
      : table_cache_(table_cache),
        options_(options),
        snapshot_(0),
        env_options_(env_options),
        icomparator_(icomparator),
        range_del_agg_(range_del_agg),
        prefix_extractor_(prefix_extractor),
        for_compaction_(for_compaction),
        skip_filters_(skip_filters),
        level_(level) {
    if (options.snapshot != nullptr) {
      snapshot_ = options.snapshot->GetSequenceNumber();
      options_.snapshot = this;
    }
    options_.iterate_lower_bound = nullptr;
    options_.iterate_upper_bound = nullptr;
    options_.ignore_range_deletions = ignore_range_deletions;
  }
  ~LazyCreateIterator() = default;

  SequenceNumber GetSequenceNumber() const override { return snapshot_; }

  InternalIterator* operator()(const FileMetaData* _f,
                               const DependenceMap& _dependence_map,
                               Arena* _arena, TableReader** _reader_ptr) {
    return table_cache_->NewIterator(
        options_, env_options_, icomparator_, *_f, _dependence_map,
        range_del_agg_, prefix_extractor_, _reader_ptr, nullptr,
        for_compaction_, _arena, skip_filters_, level_);
  }
};

}  // namespace

TableCache::TableCache(const ImmutableCFOptions& ioptions,
                       const EnvOptions& env_options, Cache* const cache)
    : ioptions_(ioptions),
      env_options_(env_options),
      cache_(cache),
      immortal_tables_(false) {
  if (ioptions_.row_cache) {
    // If the same cache is shared by multiple instances, we need to
    // disambiguate its entries.
    PutVarint64(&row_cache_id_, ioptions_.row_cache->NewId());
  }
}

TableCache::~TableCache() {}

TableReader* TableCache::GetTableReaderFromHandle(Cache::Handle* handle) {
  return reinterpret_cast<TableReader*>(cache_->Value(handle));
}

void TableCache::ReleaseHandle(Cache::Handle* handle) {
  cache_->Release(handle);
}

Status TableCache::GetTableReader(
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    bool sequential_mode, size_t readahead, bool record_read_stats,
    HistogramImpl* file_read_hist, std::unique_ptr<TableReader>* table_reader,
    const SliceTransform* prefix_extractor, bool skip_filters, int level,
    bool prefetch_index_and_filter_in_cache, bool for_compaction) {
  std::string fname =
      TableFileName(ioptions_.cf_paths, fd.GetNumber(), fd.GetPathId());
  std::unique_ptr<RandomAccessFile> file;
  Status s = ioptions_.env->NewRandomAccessFile(fname, &file, env_options);

  RecordTick(ioptions_.statistics, NO_FILE_OPENS);
  if (s.ok()) {
    if (readahead > 0 && !env_options.use_mmap_reads) {
      // Not compatible with mmap files since ReadaheadRandomAccessFile requires
      // its wrapped file's Read() to copy data into the provided scratch
      // buffer, which mmap files don't use.
      // TODO(ajkr): try madvise for mmap files in place of buffered readahead.
      file = NewReadaheadRandomAccessFile(std::move(file), readahead);
    }
    if (!sequential_mode && ioptions_.advise_random_on_open) {
      file->Hint(RandomAccessFile::RANDOM);
    }
    StopWatch sw(ioptions_.env, ioptions_.statistics, TABLE_OPEN_IO_MICROS);
    std::unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(
            std::move(file), fname, ioptions_.env,
            record_read_stats ? ioptions_.statistics : nullptr, SST_READ_MICROS,
            file_read_hist, ioptions_.rate_limiter, for_compaction,
            ioptions_.listeners));
    s = ioptions_.table_factory->NewTableReader(
        TableReaderOptions(ioptions_, prefix_extractor, env_options,
                           internal_comparator, skip_filters, immortal_tables_,
                           level, fd.GetNumber(), fd.largest_seqno),
        std::move(file_reader), fd.GetFileSize(), table_reader,
        prefetch_index_and_filter_in_cache);
    TEST_SYNC_POINT("TableCache::GetTableReader:0");
  }
  return s;
}

void TableCache::EraseHandle(const FileDescriptor& fd, Cache::Handle* handle) {
  ReleaseHandle(handle);
  uint64_t number = fd.GetNumber();
  Slice key = GetSliceForFileNumber(&number);
  cache_->Erase(key);
}

Status TableCache::FindTable(const EnvOptions& env_options,
                             const InternalKeyComparator& internal_comparator,
                             const FileDescriptor& fd, Cache::Handle** handle,
                             const SliceTransform* prefix_extractor,
                             const bool no_io, bool record_read_stats,
                             HistogramImpl* file_read_hist, bool skip_filters,
                             int level,
                             bool prefetch_index_and_filter_in_cache) {
  PERF_TIMER_GUARD(find_table_nanos);
  Status s;
  uint64_t number = fd.GetNumber();
  Slice key = GetSliceForFileNumber(&number);
  *handle = cache_->Lookup(key);
  TEST_SYNC_POINT_CALLBACK("TableCache::FindTable:0",
                           const_cast<bool*>(&no_io));

  if (*handle == nullptr) {
    if (no_io) {  // Don't do IO and return a not-found status
      return Status::Incomplete("Table not found in table_cache, no_io is set");
    }
    std::unique_ptr<TableReader> table_reader;
    s = GetTableReader(env_options, internal_comparator, fd,
                       false /* sequential mode */, 0 /* readahead */,
                       record_read_stats, file_read_hist, &table_reader,
                       prefix_extractor, skip_filters, level,
                       prefetch_index_and_filter_in_cache);
    if (!s.ok()) {
      assert(table_reader == nullptr);
      RecordTick(ioptions_.statistics, NO_FILE_ERRORS);
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      s = cache_->Insert(key, table_reader.get(), 1, &DeleteEntry<TableReader>,
                         handle);
      if (s.ok()) {
        table_reader->SetTableCacheHandle(cache_, *handle);
        // Release ownership of table reader.
        table_reader.release();
      }
    }
  }
  return s;
}

InternalIterator* TableCache::NewIterator(
    const ReadOptions& options, const EnvOptions& env_options,
    const InternalKeyComparator& icomparator, const FileMetaData& file_meta,
    const DependenceMap& dependence_map, RangeDelAggregator* range_del_agg,
    const SliceTransform* prefix_extractor, TableReader** table_reader_ptr,
    HistogramImpl* file_read_hist, bool for_compaction, Arena* arena,
    bool skip_filters, int level) {
  PERF_TIMER_GUARD(new_table_iterator_nanos);

  Status s;
  bool create_new_table_reader = false;
  TableReader* table_reader = nullptr;
  Cache::Handle* handle = nullptr;
  if (table_reader_ptr != nullptr) {
    *table_reader_ptr = nullptr;
  }
  size_t readahead = 0;
  bool record_stats = !for_compaction;
  if (file_meta.prop.is_map_sst()) {
    record_stats = false;
  } else {
    // MapSST don't handle these
    if (for_compaction) {
#ifndef NDEBUG
      bool use_direct_reads_for_compaction = env_options.use_direct_reads;
      TEST_SYNC_POINT_CALLBACK("TableCache::NewIterator:for_compaction",
                               &use_direct_reads_for_compaction);
#endif  // !NDEBUG
      if (ioptions_.new_table_reader_for_compaction_inputs) {
        // get compaction_readahead_size from env_options allows us to set the
        // value dynamically
        readahead = env_options.compaction_readahead_size;
        create_new_table_reader = true;
      }
    } else {
      readahead = options.readahead_size;
      create_new_table_reader = readahead > 0;
    }
  }

  auto& fd = file_meta.fd;
  if (create_new_table_reader) {
    std::unique_ptr<TableReader> table_reader_unique_ptr;
    s = GetTableReader(
        env_options, icomparator, fd, true /* sequential_mode */, readahead,
        record_stats, nullptr, &table_reader_unique_ptr, prefix_extractor,
        false /* skip_filters */, level,
        true /* prefetch_index_and_filter_in_cache */, for_compaction);
    if (s.ok()) {
      table_reader = table_reader_unique_ptr.release();
    }
  } else {
    table_reader = fd.table_reader;
    if (table_reader == nullptr) {
      s = FindTable(env_options, icomparator, fd, &handle, prefix_extractor,
                    options.read_tier == kBlockCacheTier /* no_io */,
                    record_stats, file_read_hist, skip_filters, level);
      if (s.ok()) {
        table_reader = GetTableReaderFromHandle(handle);
      }
    }
  }
  InternalIterator* result = nullptr;
  if (s.ok()) {
    if (!file_meta.prop.is_map_sst()) {
      if (options.table_filter &&
          !options.table_filter(*table_reader->GetTableProperties())) {
        result = NewEmptyInternalIterator<LazyBuffer>(arena);
      } else {
        result = table_reader->NewIterator(options, prefix_extractor, arena,
                                           skip_filters, for_compaction);
      }
    } else {
      ReadOptions map_options = options;
      map_options.total_order_seek = true;
      map_options.readahead_size = 0;
      result =
          table_reader->NewIterator(map_options, prefix_extractor, arena,
                                    skip_filters, false /* for_compaction */);
      if (!dependence_map.empty()) {
        bool ignore_range_deletions =
            options.ignore_range_deletions ||
            file_meta.prop.map_handle_range_deletions();
        LazyCreateIterator* lazy_create_iter;
        if (arena != nullptr) {
          void* buffer = arena->AllocateAligned(sizeof(LazyCreateIterator));
          lazy_create_iter = new (buffer) LazyCreateIterator(
              this, options, env_options, icomparator, range_del_agg,
              prefix_extractor, for_compaction, skip_filters,
              ignore_range_deletions, level);

        } else {
          lazy_create_iter = new LazyCreateIterator(
              this, options, env_options, icomparator, range_del_agg,
              prefix_extractor, for_compaction, skip_filters,
              ignore_range_deletions, level);
        }
        auto map_sst_iter = NewMapSstIterator(
            &file_meta, result, dependence_map, icomparator, lazy_create_iter,
            c_style_callback(*lazy_create_iter), arena);
        if (arena != nullptr) {
          map_sst_iter->RegisterCleanup(
              [](void* arg1, void* arg2) {
                static_cast<InternalIterator*>(arg1)->~InternalIterator();
                static_cast<LazyCreateIterator*>(arg2)->~LazyCreateIterator();
              },
              result, lazy_create_iter);
        } else {
          map_sst_iter->RegisterCleanup(
              [](void* arg1, void* arg2) {
                delete static_cast<InternalIterator*>(arg1);
                delete static_cast<LazyCreateIterator*>(arg2);
              },
              result, lazy_create_iter);
        }
        result = map_sst_iter;
      }
    }
    if (create_new_table_reader) {
      assert(handle == nullptr);
      result->RegisterCleanup(&DeleteTableReader, table_reader,
                              ioptions_.statistics);
    } else if (handle != nullptr) {
      result->RegisterCleanup(&UnrefEntry, cache_, handle);
      handle = nullptr;  // prevent from releasing below
    }

    if (for_compaction) {
      table_reader->SetupForCompaction();
    }
    if (table_reader_ptr != nullptr) {
      *table_reader_ptr = table_reader;
    }
  }
  if (s.ok() && range_del_agg != nullptr && !options.ignore_range_deletions) {
    if (range_del_agg->AddFile(fd.GetNumber())) {
      std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
          static_cast<FragmentedRangeTombstoneIterator*>(
              table_reader->NewRangeTombstoneIterator(options)));
      if (range_del_iter != nullptr) {
        s = range_del_iter->status();
      }
      if (s.ok()) {
        range_del_agg->AddTombstones(
            std::move(range_del_iter), &file_meta.smallest, &file_meta.largest);
      }
    }
  }

  if (handle != nullptr) {
    ReleaseHandle(handle);
  }
  if (!s.ok()) {
    assert(result == nullptr);
    result = NewErrorInternalIterator<LazyBuffer>(s, arena);
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       const InternalKeyComparator& internal_comparator,
                       const FileMetaData& file_meta,
                       const DependenceMap& dependence_map, const Slice& k,
                       GetContext* get_context,
                       const SliceTransform* prefix_extractor,
                       HistogramImpl* file_read_hist, bool skip_filters,
                       int level) {
  auto& fd = file_meta.fd;
  IterKey key_buffer;
  Status s;
  TableReader* t = fd.table_reader;
  Cache::Handle* handle = nullptr;
  if (t == nullptr) {
    s = FindTable(
        env_options_, internal_comparator, fd, &handle, prefix_extractor,
        options.read_tier == kBlockCacheTier /* no_io */,
        true /* record_read_stats */, file_read_hist, skip_filters, level);
    if (s.ok()) {
      t = GetTableReaderFromHandle(handle);
    }
  }
  if (s.ok()) {
    t->UpdateMaxCoveringTombstoneSeq(options, ExtractUserKey(k),
                                     get_context->max_covering_tombstone_seq());
    if (!file_meta.prop.is_map_sst()) {
      s = t->Get(options, k, get_context, prefix_extractor, skip_filters);
    } else if (dependence_map.empty()) {
      s = Status::Corruption(
          "TableCache::Get: Composite sst depend files missing");
    } else {
      // Forward query to target sst
      ReadOptions forward_options = options;
      forward_options.ignore_range_deletions |=
          file_meta.prop.map_handle_range_deletions();
      auto get_from_map = [&](const Slice& largest_key,
                              LazyBuffer&& map_value) {
        s = map_value.fetch();
        if (!s.ok()) {
          return false;
        }
        // Manual inline MapSstElement::Decode
        const char* err_msg = "Map sst invalid link_value";
        Slice map_input = map_value.slice();
        Slice smallest_key;
        uint64_t link_count;
        uint64_t flags;
        Slice find_k = k;
        auto& icomp = internal_comparator;

        if (!GetVarint64(&map_input, &flags) ||
            !GetVarint64(&map_input, &link_count) ||
            !GetLengthPrefixedSlice(&map_input, &smallest_key)) {
          s = Status::Corruption(err_msg);
          return false;
        }
        // don't care kNoRecords, Get call need load
        // max_covering_tombstone_seq
        int include_smallest = (flags & MapSstElement::kIncludeSmallest) != 0;
        int include_largest = (flags & MapSstElement::kIncludeLargest) != 0;

        // include_smallest ? cmp_result > 0 : cmp_result >= 0
        if (icomp.Compare(smallest_key, k) >= include_smallest) {
          if (icomp.user_comparator()->Compare(ExtractUserKey(smallest_key),
                                               ExtractUserKey(k)) != 0) {
            // k is out of smallest bound
            return false;
          }
          assert(ExtractInternalKeyFooter(k) >
                 ExtractInternalKeyFooter(smallest_key));
          if (include_smallest) {
            // shrink to smallest_key
            find_k = smallest_key;
          } else {
            uint64_t seq_type = ExtractInternalKeyFooter(smallest_key);
            if (seq_type == 0) {
              // 'smallest_key' has the largest seq_type of current user_key
              // k is out of smallest bound
              return false;
            }
            // make find_k a bit greater than smallest_key
            key_buffer.SetInternalKey(smallest_key, true);
            find_k = key_buffer.GetInternalKey();
            EncodeFixed64(const_cast<char*>(find_k.data() + find_k.size() - 8),
                          seq_type - 1);
          }
        }

        bool is_largest_user_key =
            icomp.user_comparator()->Compare(ExtractUserKey(largest_key),
                                             ExtractUserKey(k)) == 0;
        uint64_t min_seq_type_backup = get_context->GetMinSequenceAndType();
        if (is_largest_user_key) {
          // shrink seqno to largest_key, make sure can't read greater keys
          uint64_t seq_type = ExtractInternalKeyFooter(largest_key);
          assert(seq_type <=
                 PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
          // For safety. may kValueTypeForSeek can be 255 in the future ?
          if (seq_type == port::kMaxUint64 && !include_largest) {
            // 'largest_key' has the smallest seq_type of current user_key
            // k is out of largest bound. go next map element
            return true;
          }
          get_context->SetMinSequenceAndType(
              std::max(min_seq_type_backup, seq_type + !include_largest));
        }

        uint64_t file_number;
        for (uint64_t i = 0; i < link_count; ++i) {
          if (!GetVarint64(&map_input, &file_number)) {
            s = Status::Corruption(err_msg);
            return false;
          }
          auto find = dependence_map.find(file_number);
          if (find == dependence_map.end()) {
            s = Status::Corruption("Map sst dependence missing");
            return false;
          }
          assert(find->second->fd.GetNumber() == file_number);
          s = Get(forward_options, internal_comparator, *find->second,
                  dependence_map, find_k, get_context, prefix_extractor,
                  file_read_hist, skip_filters, level);

          if (!s.ok() || get_context->is_finished()) {
            // error or found, recovery min_seq_type_backup is unnecessary
            return false;
          }
        }
        // recovery min_seq_backup
        get_context->SetMinSequenceAndType(min_seq_type_backup);
        return is_largest_user_key;
      };
      t->RangeScan(&k, prefix_extractor, &get_from_map,
                   c_style_callback(get_from_map));
    }
  } else if (options.read_tier == kBlockCacheTier && s.IsIncomplete()) {
    // Couldn't find Table in cache but treat as kFound if no_io set
    get_context->MarkKeyMayExist();
    s = Status::OK();
  }
  if (handle != nullptr) {
    ReleaseHandle(handle);
  }
  return s;
}

Status TableCache::GetTableProperties(
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    std::shared_ptr<const TableProperties>* properties,
    const SliceTransform* prefix_extractor, bool no_io) {
  Status s;
  auto table_reader = fd.table_reader;
  // table already been pre-loaded?
  if (table_reader) {
    *properties = table_reader->GetTableProperties();

    return s;
  }

  Cache::Handle* table_handle = nullptr;
  s = FindTable(env_options, internal_comparator, fd, &table_handle,
                prefix_extractor, no_io);
  if (!s.ok()) {
    return s;
  }
  assert(table_handle);
  auto table = GetTableReaderFromHandle(table_handle);
  *properties = table->GetTableProperties();
  ReleaseHandle(table_handle);
  return s;
}

size_t TableCache::GetMemoryUsageByTableReader(
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    const SliceTransform* prefix_extractor) {
  Status s;
  auto table_reader = fd.table_reader;
  // table already been pre-loaded?
  if (table_reader) {
    return table_reader->ApproximateMemoryUsage();
  }

  Cache::Handle* table_handle = nullptr;
  s = FindTable(env_options, internal_comparator, fd, &table_handle,
                prefix_extractor, true);
  if (!s.ok()) {
    return 0;
  }
  assert(table_handle);
  auto table = GetTableReaderFromHandle(table_handle);
  auto ret = table->ApproximateMemoryUsage();
  ReleaseHandle(table_handle);
  return ret;
}

void TableCache::Evict(Cache* cache, uint64_t file_number) {
  cache->Erase(GetSliceForFileNumber(&file_number));
}

void TableCache::TEST_AddMockTableReader(TableReader* table_reader, FileDescriptor fd) {
  Status s;
  uint64_t number = fd.GetNumber();
  Slice key = GetSliceForFileNumber(&number);
  s = cache_->Insert(key, table_reader, 1, &DeleteEntry<TableReader>);
}

}  // namespace rocksdb