//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/compaction_dispatcher.h"

#include "table/get_context.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include <chrono>

#ifdef WITH_TERARK_ZIP
#include <terark/num_to_str.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/process.hpp>
#endif

#include "db/compaction_iterator.h"
#include "db/map_builder.h"
#include "db/merge_helper.h"
#include "db/range_del_aggregator.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/types.h"
#include "table/merging_iterator.h"
#include "table/table_reader.h"
#include "table/two_level_iterator.h"
#include "util/c_style_callback.h"
#include "util/filename.h"

#ifndef WITH_TERARK_ZIP
#define USE_AJSON 1
#endif

#ifdef USE_AJSON
#include "util/ajson_msd.hpp"
#else
#include <terark/io/DataIO.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/preproc.hpp>
#define AJSON(Class, ...)             \
  DATA_IO_LOAD_SAVE_E(                \
      Class,                          \
      TERARK_PP_APPLY(TERARK_PP_JOIN, \
                      TERARK_PP_MAP(TERARK_PP_PREPEND, &, __VA_ARGS__)))
#endif

struct AJsonStatus {
  unsigned char code, subcode, sev;
  std::string state;
};
AJSON(AJsonStatus, code, subcode, sev, state);

namespace ajson {
#ifdef USE_AJSON
template <>
struct json_impl<rocksdb::Status, void> {
  static inline void read(reader& rd, rocksdb::Status& v) {
    AJsonStatus s;
    json_impl<AJsonStatus>::read(rd, s);
    v = rocksdb::Status(s.code, s.subcode, s.sev,
                        s.state.empty() ? nullptr : s.state.c_str());
  }
  template <typename write_ty>
  static inline void write(write_ty& wt, rocksdb::Status const& v) {
    AJsonStatus s = {v.code(), v.subcode(), v.severity(),
                     v.getState() == nullptr ? std::string() : v.getState()};
    json_impl<AJsonStatus>::template write<write_ty>(wt, s);
  }
};

template <>
struct json_impl<rocksdb::CompactionWorkerContext::EncodedString, void> {
  static inline void read(reader& rd,
                          rocksdb::CompactionWorkerContext::EncodedString& v) {
    std::string s;
    json_impl<std::string>::read(rd, s);
    v.data.resize(s.size() / 2);
    for (size_t i = 0; i < s.size(); i += 2) {
      char x[3] = {s[i], s[i + 1], '\0'};
      v.data[i / 2] = std::stoi(x, nullptr, 16);
    }
  }
  template <typename write_ty>
  static inline void write(
      write_ty& wt, rocksdb::CompactionWorkerContext::EncodedString const& v) {
    json_impl<std::string>::template write<write_ty>(
        wt, rocksdb::Slice(v.data).ToString(true));
  }
};

template <>
struct json_impl<rocksdb::InternalKey, void> {
  static inline void read(reader& rd, rocksdb::InternalKey& v) {
    rocksdb::CompactionWorkerContext::EncodedString s;
    json_impl<rocksdb::CompactionWorkerContext::EncodedString>::read(rd, s);
    *v.rep() = std::move(s.data);
  }
  template <typename write_ty>
  static inline void write(write_ty& wt, rocksdb::InternalKey const& v) {
    rocksdb::Slice s(*v.rep());
    json_impl<std::string>::template write<write_ty>(wt, s.ToString(true));
  }
};

template <class T>
void load_from_buff(T& x, std::string& data) {
  load_from_buff(x, &data[0], data.size());
}

template <class T>
void load_from_buff(T& x, const rocksdb::Slice& data) {
  load_from_buff(x, &data.ToString()[0], data.size());
}

#else  // USE_AJSON

template <class T>
void load_from_buff(T& x, const rocksdb::Slice& data) {
  using namespace terark;
  LittleEndianDataInput<MemIO> dio;
  dio.set((char*)data.data(), data.size());
  dio >> x;
}

struct string_stream : terark::LittleEndianDataOutput<terark::AutoGrownMemIO> {
  std::string str() const { return std::string((char*)m_beg, m_pos - m_beg); }
};

template <class T>
void save_to(string_stream& ss, const T& x) {
  ss.resize(128 * 1024);
  ss << x;
}

#endif
}  // namespace ajson

#ifdef USE_AJSON
using namespace rocksdb;
#else
namespace rocksdb {

template <class DataIO>
void DataIO_loadObject(DataIO& dio, rocksdb::Status& x) {
  AJsonStatus s;
  dio >> s;
  x = rocksdb::Status(s.code, s.subcode, s.sev,
                      s.state.empty() ? nullptr : s.state.c_str());
}

template <class DataIO>
void DataIO_saveObject(DataIO& dio, const rocksdb::Status& v) {
  AJsonStatus s = {v.code(), v.subcode(), v.severity(),
                   v.getState() == nullptr ? std::string() : v.getState()};
  dio << s;
}  // namespace ajson

template <class DataIO>
void DataIO_loadObject(DataIO& dio, InternalKey& x) {
  dio >> *x.rep();
}

template <class DataIO>
void DataIO_saveObject(DataIO& dio, const InternalKey& x) {
  dio << *x.rep();
}

template <class DataIO>
void DataIO_loadObject(DataIO& dio, CompressionType& x) {
  static_assert(sizeof(CompressionType) == 1, "sizeof(CompressionType) == 1");
  dio >> (unsigned char&)x;
}

template <class DataIO>
void DataIO_saveObject(DataIO& dio, const CompressionType x) {
  dio << (unsigned char)x;
}

using EncodedString = CompactionWorkerContext::EncodedString;
AJSON(EncodedString, data);

#endif

AJSON(Dependence, file_number, entry_count);

using FileInfo = CompactionWorkerResult::FileInfo;
AJSON(FileInfo, smallest, largest, file_name, smallest_seqno, largest_seqno,
      file_size, marked_for_compaction);

AJSON(CompactionWorkerResult, status, actual_start, actual_end, files, stat_all,
      time_us);

AJSON(FileDescriptor, packed_number_and_path_id, file_size, smallest_seqno,
      largest_seqno);

AJSON(TablePropertyCache, num_entries, num_deletions, raw_key_size,
      raw_value_size, flags, purpose, max_read_amp, read_amp, dependence,
      inheritance_chain);

AJSON(FileMetaData, fd, smallest, largest, prop);

AJSON(CompressionOptions, window_bits, level, strategy, max_dict_bytes,
      zstd_max_train_bytes, enabled);

AJSON(CompactionFilterContext, is_full_compaction, is_manual_compaction,
      column_family_id);

using NameParam = CompactionWorkerContext::NameParam;
AJSON(NameParam, name, param);

AJSON(BlobConfig, blob_size, large_key_ratio);

AJSON(CompactionWorkerContext, user_comparator, merge_operator,
      merge_operator_data, value_meta_extractor_factory,
      value_meta_extractor_factory_options, compaction_filter,
      compaction_filter_factory, compaction_filter_context,
      compaction_filter_data, blob_config, table_factory, table_factory_options,
      bloom_locality, cf_paths, prefix_extractor, prefix_extractor_options,
      has_start, has_end, start, end, last_sequence,
      earliest_write_conflict_snapshot, preserve_deletes_seqnum, file_metadata,
      inputs, cf_name, target_file_size, compression, compression_opts,
      existing_snapshots, smallest_user_key, largest_user_key, level,
      output_level, number_levels, skip_filters, bottommost_level,
      allow_ingest_behind, preserve_deletes, int_tbl_prop_collector_factories);

#ifdef USE_AJSON
#else
}  // namespace rocksdb
#endif

namespace rocksdb {

template <class T>
using STMap = std::unordered_map<std::string, std::shared_ptr<T>>;

class WorkerSeparateHelper : public SeparateHelper, public LazyBufferState {
 public:
  void destroy(LazyBuffer* /*buffer*/) const override {}

  Status pin_buffer(LazyBuffer* /*buffer*/) const override {
    return Status::OK();
  }

  Status fetch_buffer(LazyBuffer* buffer) const override {
    return inplace_decode_callback_(inplace_decode_arg_, buffer,
                                    get_context(buffer));
  }

  using SeparateHelper::TransToSeparate;
  Status TransToSeparate(const Slice& internal_key, LazyBuffer& value,
                         bool is_merge, bool is_index) override {
    std::string meta = GetValueMeta(internal_key, value);
    return SeparateHelper::TransToSeparate(
        internal_key, value, value.file_number(),
        Slice(meta.data(), meta.size()), is_merge, is_index,
        value_meta_extractor_.get());
  }

  LazyBuffer TransToCombined(const Slice& user_key, uint64_t sequence,
                             LazyBuffer&& value) const override {
    auto s = value.fetch();
    if (!s.ok()) {
      return LazyBuffer(std::move(s));
    }
    uint64_t file_number = SeparateHelper::DecodeFileNumber(value.slice());
    auto find = dependence_map_->find(file_number);
    if (find == dependence_map_->end()) {
      return LazyBuffer(
          Status::Corruption("Separate value dependence missing"));
    } else {
      return LazyBuffer(
          this,
          {reinterpret_cast<uint64_t>(user_key.data()), user_key.size(),
           sequence, reinterpret_cast<uint64_t>(&*find)},
          Slice::Invalid(), find->second->fd.GetNumber());
    }
  }

  WorkerSeparateHelper(
      DependenceMap* dependence_map,
      std::unique_ptr<ValueExtractor> value_meta_extractor,
      void* inplace_decode_arg,
      Status (*inplace_decode_callback)(void* arg, LazyBuffer* buffer,
                                        LazyBufferContext* rep))
      : dependence_map_(dependence_map),
        value_meta_extractor_(std::move(value_meta_extractor)),
        inplace_decode_arg_(inplace_decode_arg),
        inplace_decode_callback_(inplace_decode_callback) {}

  DependenceMap* dependence_map_;
  std::unique_ptr<ValueExtractor> value_meta_extractor_;
  void* inplace_decode_arg_;
  Status (*inplace_decode_callback_)(void* arg, LazyBuffer* buffer,
                                     LazyBufferContext* rep);
};

std::function<CompactionWorkerResult()>
RemoteCompactionDispatcher::StartCompaction(
    const CompactionWorkerContext& context) {
  ajson::string_stream stream;
  ajson::save_to(stream, context);
  struct Result {
    Result(std::future<std::string>&& _future) : future(_future.share()) {}

    std::shared_future<std::string> future;

    CompactionWorkerResult operator()() {
      CompactionWorkerResult result;
      std::string encoded_result = future.get();
      try {
        ajson::load_from_buff(result, encoded_result);
      } catch (const std::exception& ex) {
        terark::string_appender<> detail;
        detail << "encoded_result[len=" << encoded_result.size() << "]: ";
        detail << Slice(encoded_result).ToString(true /*hex*/);
        result.status = Status::Corruption(
            terark::fstring("exception.what = ") + ex.what(), detail);
      }
      return result;
    }
  };
  std::future<std::string> str_result = DoCompaction(stream.str());
  return Result(std::move(str_result));
}

static bool g_isCompactionWorkerNode = false;
bool IsCompactionWorkerNode() { return g_isCompactionWorkerNode; }

struct RemoteCompactionDispatcher::Worker::Rep {
  EnvOptions env_options;
  Env* env;
};

RemoteCompactionDispatcher::Worker::Worker(EnvOptions env_options, Env* env) {
  rep_ = new Rep();
  rep_->env_options = env_options;
  rep_->env = env;
  g_isCompactionWorkerNode = true;
}

RemoteCompactionDispatcher::Worker::~Worker() { delete rep_; }

class RemoteCompactionProxy : public CompactionIterator::CompactionProxy {
 public:
  RemoteCompactionProxy(const CompactionWorkerContext* context)
      : CompactionProxy(),
        largest_user_key_(context->largest_user_key.data),
        level_(context->level),
        number_levels_(context->number_levels),
        bottommost_level_(context->bottommost_level),
        allow_ingest_behind_(context->allow_ingest_behind),
        preserve_deletes_(context->preserve_deletes) {}

  int level(size_t /*compaction_input_level*/ = 0) const override {
    return level_;
  }
  bool KeyNotExistsBeyondOutputLevel(
      const Slice& /*user_key*/,
      std::vector<size_t>* /*level_ptrs*/) const override {
    return false;
  }
  bool bottommost_level() const override { return bottommost_level_; }
  int number_levels() const override { return number_levels_; }
  Slice GetLargestUserKey() const override { return largest_user_key_; }
  bool allow_ingest_behind() const override { return allow_ingest_behind_; }
  bool preserve_deletes() const override { return preserve_deletes_; }

 protected:
  Slice largest_user_key_;
  int level_, number_levels_;
  bool bottommost_level_, allow_ingest_behind_, preserve_deletes_;
};

static std::string make_error(Status&& status) {
  CompactionWorkerResult result;
  result.status = std::move(status);
  ajson::string_stream stream;
  ajson::save_to(stream, result);
  return stream.str();
};

std::string RemoteCompactionDispatcher::Worker::DoCompaction(Slice data) {
  CompactionWorkerContext context;
  ajson::load_from_buff(context, data);
  context.compaction_filter_context.smallest_user_key =
      context.smallest_user_key;
  context.compaction_filter_context.largest_user_key = context.largest_user_key;

  ImmutableDBOptions immutable_db_options = ImmutableDBOptions(DBOptions());
  ColumnFamilyOptions cf_options;
  if (context.user_comparator.empty()) {
    return make_error(Status::Corruption("Comparator name is empty!"));
  } else {
    cf_options.comparator = Comparator::create(context.user_comparator);
    if (!cf_options.comparator) {
      return make_error(Status::Corruption("Can not find comparator",
                                           context.user_comparator));
    }
  }
  if (!context.merge_operator.empty()) {
    cf_options.merge_operator.reset(MergeOperator::create(
        context.merge_operator, context.merge_operator_data));
    if (!cf_options.merge_operator) {
      return make_error(Status::Corruption("Missing merge_operator !"));
    }
  }
  if (!context.value_meta_extractor_factory.empty()) {
    cf_options.value_meta_extractor_factory.reset(ValueExtractorFactory::create(
        context.value_meta_extractor_factory,
        context.value_meta_extractor_factory_options));
    if (!cf_options.value_meta_extractor_factory) {
      return make_error(Status::Corruption("Missing value_meta_extractor !"));
    }
  }
  std::unique_ptr<CompactionFilter> filter_ptr;
  if (!context.compaction_filter.empty()) {
    if (!context.compaction_filter_factory.empty()) {
      return make_error(Status::Corruption(
          "CompactonFilter and CompactionFilterFactory are both specified"));
    }
    filter_ptr.reset(CompactionFilter::create(
        context.compaction_filter, context.compaction_filter_data,
        context.compaction_filter_context));
    if (!filter_ptr) {
      return make_error(Status::Corruption("Missing CompactionFilterFactory!"));
    }
    cf_options.compaction_filter = filter_ptr.get();
  } else if (!context.compaction_filter_factory.empty()) {
    cf_options.compaction_filter_factory.reset(CompactionFilterFactory::create(
        context.compaction_filter_factory, context.compaction_filter_data));
    if (!cf_options.compaction_filter_factory) {
      return make_error(Status::Corruption("Missing CompactionFilterFactory!"));
    }
  }
  if (context.table_factory.empty()) {
    return make_error(Status::Corruption("Bad table_factory name !"));
  } else {
    Status s;
    cf_options.table_factory.reset(TableFactory::create(
        context.table_factory, context.table_factory_options, &s));
    if (!cf_options.table_factory) {
      return make_error(std::move(s));
    }
  }
  cf_options.bloom_locality = context.bloom_locality;
  for (auto& path : context.cf_paths) {
    cf_options.cf_paths.emplace_back(DbPath(path, 0));
  }
  if (!context.prefix_extractor.empty()) {
    cf_options.prefix_extractor.reset(SliceTransform::create(
        context.prefix_extractor, context.prefix_extractor_options));
    if (!cf_options.prefix_extractor) {
      return make_error(Status::Corruption("Missing prefix_extractor !"));
    }
  }
  ImmutableCFOptions immutable_cf_options(immutable_db_options, cf_options);
  MutableCFOptions mutable_cf_options(cf_options);

  struct CollectorFactoriesHolder {
    std::vector<std::unique_ptr<IntTblPropCollectorFactory>> data;

    ~CollectorFactoriesHolder() {
      for (auto& ptr : data) {
        ptr.release();
      }
    }
  } int_tbl_prop_collector_factories;
  for (auto& collector : context.int_tbl_prop_collector_factories) {
    auto user_fac = TablePropertiesCollectorFactory::create(collector.name);
    if (!user_fac) {
      return make_error(
          Status::Corruption("Missing int_tbl_prop_collector_factories !"));
    }
    if (user_fac->NeedSerialize()) {
      Status s = user_fac->Deserialize(collector.param);
      if (!s.ok()) {
        return make_error(std::move(s));
      }
    }
    int_tbl_prop_collector_factories.data.emplace_back(
        new UserKeyTablePropertiesCollectorFactory(
            user_fac->shared_from_this()));
  }
  const Slice* start = nullptr;
  const Slice* end = nullptr;
  Slice start_slice, end_slice;
  if (context.has_start) {
    start_slice = context.start;
    start = &start_slice;
  }
  if (context.has_end) {
    end_slice = context.end;
    end = &end_slice;
  }

  auto icmp = &immutable_cf_options.internal_comparator;
  auto ucmp = icmp->user_comparator();
  Env* env = rep_->env;
  auto& env_opt = rep_->env_options;

  using namespace std::chrono;
  auto start_time = system_clock::now();

  // start run
  DependenceMap contxt_dependence_map;
  for (auto& pair : context.file_metadata) {
    contxt_dependence_map.emplace(pair.first, &pair.second);
  }
  std::unordered_map<int, std::vector<const FileMetaData*>> inputs;
  for (auto pair : context.inputs) {
    auto found = contxt_dependence_map.find(pair.second);
    if (contxt_dependence_map.end() != found) {
      inputs[pair.first].push_back(found->second);
    } else {
      assert(false);
    }
  }
  std::unordered_map<uint64_t, std::unique_ptr<TableReader>> table_cache;
  std::mutex table_cache_mutex;
  auto get_table_reader = [&](uint64_t file_number, TableReader** reader_ptr) {
    std::lock_guard<std::mutex> lock(table_cache_mutex);
    auto find = table_cache.find(file_number);
    if (find == table_cache.end()) {
      assert(contxt_dependence_map.count(file_number) > 0);
      const FileMetaData* file_metadata = contxt_dependence_map[file_number];
      std::string file_name =
          TableFileName(immutable_cf_options.cf_paths, file_number,
                        file_metadata->fd.GetPathId());
      std::unique_ptr<RandomAccessFile> file;
      auto s = env->NewRandomAccessFile(file_name, &file, env_opt);
      if (!s.ok()) {
        return s;
      }
      std::unique_ptr<RandomAccessFileReader> file_reader(
          new RandomAccessFileReader(std::move(file), file_name, env));
      std::unique_ptr<TableReader> reader;
      TableReaderOptions table_reader_options(
          immutable_cf_options, mutable_cf_options.prefix_extractor.get(),
          env_opt, *icmp, true, false, -1, file_number);
      s = immutable_cf_options.table_factory->NewTableReader(
          table_reader_options, std::move(file_reader),
          file_metadata->fd.file_size, &reader, false);
      if (!s.ok()) {
        return s;
      }
      find = table_cache.emplace(file_number, std::move(reader)).first;
    }
    if (reader_ptr != nullptr) {
      *reader_ptr = find->second.get();
    }
    return Status::OK();
  };
  struct c_style_new_iterator_t {
    void* arg;
    IteratorCache::CreateIterCallback callback;
  } c_style_new_iterator;
  auto new_iterator = [&](const FileMetaData* file_metadata,
                          const DependenceMap& depend_map, Arena* arena,
                          TableReader** reader_ptr) -> InternalIterator* {
    TableReader* reader;
    auto s = get_table_reader(file_metadata->fd.GetNumber(), &reader);
    if (!s.ok()) {
      return NewErrorInternalIterator(s);
    }
    if (reader_ptr != nullptr) {
      *reader_ptr = reader;
    }
    auto iterator = reader->NewIterator(
        ReadOptions(), mutable_cf_options.prefix_extractor.get(), arena);
    if (file_metadata->prop.is_map_sst() && !depend_map.empty()) {
      auto sst_iterator = NewMapSstIterator(
          file_metadata, iterator, depend_map, *icmp, c_style_new_iterator.arg,
          c_style_new_iterator.callback, arena);
      sst_iterator->RegisterCleanup(
          [](void* arg1, void* arg2) {
            auto iter_ptr = static_cast<InternalIterator*>(arg1);
            if (arg2 == nullptr) {  // arena
              delete iter_ptr;
            } else {
              static_cast<InternalIterator*>(iter_ptr)->~InternalIterator();
            }
          },
          iterator, arena);
      iterator = sst_iterator;
    }
    return iterator;
  };
  c_style_new_iterator.arg = &new_iterator;
  c_style_new_iterator.callback = c_style_callback(new_iterator);

  auto separate_inplace_decode = [&](LazyBuffer* buffer,
                                     LazyBufferContext* context) -> Status {
    Slice user_key(reinterpret_cast<const char*>(context->data[0]),
                   context->data[1]);
    uint64_t sequence = context->data[2];
    auto pair = *reinterpret_cast<DependenceMap::value_type*>(context->data[3]);
    bool value_found = false;
    SequenceNumber context_seq;
    GetContext get_context(ucmp, nullptr, immutable_cf_options.info_log,
                           nullptr, GetContext::kNotFound, user_key, buffer,
                           &value_found, nullptr, nullptr, nullptr, env,
                           &context_seq);
    IterKey iter_key;
    iter_key.SetInternalKey(user_key, sequence, kValueTypeForSeek);
    TableReader* reader;
    auto s = get_table_reader(pair.second->fd.GetNumber(), &reader);
    if (!s.ok()) {
      return s;
    }
    ReadOptions options;
    reader->Get(options, iter_key.GetInternalKey(), &get_context,
                mutable_cf_options.prefix_extractor.get(), true);
    if (!s.ok()) {
      return s;
    }
    if (context_seq != sequence ||
        (get_context.State() != GetContext::kFound &&
         get_context.State() != GetContext::kMerge)) {
      if (get_context.State() == GetContext::kCorrupt) {
        return std::move(get_context).CorruptReason();
      } else {
        char buf[128];
        snprintf(buf, sizeof buf,
                 "file number = %" PRIu64 "(%" PRIu64 "), sequence = %" PRIu64,
                 pair.second->fd.GetNumber(), pair.first, sequence);
        return Status::Corruption("Separate value missing", buf);
      }
    }
    assert(buffer->file_number() == pair.second->fd.GetNumber());
    return Status::OK();
  };

  auto create_value_meta_extractor = [&] {
    std::unique_ptr<ValueExtractor> value_meta_extractor;
    if (immutable_cf_options.value_meta_extractor_factory != nullptr) {
      ValueExtractorContext ve_context = {
          context.compaction_filter_context.column_family_id};
      value_meta_extractor = immutable_cf_options.value_meta_extractor_factory
                                 ->CreateValueExtractor(ve_context);
    }
    return value_meta_extractor;
  };

  WorkerSeparateHelper separate_helper(
      &contxt_dependence_map, create_value_meta_extractor(),
      &separate_inplace_decode, c_style_callback(separate_inplace_decode));

  CompactionRangeDelAggregator range_del_agg(icmp, context.existing_snapshots);

  Arena arena;
  auto create_input_iterator = [&]() -> InternalIterator* {
    MergeIteratorBuilder merge_iter_builder(icmp, &arena);
    for (auto& pair : inputs) {
      if (pair.first == 0 || pair.second.size() == 1) {
        merge_iter_builder.AddIterator(new_iterator(
            pair.second.front(), contxt_dependence_map, &arena, nullptr));
      } else {
        auto map_iter = NewMapElementIterator(
            pair.second.data(), pair.second.size(), icmp,
            c_style_new_iterator.arg, c_style_new_iterator.callback, &arena);
        auto level_iter = NewMapSstIterator(
            nullptr, map_iter, contxt_dependence_map, *icmp,
            c_style_new_iterator.arg, c_style_new_iterator.callback, &arena);
        level_iter->RegisterCleanup(
            [](void* arg1, void* /*arg2*/) {
              static_cast<InternalIterator*>(arg1)->~InternalIterator();
            },
            map_iter, nullptr);
        merge_iter_builder.AddIterator(level_iter);
      }
    }
    return merge_iter_builder.Finish();
  };
  ScopedArenaIterator input(create_input_iterator());

  auto compaction_filter = immutable_cf_options.compaction_filter;
  std::unique_ptr<CompactionFilter> compaction_filter_from_factory = nullptr;
  if (compaction_filter == nullptr &&
      immutable_cf_options.compaction_filter_factory != nullptr) {
    compaction_filter_from_factory =
        immutable_cf_options.compaction_filter_factory->CreateCompactionFilter(
            context.compaction_filter_context);
    compaction_filter = compaction_filter_from_factory.get();
  }

  MergeHelper merge(env, ucmp, immutable_cf_options.merge_operator,
                    compaction_filter, immutable_db_options.info_log.get(),
                    false /* internal key corruption is expected */,
                    context.existing_snapshots.empty()
                        ? 0
                        : context.existing_snapshots.back());

  CompactionWorkerResult result;
  InternalKey& actual_start = result.actual_start;
  InternalKey& actual_end = result.actual_end;

  std::unique_ptr<CompactionIterator> c_iter(new CompactionIterator(
      input.get(), &separate_helper, end, ucmp, &merge, context.last_sequence,
      &context.existing_snapshots, context.earliest_write_conflict_snapshot,
      nullptr, env, false, false, &range_del_agg,
      std::unique_ptr<CompactionIterator::CompactionProxy>(
          new RemoteCompactionProxy(&context)),
      context.blob_config, compaction_filter, nullptr,
      context.preserve_deletes_seqnum));

  if (start != nullptr) {
    actual_start.SetMinPossibleForUserKey(*start);
    input->Seek(actual_start.Encode());
  } else {
    actual_start.SetMinPossibleForUserKey(context.smallest_user_key);
    input->SeekToFirst();
  }
  c_iter->SeekToFirst();

  struct SecondPassIterStorage {
    std::aligned_storage<sizeof(CompactionRangeDelAggregator),
                         alignof(CompactionRangeDelAggregator)>::type
        range_del_agg;
    std::unique_ptr<CompactionFilter> compaction_filter_holder;
    const CompactionFilter* compaction_filter = nullptr;
    std::aligned_storage<sizeof(MergeHelper), alignof(MergeHelper)>::type merge;
    ScopedArenaIterator input;

    ~SecondPassIterStorage() {
      if (input.get() != nullptr) {
        if (compaction_filter) {
          // assert(!ExistFutureAction(compaction_filter));
          EraseFutureAction(compaction_filter);
        }
        input.set(nullptr);
        auto merge_ptr = reinterpret_cast<MergeHelper*>(&merge);
        merge_ptr->~MergeHelper();
        compaction_filter_holder.reset();
        auto range_del_agg_ptr =
            reinterpret_cast<CompactionRangeDelAggregator*>(&range_del_agg);
        range_del_agg_ptr->~CompactionRangeDelAggregator();
      }
    }
  } second_pass_iter_storage;

  auto make_compaction_iterator = [&]() {
    Status s;
    auto range_del_agg_ptr = new (&second_pass_iter_storage.range_del_agg)
        CompactionRangeDelAggregator(icmp, context.existing_snapshots);
    second_pass_iter_storage.compaction_filter =
        immutable_cf_options.compaction_filter;
    if (second_pass_iter_storage.compaction_filter == nullptr &&
        immutable_cf_options.compaction_filter_factory != nullptr) {
      second_pass_iter_storage.compaction_filter_holder =
          immutable_cf_options.compaction_filter_factory
              ->CreateCompactionFilter(context.compaction_filter_context);
      second_pass_iter_storage.compaction_filter =
          second_pass_iter_storage.compaction_filter_holder.get();
    }
    auto merge_ptr = new (&second_pass_iter_storage.merge) MergeHelper(
        env, ucmp, immutable_cf_options.merge_operator, compaction_filter,
        immutable_db_options.info_log.get(),
        false /* internal key corruption is expected */,
        context.existing_snapshots.empty() ? 0
                                           : context.existing_snapshots.back());
    if (s.ok()) {
      second_pass_iter_storage.input.set(create_input_iterator());
    } else {
      second_pass_iter_storage.input.set(NewErrorInternalIterator(s, &arena));
    }
    std::unique_ptr<CompactionIterator::CompactionProxy> compaction(
        new RemoteCompactionProxy(&context));
    return new CompactionIterator(
        second_pass_iter_storage.input.get(), &separate_helper, end, ucmp,
        merge_ptr, context.last_sequence, &context.existing_snapshots,
        context.earliest_write_conflict_snapshot, nullptr, env, false, false,
        range_del_agg_ptr, std::move(compaction), context.blob_config,
        second_pass_iter_storage.compaction_filter, nullptr,
        context.preserve_deletes_seqnum);
  };
  std::unique_ptr<InternalIterator> second_pass_iter(
      NewCompactionIterator(c_style_callback(make_compaction_iterator),
                            &make_compaction_iterator, start));

  auto create_builder = [&](std::unique_ptr<WritableFileWriter>* writer_ptr,
                            std::unique_ptr<TableBuilder>* builder_ptr) {
    std::string file_name = GenerateOutputFileName(result.files.size());
    Status s;
    TableBuilderOptions table_builder_options(
        immutable_cf_options, mutable_cf_options, *icmp,
        &int_tbl_prop_collector_factories.data, context.compression,
        context.compression_opts, nullptr /* compression_dict */,
        context.skip_filters, context.cf_name, -1 /* level */,
        0 /* compaction_load */);
    table_builder_options.smallest_user_key = context.smallest_user_key;
    table_builder_options.largest_user_key = context.largest_user_key;
    std::unique_ptr<WritableFile> sst_file;
    s = env->NewWritableFile(file_name, &sst_file, env_opt);
    if (!s.ok()) {
      return s;
    }
    writer_ptr->reset(new WritableFileWriter(std::move(sst_file), file_name,
                                             env_opt, nullptr,
                                             immutable_db_options.listeners));
    builder_ptr->reset(immutable_cf_options.table_factory->NewTableBuilder(
        table_builder_options, 0, writer_ptr->get()));

    if ((compaction_filter == nullptr ||
         compaction_filter->IsStableChangeValue()) &&
        (immutable_cf_options.merge_operator == nullptr ||
         immutable_cf_options.merge_operator->IsStableMerge())) {
      (*builder_ptr)->SetSecondPassIterator(second_pass_iter.get());
    }
    return s;
  };
  std::unique_ptr<WritableFileWriter> writer;
  std::unique_ptr<TableBuilder> builder;
  FileMetaData meta;
  std::unordered_map<uint64_t, uint64_t> dependence;
  auto finish_output_file = [&](Status s, const Slice* next_key) -> Status {
    if (s.ok() && !range_del_agg.IsEmpty()) {
      Slice lower_bound_guard, upper_bound_guard;
      std::string smallest_user_key;
      const Slice *lower_bound, *upper_bound;
      if (result.files.size() == 1) {
        lower_bound = start;
      } else if (meta.smallest.size() > 0) {
        smallest_user_key = meta.smallest.user_key().ToString(false /*hex*/);
        lower_bound_guard = Slice(smallest_user_key);
        lower_bound = &lower_bound_guard;
      } else {
        lower_bound = nullptr;
      }
      if (next_key != nullptr) {
        upper_bound_guard = ExtractUserKey(*next_key);
        assert(end == nullptr || ucmp->Compare(upper_bound_guard, *end) < 0);
        assert(meta.largest.size() == 0 ||
               ucmp->Compare(meta.largest.user_key(), upper_bound_guard) != 0);
        upper_bound = &upper_bound_guard;
      } else {
        upper_bound = end;
      }
      auto earliest_snapshot = kMaxSequenceNumber;
      if (context.existing_snapshots.size() > 0) {
        earliest_snapshot = context.existing_snapshots[0];
      }
      assert(end == nullptr || upper_bound == nullptr ||
             ucmp->Compare(*upper_bound, *end) <= 0);
      InternalKey smallest_candidate;
      InternalKey largest_candidate;
      auto it = range_del_agg.NewIterator();
      for (it->SeekToFirst(); it->Valid(); it->Next()) {
        auto tombstone = it->Tombstone();
        if (lower_bound != nullptr &&
            ucmp->Compare(tombstone.end_key_, *lower_bound) <= 0) {
          continue;
        }
        if (upper_bound != nullptr &&
            ucmp->Compare(tombstone.start_key_, *upper_bound) >= 0) {
          break;
        }
        if (context.bottommost_level && tombstone.seq_ <= earliest_snapshot) {
          continue;
        }
        if (lower_bound != nullptr &&
            ucmp->Compare(tombstone.start_key_, *lower_bound) < 0) {
          tombstone.start_key_ = *lower_bound;
          smallest_candidate.Set(*lower_bound, tombstone.seq_,
                                 kTypeRangeDeletion);
        } else {
          smallest_candidate.Set(tombstone.start_key_, tombstone.seq_,
                                 kTypeRangeDeletion);
        }
        if (upper_bound != nullptr &&
            ucmp->Compare(tombstone.end_key_, *upper_bound) > 0) {
          tombstone.end_key_ = *upper_bound;
          largest_candidate.Set(*upper_bound, kMaxSequenceNumber,
                                kTypeRangeDeletion);
        } else {
          largest_candidate.Set(tombstone.end_key_, kMaxSequenceNumber,
                                kTypeRangeDeletion);
        }
        assert(lower_bound == nullptr ||
               ucmp->Compare(*lower_bound, tombstone.start_key_) <= 0);
        assert(lower_bound == nullptr ||
               ucmp->Compare(*lower_bound, tombstone.end_key_) < 0);
        assert(upper_bound == nullptr ||
               ucmp->Compare(*upper_bound, tombstone.start_key_) > 0);
        assert(upper_bound == nullptr ||
               ucmp->Compare(*upper_bound, tombstone.end_key_) >= 0);
        if (ucmp->Compare(tombstone.start_key_, tombstone.end_key_) >= 0) {
          continue;
        }

        auto kv = tombstone.Serialize();
        s = builder->AddTombstone(kv.first.Encode(), LazyBuffer(kv.second));
        if (!s.ok()) {
          break;
        }
        meta.UpdateBoundariesForRange(smallest_candidate, largest_candidate,
                                      tombstone.seq_, *icmp);
      }
    }
    if (s.ok()) {
      meta.marked_for_compaction = builder->NeedCompact();
      meta.prop.num_entries = builder->NumEntries();
      for (auto& pair : dependence) {
        meta.prop.dependence.emplace_back(Dependence{pair.first, pair.second});
      }
      terark::sort_a(meta.prop.dependence, TERARK_CMP(file_number, <));
      auto shrinked_snapshots = meta.ShrinkSnapshot(context.existing_snapshots);
      s = builder->Finish(&meta.prop, &shrinked_snapshots);
    } else {
      builder->Abandon();
    }
    const uint64_t current_bytes = builder->FileSize();
    if (s.ok()) {
      meta.fd.file_size = current_bytes;
    }
    if (s.ok()) {
      s = writer->Sync(true);
    }
    if (s.ok()) {
      s = writer->Close();
    }

    TableProperties tp;
    if (s.ok()) {
      tp = builder->GetTableProperties();
      meta.prop.num_deletions = tp.num_deletions;
      meta.prop.raw_key_size = tp.raw_key_size;
      meta.prop.raw_value_size = tp.raw_value_size;
      meta.prop.flags |= tp.num_range_deletions > 0
                             ? 0
                             : TablePropertyCache::kNoRangeDeletions;
      meta.prop.flags |=
          tp.snapshots.empty() ? 0 : TablePropertyCache::kHasSnapshots;
    }
    if (s.ok()) {
      if (next_key != nullptr) {
        actual_end.SetMinPossibleForUserKey(ExtractUserKey(*next_key));
      } else if (end != nullptr) {
        actual_end.SetMinPossibleForUserKey(*end);
      } else {
        actual_end.rep()->clear();
      }
      CompactionWorkerResult::FileInfo file_info;
      if (meta.prop.num_entries > 0 || tp.num_range_deletions > 0) {
        file_info.file_name = writer->file_name();
      }
      file_info.smallest = meta.smallest;
      file_info.largest = meta.largest;
      file_info.smallest_seqno = meta.fd.smallest_seqno;
      file_info.largest_seqno = meta.fd.largest_seqno;
      file_info.file_size = meta.fd.file_size;
      file_info.marked_for_compaction = builder->NeedCompact();
      result.files.emplace_back(file_info);
    }
    meta = FileMetaData();
    builder.reset();
    writer.reset();
    return s;
  };

  Status& status = result.status;
  const Slice* next_key = nullptr;
  while (status.ok() && c_iter->Valid()) {
    // Invariant: c_iter.status() is guaranteed to be OK if c_iter->Valid()
    // returns true.
    const Slice& key = c_iter->key();
    const LazyBuffer& value = c_iter->value();
    if (c_iter->ikey().type == kTypeValueIndex ||
        c_iter->ikey().type == kTypeMergeIndex) {
      assert(value.file_number() != uint64_t(-1));
      auto ib = dependence.emplace(value.file_number(), 1);
      if (!ib.second) {
        ++ib.first->second;
      }
    }

    assert(end == nullptr || ucmp->Compare(c_iter->user_key(), *end) < 0);

    // Open output file if necessary
    if (!builder) {
      status = create_builder(&writer, &builder);
      if (!status.ok()) {
        break;
      }
    }
    assert(builder);
    status = builder->Add(key, value);
    if (!status.ok()) {
      break;
    }
    size_t current_output_file_size = builder->FileSize();
    meta.UpdateBoundaries(key, c_iter->ikey().sequence);

    bool output_file_ended = false;
    Status input_status;
    if (context.target_file_size > 0 &&
        current_output_file_size >= context.target_file_size) {
      input_status = input->status();
      output_file_ended = true;
    }
    c_iter->Next();
    if (output_file_ended) {
      if (c_iter->Valid()) {
        next_key = &c_iter->key();
      }
      if (next_key != nullptr && ucmp->Compare(ExtractUserKey(*next_key),
                                               meta.largest.user_key()) == 0) {
        output_file_ended = false;
      }
    }
    if (output_file_ended) {
      status = finish_output_file(input_status, next_key);
      dependence.clear();
      break;
    }
  }

  if (status.ok()) {
    status = input->status();
  }
  if (status.ok()) {
    status = c_iter->status();
  }

  if (status.ok() && !builder && result.files.empty() &&
      !range_del_agg.IsEmpty()) {
    status = create_builder(&writer, &builder);
  }
  if (builder) {
    status = finish_output_file(status, nullptr);
    dependence.clear();
  }

  if (compaction_filter) {
    if (ReapMatureAction(compaction_filter, &result.stat_all)) {
      fprintf(stderr, "INFO: ReapMatureAction(compaction_filter=%p) = %s\n",
              compaction_filter, result.stat_all.c_str());
    } else {
      fprintf(stderr, "ERROR: ReapMatureAction(compaction_filter=%p) = false\n",
              compaction_filter);
    }
  } else {
    fprintf(
        stderr,
        "INFO: compaction_filter = null, name = { filter: %s, factory: %s }\n",
        context.compaction_filter.c_str(),
        context.compaction_filter_factory.c_str());
  }
  if (second_pass_iter_storage.compaction_filter) {
    bool ret = EraseFutureAction(second_pass_iter_storage.compaction_filter);
    fprintf(stderr,
            "INFO: EraseFutureAction(secondpass.compaction_filter=%p) = %d\n",
            second_pass_iter_storage.compaction_filter, ret);
  } else {
    fprintf(stderr,
            "INFO: secondpass.compaction_filter = null, name = { filter: %s, "
            "factory: %s }\n",
            context.compaction_filter.c_str(),
            context.compaction_filter_factory.c_str());
  }

  c_iter.reset();
  input.set(nullptr);

  auto finish_time = system_clock::now();
  auto duration = duration_cast<microseconds>(finish_time - start_time);
  result.time_us = duration.count();
  ajson::string_stream stream;
  ajson::save_to(stream, result);
  return stream.str();
}

void RemoteCompactionDispatcher::Worker::DebugSerializeCheckResult(Slice data) {
#ifdef WITH_TERARK_ZIP
  using namespace terark;
  LittleEndianDataInput<MemIO> dio;
  dio.set((void*)(data.data_), data.size());
  CompactionWorkerResult res;
  dio >> res;
  string_appender<> str;
  str << "CompactionWorkerResult: time_us = " << res.time_us << " ("
      << (res.time_us * 1e-6) << " sec), ";
  str << "  status = " << res.status.ToString() << "\n";
  str << "  actual_start = " << res.actual_start.DebugString(true) << "\n";
  str << "  actual_end   = " << res.actual_end.DebugString(true) << "\n";
  str << "  files[size=" << res.files.size() << "]\n";
  for (size_t i = 0; i < res.files.size(); ++i) {
    const auto& f = res.files[i];
    str << "    " << i << " = " << f.file_name
        << " : marked_for_compaction = " << f.marked_for_compaction
        << "  filesize = " << f.file_size << "\n";
    str << "    seq_smallest = " << f.smallest_seqno
        << "  key_smallest = " << f.smallest.DebugString(true) << "\n";
    str << "    seq__largest = " << f.largest_seqno
        << "  key__largest = " << f.largest.DebugString(true) << "\n";
  }
  str << "  stat_all[size=" << res.stat_all.size() << "] = " << res.stat_all
      << "\n";
  intptr_t wlen = ::write(2, str.data(), str.size());
  if (size_t(wlen) != str.size()) {
    abort();
  }
#endif
}

const char* RemoteCompactionDispatcher::Name() const {
  return "RemoteCompactionDispatcher";
}

class CommandLineCompactionDispatcher : public RemoteCompactionDispatcher {
  std::string m_cmd;

 public:
  CommandLineCompactionDispatcher(std::string&& cmd) : m_cmd(std::move(cmd)) {}

  std::future<std::string> DoCompaction(std::string data) override {
    // if use future vfork_cmd, we have no a chance to print log
    // return terark::vfork_cmd(m_cmd, data, "/tmp/compact-");
    //
    // use onFinish callback, so we can print log
    auto promise = std::make_shared<std::promise<std::string>>();
    std::future<std::string> future = promise->get_future();
    size_t datalen = data.size();
    auto onFinish = [this, promise, datalen](std::string&& result,
                                             const std::exception* ex) {
      fprintf(stderr,
              "INFO: CompactCmd(%s, datalen=%zd) = exception[%p] = %s, "
              //"result[len=%zd]: %s\n"
              "result[len=%zd]\n",
              this->m_cmd.c_str(), datalen, ex, ex ? ex->what() : "",
              result.size()
              //, Slice(result).ToString(true).c_str()
      );
      promise->set_value(std::move(result));
      if (ex) {
        try {
          throw *ex;
        } catch (...) {
          promise->set_exception(std::current_exception());
        }
      }
    };
    terark::vfork_cmd(m_cmd, data, std::move(onFinish), "/tmp/compact-");
    return future;
  }
};

std::shared_ptr<CompactionDispatcher> NewCommandLineCompactionDispatcher(
    std::string cmd) {
  return std::make_shared<CommandLineCompactionDispatcher>(std::move(cmd));
}

}  // namespace rocksdb
