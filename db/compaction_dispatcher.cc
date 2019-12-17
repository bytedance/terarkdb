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
#include "rocksdb/convenience.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "db/compaction_iterator.h"
#include "db/map_builder.h"
#include "db/merge_helper.h"
#include "db/range_del_aggregator.h"
#include "table/merging_iterator.h"
#include "table/table_reader.h"
#include "table/two_level_iterator.h"
#include "util/ajson_msd.hpp"
#include "util/c_style_callback.h"
#include "util/filename.h"

#include <terark/util/process.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/num_to_str.hpp>

struct AJsonStatus {
  unsigned char code, subcode, sev;
  std::string state;
};
AJSON(AJsonStatus, code, subcode, sev, state);

namespace ajson {
template<>
struct json_impl<rocksdb::Status, void> {
  static inline void read(reader& rd, rocksdb::Status& v) {
    AJsonStatus s;
    json_impl<AJsonStatus>::read(rd, s);
    v = rocksdb::Status(s.code, s.subcode, s.sev,
                        s.state.empty() ? nullptr : s.state.c_str());
  }
  template<typename write_ty>
  static inline void write(write_ty& wt, rocksdb::Status const& v) {
    AJsonStatus s = {
        v.code(), v.subcode(), v.severity(),
        v.getState() == nullptr ? std::string() : v.getState()
    };
    json_impl<AJsonStatus>::template write<write_ty>(wt, s);
  }
};

template<>
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
  template<typename write_ty>
  static inline void write(
      write_ty& wt, rocksdb::CompactionWorkerContext::EncodedString const& v) {
    json_impl<std::string>::template write<write_ty>(
        wt, rocksdb::Slice(v.data).ToString(true));
  }
};

template<>
struct json_impl<rocksdb::InternalKey, void> {
  static inline void read(reader& rd, rocksdb::InternalKey& v) {
    rocksdb::CompactionWorkerContext::EncodedString s;
    json_impl<rocksdb::CompactionWorkerContext::EncodedString>::read(rd, s);
    *v.rep() = std::move(s.data);
  }
  template<typename write_ty>
  static inline void write(write_ty& wt, rocksdb::InternalKey const& v) {
    rocksdb::Slice s(*v.rep());
    json_impl<std::string>::template write<write_ty>(wt, s.ToString(true));
  }
};
}

AJSON(rocksdb::Dependence, file_number, entry_count);

AJSON(rocksdb::CompactionWorkerResult::FileInfo, smallest, largest, file_name,
                                                 smallest_seqno, largest_seqno,
                                                 file_size,
                                                 marked_for_compaction);

AJSON(rocksdb::CompactionWorkerResult, status, actual_start, actual_end, files);

AJSON(rocksdb::FileDescriptor, packed_number_and_path_id, file_size,
                               smallest_seqno, largest_seqno);

AJSON(rocksdb::TablePropertyCache, purpose, max_read_amp, read_amp, dependence,
                                   inheritance_chain);

AJSON(rocksdb::FileMetaData, fd, smallest, largest, prop);

AJSON(rocksdb::CompressionOptions, window_bits, level, strategy, max_dict_bytes,
                                   zstd_max_train_bytes, enabled);

AJSON(rocksdb::CompactionFilter::Context, is_full_compaction,
                                          is_manual_compaction,
                                          column_family_id);

AJSON(rocksdb::CompactionWorkerContext::NameParam, name, param);
AJSON(rocksdb::CompactionWorkerContext, user_comparator, merge_operator,
                                        merge_operator_data, compaction_filter,
                                        compaction_filter_factory,
                                        compaction_filter_context,
                                        compaction_filter_data, blob_size,
                                        table_factory, table_factory_options,
                                        bloom_locality, cf_paths,
                                        prefix_extractor,
                                        prefix_extractor_options,
                                        has_start, has_end,
                                        start, end, last_sequence,
                                        earliest_write_conflict_snapshot,
                                        preserve_deletes_seqnum, file_metadata,
                                        inputs, cf_name, target_file_size,
                                        compression, compression_opts,
                                        existing_snapshots, bottommost_level,
                                        int_tbl_prop_collector_factories);

namespace rocksdb {

template<class T>
using STMap = std::unordered_map<std::string, std::shared_ptr<T>>;

class WorkerSeparateHelper : public SeparateHelper, public LazyBufferState {
 public:
  void destroy(LazyBuffer* /*buffer*/) const override {}

  void pin_buffer(LazyBuffer* /*buffer*/) const override {}

  Status fetch_buffer(LazyBuffer* buffer) const override {
    return inplace_decode_callback_(inplace_decode_arg_, buffer,
                                    get_context(buffer));
  }

  void TransToCombined(const Slice& user_key, uint64_t sequence,
                       LazyBuffer& value) const override {
    auto s = value.fetch();
    if (!s.ok()) {
      value.reset(std::move(s));
      return;
    }
    uint64_t file_number = SeparateHelper::DecodeFileNumber(value.slice());
    auto find = dependence_map_->find(file_number);
    if (find == dependence_map_->end()) {
      value.reset(Status::Corruption("Separate value dependence missing"));
    } else {
      value.reset(this, {reinterpret_cast<uint64_t>(user_key.data()),
                         user_key.size(), sequence,
                         reinterpret_cast<uint64_t>(&*find)},
                  Slice::Invalid(), find->second->fd.GetNumber());
    }
  }

  WorkerSeparateHelper(
      DependenceMap* dependence_map, void* inplace_decode_arg,
      Status (*inplace_decode_callback)(void* arg, LazyBuffer* buffer,
                                        LazyBufferContext* rep))
    : dependence_map_(dependence_map),
      inplace_decode_arg_(inplace_decode_arg),
      inplace_decode_callback_(inplace_decode_callback) {}

  DependenceMap* dependence_map_;
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
      ajson::load_from_buff(result, &encoded_result[0],
                            encoded_result.size());
      return result;
    }
  };
  return Result(DoCompaction(stream.str()));
}

static bool g_isCompactionWorkerNode = false;
bool IsCompactionWorkerNode() {
  return g_isCompactionWorkerNode;
}

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

RemoteCompactionDispatcher::Worker::~Worker() {
  delete rep_;
}

std::string RemoteCompactionDispatcher::Worker::DoCompaction(
    const std::string& data) {
  CompactionWorkerContext context;
  ajson::load_from_buff(context, &std::string(data)[0], data.size());

  auto make_error = [](Status status) {
    CompactionWorkerResult result;
    result.status = std::move(status);
    ajson::string_stream stream;
    ajson::save_to(stream, result);
    return stream.str();
  };

  ImmutableDBOptions immutable_db_options = ImmutableDBOptions(DBOptions());
  ColumnFamilyOptions cf_options;
  if (context.user_comparator.empty()) {
    return make_error(Status::Corruption("Bad comparator name !"));
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
  std::unique_ptr<CompactionFilter> filter_ptr;
  if (!context.compaction_filter.empty()) {
    if (!context.compaction_filter_factory.empty()) {
      return make_error(Status::Corruption(
        "CompactonFilter and CompactionFilterFactory are both specified"));
    }
    filter_ptr.reset(
      CompactionFilter::create(context.compaction_filter,
                               context.compaction_filter_data.data,
                               context.compaction_filter_context));
    if (!filter_ptr) {
      return make_error(Status::Corruption("Missing CompactionFilterFactory!"));
    }
    cf_options.compaction_filter = filter_ptr.get();
  }
  else if (!context.compaction_filter_factory.empty()) {
    cf_options.compaction_filter_factory.reset(
      CompactionFilterFactory::create(context.compaction_filter_factory,
                                      context.compaction_filter_data.data));
    if (!cf_options.compaction_filter_factory) {
      return make_error(Status::Corruption("Missing CompactionFilterFactory!"));
    }
  }
  cf_options.blob_size = context.blob_size;
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
       context.prefix_extractor,
       context.prefix_extractor_options));
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
      new UserKeyTablePropertiesCollectorFactory(user_fac->shared_from_this()));
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

  // start run
  DependenceMap contxt_dependence_map;
  for (auto& pair : context.file_metadata) {
    contxt_dependence_map.emplace(pair.first, &pair.second);
  }
  std::unordered_map<int, std::vector<const FileMetaData*>> inputs;
  for (auto pair : context.inputs) {
    assert(contxt_dependence_map.find(pair.second) !=
           contxt_dependence_map.end());
    inputs[pair.first].push_back(contxt_dependence_map[pair.second]);
  }
  std::unordered_map<uint64_t, std::unique_ptr<rocksdb::TableReader>>
      table_cache;
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
      std::unique_ptr<rocksdb::RandomAccessFile> file;
      auto s = rep_->env->NewRandomAccessFile(file_name, &file,
                                              rep_->env_options);
      if (!s.ok()) {
        return s;
      }
      std::unique_ptr<rocksdb::RandomAccessFileReader> file_reader(
          new rocksdb::RandomAccessFileReader(std::move(file), file_name,
                                              rep_->env));
      std::unique_ptr<rocksdb::TableReader> reader;
      TableReaderOptions table_reader_options(
          immutable_cf_options, mutable_cf_options.prefix_extractor.get(),
          rep_->env_options, *icmp, true, false, -1, file_number);
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
  struct {
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
    auto iterator =
        reader->NewIterator(ReadOptions(),
                            mutable_cf_options.prefix_extractor.get(), arena);
    if (file_metadata->prop.purpose == kMapSst && !depend_map.empty()) {
      auto sst_iterator = NewMapSstIterator(file_metadata, iterator, depend_map,
                                            *icmp, c_style_new_iterator.arg,
                                            c_style_new_iterator.callback,
                                            arena);
      sst_iterator->RegisterCleanup([](void* arg1, void* arg2) {
        auto iter_ptr = static_cast<InternalIterator*>(arg1);
        if (arg2 == nullptr) { // arena
          delete iter_ptr;
        } else {
          static_cast<InternalIterator*>(iter_ptr)->~InternalIterator();
        }
      }, iterator, arena);
      iterator = sst_iterator;
    }
    return iterator;
  };
  c_style_new_iterator.arg = &new_iterator;
  c_style_new_iterator.callback = c_style_callback(new_iterator);

  auto separate_inplace_decode = [&](LazyBuffer* buffer,
                                     LazyBufferContext* context) {
    Slice user_key(reinterpret_cast<const char*>(context->data[0]),
                   context->data[1]);
    uint64_t sequence = context->data[2];
    auto pair =
        *reinterpret_cast<DependenceMap::value_type*>(context->data[3]);
    bool value_found = false;
    SequenceNumber context_seq;
    GetContext get_context(ucmp, nullptr, immutable_cf_options.info_log,
                           nullptr, GetContext::kNotFound, user_key, buffer,
                           &value_found, nullptr, nullptr, nullptr, rep_->env,
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

  WorkerSeparateHelper separate_helper(
      &contxt_dependence_map, &separate_inplace_decode,
      c_style_callback(separate_inplace_decode));

  CompactionRangeDelAggregator range_del_agg(icmp, context.existing_snapshots);

  Arena arena;
  auto create_input_iterator = [&] {
    MergeIteratorBuilder merge_iter_builder(icmp, &arena);
    for (auto& pair : inputs) {
      if (pair.first == 0 || pair.second.size() == 1) {
        merge_iter_builder.AddIterator(new_iterator(pair.second.front(),
                                                    contxt_dependence_map,
                                                    &arena, nullptr));
      } else {
        auto map_iter = NewMapElementIterator(pair.second.data(),
                                              pair.second.size(), icmp,
                                              c_style_new_iterator.arg,
                                              c_style_new_iterator.callback,
                                              &arena);
        auto level_iter = NewMapSstIterator(nullptr, map_iter,
                                            contxt_dependence_map,
                                            *icmp, c_style_new_iterator.arg,
                                            c_style_new_iterator.callback,
                                            &arena);
        level_iter->RegisterCleanup([](void* arg1, void* /*arg2*/) {
          static_cast<InternalIterator*>(arg1)->~InternalIterator();
        }, map_iter, nullptr);
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
    auto factory = immutable_cf_options.compaction_filter_factory;
    compaction_filter_from_factory =
        factory->CreateCompactionFilter(context.compaction_filter_context);
    compaction_filter = compaction_filter_from_factory.get();
  }

  MergeHelper merge(
      rep_->env, ucmp, immutable_cf_options.merge_operator,
      compaction_filter, immutable_db_options.info_log.get(),
      false /* internal key corruption is expected */,
      context.existing_snapshots.empty()
          ? 0 : context.existing_snapshots.back());

  CompactionWorkerResult result;
  InternalKey& actual_start = result.actual_start;
  InternalKey& actual_end = result.actual_end;

  std::unique_ptr<CompactionIterator> c_iter(new CompactionIterator(
      input.get(), &separate_helper, end, ucmp, &merge, context.last_sequence,
      &context.existing_snapshots, context.earliest_write_conflict_snapshot,
      nullptr, rep_->env, false, false, &range_del_agg, nullptr,
      mutable_cf_options.blob_size, compaction_filter, nullptr,
      context.preserve_deletes_seqnum));

  if (start != nullptr) {
    actual_start.SetMinPossibleForUserKey(*start);
    input->Seek(actual_start.Encode());
  } else {
    input->SeekToFirst();
    actual_start.SetMinPossibleForUserKey(ExtractUserKey(input->key()));
  }
  c_iter->SeekToFirst();

  struct SecondPassIterStorage {
    std::aligned_storage<sizeof(CompactionRangeDelAggregator),
        alignof(CompactionRangeDelAggregator)>::type
        range_del_agg;
    std::unique_ptr<CompactionFilter> compaction_filter_holder;
    const CompactionFilter* compaction_filter;
    std::aligned_storage<sizeof(MergeHelper), alignof(MergeHelper)>::type
        merge;
    ScopedArenaIterator input;

    ~SecondPassIterStorage() {
      if (input.get() != nullptr) {
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

  auto make_compaction_iterator = [&] {
    Status s;
    auto range_del_agg_ptr =
        new(&second_pass_iter_storage.range_del_agg)
            CompactionRangeDelAggregator(icmp, context.existing_snapshots);
    second_pass_iter_storage.compaction_filter =
        immutable_cf_options.compaction_filter;
    if (second_pass_iter_storage.compaction_filter == nullptr &&
        immutable_cf_options.compaction_filter_factory != nullptr) {
      second_pass_iter_storage.compaction_filter_holder =
          immutable_cf_options.compaction_filter_factory->
              CreateCompactionFilter(context.compaction_filter_context);
    }
    auto merge_ptr =
        new(&second_pass_iter_storage.merge) MergeHelper(
            rep_->env, ucmp, immutable_cf_options.merge_operator,
            compaction_filter, immutable_db_options.info_log.get(),
            false /* internal key corruption is expected */,
            context.existing_snapshots.empty()
            ? 0 : context.existing_snapshots.back());
    if (s.ok()) {
      second_pass_iter_storage.input.set(create_input_iterator());
    } else {
      second_pass_iter_storage.input.set(NewErrorInternalIterator(s, &arena));
    }
    return new CompactionIterator(
        second_pass_iter_storage.input.get(), &separate_helper, end, ucmp,
        merge_ptr, context.last_sequence, &context.existing_snapshots,
        context.earliest_write_conflict_snapshot, nullptr, rep_->env, false,
        false, range_del_agg_ptr, nullptr, mutable_cf_options.blob_size,
        second_pass_iter_storage.compaction_filter, nullptr,
        context.preserve_deletes_seqnum);
  };
  std::unique_ptr<InternalIterator> second_pass_iter(
      NewCompactionIterator(c_style_callback(make_compaction_iterator),
                            &make_compaction_iterator, start));

  auto create_builder =
      [&](std::unique_ptr<WritableFileWriter>* writer_ptr,
          std::unique_ptr<TableBuilder>* builder_ptr) {
        std::string file_name = GenerateOutputFileName(result.files.size());
        Status s;
        TableBuilderOptions table_builder_options(
            immutable_cf_options, mutable_cf_options, *icmp,
            &int_tbl_prop_collector_factories.data, context.compression,
            context.compression_opts, nullptr, true, false, context.cf_name,
            -1, 0);
        std::unique_ptr<WritableFile> sst_file;
        s = rep_->env->NewWritableFile(file_name, &sst_file, rep_->env_options);
        if (!s.ok()) {
          return s;
        }
        writer_ptr->reset(
            new WritableFileWriter(std::move(sst_file), file_name,
                                   rep_->env_options, nullptr,
                                   immutable_db_options.listeners));
        builder_ptr->reset(immutable_cf_options.table_factory->NewTableBuilder(
            table_builder_options, 0, writer_ptr->get()));
        (*builder_ptr)->SetSecondPassIterator(second_pass_iter.get());
        return s;
      };
  auto finish_output_file =
      [&](Status s, FileMetaData* meta,
          std::unique_ptr<WritableFileWriter>* writer_ptr,
          std::unique_ptr<TableBuilder>* builder_ptr,
          const std::unordered_map<uint64_t, uint64_t>& dependence,
          const Slice* next_key) {
    auto writer = writer_ptr->get();
    auto builder = builder_ptr->get();
    if (s.ok()) {
      Slice lower_bound_guard, upper_bound_guard;
      std::string smallest_user_key;
      const Slice *lower_bound, *upper_bound;
      bool lower_bound_from_sub_compact = false;
      if (result.files.size() == 1) {
        lower_bound = start;
        lower_bound_from_sub_compact = true;
      } else if (meta->smallest.size() > 0) {
        smallest_user_key = meta->smallest.user_key().ToString(false /*hex*/);
        lower_bound_guard = Slice(smallest_user_key);
        lower_bound = &lower_bound_guard;
      } else {
        lower_bound = nullptr;
      }
      if (next_key != nullptr) {
        upper_bound_guard = ExtractUserKey(*next_key);
        if (end != nullptr &&
            ucmp->Compare(upper_bound_guard, *end) >= 0) {
          upper_bound = end;
        } else {
          upper_bound = &upper_bound_guard;
        }
      } else {
        upper_bound = end;
      }
      auto earliest_snapshot = kMaxSequenceNumber;
      if (context.existing_snapshots.size() > 0) {
        earliest_snapshot = context.existing_snapshots[0];
      }
      bool has_overlapping_endpoints;
      if (upper_bound != nullptr && meta->largest.size() > 0) {
        has_overlapping_endpoints =
            ucmp->Compare(meta->largest.user_key(), *upper_bound) == 0;
      } else {
        has_overlapping_endpoints = false;
      }
      assert(end == nullptr || upper_bound == nullptr ||
             ucmp->Compare(*upper_bound, *end) <= 0);
      auto it = range_del_agg.NewIterator(lower_bound, upper_bound,
                                          has_overlapping_endpoints);
      if (lower_bound != nullptr) {
        it->Seek(*lower_bound);
      } else {
        it->SeekToFirst();
      }
      for (; it->Valid(); it->Next()) {
        auto tombstone = it->Tombstone();
        if (upper_bound != nullptr) {
          int cmp = ucmp->Compare(*upper_bound, tombstone.start_key_);
          if ((has_overlapping_endpoints && cmp < 0) ||
              (!has_overlapping_endpoints && cmp <= 0)) {
            break;
          }
        }

        if (context.bottommost_level && tombstone.seq_ <= earliest_snapshot) {
          continue;
        }

        auto kv = tombstone.Serialize();
        assert(lower_bound == nullptr ||
               ucmp->Compare(*lower_bound, kv.second) < 0);
        s = builder->Add(kv.first.Encode(), LazyBuffer(kv.second));
        if (!s.ok()) {
          break;
        }
        InternalKey smallest_candidate = std::move(kv.first);
        if (lower_bound != nullptr &&
            ucmp->Compare(smallest_candidate.user_key(), *lower_bound) <= 0) {
          smallest_candidate = InternalKey(
              *lower_bound, lower_bound_from_sub_compact ? tombstone.seq_ : 0,
              kTypeRangeDeletion);
        }
        InternalKey largest_candidate = tombstone.SerializeEndKey();
        if (upper_bound != nullptr &&
            ucmp->Compare(*upper_bound, largest_candidate.user_key()) <= 0) {
          largest_candidate =
              InternalKey(*upper_bound, kMaxSequenceNumber, kTypeRangeDeletion);
        }
#ifndef NDEBUG
        SequenceNumber smallest_ikey_seqnum = kMaxSequenceNumber;
        if (meta->smallest.size() > 0) {
          smallest_ikey_seqnum = GetInternalKeySeqno(meta->smallest.Encode());
        }
#endif
        meta->UpdateBoundariesForRange(smallest_candidate, largest_candidate,
                                       tombstone.seq_, *icmp);
        assert(smallest_ikey_seqnum == 0 ||
               ExtractInternalKeyFooter(meta->smallest.Encode()) !=
               PackSequenceAndType(0, kTypeRangeDeletion));
      }
      meta->marked_for_compaction = builder->NeedCompact();
    }
    if (s.ok()) {
      meta->prop.num_entries = builder->NumEntries();
      for (auto& pair : dependence) {
        meta->prop.dependence.emplace_back(Dependence{pair.first, pair.second});
      }
      std::sort(meta->prop.dependence.begin(), meta->prop.dependence.end(),
                [](const Dependence& l, const Dependence& r) {
                  return l.file_number < r.file_number;
                });
      s = builder->Finish(&meta->prop);
    } else {
      builder->Abandon();
    }
    const uint64_t current_bytes = builder->FileSize();
    if (s.ok()) {
      meta->fd.file_size = current_bytes;
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
      if (meta->prop.num_entries > 0 || tp.num_range_deletions > 0) {
        file_info.file_name = writer->file_name();
      }
      file_info.smallest = meta->smallest;
      file_info.largest = meta->largest;
      file_info.smallest_seqno = meta->fd.smallest_seqno;
      file_info.largest_seqno = meta->fd.largest_seqno;
      file_info.file_size = meta->fd.file_size;
      file_info.marked_for_compaction = builder->NeedCompact();
      result.files.emplace_back(file_info);
    }
    *meta = FileMetaData();
    builder_ptr->reset();
    writer_ptr->reset();
    return s;
  };
  std::unique_ptr<WritableFileWriter> writer;
  std::unique_ptr<TableBuilder> builder;
  FileMetaData meta;
  std::unordered_map<uint64_t, uint64_t> dependence;

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
      status = finish_output_file(input_status, &meta, &writer, &builder,
                                  dependence, next_key);
      dependence.clear();
      if (next_key != nullptr) {
        actual_end.SetMinPossibleForUserKey(ExtractUserKey(*next_key));
      }
      break;
    }
  }

  if (status.ok()) {
    status = input->status();
  }
  if (status.ok()) {
    status = c_iter->status();
  }

  if (compaction_filter) {
    ReapMatureAction(compaction_filter, &result.stat_all);
  }
  if (second_pass_iter_storage.compaction_filter) {
    EraseFutureAction(second_pass_iter_storage.compaction_filter);
  }
  if (status.ok() && !builder && result.files.empty() &&
      !range_del_agg.IsEmpty()) {
    status = create_builder(&writer, &builder);
  }
  if (builder) {
    status = finish_output_file(status, &meta, &writer, &builder, dependence,
                                nullptr);
    dependence.clear();
  }

  c_iter.reset();
  input.set(nullptr);

  ajson::string_stream stream;
  ajson::save_to(stream, result);
  return stream.str();
}

const char* RemoteCompactionDispatcher::Name() const {
  return "RemoteCompactionDispatcher";
}

class CommandLineCompactionDispatcher : public RemoteCompactionDispatcher {
  std::string m_cmd;

public:
  CommandLineCompactionDispatcher(std::string&& cmd) : m_cmd(std::move(cmd)) {}

  std::future<std::string> DoCompaction(const std::string& data) override {
    std::promise<std::string> promise;
    std::future<std::string> future = promise.get_future();
    std::thread([this, data](std::promise<std::string>&& prom) {
      bool tmp_file_created = false;
      char tmp_file[] = "/tmp/Compaction-XXXXXX";
      try {
        int fd = mkstemp(tmp_file);
        if (fd < 0) {
          THROW_STD(runtime_error, "mkstemp(%s) = %s", tmp_file,
                    strerror(errno));
        }
        tmp_file_created = true;
        using namespace terark;
        {
          // use  " > /dev/fd/xxx" will prevent from tmp_file being
          // deleted unexpected
          string_appender<> cmdw;
          cmdw.reserve(m_cmd.size() + 32);
          cmdw << m_cmd << " > /dev/fd/" << fd;
          ProcPipeStream proc(cmdw, "w");
          proc.ensureWrite(data.c_str(), data.size());
        }
        //
        // now cmd sub process must have finished
        //
        Auto_fclose tmp_result_file(fdopen(fd, "r"));
        if (!tmp_result_file) {
          THROW_STD(runtime_error, "fdopen(fd=%d(fname=%s), r) = %s", fd,
                    tmp_file, strerror(errno));
        }
        terark::LineBuf result;
        result.read_all(tmp_result_file);
        prom.set_value(std::string(result.p, result.n));
      }
      catch (...) {
        prom.set_exception(std::current_exception());
      }
      if (tmp_file_created) {
        ::remove(tmp_file);
      }
    }, std::move(promise)).detach();
    return future;
  }
};

std::shared_ptr<CompactionDispatcher>
NewCommandLineCompactionDispatcher(std::string cmd) {
  return std::make_shared<CommandLineCompactionDispatcher>(std::move(cmd));
}

}  // namespace rocksdb
