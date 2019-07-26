//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/compaction_worker.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "rocksdb/table.h"
#include "db/compaction_iterator.h"
#include "db/map_builder.h"
#include "db/merge_helper.h"
#include "db/range_del_aggregator.h"
#include "table/merging_iterator.h"
#include "table/table_reader.h"
#include "table/two_level_iterator.h"
#include "util/c_style_callback.h"

namespace rocksdb {

template<class T>
using MapType = std::unordered_map<std::string, std::shared_ptr<T>>;

std::packaged_task<CompactionWorkerResult()>
RemoteCompactionWorker::StartCompaction(
    VersionStorageInfo* input_version,
    const ImmutableCFOptions& immutable_cf_options,
    const MutableCFOptions& mutable_cf_options,
    std::vector<std::pair<int, std::vector<const FileMetaData*>>> inputs,
    uint64_t target_file_size, CompressionType compression,
    CompressionOptions compression_opts, const Slice* start, const Slice* end) {
  std::string data;
  // TODO encode all params into data
  struct ResultDecoder {
    std::future<std::string> future;
    CompactionWorkerResult operator()() {
      CompactionWorkerResult decoded_result;
      std::string encoded_result = future.get();
      // TODO decode result;
      return decoded_result;
    }
  };
  ResultDecoder decoder;
  decoder.future = DoCompaction(data);
  return std::packaged_task<CompactionWorkerResult()>(std::move(decoder));
}

struct RemoteCompactionWorker::Client::Rep {
  std::unordered_map<std::string, const Comparator*> comparator_map;
  MapType<TableFactory> table_factory_map;
  MapType<MergeOperator> merge_operator_map;
  MapType<CompactionFilterFactory> compaction_filter_factory_map;
  EnvOptions env_options;
  Env* env;
};

void RemoteCompactionWorker::Client::RegistComparator(
    const Comparator* comparator) {
  comparator_map.emplace(comparator->Name(), comparator);
}

void RemoteCompactionWorker::Client::RegistTableFactory(
    std::shared_ptr<TableFactory> table_factory) {
  rep_->table_factory_map.emplace(table_factory->Name(), table_factory);
}

void RemoteCompactionWorker::Client::RegistMergeOperator(
    std::shared_ptr<MergeOperator> merge_operator) {
  rep_->merge_operator_map.emplace(merge_operator->Name(), merge_operator);
}

void RemoteCompactionWorker::Client::RegistCompactionFilter(
    std::shared_ptr<CompactionFilterFactory> compaction_filter_factory) {
  rep_->compaction_filter_factory_map.emplace(compaction_filter_factory->Name(),
                                              compaction_filter_factory);
}

RemoteCompactionWorker::Client::Client(EnvOptions env_options, Env* env) {
  rep_ = new Rep();
  rep_->env_options = env_options;
  rep_->env = env;
}

RemoteCompactionWorker::Client::~Client() {
  delete rep_;
}

struct CompactionContext {
  const Slice* start;
  const Slice* end;
  SequenceNumber last_sequence;
  SequenceNumber earliest_write_conflict_snapshot;
  SequenceNumber preserve_deletes_seqnum;
  std::vector<FileMetaData> file_metadata;
  std::vector<std::pair<int, std::vector<const FileMetaData*>>> inputs;
  TableFactory* table_factory;
  CompactionFilter* compaction_filter;
  MergeOperator* merge_operator;
  InternalKeyComparator internal_comparator;
  std::vector<SequenceNumber> existing_snapshots;
};

std::string RemoteCompactionWorker::Client::DoCompaction(
    const std::string& data) {
  // TODO load ctx from data;
  CompactionContext *ctx;

  ImmutableDBOptions immutable_db_options;
  ImmutableCFOptions immutable_cf_options;
  MutableCFOptions mutable_cf_options;

  std::unordered_map<uint64_t, std::unique_ptr<rocksdb::TableReader>>
  table_cache;
  DependFileMap depend_map;
  for (auto& f : ctx->file_metadata) {
    depend_map.emplace(f.fd.GetNumber(), &f);
  }
  std::mutex table_cache_mutex;
  TableReaderOptions table_reader_options(immutable_cf_options, nullptr,
                                          rep_->env_options,
                                          ctx->internal_comparator);
  void* create_iter_arg;
  IteratorCache::CreateIterCallback create_iter_callback;
  auto new_iterator = [&](uint64_t file_number)->InternalIterator* {
    std::lock_guard<std::mutex> lock(table_cache_mutex);
    auto find = table_cache.find(file_number);
    if (find == table_cache.end()) {
      std::string file_name;
      uint64_t file_size;
      auto s = rep_->env->GetFileSize(file_name, &file_size);
      if (!s.ok()) {
        // TODO ???
        return nullptr;
      }
      std::unique_ptr<rocksdb::RandomAccessFile> file;
      rep_->env->NewRandomAccessFile(file_name, &file, rep_->env_options);
      std::unique_ptr<rocksdb::RandomAccessFileReader> file_reader(
          new rocksdb::RandomAccessFileReader(std::move(file), file_name,
                                              rep_->env));
      std::unique_ptr<rocksdb::TableReader> reader;
      s = ctx->table_factory->NewTableReader(table_reader_options,
                                             std::move(file_reader), file_size,
                                             &reader, false);
      if (!s.ok()) {
        // TODO ???
        return nullptr;
      }
      find = table_cache.emplace(file_number, std::move(reader)).first;
    }
    auto iterator = find->second->NewIterator(ReadOptions(), nullptr);
    auto meta_find = depend_map.find(file_number);
    assert(meta_find != depend_map.end());
    if (meta_find->second->sst_purpose == kMapSst) {
      auto sst_iterator = NewMapSstIterator(meta_find->second, iterator,
                                            depend_map,
                                            ctx->internal_comparator,
                                            create_iter_arg,
                                            create_iter_callback);
      sst_iterator->RegisterCleanup([](void* arg1, void* /*arg2*/) {
        delete static_cast<InternalIterator*>(arg1);
      }, iterator, nullptr);
      iterator = sst_iterator;
    }
    return iterator;
  };
  auto create_itet = [&](const FileMetaData* file_metadata,
                         const DependFileMap&, Arena*,
                         TableReader** reader_ptr) {
    auto iterator = new_iterator(file_metadata->fd.GetNumber());
    if (reader_ptr != nullptr) {
      auto find = table_cache.find(file_metadata->fd.GetNumber());
      assert(find != table_cache.end());
      *reader_ptr = find->second.get();
    }
    return iterator;
  };
  create_iter_arg = &create_iter_arg;
  create_iter_callback = c_style_callback(create_itet);

  CompactionRangeDelAggregator range_del_agg(&ctx->internal_comparator,
                                             ctx->existing_snapshots);

  MergeIteratorBuilder merge_iter_builder(&ctx->internal_comparator, nullptr);
  for (auto& pair : ctx->inputs) {
    if (pair.first == 0 || pair.second.size() == 1) {
      merge_iter_builder.AddIterator(
          new_iterator(pair.second.front()->fd.GetNumber()));
    } else {
      auto map_iter = NewMapElementIterator(pair.second.data(),
                                            pair.second.size(),
                                            &ctx->internal_comparator,
                                            create_iter_arg,
                                            create_iter_callback);
      auto level_iter = NewMapSstIterator(nullptr, map_iter, depend_map,
                                          ctx->internal_comparator,
                                          create_iter_arg,
                                          create_iter_callback);
      level_iter->RegisterCleanup([](void* arg1, void* /*arg2*/) {
        delete static_cast<InternalIterator*>(arg1);
      }, map_iter, nullptr);
      merge_iter_builder.AddIterator(level_iter);
    }
  }
  std::unique_ptr<InternalIterator> input(merge_iter_builder.Finish());

  auto compaction_filter = ctx->compaction_filter;

  MergeHelper merge(
      rep_->env, ctx->internal_comparator.user_comparator(),
      ctx->merge_operator, compaction_filter,
      immutable_db_options.info_log.get(),
      false /* internal key corruption is expected */,
      ctx->existing_snapshots.empty() ? 0 : ctx->existing_snapshots.back());

  std::unique_ptr<CompactionIterator> c_iter(new CompactionIterator(
      input.get(), end, ctx->internal_comparator.user_comparator(), &merge,
      ctx->last_sequence, &ctx->existing_snapshots,
      ctx->earliest_write_conflict_snapshot, nullptr, rep_->env, false, false,
      range_del_agg, nullptr, compaction_filter, nullptr,
      ctx->preserve_deletes_seqnum));

  // TODO ... Do Compaction ...

  return {};
}

}  // namespace rocksdb
