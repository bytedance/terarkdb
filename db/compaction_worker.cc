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
#include "util/ajson_msd.hpp"
#include "util/c_style_callback.h"
#include "util/filename.h"

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
    v = rocksdb::Status(s.code, s.subcode, s.sev, s.state.c_str());
  }

  template<typename write_ty>
  static inline void write(write_ty& wt, rocksdb::Status const& v) {
    AJsonStatus s = {
        v.code(), v.subcode(), v.severity(), v.getState()
    };
    json_impl<AJsonStatus>::template write<write_ty>(wt, s);
  }
};
}

AJSON(rocksdb::CompactionWorkerResult::FileInfo, input_start, input_end,
      file_name, smallest_seqno, largest_seqno);
AJSON(rocksdb::CompactionWorkerResult, status, files);

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
      ajson::load_from_buff(decoded_result, &encoded_result[0],
                            encoded_result.size());
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
  rep_->comparator_map.emplace(comparator->Name(), comparator);
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
  std::string cf_name;
  InternalKeyComparator internal_comparator;
  uint64_t target_file_size;
  CompressionType compression;
  CompressionOptions compression_opts;
  std::vector<SequenceNumber> existing_snapshots;
  bool bottommost_level;
};

std::string RemoteCompactionWorker::Client::DoCompaction(
    const std::string& data) {
  // TODO load ctx & options from data;
  CompactionContext *ctx = nullptr;

  ImmutableDBOptions immutable_db_options;
  ImmutableCFOptions immutable_cf_options(immutable_db_options,
                                          ColumnFamilyOptions());
  MutableCFOptions mutable_cf_options;

  auto ucmp = ctx->internal_comparator.user_comparator();

  // start run
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
    assert(depend_map.find(file_number) != depend_map.end());
    const FileMetaData* file_meta = depend_map[file_number];
    auto find = table_cache.find(file_number);
    if (find == table_cache.end()) {
      std::string file_name =
          MakeTableFileName(
              immutable_cf_options.cf_paths[file_meta->fd.GetPathId()].path,
              file_meta->fd.GetNumber());
      uint64_t file_size;
      auto s = rep_->env->GetFileSize(file_name, &file_size);
      if (!s.ok()) {
        return NewErrorInternalIterator(s);
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
        return NewErrorInternalIterator(s);
      }
      find = table_cache.emplace(file_number, std::move(reader)).first;
    }
    auto iterator = find->second->NewIterator(ReadOptions(), nullptr);
    if (file_meta->sst_purpose == kMapSst) {
      auto sst_iterator = NewMapSstIterator(file_meta, iterator, depend_map,
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
  auto create_iter = [&](const FileMetaData* file_metadata,
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
  create_iter_callback = c_style_callback(create_iter);

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
      rep_->env, ucmp, ctx->merge_operator, compaction_filter,
      immutable_db_options.info_log.get(),
      false /* internal key corruption is expected */,
      ctx->existing_snapshots.empty() ? 0 : ctx->existing_snapshots.back());

  std::unique_ptr<CompactionIterator> c_iter(new CompactionIterator(
      input.get(), ctx->end, ucmp, &merge, ctx->last_sequence,
      &ctx->existing_snapshots, ctx->earliest_write_conflict_snapshot, nullptr,
      rep_->env, false, false, &range_del_agg, nullptr, compaction_filter,
      nullptr, ctx->preserve_deletes_seqnum));

  InternalKey actual_start, actual_end;

  const Slice* start = ctx->start;
  const Slice* end = ctx->end;
  if (start != nullptr) {
    actual_start.SetMinPossibleForUserKey(*start);
    input->Seek(actual_start.Encode());
  } else {
    input->SeekToFirst();
    actual_start.SetMinPossibleForUserKey(ExtractUserKey(input->key()));
  }
  c_iter->SeekToFirst();
  // TODO init citer_2
  //std::unique_ptr<CompactionIterator> c_iter2;
  //auto second_pass_iter = c_iter2->AdaptToInternalIterator();
  std::unique_ptr<InternalIterator> second_pass_iter;

  CompactionWorkerResult result;

  std::vector<std::unique_ptr<IntTblPropCollectorFactory>>
  int_tbl_prop_collector_factories;

  auto create_builder =
      [&](std::unique_ptr<WritableFileWriter>* writer_ptr,
          std::unique_ptr<TableBuilder>* builder_ptr)->Status {
    std::string file_name = GenerateOutputFileName(result.files.size());
    Status s;
    TableBuilderOptions table_builder_options(
        immutable_cf_options, mutable_cf_options, ctx->internal_comparator,
        &int_tbl_prop_collector_factories, ctx->compression,
        ctx->compression_opts, nullptr, true, false, ctx->cf_name, -1);
    std::unique_ptr<WritableFile> sst_file;
    s = rep_->env->NewWritableFile(file_name, &sst_file, rep_->env_options);
    if (!s.ok()) {
      return s;
    }
    writer_ptr->reset(
        new WritableFileWriter(std::move(sst_file), file_name,
            rep_->env_options, nullptr, immutable_db_options.listeners));
    builder_ptr->reset(ctx->table_factory->NewTableBuilder(
        table_builder_options, 0, writer_ptr->get()));
    (*builder_ptr)->SetSecondPassIterator(second_pass_iter.get());
    return s;
  };
  auto finish_output_file = [&](Status s, FileMetaData* meta,
                                std::unique_ptr<WritableFileWriter>* writer_ptr,
                                std::unique_ptr<TableBuilder>* builder_ptr,
                                const Slice* next_key)->Status {
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
      if (ctx->existing_snapshots.size() > 0) {
        earliest_snapshot = ctx->existing_snapshots[0];
      }
      bool has_overlapping_endpoints;
      if (upper_bound != nullptr && meta->largest.size() > 0) {
        has_overlapping_endpoints =
            ucmp->Compare(meta->largest.user_key(), *upper_bound) == 0;
      } else {
        has_overlapping_endpoints = false;
      }
      assert(end == nullptr || upper_bound == nullptr ||
             ucmp->Compare(*upper_bound , *end) <= 0);
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

        if (ctx->bottommost_level && tombstone.seq_ <= earliest_snapshot) {
          continue;
        }

        auto kv = tombstone.Serialize();
        assert(lower_bound == nullptr ||
               ucmp->Compare(*lower_bound, kv.second) < 0);
        builder->Add(kv.first.Encode(), kv.second);
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
                                       tombstone.seq_,
                                       ctx->internal_comparator);
        assert(smallest_ikey_seqnum == 0 ||
               ExtractInternalKeyFooter(meta->smallest.Encode()) !=
               PackSequenceAndType(0, kTypeRangeDeletion));
      }
      meta->marked_for_compaction = builder->NeedCompact();
    }
    const uint64_t current_entries = builder->NumEntries();
    if (s.ok()) {
      s = builder->Finish();
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
      file_info.input_start = *actual_start.rep();
      file_info.input_end = *actual_end.rep();
      if (current_entries > 0 || tp.num_range_deletions > 0) {
        file_info.file_name = writer->file_name();
      }
      file_info.smallest_seqno = meta->fd.smallest_seqno;
      file_info.largest_seqno = meta->fd.largest_seqno;
      result.files.emplace_back(file_info);
      actual_start = std::move(actual_end);
      actual_end.rep()->clear();
    }
    *meta = FileMetaData();
    builder_ptr->reset();
    writer_ptr->reset();
    return s;
  };
  std::unique_ptr<WritableFileWriter> writer;
  std::unique_ptr<TableBuilder> builder;
  FileMetaData meta;

  Status& status = result.status;
  const Slice* next_key = nullptr;
  while (status.ok() && c_iter->Valid()) {
    // Invariant: c_iter.status() is guaranteed to be OK if c_iter->Valid()
    // returns true.
    const Slice& key = c_iter->key();
    const Slice& value = c_iter->value();

    assert(end == nullptr || ucmp->Compare(c_iter->user_key(), *end) < 0);

    // Open output file if necessary
    if (!builder) {
      status = create_builder(&writer, &builder);
      if (!status.ok()) {
        break;
      }
    }
    assert(builder);
    builder->Add(key, value);
    size_t current_output_file_size = builder->FileSize();
    meta.UpdateBoundaries(key, c_iter->ikey().sequence);

    bool output_file_ended = false;
    Status input_status;
    if (current_output_file_size >= ctx->target_file_size) {
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
                                  next_key);
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

  if (status.ok() && !builder && result.files.empty() &&
      !range_del_agg.IsEmpty()) {
    status = create_builder(&writer, &builder);
  }
  if (builder) {
    status = finish_output_file(status, &meta, &writer, &builder, nullptr);
  }
  c_iter.reset();
  input.reset();

  ajson::string_stream stream;
  ajson::save_to(stream, result);
  return stream.str();
}

}  // namespace rocksdb
