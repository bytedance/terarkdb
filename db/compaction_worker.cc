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
    json_impl<std::string>::template write<write_ty>(wt,
                                                     v.Encode().ToString(true));
  }
};
}

AJSON(rocksdb::CompactionWorkerResult::FileInfo, smallest, largest, file_name,
                                                 smallest_seqno, largest_seqno,
                                                 file_size, being_compacted);

AJSON(rocksdb::CompactionWorkerResult, status, actual_start, actual_end, files);

AJSON(rocksdb::FileDescriptor, packed_number_and_path_id, file_size,
                               smallest_seqno, largest_seqno);

AJSON(rocksdb::FileMetaData, fd, smallest, largest, sst_purpose, sst_depend);

AJSON(rocksdb::CompressionOptions, window_bits, level, strategy, max_dict_bytes,
                                   zstd_max_train_bytes, enabled);

AJSON(rocksdb::CompactionFilter::Context, is_full_compaction,
                                          is_manual_compaction,
                                          column_family_id);

AJSON(rocksdb::CompactionWorkerContext, user_comparator, merge_operator,
                                        merge_operator_data, compaction_filter,
                                        compaction_filter_factory,
                                        compaction_filter_context,
                                        compaction_filter_data, table_factory,
                                        table_factory_options, bloom_locality,
                                        cf_paths, prefix_extractor, has_start,
                                        has_end, start, end, last_sequence,
                                        earliest_write_conflict_snapshot,
                                        preserve_deletes_seqnum, file_metadata,
                                        inputs, cf_name, target_file_size,
                                        compression, compression_opts,
                                        existing_snapshots, bottommost_level,
                                        int_tbl_prop_collector_factories);

namespace rocksdb {

template<class T>
using TMap = std::unordered_map<std::string, T>;
template<class T>
using STMap = std::unordered_map<std::string, std::shared_ptr<T>>;

std::function<CompactionWorkerResult()>
RemoteCompactionWorker::StartCompaction(
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

struct RemoteCompactionWorker::Client::Rep {
  EnvOptions env_options;
  Env* env;
  TMap<const Comparator*> comparator_map;
  STMap<const SliceTransform> prefix_extractor_map;
  TMap<CreateTableFactoryCallback> table_factory_map;
  TMap<CreateMergeOperatorCallback> merge_operator_map;
  TMap<const CompactionFilter*> compaction_filter_map;
  STMap<CompactionFilterFactory> compaction_filter_factory_map;
  STMap<IntTblPropCollectorFactory> int_tbl_prop_collector_factory_map;
};

void RemoteCompactionWorker::Client::RegistComparator(
    const Comparator* comparator) {
  rep_->comparator_map[comparator->Name()] = comparator;
}

void RemoteCompactionWorker::Client::RegistPrefixExtractor(
    std::shared_ptr<const SliceTransform> prefix_extractor) {
  rep_->prefix_extractor_map[prefix_extractor->Name()] = prefix_extractor;
}

void RemoteCompactionWorker::Client::RegistTableFactory(
    const char* name, CreateTableFactoryCallback callback) {
  rep_->table_factory_map[name] = callback;
}

void RemoteCompactionWorker::Client::RegistMergeOperator(
    CreateMergeOperatorCallback merge_operator_callback) {
  std::shared_ptr<MergeOperator> merge_operator;
  auto s = merge_operator_callback(&merge_operator);
  if (s.ok()) {
    rep_->merge_operator_map[merge_operator->Name()] =
        merge_operator_callback;
  }
}

void RemoteCompactionWorker::Client::RegistCompactionFilter(
    const CompactionFilter* compaction_filter) {
  rep_->compaction_filter_map[compaction_filter->Name()] = compaction_filter;
}

void RemoteCompactionWorker::Client::RegistCompactionFilterFactory(
    std::shared_ptr<CompactionFilterFactory> compaction_filter_factory) {
  rep_->compaction_filter_factory_map[compaction_filter_factory->Name()] =
      compaction_filter_factory;
}

void RemoteCompactionWorker::Client::RegistTablePropertiesCollectorFactory(
    std::shared_ptr<TablePropertiesCollectorFactory>
        table_prop_collector_factory) {
  rep_->int_tbl_prop_collector_factory_map[
      table_prop_collector_factory->Name()] =
      std::make_shared<UserKeyTablePropertiesCollectorFactory>(
          table_prop_collector_factory);
}

RemoteCompactionWorker::Client::Client(EnvOptions env_options, Env* env) {
  rep_ = new Rep();
  rep_->env_options = env_options;
  rep_->env = env;
  RegistComparator(BytewiseComparator());
  RegistComparator(ReverseBytewiseComparator());
  RegistTableFactory(
      NewBlockBasedTableFactory(BlockBasedTableOptions())->Name(),
      [](std::shared_ptr<TableFactory>* ptr, const std::string& options) {
        ptr->reset();
        BlockBasedTableOptions base, bbto;
        auto s = GetBlockBasedTableOptionsFromString(base, options, &bbto);
        if (s.ok()) {
          ptr->reset(NewBlockBasedTableFactory(bbto));
        }
        return s;
      });
  RegistTableFactory(
      NewCuckooTableFactory(CuckooTableOptions())->Name(),
      [](std::shared_ptr<TableFactory>* ptr, const std::string& options) {
        ptr->reset();
        CuckooTableOptions cto;
        std::unordered_map<std::string, std::string> opts_map;
        Status s = StringToMap(options, &opts_map);
        if (!s.ok()) {
          return s;
        }
        auto get_option = [&](const char* opt) {
          auto find = opts_map.find(opt);
          if (find == opts_map.end()) {
            return std::string();
          }
          return find->second;
        };
        std::string opt;
        if (!(opt = get_option("hash_table_ratio")).empty()) {
          cto.hash_table_ratio = std::atof(opt.c_str());
        }
        if (!(opt = get_option("max_search_depth")).empty()) {
          cto.max_search_depth = std::atoi(opt.c_str());
        }
        if (!(opt = get_option("cuckoo_block_size")).empty()) {
          cto.cuckoo_block_size = std::atoi(opt.c_str());
        }
        if (!(opt = get_option("identity_as_first_hash")).empty()) {
          cto.identity_as_first_hash = std::atoi(opt.c_str());
        }
        ptr->reset(NewCuckooTableFactory(cto));
        return Status::OK();
      });
  RegistTableFactory(
      NewPlainTableFactory(PlainTableOptions())->Name(),
      [](std::shared_ptr<TableFactory>* ptr, const std::string& options) {
        ptr->reset();
        PlainTableOptions base, pto;
        auto s = GetPlainTableOptionsFromString(base, options, &pto);
        if (s.ok()) {
          ptr->reset(NewPlainTableFactory(pto));
        }
        return s;
      });
}

RemoteCompactionWorker::Client::~Client() {
  delete rep_;
}

std::string RemoteCompactionWorker::Client::DoCompaction(
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
    auto find = rep_->comparator_map.find(context.user_comparator);
    if (find == rep_->comparator_map.end()) {
      return make_error(Status::Corruption("Missing comparator !"));
    }
    cf_options.comparator = find->second;
  }
  if (!context.merge_operator.empty()) {
    auto find = rep_->merge_operator_map.find(context.merge_operator);
    if (find == rep_->merge_operator_map.end()) {
      return make_error(Status::Corruption("Missing merge_operator !"));
    }
    auto s = find->second(&cf_options.merge_operator);
    if (s.ok() && cf_options.merge_operator &&
        !context.merge_operator_data.empty()) {
      s = cf_options.merge_operator->Deserialize(context.merge_operator_data);
    }
    if (!s.ok()) {
      return make_error(std::move(s));
    }
  }
  if (!context.compaction_filter.empty()) {
    auto find = rep_->compaction_filter_map.find(context.compaction_filter);
    if (find == rep_->compaction_filter_map.end()) {
      return make_error(Status::Corruption("Missing compaction_filter !"));
    }
    cf_options.compaction_filter = find->second;
  }
  if (!context.compaction_filter_factory.empty()) {
    auto find = rep_->compaction_filter_factory_map.find(
        context.compaction_filter_factory);
    if (find == rep_->compaction_filter_factory_map.end()) {
      return
          make_error(Status::Corruption("Missing compaction_filter_factory !"));
    }
    cf_options.compaction_filter_factory = find->second;
  }
  if (context.table_factory.empty()) {
    return make_error(Status::Corruption("Bad table_factory name !"));
  } else {
    auto find = rep_->table_factory_map.find(context.table_factory);
    if (find == rep_->table_factory_map.end()) {
      return make_error(Status::Corruption("Missing table_factory !"));
    }
    auto s =
        find->second(&cf_options.table_factory, context.table_factory_options);
    if (!s.ok()) {
      return make_error(std::move(s));
    }
  }
  cf_options.bloom_locality = context.bloom_locality;
  for (auto& path : context.cf_paths) {
    cf_options.cf_paths.emplace_back(DbPath(path, 0));
  }
  if (!context.prefix_extractor.empty()) {
    auto find = rep_->prefix_extractor_map.find(
        context.prefix_extractor);
    if (find == rep_->prefix_extractor_map.end()) {
      return make_error(Status::Corruption("Missing prefix_extractor !"));
    }
    cf_options.prefix_extractor = find->second;
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
    auto find = rep_->int_tbl_prop_collector_factory_map.find(collector);
    if (find == rep_->int_tbl_prop_collector_factory_map.end()) {
      return make_error(
          Status::Corruption("Missing int_tbl_prop_collector_factories !"));
    }
    int_tbl_prop_collector_factories.data.emplace_back(find->second.get());
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
  DependFileMap context_depend_map;
  for (auto& f : context.file_metadata) {
    context_depend_map.emplace(f.fd.GetNumber(), &f);
  }
  std::unordered_map<int, std::vector<const FileMetaData*>> inputs;
  for (auto pair : context.inputs) {
    assert(context_depend_map.find(pair.second) != context_depend_map.end());
    inputs[pair.first].push_back(context_depend_map[pair.second]);
  }
  std::unordered_map<uint64_t, std::unique_ptr<rocksdb::TableReader>>
      table_cache;
  std::mutex table_cache_mutex;
  TableReaderOptions table_reader_options(
      immutable_cf_options, mutable_cf_options.prefix_extractor.get(),
      rep_->env_options, *icmp);
  struct {
    void* arg;
    IteratorCache::CreateIterCallback callback;
  } c_style_new_iterator;
  auto new_iterator = [&](const FileMetaData* file_metadata,
                          const DependFileMap& depend_map, Arena* arena,
                          TableReader** reader_ptr) -> InternalIterator* {
    std::lock_guard<std::mutex> lock(table_cache_mutex);
    uint64_t file_number = file_metadata->fd.GetNumber();
    auto find = table_cache.find(file_number);
    if (find == table_cache.end()) {
      std::string file_name =
          TableFileName(immutable_cf_options.cf_paths, file_number,
                        file_metadata->fd.GetPathId());
      std::unique_ptr<rocksdb::RandomAccessFile> file;
      auto s = rep_->env->NewRandomAccessFile(file_name, &file,
                                              rep_->env_options);
      if (!s.ok()) {
        return NewErrorInternalIterator(s);
      }
      std::unique_ptr<rocksdb::RandomAccessFileReader> file_reader(
          new rocksdb::RandomAccessFileReader(std::move(file), file_name,
                                              rep_->env));
      std::unique_ptr<rocksdb::TableReader> reader;
      s = immutable_cf_options.table_factory->NewTableReader(
          table_reader_options, std::move(file_reader),
          file_metadata->fd.file_size, &reader, false);
      if (!s.ok()) {
        return NewErrorInternalIterator(s);
      }
      find = table_cache.emplace(file_number, std::move(reader)).first;
      if (reader_ptr != nullptr) {
        *reader_ptr = find->second.get();
      }
    }
    auto iterator =
        find->second->NewIterator(ReadOptions(),
                                  mutable_cf_options.prefix_extractor.get(),
                                  arena);
    if (file_metadata->sst_purpose == kMapSst && !depend_map.empty()) {
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

  CompactionRangeDelAggregator range_del_agg(icmp, context.existing_snapshots);

  Arena arena;
  MergeIteratorBuilder merge_iter_builder(icmp, &arena);
  for (auto& pair : inputs) {
    if (pair.first == 0 || pair.second.size() == 1) {
      merge_iter_builder.AddIterator(new_iterator(pair.second.front(),
                                                  context_depend_map, &arena,
                                                  nullptr));
    } else {
      auto map_iter = NewMapElementIterator(pair.second.data(),
                                            pair.second.size(), icmp,
                                            c_style_new_iterator.arg,
                                            c_style_new_iterator.callback,
                                            &arena);
      auto level_iter = NewMapSstIterator(nullptr, map_iter, context_depend_map,
                                          *icmp, c_style_new_iterator.arg,
                                          c_style_new_iterator.callback,
                                          &arena);
      level_iter->RegisterCleanup([](void* arg1, void* /*arg2*/) {
        static_cast<InternalIterator*>(arg1)->~InternalIterator();
      }, map_iter, nullptr);
      merge_iter_builder.AddIterator(level_iter);
    }
  }
  ScopedArenaIterator input(merge_iter_builder.Finish());

  auto compaction_filter = immutable_cf_options.compaction_filter;
  std::unique_ptr<CompactionFilter> compaction_filter_from_factory = nullptr;
  if (compaction_filter == nullptr &&
      immutable_cf_options.compaction_filter_factory != nullptr) {
    auto factory = immutable_cf_options.compaction_filter_factory;
    compaction_filter_from_factory =
        factory->CreateCompactionFilter(context.compaction_filter_context);
    if (compaction_filter_from_factory) {
      auto s = compaction_filter_from_factory->Deserialize(
          context.compaction_filter_data);
      if (!s.ok()) {
        return make_error(std::move(s));
      }
    }
    compaction_filter = compaction_filter_from_factory.get();
  }

  MergeHelper merge(
      rep_->env, ucmp, immutable_cf_options.merge_operator,
      compaction_filter, immutable_db_options.info_log.get(),
      false /* internal key corruption is expected */,
      context.existing_snapshots.empty()
          ? 0 : context.existing_snapshots.back());

  std::unique_ptr<CompactionIterator> c_iter(new CompactionIterator(
      input.get(), end, ucmp, &merge, context.last_sequence,
      &context.existing_snapshots, context.earliest_write_conflict_snapshot,
      nullptr, rep_->env, false, false, &range_del_agg, nullptr,
      compaction_filter, nullptr, context.preserve_deletes_seqnum));

  InternalKey actual_start, actual_end;

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

  auto create_builder =
      [&](std::unique_ptr<WritableFileWriter>* writer_ptr,
          std::unique_ptr<TableBuilder>* builder_ptr) {
        std::string file_name = GenerateOutputFileName(result.files.size());
        Status s;
        TableBuilderOptions table_builder_options(
            immutable_cf_options, mutable_cf_options, *icmp,
            &int_tbl_prop_collector_factories.data, context.compression,
            context.compression_opts, nullptr, true, false, context.cf_name,
            -1);
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
  auto finish_output_file = [&](Status s, FileMetaData* meta,
                                std::unique_ptr<WritableFileWriter>* writer_ptr,
                                std::unique_ptr<TableBuilder>* builder_ptr,
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
                                       tombstone.seq_, *icmp);
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
      if (current_entries > 0 || tp.num_range_deletions > 0) {
        file_info.file_name = writer->file_name();
      }
      file_info.smallest = std::move(*meta->smallest.rep());
      file_info.largest = std::move(*meta->largest.rep());
      file_info.smallest_seqno = meta->fd.smallest_seqno;
      file_info.largest_seqno = meta->fd.largest_seqno;
      file_info.file_size = meta->fd.file_size;
      file_info.being_compacted = builder->NeedCompact();
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
  input.set(nullptr);
  result.actual_start = std::move(*actual_start.rep());
  result.actual_end = std::move(*actual_end.rep());

  ajson::string_stream stream;
  ajson::save_to(stream, result);
  return stream.str();
}

}  // namespace rocksdb
