//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table_reader.h"
#include "rocksdb/statistics.h"
#include "table/get_context.h"
#include "table/scoped_arena_iterator.h"
#include "util/arena.h"

namespace rocksdb {

Status TableReader::RowCachedGet(
    const rocksdb::ReadOptions& readOptions, const Slice& key,
    SequenceNumber largest_seqno, Cache* row_cache,
    const rocksdb::Slice& row_cache_id, rocksdb::Statistics* statistics,
    rocksdb::GetContext* get_context,
    const rocksdb::SliceTransform* prefix_extractor, bool skip_filters) {
  assert(row_cache != nullptr && !get_context->NeedToReadSequence());

#ifndef ROCKSDB_LITE
  IterKey cache_key;
  if (RowCacheContext::GetFromRowCache(readOptions, key, largest_seqno,
                                       &cache_key, row_cache, row_cache_id,
                                       FileNumber(), statistics, get_context)) {
    return Status::OK();
  }
  assert(!cache_key.GetUserKey().empty());
  RowCacheContext row_cache_context;
  get_context->SetReplayLog(RowCacheContext::AddReplayLog,
                            &row_cache_context);
#endif  // ROCKSDB_LITE
  UpdateMaxCoveringTombstoneSeq(readOptions, ExtractUserKey(key),
                                get_context->max_covering_tombstone_seq());
  Status s = Get(readOptions, key, get_context, prefix_extractor, skip_filters);
#ifndef ROCKSDB_LITE
  get_context->SetReplayLog(nullptr, nullptr);
  if (s.ok()) {
    s = row_cache_context.AddToCache(cache_key, row_cache);
  }
#endif  // ROCKSDB_LITE
  return s;
}

void TableReader::RangeScan(const Slice* begin,
                            const SliceTransform* prefix_extractor, void* arg,
                            bool (*callback_func)(void* arg, const Slice& key,
                                                  LazySlice&& value)) {
  Arena arena;
  ScopedArenaIterator iter(
      NewIterator(ReadOptions(), prefix_extractor, &arena));
  for (begin == nullptr ? iter->SeekToFirst() : iter->Seek(*begin);
       iter->Valid() && callback_func(arg, iter->key(), iter->value());
       iter->Next()) {
  }
}

void TableReader::UpdateMaxCoveringTombstoneSeq(
    const rocksdb::ReadOptions& readOptions, const rocksdb::Slice& user_key,
    rocksdb::SequenceNumber* max_covering_tombstone_seq) {
  if (max_covering_tombstone_seq != nullptr &&
      !readOptions.ignore_range_deletions) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
        NewRangeTombstoneIterator(readOptions));
    if (range_del_iter != nullptr) {
      *max_covering_tombstone_seq = std::max(
          *max_covering_tombstone_seq,
          range_del_iter->MaxCoveringTombstoneSeqnum(user_key));
    }
  }
}

}  // namespace rocksdb
