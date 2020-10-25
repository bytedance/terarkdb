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

Status TableReader::RangeScan(const Slice* begin,
                              const SliceTransform* prefix_extractor, void* arg,
                              bool (*callback_func)(void* arg, const Slice& key,
                                                    LazyBuffer&& value)) {
  Arena arena;
  ScopedArenaIterator iter(
      NewIterator(ReadOptions(), prefix_extractor, &arena));
  for (begin == nullptr ? iter->SeekToFirst() : iter->Seek(*begin);
       iter->Valid() && callback_func(arg, iter->key(), iter->value());
       iter->Next()) {
  }
  return iter->status();
}

void TableReader::UpdateMaxCoveringTombstoneSeq(
    const rocksdb::ReadOptions& readOptions, const rocksdb::Slice& user_key,
    rocksdb::SequenceNumber* max_covering_tombstone_seq) {
  if (max_covering_tombstone_seq != nullptr &&
      !readOptions.ignore_range_deletions) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
        NewRangeTombstoneIterator(readOptions));
    if (range_del_iter != nullptr) {
      *max_covering_tombstone_seq =
          std::max(*max_covering_tombstone_seq,
                   range_del_iter->MaxCoveringTombstoneSeqnum(user_key));
    }
  }
}

}  // namespace rocksdb
