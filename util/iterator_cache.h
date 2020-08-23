//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <cstdint>
#include <unordered_map>

#include "db/version_edit.h"
#include "table/internal_iterator.h"
#include "util/arena.h"

namespace rocksdb {

struct FileMetaData;
class RangeDelAggregator;
class TableReader;

class IteratorCache {
 public:
  using CreateIterCallback = InternalIterator* (*)(void* arg,
                                                   const FileMetaData*,
                                                   const DependenceMap&, Arena*,
                                                   TableReader**);

  IteratorCache(const DependenceMap& dependence_map, void* create_iter_arg,
                const CreateIterCallback& create_iter);
  ~IteratorCache();

  InternalIterator* GetIterator(const FileMetaData* f,
                                TableReader** reader_ptr = nullptr);

  InternalIterator* GetIterator(
      uint64_t file_number, TableReader** reader_ptr = nullptr,
      const FileMetaData** file_metadata_ptr = nullptr);

  Status GetReader(uint64_t file_number, TableReader** reader_ptr = nullptr,
                   const FileMetaData** file_metadata_ptr = nullptr);

  void PutFileMetaData(FileMetaData* f);
  const FileMetaData* GetFileMetaData(uint64_t file_number);

  Arena* GetArena() { return &arena_; }

 private:
  const DependenceMap& dependence_map_;
  DependenceMap dependence_map_ext_;
  void* callback_arg_;
  CreateIterCallback create_iter_;
  Arena arena_;

  struct CacheItem {
    InternalIterator* iter;
    TableReader* reader;
    const FileMetaData* meta;
  };
  std::unordered_map<uint64_t, CacheItem> iterator_map_;
};

}  // namespace rocksdb
