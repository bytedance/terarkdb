//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/iterator_cache.h"

#include "db/range_del_aggregator.h"
#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

IteratorCache::IteratorCache(const DependenceMap& dependence_map,
                             void* callback_arg,
                             const CreateIterCallback& create_iter)
    : dependence_map_(dependence_map),
      callback_arg_(callback_arg),
      create_iter_(create_iter) {}

IteratorCache::~IteratorCache() {
  for (auto pair : iterator_map_) {
    pair.second.iter->~InternalIterator();
  }
}

InternalIterator* IteratorCache::GetIterator(const FileMetaData* f,
                                             TableReader** reader_ptr) {
  auto find = iterator_map_.find(f->fd.GetNumber());
  if (find != iterator_map_.end()) {
    if (reader_ptr != nullptr) {
      *reader_ptr = find->second.reader;
    }
    return find->second.iter;
  }
  CacheItem item;
  item.iter =
      create_iter_(callback_arg_, f, dependence_map_, &arena_, &item.reader);
  item.meta = f;
  assert(item.iter != nullptr);
  iterator_map_.emplace(f->fd.GetNumber(), item);
  if (reader_ptr != nullptr) {
    *reader_ptr = item.reader;
  }
  return item.iter;
}

InternalIterator* IteratorCache::GetIterator(
    uint64_t file_number, TableReader** reader_ptr,
    const FileMetaData** file_metadata_ptr) {
  auto find = iterator_map_.find(file_number);
  if (find != iterator_map_.end()) {
    if (reader_ptr != nullptr) {
      *reader_ptr = find->second.reader;
    }
    return find->second.iter;
  }
  CacheItem item;
  item.meta = GetFileMetaData(file_number);
  if (item.meta == nullptr) {
    auto s = Status::Corruption(
        "IteratorCache::GetIterator: Composite sst depend files missing");
    item.iter = NewErrorInternalIterator<LazyBuffer>(s, &arena_);
    item.reader = nullptr;
  } else {
    item.iter = create_iter_(callback_arg_, item.meta, dependence_map_, &arena_,
                             &item.reader);
  }
  iterator_map_.emplace(file_number, item);
  if (reader_ptr != nullptr) {
    *reader_ptr = item.reader;
  }
  if (file_metadata_ptr != nullptr) {
    *file_metadata_ptr = item.meta;
  }
  return item.iter;
}

Status IteratorCache::GetReader(uint64_t file_number, TableReader** reader_ptr,
                                const FileMetaData** file_metadata_ptr) {
  auto find = iterator_map_.find(file_number);
  if (find != iterator_map_.end()) {
    if (reader_ptr != nullptr) {
      *reader_ptr = find->second.reader;
    }
    if (file_metadata_ptr != nullptr) {
      *file_metadata_ptr = find->second.meta;
    }
    return find->second.iter->status();
  }
  CacheItem item;
  item.meta = GetFileMetaData(file_number);
  if (item.meta == nullptr) {
    return Status::Corruption(
        "IteratorCache::GetReader: Composite sst depend files missing");
  } else {
    item.iter = create_iter_(callback_arg_, item.meta, dependence_map_, &arena_,
                             &item.reader);
    assert(item.iter != nullptr);
  }
  iterator_map_.emplace(file_number, item);
  if (reader_ptr != nullptr) {
    *reader_ptr = item.reader;
  }
  if (file_metadata_ptr != nullptr) {
    *file_metadata_ptr = item.meta;
  }
  return item.iter->status();
}

void IteratorCache::PutFileMetaData(FileMetaData* f) {
  dependence_map_ext_.emplace(f->fd.GetNumber(), f);
}

const FileMetaData* IteratorCache::GetFileMetaData(uint64_t file_number) {
  auto find = dependence_map_.find(file_number);
  if (find == dependence_map_.end()) {
    find = dependence_map_ext_.find(file_number);
  }
  if (find != dependence_map_.end()) {
    return find->second;
  }
  return nullptr;
}

}  // namespace TERARKDB_NAMESPACE
