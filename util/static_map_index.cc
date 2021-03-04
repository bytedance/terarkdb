#include "util/static_map_index.h"

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/slice.h"
#include "table/table_reader.h"

namespace TERARKDB_NAMESPACE {

Status BuildStaticMapIndex(InternalIterator *iter, StaticMapIndex *index) {
  Status status;
  int key_nums = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    key_nums++;
  }
  int key_lens = 0;
  int value_lens = 0;
  int *key_offset = new int[key_nums + 1];
  int *value_offset = new int[key_nums + 1];
  int i = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    auto lazy_val = iter->value();
    auto s = lazy_val.fetch();
    if (!s.ok()) {
      return s;
    }
    key_offset[i] = key_lens;
    value_offset[i] = value_lens;
    key_lens += iter->key().size();
    value_lens += lazy_val.slice().size();
    i++;
  }
  key_offset[key_nums] = key_lens;
  value_offset[key_nums] = value_lens;
  char *key_buffer = (char *)malloc(key_lens);
  char *value_buffer = (char *)malloc(value_lens);
  i = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    auto lazy_val = iter->value();
    auto s = lazy_val.fetch();
    if (!s.ok()) {
      return s;
    }
    memcpy(key_buffer + key_offset[i], iter->key().data(), iter->key().size());
    memcpy(value_buffer + value_offset[i], lazy_val.slice().data(),
           lazy_val.slice().size());
    i++;
  }
  index->key_buff = key_buffer;
  index->value_buff = value_buffer;
  index->key_offset = key_offset;
  index->value_offset = value_offset;
  index->key_nums = key_nums;
  index->key_len = key_lens;
  index->value_len = value_lens;
  return status;
}

}  // namespace TERARKDB_NAMESPACE