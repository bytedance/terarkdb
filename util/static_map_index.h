//
// Created by wangyi.ywq on 2021/3/3.
//

#ifndef TERARKDB_STATIC_MAP_INDEX_H
#define TERARKDB_STATIC_MAP_INDEX_H

#include <assert.h>
#include <stdio.h>

#include <iostream>
#include <vector>

#include "db/dbformat.h"
#include "db/map_builder.h"
#include "rocksdb/slice.h"
#include "rocksdb/terark_namespace.h"
#include "util/coding.h"

namespace TERARKDB_NAMESPACE {

struct StaticMapIndex {
  StaticMapIndex(const InternalKeyComparator* _c)
      : c(_c){

        };
  ~StaticMapIndex() {
    if (key_buff != nullptr) {
      delete[] key_buff;
      delete[] value_buff;
      delete[] key_offset;
      delete[] value_offset;
    }
  };
  char* key_buff = nullptr;
  char* value_buff = nullptr;
  int* key_offset = nullptr;
  int* value_offset = nullptr;
  int key_nums;
  int key_len;
  int value_len;

  char* get_key_offset(int id) const {
    assert(id < key_nums);
    return key_buff + key_offset[id];
  }
  int get_key_len(int id) const {
    assert(id < key_nums);
    return key_offset[id + 1] - key_offset[id];
  }
  char* get_value_offset(int id) const {
    assert(id < key_nums);
    return value_buff + value_offset[id];
  }
  int get_value_len(int id) const {
    assert(id < key_nums);
    return value_offset[id + 1] - value_offset[id];
  }
  inline const Slice getKey(int id) const {
    return Slice(get_key_offset(id), get_key_len(id));
  }
  inline const Slice getValue(int id) const {
    return Slice(get_value_offset(id), get_value_len(id));
  }
  int size(){
    return key_len + value_len + key_nums * 16 + 16;
  }
  int getIdx(const Slice& key) {
    int l = 0, r = key_nums - 1;
    while (l < r) {
      int mid = (l + r) >> 1;
      if (c->Compare(key, getKey(mid)) >= 0)
        r = mid;
      else
        l = mid + 1;
    }
    return l;
  }
  bool DecodeFrom(Slice& map_input) {
    Slice smallest_key;
    uint64_t link_count;
    std::vector<int> dependence;
    uint64_t flags;
    if (!GetVarint64(&map_input, &flags) ||
        !GetVarint64(&map_input, &link_count) ||
        !GetLengthPrefixedSlice(&map_input, &smallest_key)) {
      std::cout << "parse error" << std::endl;
      return false;
    }
    uint64_t file_number;
    for (uint64_t i = 0; i < link_count; ++i) {
      if (!GetVarint64(&map_input, &file_number)) {
        std::cout << "parse error" << std::endl;
      }
      dependence.push_back(file_number);
    }
    InternalKey ikey;
    ikey.DecodeFrom(smallest_key);
    std::cout << "link_count:" << link_count << " smallest_key:" << ikey.DebugString() << std::endl;
  }
  void DebugString() {
    InternalKey ikey;
    for (int i = 0; i < key_nums; i++) {
      ikey.DecodeFrom(getKey(i));
      std::cout << ikey.DebugString() << std::endl;
      Slice v = getValue(i);
      DecodeFrom(v);
    }
  }
  const InternalKeyComparator* c;
};
extern Status BuildStaticMapIndex(InternalIterator* iter,
                                  StaticMapIndex* index);

}  // namespace TERARKDB_NAMESPACE
#endif  // TERARKDB_STATIC_MAP_INDEX_H
