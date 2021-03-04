//
// Created by wangyi.ywq on 2021/3/3.
//

#ifndef TERARKDB_STATIC_MAP_INDEX_H
#define TERARKDB_STATIC_MAP_INDEX_H

#include <assert.h>
#include <stdio.h>

#include <vector>

#include "db/dbformat.h"
#include "rocksdb/slice.h"
#include "rocksdb/terark_namespace.h"
#include "db/map_builder.h"

namespace TERARKDB_NAMESPACE {

struct StaticMapIndex {
  StaticMapIndex(const InternalKeyComparator* _c) : c(_c){

                                                    };
  ~StaticMapIndex(){
    if(key_buff != nullptr){
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
  int getIdx(Slice key) {
    int l = 0, r = key_nums - 1;
    while (l <= r) {
      int mid = (l + r) >> 1;
      if (c->Compare(key, getKey(mid)) > 0)
        l = mid + 1;
      else
        r = mid - 1;
    }
    return l;
  }
  const InternalKeyComparator* c;
};
extern Status BuildStaticMapIndex(InternalIterator* iter,
                     StaticMapIndex* index);

}  // namespace TERARKDB_NAMESPACE
#endif  // TERARKDB_STATIC_MAP_INDEX_H
