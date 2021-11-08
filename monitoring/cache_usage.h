//
// Created by wangyi on 2021/11/5.
//
#pragma once
#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

extern void CollectCacheUsage(uint64_t file_number, size_t charge);
extern void GetCacheUsage(std::vector<std::pair<uint64_t, size_t>>* usage);

}
