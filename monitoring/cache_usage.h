//
// Created by wangyi on 2021/11/5.
//
#pragma once
#include "rocksdb/terark_namespace.h"
#include <vector>
#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>
namespace TERARKDB_NAMESPACE {

extern void CollectCacheUsage(uint64_t file_number, ssize_t charge);
extern void GetCacheUsage(std::vector<std::pair<uint64_t, size_t>>* usage);

}
