#include <options/db_options.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/options_util.h>
#include <util/gflags_compat.h>
#include <util/hash.h>
#include <table/terark_zip_table.h>
#include <rocksdb/table.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <random>
#include <string>

DEFINE_uint64(record_count, 1000000, "the number of loaded data");
DEFINE_bool(disable_force_memory, false, "load map sst in table mem reader");
DEFINE_bool(lazy, false, "lazy compaction");
DEFINE_bool(use_user_range, false, "CompactRange use 0~MAX");
DEFINE_bool(use_seek_range, false, "CompactRange use key of iter seek");
DEFINE_string(table_factory, "BlockBasedTable", "table factory");