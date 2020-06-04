#pragma warning(disable:4996)

#include <sys/time.h>

#include <cctype>
#include <chrono>
#include <cinttypes>
#include <condition_variable>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include <db/memtable.h>
#include <db/dbformat.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/experimental.h>
#include <rocksdb/memtablerep.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/table.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <table/iterator_wrapper.h>
#include <table/table_builder.h>
#include <table/table_reader.h>
#include <table/terark_zip_common.h>
#include <table/terark_zip_table.h>
#include <terark/fsa/cspptrie.inl>
#include <terark/idx/terark_zip_index.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/lcast.hpp>
#include <terark/mempool_lock_none.hpp>
#include <terark/stdtypes.hpp>
#include <terark/util/mmap.hpp>
#include <terark/zbs/dict_zip_blob_store.hpp>
#include <terark/zbs/sufarr_inducedsort.h>
#include <terark/zbs/zip_reorder_map.hpp>
#include <util/coding.h>
#include <util/filename.h>
#include <utilities/merge_operators/string_append/stringappend2.h>

typedef int8_t  I1;
typedef int16_t I2;
typedef int32_t I4;
typedef int64_t I8;

typedef uint8_t  U1;
typedef uint16_t U2;
typedef uint32_t U4;
typedef uint64_t U8;

typedef std::string t_str;

t_str get_key(U8 u8_num) {
  char buffer[32];
  snprintf(buffer, sizeof(buffer), "%012zd", u8_num);
  return buffer;
}

t_str get_val(U8 i) {
  static std::string str = [] {
    std::string s =
        "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890";
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    s += s;
    std::shuffle(s.begin(), s.end(), std::mt19937_64());
    return s;
  }();
  size_t pos = (i * 11) % str.size();
  size_t size = std::min(str.size() - pos, (i * 13) % 64);
  std::string value = get_key(i);
  value.append("#");
  value.append(str.data() + pos, str.data() + pos + size);
  value.append("#");
  return value;
}