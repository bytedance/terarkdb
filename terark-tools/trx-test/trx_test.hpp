#include <sys/epoll.h>

#include <cctype>
#include <chrono>
#include <cinttypes>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <memory>
#include <thread>
#include <utility>
#include <vector>
#define GUJIA_HAS_EPOLL
namespace gujia {
typedef struct epoll_event Event;
}

#include <rocksdb/compaction_filter.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/experimental.h>
#include <rocksdb/memtablerep.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <table/get_context.h>
#include <table/iterator_wrapper.h>
#include <table/table_builder.h>
#include <table/table_reader.h>
#include <util/coding.h>
#include <util/filename.h>
#include <util/gflags_compat.h>
#include <utilities/merge_operators.h>

DEFINE_bool(open_and_seek,false,"open and seek");