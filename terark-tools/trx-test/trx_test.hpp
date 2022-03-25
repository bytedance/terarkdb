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

#include <terarkdb/compaction_filter.h>
#include <terarkdb/convenience.h>
#include <terarkdb/db.h>
#include <terarkdb/experimental.h>
#include <terarkdb/memtablerep.h>
#include <terarkdb/merge_operator.h>
#include <terarkdb/slice.h>
#include <terarkdb/slice_transform.h>
#include <terarkdb/sst_file_writer.h>
#include <terarkdb/table.h>
#include <terarkdb/utilities/optimistic_transaction_db.h>
#include <terarkdb/utilities/transaction_db.h>
#include <terarkdb/utilities/write_batch_with_index.h>
#include <table/get_context.h>
#include <table/iterator_wrapper.h>
#include <table/table_builder.h>
#include <table/table_reader.h>
#include <util/coding.h>
#include <util/filename.h>
#include <util/gflags_compat.h>
#include <utilities/merge_operators.h>

DEFINE_bool(open_and_seek,false,"open and seek");