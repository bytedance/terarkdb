//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>

#include "monitoring/instrumented_mutex.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/terark_namespace.h"
#include "rocksdb/trace_reader_writer.h"
#include "util/trace_replay.h"

namespace TERARKDB_NAMESPACE {

extern const uint64_t kMicrosInSecond;

// Lookup context for tracing block cache accesses.
// We trace block accesses at five places:
// 1. BlockBasedTable::GetFilter
// 2. BlockBasedTable::GetUncompressedDict.
// 3. BlockBasedTable::MaybeReadAndLoadToCache. (To trace access on data, index,
//    and range deletion block.)
// 4. BlockBasedTable::Get. (To trace the referenced key and whether the
//    referenced key exists in a fetched data block.)
// 5. BlockBasedTable::MultiGet. (To trace the referenced key and whether the
//    referenced key exists in a fetched data block.)
// The context is created at:
// 1. BlockBasedTable::Get. (kUserGet)
// 2. BlockBasedTable::MultiGet. (kUserMGet)
// 3. BlockBasedTable::NewIterator. (either kUserIterator, kCompaction, or
//    external SST ingestion calls this function.)
// 4. BlockBasedTable::Open. (kPrefetch)
// 5. Index/Filter::CacheDependencies. (kPrefetch)
// 6. BlockBasedTable::ApproximateOffsetOf. (kCompaction or
// kUserApproximateSize).
struct BlockCacheLookupContext {
  BlockCacheLookupContext(const TableReaderCaller& _caller) : caller(_caller) {}
  const TableReaderCaller caller;
  // These are populated when we perform lookup/insert on block cache. The block
  // cache tracer uses these inforation when logging the block access at
  // BlockBasedTable::GET and BlockBasedTable::MultiGet.
  bool is_cache_hit = false;
  bool no_insert = false;
  TraceType block_type = TraceType::kTraceMax;
  uint64_t block_size = 0;
  std::string block_key;
  uint64_t num_keys_in_block = 0;

  void FillLookupContext(bool _is_cache_hit, bool _no_insert,
                         TraceType _block_type, uint64_t _block_size,
                         const std::string& _block_key,
                         uint64_t _num_keys_in_block) {
    is_cache_hit = _is_cache_hit;
    no_insert = _no_insert;
    block_type = _block_type;
    block_size = _block_size;
    block_key = _block_key;
    num_keys_in_block = _num_keys_in_block;
  }
};

enum Boolean : char { kTrue = 1, kFalse = 0 };

struct BlockCacheTraceRecord {
  // Required fields for all accesses.
  uint64_t access_timestamp = 0;
  std::string block_key;
  TraceType block_type = TraceType::kTraceMax;
  uint64_t block_size = 0;
  uint64_t cf_id = 0;
  std::string cf_name;
  uint32_t level = 0;
  uint64_t sst_fd_number = 0;
  TableReaderCaller caller = TableReaderCaller::kMaxBlockCacheLookupCaller;
  Boolean is_cache_hit = Boolean::kFalse;
  Boolean no_insert = Boolean::kFalse;

  // Required fields for data block and user Get/Multi-Get only.
  std::string referenced_key;
  uint64_t referenced_data_size = 0;
  uint64_t num_keys_in_block = 0;
  Boolean referenced_key_exist_in_block = Boolean::kFalse;

  BlockCacheTraceRecord() {}

  BlockCacheTraceRecord(uint64_t _access_timestamp, std::string _block_key,
                        TraceType _block_type, uint64_t _block_size,
                        uint64_t _cf_id, std::string _cf_name, uint32_t _level,
                        uint64_t _sst_fd_number, TableReaderCaller _caller,
                        bool _is_cache_hit, bool _no_insert,
                        std::string _referenced_key = "",
                        uint64_t _referenced_data_size = 0,
                        uint64_t _num_keys_in_block = 0,
                        bool _referenced_key_exist_in_block = false)
      : access_timestamp(_access_timestamp),
        block_key(_block_key),
        block_type(_block_type),
        block_size(_block_size),
        cf_id(_cf_id),
        cf_name(_cf_name),
        level(_level),
        sst_fd_number(_sst_fd_number),
        caller(_caller),
        is_cache_hit(_is_cache_hit ? Boolean::kTrue : Boolean::kFalse),
        no_insert(_no_insert ? Boolean::kTrue : Boolean::kFalse),
        referenced_key(_referenced_key),
        referenced_data_size(_referenced_data_size),
        num_keys_in_block(_num_keys_in_block),
        referenced_key_exist_in_block(
            _referenced_key_exist_in_block ? Boolean::kTrue : Boolean::kFalse) {
  }
};

struct BlockCacheTraceHeader {
  uint64_t start_time;
  uint32_t rocksdb_major_version;
  uint32_t rocksdb_minor_version;
};

class BlockCacheTraceHelper {
 public:
  static bool ShouldTraceReferencedKey(TraceType block_type,
                                       TableReaderCaller caller);

  static const std::string kUnknownColumnFamilyName;
};

// BlockCacheTraceWriter captures all RocksDB block cache accesses using a
// user-provided TraceWriter. Every RocksDB operation is written as a single
// trace. Each trace will have a timestamp and type, followed by the trace
// payload.
class BlockCacheTraceWriter {
 public:
  BlockCacheTraceWriter(Env* env, const TraceOptions& trace_options,
                        std::unique_ptr<TraceWriter>&& trace_writer);
  ~BlockCacheTraceWriter() = default;
  // No copy and move.
  BlockCacheTraceWriter(const BlockCacheTraceWriter&) = delete;
  BlockCacheTraceWriter& operator=(const BlockCacheTraceWriter&) = delete;
  BlockCacheTraceWriter(BlockCacheTraceWriter&&) = delete;
  BlockCacheTraceWriter& operator=(BlockCacheTraceWriter&&) = delete;

  // Pass Slice references to avoid copy.
  Status WriteBlockAccess(const BlockCacheTraceRecord& record,
                          const Slice& block_key, const Slice& cf_name,
                          const Slice& referenced_key);

  // Write a trace header at the beginning, typically on initiating a trace,
  // with some metadata like a magic number and RocksDB version.
  Status WriteHeader();

 private:
  Env* env_;
  TraceOptions trace_options_;
  std::unique_ptr<TraceWriter> trace_writer_;
};

// BlockCacheTraceReader helps read the trace file generated by
// BlockCacheTraceWriter using a user provided TraceReader.
class BlockCacheTraceReader {
 public:
  BlockCacheTraceReader(std::unique_ptr<TraceReader>&& reader);
  ~BlockCacheTraceReader() = default;
  // No copy and move.
  BlockCacheTraceReader(const BlockCacheTraceReader&) = delete;
  BlockCacheTraceReader& operator=(const BlockCacheTraceReader&) = delete;
  BlockCacheTraceReader(BlockCacheTraceReader&&) = delete;
  BlockCacheTraceReader& operator=(BlockCacheTraceReader&&) = delete;

  Status ReadHeader(BlockCacheTraceHeader* header);

  Status ReadAccess(BlockCacheTraceRecord* record);

 private:
  std::unique_ptr<TraceReader> trace_reader_;
};

// A block cache tracer. It downsamples the accesses according to
// trace_options and uses BlockCacheTraceWriter to write the access record to
// the trace file.
class BlockCacheTracer {
 public:
  BlockCacheTracer();
  ~BlockCacheTracer();
  // No copy and move.
  BlockCacheTracer(const BlockCacheTracer&) = delete;
  BlockCacheTracer& operator=(const BlockCacheTracer&) = delete;
  BlockCacheTracer(BlockCacheTracer&&) = delete;
  BlockCacheTracer& operator=(BlockCacheTracer&&) = delete;

  // Start writing block cache accesses to the trace_writer.
  Status StartTrace(Env* env, const TraceOptions& trace_options,
                    std::unique_ptr<TraceWriter>&& trace_writer);

  // Stop writing block cache accesses to the trace_writer.
  void EndTrace();

  bool is_tracing_enabled() const {
    return writer_.load(std::memory_order_relaxed);
  }

  Status WriteBlockAccess(const BlockCacheTraceRecord& record,
                          const Slice& block_key, const Slice& cf_name,
                          const Slice& referenced_key);

 private:
  TraceOptions trace_options_;
  // A mutex protects the writer_.
  InstrumentedMutex trace_writer_mutex_;
  std::atomic<BlockCacheTraceWriter*> writer_;
};

}  // namespace TERARKDB_NAMESPACE
