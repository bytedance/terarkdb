//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/compaction_worker.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif


namespace rocksdb {

std::packaged_task<CompactionWorkerResult()>
RemoteCompactionWorker::StartCompaction(
    VersionStorageInfo* input_version,
    const ImmutableCFOptions& immutable_cf_options,
    const MutableCFOptions& mutable_cf_options,
    std::vector<std::pair<int, const FileMetaData*>> inputs,
    uint64_t target_file_size, CompressionType compression,
    CompressionOptions compression_opts, const Slice* start, const Slice* end) {
  std::string data;
  // TODO encode all params into data
  struct ResultDecoder {
    std::future<std::string> future;
    CompactionWorkerResult operator()() {
      CompactionWorkerResult decoded_result;
      std::string encoded_result = future.get();
      // TODO decode result;
      return decoded_result;
    }
  };
  ResultDecoder decoder;
  decoder.future = DoCompaction(data);
  return std::packaged_task<CompactionWorkerResult()>(std::move(decoder));
}

struct RemoteCompactionWorker::Client::Rep {

};

RemoteCompactionWorker::Client::Client() {
  rep_ = new Rep();
}

RemoteCompactionWorker::Client::~Client() {
  delete rep_;
}

std::string RemoteCompactionWorker::Client::DoCompaction(
    const std::string& data) {
  // TODO decode data to params
}

}  // namespace rocksdb
