//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/iterator.h"
#include "rocksdb/status.h"
#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

class BlockHandle;

// Seek to the properties block.
// If it successfully seeks to the properties block, "is_found" will be
// set to true.
Status SeekToPropertiesBlock(InternalIteratorBase<Slice>* meta_iter,
                             bool* is_found);

// Seek to the compression dictionary block.
// If it successfully seeks to the properties block, "is_found" will be
// set to true.
Status SeekToCompressionDictBlock(InternalIteratorBase<Slice>* meta_iter,
                                  bool* is_found, BlockHandle* block_handle);

// TODO(andrewkr) should not put all meta block in table_properties.h/cc
Status SeekToRangeDelBlock(InternalIteratorBase<Slice>* meta_iter,
                           bool* is_found, BlockHandle* block_handle);

}  // namespace TERARKDB_NAMESPACE
