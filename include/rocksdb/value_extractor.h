// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Class for specifying user-defined functions which perform a
// transformation on a slice.  It is not required that every slice
// belong to the domain and/or range of a function.  Subclasses should
// define InDomain and InRange to determine which slices are in either
// of these sets respectively.

#pragma once

#include <string>
#include <terark/util/factory.hpp>

namespace rocksdb {

class Slice;
class Status;

struct ValueExtractorContext {
  // Which column family this compaction is for.
  uint32_t column_family_id;
};

class ValueExtractor {
 public:
  virtual ~ValueExtractor(){};

  // Extract a custom info from a specified key value pair. This method is
  // called when a value will trans to separate.
  virtual Status Extract(const Slice& key, const Slice& value,
                         std::string* output) const = 0;
};

class ValueExtractorFactory
    : public terark::Factoryable<ValueExtractorFactory*, Slice> {
 public:
  virtual ~ValueExtractorFactory() {}

  using Context = ValueExtractorContext;

  virtual std::unique_ptr<ValueExtractor> CreateValueExtractor(
      const Context& context) = 0;

  // Returns a name that identifies this value extractor factory.
  virtual const char* Name() const = 0;

  virtual Status Serialize(std::string* /*bytes*/) const {
    return Status::NotSupported();
  }
};

}  // namespace rocksdb
