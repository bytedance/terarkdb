//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines a collection of statistics collectors.
#pragma once

#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "terarkdb/table_properties.h"
#include "terarkdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

class Env;
class TtlExtractorFactory;

// Base class for internal table properties collector.
class IntTblPropCollector {
 public:
  virtual ~IntTblPropCollector() {}
  virtual Status Finish(UserCollectedProperties* properties) = 0;

  virtual const char* Name() const = 0;

  // @params key    the user key that is inserted into the table.
  // @params value  the value that is inserted into the table.
  virtual Status InternalAdd(const Slice& key, const Slice& value,
                             uint64_t file_size) = 0;

  virtual UserCollectedProperties GetReadableProperties() const = 0;

  virtual bool NeedCompact() const { return false; }
};

// Factory for internal table properties collector.
class IntTblPropCollectorFactory {
 public:
  virtual ~IntTblPropCollectorFactory() {}
  // has to be thread-safe
  virtual IntTblPropCollector* CreateIntTblPropCollector(
      const TablePropertiesCollectorFactory::Context&) = 0;

  // The name of the properties collector can be used for debugging purpose.
  virtual const char* Name() const = 0;

  virtual bool NeedSerialize() const { return false; }
  virtual Status Serialize(
      std::string*, const TablePropertiesCollectorFactory::Context&) const {
    return Status::NotSupported("Serialize()", this->Name());
  }
  virtual Status Deserialize(Slice) {
    return Status::NotSupported("Deserialize()", this->Name());
  }
};

// Collecting the statistics for internal keys. Visible only by internal
// rocksdb modules.
class InternalKeyPropertiesCollector : public IntTblPropCollector {
 public:
  virtual Status InternalAdd(const Slice& key, const Slice& value,
                             uint64_t file_size) override;

  virtual Status Finish(UserCollectedProperties* properties) override;

  virtual const char* Name() const override {
    return "InternalKeyPropertiesCollector";
  }

  UserCollectedProperties GetReadableProperties() const override;

 private:
  uint64_t deleted_keys_ = 0;
  uint64_t merge_operands_ = 0;
};

class InternalKeyPropertiesCollectorFactory
    : public IntTblPropCollectorFactory {
 public:
  virtual IntTblPropCollector* CreateIntTblPropCollector(
      const TablePropertiesCollectorFactory::Context&) override {
    return new InternalKeyPropertiesCollector();
  }

  virtual const char* Name() const override {
    return "InternalKeyPropertiesCollectorFactory";
  }
};

// When rocksdb creates a new table, it will encode all "user keys" into
// "internal keys", which contains meta information of a given entry.
//
// This class extracts user key from the encoded internal key when Add() is
// invoked.
class UserKeyTablePropertiesCollector : public IntTblPropCollector {
 public:
  // transfer of ownership
  explicit UserKeyTablePropertiesCollector(TablePropertiesCollector* collector)
      : collector_(collector) {}

  virtual ~UserKeyTablePropertiesCollector() {}

  virtual Status InternalAdd(const Slice& key, const Slice& value,
                             uint64_t file_size) override;

  virtual Status Finish(UserCollectedProperties* properties) override;

  virtual const char* Name() const override { return collector_->Name(); }

  UserCollectedProperties GetReadableProperties() const override;

  virtual bool NeedCompact() const override {
    return collector_->NeedCompact();
  }

 protected:
  std::unique_ptr<TablePropertiesCollector> collector_;
};

class UserKeyTablePropertiesCollectorFactory
    : public IntTblPropCollectorFactory {
 public:
  explicit UserKeyTablePropertiesCollectorFactory(
      std::shared_ptr<TablePropertiesCollectorFactory> user_collector_factory)
      : user_collector_factory_(user_collector_factory) {}
  virtual IntTblPropCollector* CreateIntTblPropCollector(
      const TablePropertiesCollectorFactory::Context& context) override {
    return new UserKeyTablePropertiesCollector(
        user_collector_factory_->CreateTablePropertiesCollector(context));
  }

  virtual const char* Name() const override {
    return user_collector_factory_->Name();
  }

  bool NeedSerialize() const override {
    return user_collector_factory_->NeedSerialize();
  }
  Status Serialize(
      std::string* bytes,
      const TablePropertiesCollectorFactory::Context& ctx) const override {
    return user_collector_factory_->Serialize(bytes, ctx);
  }
  Status Deserialize(Slice bytes) override {
    return user_collector_factory_->Deserialize(bytes);
  }

 private:
  std::shared_ptr<TablePropertiesCollectorFactory> user_collector_factory_;
};

extern IntTblPropCollectorFactory* NewTtlIntTblPropCollectorFactory(
    const TtlExtractorFactory* ttl_extractor_factory, double ttl_gc_ratio,
    size_t ttl_max_scan_cap);

}  // namespace TERARKDB_NAMESPACE
