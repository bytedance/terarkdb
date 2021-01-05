//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/table_properties_collector.h"

#include "db/dbformat.h"
#include "monitoring/histogram.h"
#include "rocksdb/ttl_extractor.h"
#include "util/coding.h"
#include "util/string_util.h"
#include "utilities/util/factory.h"

#include "rocksdb/terark_namespace.h"
namespace TERARKDB_NAMESPACE {
namespace {

uint64_t GetUint64Property(const UserCollectedProperties& props,
                           const std::string& property_name,
                           bool* property_present) {
  auto pos = props.find(property_name);
  if (pos == props.end()) {
    *property_present = false;
    return 0;
  }
  Slice raw = pos->second;
  uint64_t val = 0;
  *property_present = true;
  return GetVarint64(&raw, &val) ? val : 0;
}

}  // namespace

Status UserKeyTablePropertiesCollector::InternalAdd(const Slice& key,
                                                    const Slice& value,
                                                    uint64_t file_size) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(key, &ikey)) {
    return Status::InvalidArgument("Invalid internal key");
  }

  return collector_->AddUserKey(ikey.user_key, value, GetEntryType(ikey.type),
                                ikey.sequence, file_size);
}

Status UserKeyTablePropertiesCollector::Finish(
    UserCollectedProperties* properties) {
  return collector_->Finish(properties);
}

UserCollectedProperties UserKeyTablePropertiesCollector::GetReadableProperties()
    const {
  return collector_->GetReadableProperties();
}

class TtlIntTblPropCollector : public IntTblPropCollector {
  TtlExtractor* ttl_extractor_;
  double ttl_gc_ratio_;
  size_t ttl_max_scan_cap_;
  HistogramImpl histogram_;
  std::string name_;

 public:
  TtlIntTblPropCollector(TtlExtractor* _ttl_extractor, double _ttl_gc_ratio,
                         size_t _ttl_max_scan_cap, const std::string& _name)
      : ttl_extractor_(_ttl_extractor),
        ttl_gc_ratio_(_ttl_gc_ratio),
        ttl_max_scan_cap_(_ttl_max_scan_cap),
        name_(_name) {}
  ~TtlIntTblPropCollector() { delete ttl_extractor_; }
  Status Finish(UserCollectedProperties* properties) override {
    // do nothing
  }

  const char* Name() const override { return name_.c_str(); }

  // @params key    the user key that is inserted into the table.
  // @params value  the value that is inserted into the table.
  Status InternalAdd(const Slice& key, const Slice& value,
                     uint64_t file_size) override {
#error TODO
    return Status::OK();
  }

  UserCollectedProperties GetReadableProperties() const override {
    return UserCollectedProperties();
  }
};

// Factory for internal table properties collector.
class TtlIntTblPropCollectorFactory : public IntTblPropCollectorFactory {
  TtlExtractorFactory* ttl_extractor_factory_;
  double ttl_gc_ratio_;
  size_t ttl_max_scan_cap_;
  std::string name_;

 public:
  TtlIntTblPropCollectorFactory(TtlExtractorFactory* _ttl_extractor_factory,
                                double _ttl_gc_ratio, size_t _ttl_max_scan_cap)
      : ttl_extractor_factory_(_ttl_extractor_factory),
        ttl_gc_ratio_(_ttl_gc_ratio),
        ttl_max_scan_cap_(_ttl_max_scan_cap) {
    name_ =
        std::string("TtlCollectorFactory.") + ttl_extractor_factory_->Name();
  }
  // has to be thread-safe
  IntTblPropCollector* CreateIntTblPropCollector(
      const TablePropertiesCollectorFactory::Context& context) override {
    TtlExtractorContext ttl_context;
    ttl_context.column_family_id = context.column_family_id;
    auto ttl_extractor =
        ttl_extractor_factory_->CreateTtlExtractor(ttl_context);
    return new TtlIntTblPropCollector(
        ttl_extractor.release(), ttl_gc_ratio_, ttl_max_scan_cap_,
        std::string("TtlCollector.") + ttl_extractor_factory_->Name());
  }

  // The name of the properties collector can be used for debugging purpose.
  const char* Name() const override { name_.c_str(); }

  bool NeedSerialize() const override { return false; }
};

IntTblPropCollectorFactory* NewTtlIntTblPropCollectorFactory(
    TtlExtractorFactory* ttl_extractor_factory,
    const TtlExtractorContext& context, double ttl_gc_ratio,
    size_t ttl_max_scan_cap);

uint64_t GetDeletedKeys(const UserCollectedProperties& props) {
  bool property_present_ignored;
  return GetUint64Property(props, TablePropertiesNames::kDeletedKeys,
                           &property_present_ignored);
}

uint64_t GetMergeOperands(const UserCollectedProperties& props,
                          bool* property_present) {
  return GetUint64Property(props, TablePropertiesNames::kMergeOperands,
                           property_present);
}

}  // namespace TERARKDB_NAMESPACE

TERARK_FACTORY_INSTANTIATE_GNS(TERARKDB_NAMESPACE::TablePropertiesCollectorFactory*);
