//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/table_properties_collector.h"

#include "db/dbformat.h"
#include "monitoring/histogram.h"
#include "rocksdb/terark_namespace.h"
#include "rocksdb/ttl_extractor.h"
#include "util/coding.h"
#include "util/string_util.h"
#include "utilities/util/factory.h"

namespace TERARKDB_NAMESPACE {

const uint64_t kFiftyYearSecondsNumber = 1576800000;
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
 private:
  TtlExtractor* ttl_extractor_;
  double ttl_gc_ratio_;
  size_t ttl_max_scan_cap_;
  HistogramImpl histogram_;
  std::vector<uint64_t> ttl_seconds_slice_window_;
  std::deque<size_t> slice_window_ttl_index_;
  size_t slice_index_ = 0;
  std::string name_;
  uint64_t raw_key_value_size_ = 0;
  uint64_t ttl_key_value_size_ = 0;

  uint64_t min_scan_cap_ttl_seconds_ = std::numeric_limits<uint64_t>::max();
  uint64_t min_gc_ratio_ttl_seconds_ = std::numeric_limits<uint64_t>::max();
  void AddTtlToSliceWindow(uint64_t ttl) {
    if (slice_index_ < ttl_max_scan_cap_) {
      while (!slice_window_ttl_index_.empty() &&
             ttl >= ttl_seconds_slice_window_[slice_window_ttl_index_.back()]) {
        slice_window_ttl_index_.pop_back();
      }
      ttl_seconds_slice_window_.emplace_back(ttl);
    } else {
      assert(ttl_seconds_slice_window_.size() == ttl_max_scan_cap_);
      while (!slice_window_ttl_index_.empty() &&
             slice_window_ttl_index_.front() <=
                 slice_index_ - ttl_max_scan_cap_) {
        slice_window_ttl_index_.pop_front();
      }
      while (!slice_window_ttl_index_.empty() &&
             ttl >= ttl_seconds_slice_window_[slice_window_ttl_index_.back() %
                                              ttl_max_scan_cap_]) {
        slice_window_ttl_index_.pop_back();
      }
      ttl_seconds_slice_window_[slice_index_ % ttl_max_scan_cap_] = ttl;
    }
    slice_window_ttl_index_.push_back(slice_index_);
    slice_index_++;
    if (slice_index_ >= ttl_max_scan_cap_) {
      min_scan_cap_ttl_seconds_ =
          std::min(min_scan_cap_ttl_seconds_,
                   ttl_seconds_slice_window_[slice_window_ttl_index_.front() %
                                             ttl_max_scan_cap_]);
    }
  }

 public:
  TtlIntTblPropCollector(TtlExtractor* _ttl_extractor, double _ttl_gc_ratio,
                         size_t _ttl_max_scan_cap, const std::string& _name)
      : ttl_extractor_(_ttl_extractor),
        ttl_gc_ratio_(_ttl_gc_ratio),
        ttl_max_scan_cap_(_ttl_max_scan_cap),
        name_(_name) {}
  ~TtlIntTblPropCollector() { delete ttl_extractor_; }
  Status Finish(UserCollectedProperties* properties) override {
    // uint64_t max_uint64_t = std::numeric_limits<uint64_t>::max();
    if (!histogram_.Empty() &&
        ttl_key_value_size_ >= ttl_gc_ratio_ * raw_key_value_size_) {
      min_gc_ratio_ttl_seconds_ =
          static_cast<uint64_t>(histogram_.Percentile(ttl_gc_ratio_ * 100.0));
    }
    std::string temp_ttl_str[2];
    PutFixed64(&temp_ttl_str[0], min_gc_ratio_ttl_seconds_);
    PutFixed64(&temp_ttl_str[1], min_gc_ratio_ttl_seconds_);
    properties->insert(
        {TablePropertiesNames::kEarliestTimeBeginCompact, temp_ttl_str[0]});
    properties->insert(
        {TablePropertiesNames::kLatestTimeEndCompact, temp_ttl_str[1]});
  }

  const char* Name() const override { return name_.c_str(); }

  // @params key    the user key that is inserted into the table.
  // @params value  the value that is inserted into the table.
  Status InternalAdd(const Slice& key, const Slice& value,
                     uint64_t file_size) override {
    raw_key_value_size_ += key.size() + value.size();
    EntryType entry_type = GetEntryType(ExtractValueType(key));
    if (entry_type == kEntryMerge || entry_type == kEntryPut ||
        entry_type == kEntryMergeIndex || entry_type == kEntryValueIndex) {
      bool has_ttl = false;
      std::chrono::seconds ttl(0);
      assert(ttl_extractor_ != nullptr);

      Slice user_key = ExtractUserKey(key);
      Slice value_or_meta = value;
      if (entry_type == kEntryMergeIndex || entry_type == kEntryValueIndex) {
        value_or_meta = SeparateHelper::DecodeValueMeta(value);
      }
      Status s = ttl_extractor_->Extract(entry_type, user_key, value_or_meta,
                                         &has_ttl, &ttl);
      if (!s.ok()) {
        return s;
      }
      if (has_ttl) {
        ttl_key_value_size_ += key.size() + value.size();
        uint64_t key_ttl =
            std::min(static_cast<uint64_t>(ttl.count()),
                     kFiftyYearSecondsNumber);  // ttl is limited to 50 years.
        histogram_.Add(key_ttl);
        AddTtlToSliceWindow(key_ttl);
      } else {
        ttl_seconds_slice_window_.clear();
        slice_window_ttl_index_.clear();
        slice_index_ = 0;
      }
    } else if (entry_type < kEntryOther) {
      // Delete Key is always not found for scan operation.
      AddTtlToSliceWindow(0ul);
    }
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
    size_t ttl_max_scan_cap) {}

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

TERARK_FACTORY_INSTANTIATE_GNS(
    TERARKDB_NAMESPACE::TablePropertiesCollectorFactory*);
