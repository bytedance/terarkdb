
#include "terark_zip_table_builder.h"

#include <zstd/zstd.h>

#include <boost/range/algorithm.hpp>
#include <cfloat>
#include <exception>
#include <future>
#include <terark/io/MemStream.hpp>
#include <terark/lcast.hpp>
#include <terark/num_to_str.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/zbs/blob_store_file_header.hpp>
#include <terark/zbs/entropy_zip_blob_store.hpp>
#include <terark/zbs/mixed_len_blob_store.hpp>
#include <terark/zbs/plain_blob_store.hpp>
#include <terark/zbs/zero_length_blob_store.hpp>
#include <terark/zbs/zip_offset_blob_store.hpp>

#include "db/version_edit.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/terark_namespace.h"
#include "table/meta_blocks.h"
#include "table/terark_zip_common.h"
#include "util/async_task.h"
#include "util/c_style_callback.h"
#include "util/string_util.h"
#include "util/xxhash.h"

namespace TERARKDB_NAMESPACE {

using namespace terark;

std::mutex g_sumMutex;
size_t g_sumKeyLen = 0;
size_t g_sumValueLen = 0;
size_t g_sumUserKeyLen = 0;
size_t g_sumUserKeyNum = 0;
size_t g_sumEntryNum = 0;
long long g_lastTime = g_pf.now();

// wait
namespace {
struct PendingTask {
  const TerarkZipTableBuilder* tztb;
  long long startTime;
};
}  // namespace
static std::mutex zipMutex;
static std::condition_variable zipCond;
static valvec<PendingTask> waitQueue;
static size_t sumWaitingMem = 0;
static size_t sumWorkingMem = 0;

template <class ByteArray>
static Status WriteBlock(const ByteArray& blockData, WritableFileWriter* file,
                         uint64_t* offset, BlockHandle* block_handle) {
  block_handle->set_offset(*offset);
  block_handle->set_size(blockData.size());
  Status s = file->Append(SliceOf(blockData));
  if (s.ok()) {
    *offset += blockData.size();
  }
  return s;
}

static std::string GetTimestamp() {
  using namespace std::chrono;
  uint64_t timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count();
  return terark::lcast(timestamp);
}

///////////////////////////////////////////////////////////////////
// hack MyRocks Rdb_tbl_prop_coll
TERARKDB_NAMESPACE::Status MyRocksTablePropertiesCollectorHack(
    IntTblPropCollector* collector, const TerarkZipMultiOffsetInfo& offset_info,
    UserCollectedProperties& user_collected_properties,
    bool is_reverse_bytewise_order) {
  if (fstring(collector->Name()) != "Rdb_tbl_prop_coll") {
    return Status::OK();
  }
  TERARK_UNUSED_VAR(offset_info);
  TERARK_UNUSED_VAR(user_collected_properties);
  TERARK_UNUSED_VAR(is_reverse_bytewise_order);
#if 0
  // TODO
  if (offset_info.prefixLen_ != 4) { // prefix mismatch, can't hack ...
    return Status::OK();
  }
  auto find = user_collected_properties.find("__indexstats__");
  assert(find != user_collected_properties.end());
  if (find == user_collected_properties.end()) {
    return Status::OK();
  }
  std::string Rdb_index_stats = find->second;
  terark::BigEndianDataInput<terark::MemIO> input;
  terark::BigEndianDataOutput<terark::MemIO> output;
  input.set((void*)Rdb_index_stats.data(), Rdb_index_stats.size());
  uint16_t version = 0;
  input >> version;     // version
  assert(version >= 1); // INDEX_STATS_VERSION_INITIAL
  assert(version <= 2); // INDEX_STATS_VERSION_ENTRY_TYPES
  if (version < 1 || version > 2) {
    return Status::Corruption("Rdb_tbl_prop_coll hack fail",
                              "Unsupported version");
  }
  auto getActualSize = [&offset_info](size_t i) -> uint64_t {
    auto& info = offset_info.offset_[i];
    if (i == 0)
      return info.key + info.value;
    auto& last = offset_info.offset_[i - 1];
    return info.key + info.value - last.key - last.value;
  };
  for (size_t i = 0; i < offset_info.offset_.size(); ++i) {
    size_t ii = is_reverse_bytewise_order ? offset_info.partCount_ - i - 1 : i;
    input.skip(4);                  // cf_id
    uint32_t index_id;              // index_id
    input.ensureRead(&index_id, 4);
    fstring prefix = fstring(offset_info.prefixSet_).substr(ii * 4, 4);
    if (fstring((char*)&index_id, 4) != prefix) {
      return Status::Corruption("Rdb_tbl_prop_coll hack fail",
                                "Mismatch index_id or prefix");
    }
    input.skip(8 * 2);              // data_size, rows
    output.set(input.current(), 8); // actual_disk_size addr
    output << getActualSize(ii);
    input.skip(8);                  // actual_disk_size
    uint64_t distinct_keys_per_prefix_size = 0;
    input >> distinct_keys_per_prefix_size;
    if (version >= 2) {
      // INDEX_STATS_VERSION_ENTRY_TYPES
      // entry_deletes
      // entry_single_deletes
      // entry_merges
      // entry_others
      input.skip(8 * 4);
    }
    // distinct_keys_per_prefix
    input.skip(distinct_keys_per_prefix_size * 8);
  }
  find->second = Rdb_index_stats;
  assert(input.current() == input.end());
#endif
  return Status::OK();
}
///////////////////////////////////////////////////////////////////

extern void TerarkZipConfigCompactionWorkerFromEnv(TerarkZipTableOptions&);

TerarkZipTableBuilder::TerarkZipTableBuilder(
    const TerarkZipTableFactory* table_factory,
    const TerarkZipTableOptions& tzto, const TableBuilderOptions& tbo,
    uint32_t column_family_id, WritableFileWriter* file, size_t key_prefixLen)
    : table_options_(tzto),
      table_factory_(table_factory),
      ioptions_(tbo.ioptions),
      range_del_block_(1),
      prefixLen_(key_prefixLen),
      compaction_load_(0) {
  tiopt_.debugLevel = table_options_.debugLevel;
  tiopt_.indexNestLevel = table_options_.indexNestLevel;
  tiopt_.indexNestScale = table_options_.indexNestScale;
  tiopt_.indexTempLevel = table_options_.indexTempLevel;
  tiopt_.indexType = table_options_.indexType;
  tiopt_.localTempDir = table_options_.localTempDir;
  tiopt_.smallTaskMemory = table_options_.smallTaskMemory;
  tiopt_.compressGlobalDict = !table_options_.disableCompressDict;
  tiopt_.cbtHashBits = tbo.skip_filters ? 0 : table_options_.cbtHashBits;
  tiopt_.cbtEntryPerTrie = table_options_.cbtEntryPerTrie;
  tiopt_.cbtMinKeySize = table_options_.cbtMinKeySize;
  tiopt_.cbtMinKeyRatio = table_options_.cbtMinKeyRatio;
  try {
    if (IsCompactionWorkerNode()) {
      TerarkZipConfigCompactionWorkerFromEnv(table_options_);
    }
    singleIndexMaxSize_ = std::min(table_options_.softZipWorkingMemLimit,
                                   table_options_.singleIndexMaxSize);
    level_ = tbo.level;
    if (tbo.compaction_load > 0) {
      double load =
          tbo.compaction_load * tbo.ioptions.num_levels -
          (tbo.level + tbo.ioptions.num_levels) % tbo.ioptions.num_levels;
      if (load > 0) {
        compaction_load_ = std::min(1., load);
      }
    } else if (tbo.compaction_load < 0) {
      compaction_load_ = -tbo.compaction_load;
    }

    estimateRatio_ = table_factory_->GetCollect().estimate();

    properties_.fixed_key_len = 0;
    properties_.num_data_blocks = 1;
    properties_.column_family_id = column_family_id;
    properties_.column_family_name = tbo.column_family_name;
    properties_.comparator_name = ioptions_.user_comparator
                                      ? ioptions_.user_comparator->Name()
                                      : "nullptr";
    properties_.merge_operator_name =
        ioptions_.merge_operator ? ioptions_.merge_operator->Name() : "nullptr";
    properties_.value_meta_extractor_name =
        ioptions_.value_meta_extractor_factory != nullptr
            ? ioptions_.value_meta_extractor_factory->Name()
            : "nullptr";
    properties_.compression_name =
        CompressionTypeToString(tbo.compression_type);
    properties_.prefix_extractor_name =
        tbo.moptions.prefix_extractor ? tbo.moptions.prefix_extractor->Name()
                                      : "nullptr";

    isReverseBytewiseOrder_ =
        IsBackwardBytewiseComparator(properties_.comparator_name);

    tbo.PushIntTblPropCollectors(&collectors_,
                                 (uint32_t)properties_.column_family_id);

    std::string property_collectors_names = "[";
    for (size_t i = 0;
         i < ioptions_.table_properties_collector_factories.size(); ++i) {
      if (i != 0) {
        property_collectors_names += ",";
      }
      property_collectors_names +=
          ioptions_.table_properties_collector_factories[i]->Name();
    }
    property_collectors_names += "]";
    properties_.property_collectors_names = property_collectors_names;

    file_ = file;
    sampleUpperBound_ =
        uint64_t(randomGenerator_.max() * table_options_.sampleRatio);
    tmpSentryFile_.path = table_options_.localTempDir + "/Terark-XXXXXX";
    tmpSentryFile_.open_temp();
    tmpSampleFile_.path = tmpSentryFile_.path + ".sample";
    tmpSampleFile_.open();
    tmpIndexFile_.fpath = tmpSentryFile_.path + ".index";
    tmpStoreFile_.fpath = tmpSentryFile_.path + ".bs";
    tmpZipStoreFile_.fpath = tmpSentryFile_.path + ".zbs";
    if (table_options_.debugLevel == 3) {
      tmpDumpFile_.open(tmpSentryFile_.path + ".dump", "wb+");
    }
  } catch (const std::exception& ex) {
    WARN_EXCEPT(tbo.ioptions.info_log, "%s: Exception: %s",
                BOOST_CURRENT_FUNCTION, ex.what());
    throw;
  }
}

DictZipBlobStore::ZipBuilder* TerarkZipTableBuilder::createZipBuilder() const {
  DictZipBlobStore::Options dzopt;
  dzopt.entropyAlgo =
      DictZipBlobStore::Options::EntropyAlgo(table_options_.entropyAlgo);
  dzopt.checksumLevel = table_options_.checksumLevel;
  dzopt.offsetArrayBlockUnits = table_options_.offsetArrayBlockUnits;
  dzopt.useSuffixArrayLocalMatch = table_options_.useSuffixArrayLocalMatch;
  dzopt.enableLake = table_options_.optimizeCpuL3Cache;
  dzopt.compressGlobalDict = !table_options_.disableCompressDict;
  return DictZipBlobStore::createZipBuilder(dzopt);
}

TerarkZipTableBuilder::~TerarkZipTableBuilder() {
  std::unique_lock<std::mutex> zipLock(zipMutex);
  waitQueue.trim(boost::remove_if(waitQueue, TERARK_GET(.tztb) == this));
}

uint64_t TerarkZipTableBuilder::FileSize() const {
  if (offset_ == 0) {
    // for compaction caller to split file by increasing size
    return estimateOffset_;
  } else {
    return offset_;
  }
}

TableProperties TerarkZipTableBuilder::GetTableProperties() const {
  TableProperties ret = properties_;
  if (!closed_) {
    // don't call MyRocksTablePropertiesCollectorHack before Finish()
    return ret;
  }
  for (const auto& collector : collectors_) {
    for (const auto& prop : collector->GetReadableProperties()) {
      ret.readable_properties.insert(prop);
    }
    collector->Finish(&ret.user_collected_properties);
    MyRocksTablePropertiesCollectorHack(collector.get(), offset_info_,
                                        ret.user_collected_properties,
                                        isReverseBytewiseOrder_);
  }
  ret.user_collected_properties.emplace(kTerarkZipTableDictSize,
                                        lcast(dictSize_));
  return ret;
}

TerarkZipTableBuilder::RangeStatus::RangeStatus(fstring key,
                                                size_t globalPrefixLen,
                                                uint64_t _seqType) {
  stat.minKey.assign(key);
  seqType = _seqType;
  prefixVec.push_back(key.substr(0, globalPrefixLen));
}

void TerarkZipTableBuilder::RangeStatus::AddKey(fstring key,
                                                size_t globalPrefixLen,
                                                size_t samePrefix,
                                                size_t valueLen, bool zeroSeq) {
  if (samePrefix == size_t(-1)) {
    stat.maxKey.assign(key);
    samePrefix = 0;
  }
  valueBits.push_back(false);
  size_t prefixSize =
      std::min(key.size(), std::max(samePrefix, prevSamePrefix) + 1);
  size_t suffixSize = key.size() - prefixSize;
  ++stat.keyCount;
  stat.minKeyLen = std::min(key.size(), stat.minKeyLen);
  stat.maxKeyLen = std::max(key.size(), stat.maxKeyLen);
  stat.sumKeyLen += key.size();
  stat.sumValueLen += valueLen;
  stat.sumPrefixLen += prefixSize;
  stat.minPrefixLen = std::min(stat.minPrefixLen, prefixSize);
  stat.maxPrefixLen = std::max(stat.maxPrefixLen, prefixSize);
  stat.minSuffixLen = std::min(stat.minSuffixLen, suffixSize);
  stat.maxSuffixLen = std::max(stat.maxSuffixLen, suffixSize);
  auto& diff = stat.diff;
  if (diff.size() < samePrefix) {
    diff.resize(samePrefix);
  }
  for (size_t i = 0; i < samePrefix; ++i) {
    ++diff[i].cur;
    ++diff[i].cnt;
  }
  for (size_t i = samePrefix; i < diff.size(); ++i) {
    diff[i].max = std::max(diff[i].cur, diff[i].max);
    diff[i].cur = 0;
  }
  prevSamePrefix = samePrefix;
  zeroSeqCount += zeroSeq;
  valueHist[valueLen]++;
  if (prefixVec.back() != key.substr(0, globalPrefixLen)) {
    prefixVec.push_back(key.substr(0, globalPrefixLen));
  }
}

void TerarkZipTableBuilder::RangeStatus::AddValueBit() {
  valueBits.push_back(true);
}

TerarkZipTableBuilder::KeyValueStatus::KeyValueStatus(
    TERARKDB_NAMESPACE::TerarkZipTableBuilder::RangeStatus&& s,
    freq_hist_o1&& f) {
  status = std::move(s);
  valueFreq = std::move(f);
  isFullValue = true;
  for (auto& pair : status.fileVec) {
    if (!pair->isFullValue) {
      isFullValue = false;
    }
  }
  status.valueHist.finish();
  valueFreq.finish();
  valueEntropyLen = freq_hist_o1::estimate_size(valueFreq.histogram());
  assert(status.stat.keyCount == status.valueHist.m_cnt_sum);
}

std::shared_ptr<FilePair> TerarkZipTableBuilder::NewFilePair() {
  auto pair = std::make_shared<FilePair>();
  char buffer[32];
  snprintf(buffer, sizeof buffer, ".key.%06zd", nameSeed_);
  pair->key.path = tmpSentryFile_.path + buffer;
  pair->key.open();
  snprintf(buffer, sizeof buffer, ".value.%06zd", nameSeed_);
  pair->value.path = tmpSentryFile_.path + buffer;
  pair->value.open();
  ++nameSeed_;
  return pair;
};

Status TerarkZipTableBuilder::Add(const Slice& key,
                                  const LazyBuffer& lazy_value) try {
  auto s = lazy_value.fetch();
  if (!s.ok()) {
    return s;
  }
  const Slice& value = lazy_value.slice();
  if (table_options_.debugLevel == 3) {
    TERARKDB_NAMESPACE::ParsedInternalKey ikey;
    TERARKDB_NAMESPACE::ParseInternalKey(key, &ikey);
    fprintf(tmpDumpFile_, "DEBUG: 1st pass => %s / %s \n",
            ikey.DebugString(true).c_str(), value.ToString(true).c_str());
  }
  if (properties_.num_entries > 0 &&
      ioptions_.internal_comparator.Compare(key, prevKey_.Encode()) <= 0) {
    assert(ioptions_.internal_comparator.Compare(key, prevKey_.Encode()) > 0);
    return Status::Corruption("TerarkZipTableBuilder::Add: overlapping key");
  }
  ++properties_.num_entries;
  properties_.raw_key_size += key.size();
  properties_.raw_value_size += value.size();

  uint64_t seqType = DecodeFixed64(key.data() + key.size() - 8);
  ValueType value_type = ValueType(seqType & 255);
  if (value_type == kTypeDeletion || value_type == kTypeSingleDeletion) {
    ++properties_.num_deletions;
  } else if (value_type == kTypeMerge) {
    ++properties_.num_merge_operands;
  }
  assert(key.size() >= 8);
  fstring userKey(key.data(), key.size() - 8);
  assert(userKey.size() >= prefixLen_);
  auto ShouldStartBuild = [&] {
    size_t indexSize = UintVecMin0::compute_mem_size_by_max_val(
        r22_->stat.sumKeyLen, r22_->stat.keyCount);
    size_t indexBuildMemSize = r22_->stat.sumKeyLen + indexSize;
    if (terark_unlikely(indexBuildMemSize > singleIndexMaxSize_)) {
      return true;
    }
    return !MergeRangeStatus(
        r22_.get(), r11_.get(), r21_.get(),
        freq_hist_o1::estimate_size_unfinish(freq_[2]->k, freq_[1]->k));
  };
  fstring prevUserKey =
      prevKey_.size() == 0 ? fstring() : fstringOf(prevKey_.user_key());
  size_t samePrefix = userKey.commonPrefixLen(prevUserKey);
  if (!r00_ || (prevUserKey != userKey &&
                keyDataSize_ > table_options_.singleIndexMinSize)) {
    if (!r00_) {
      assert(prefixBuildInfos_.empty());
      t0 = g_pf.now();
      assert(!r22_ && !r11_ && !r21_ && !r10_ && !r20_);
      freq_[0].reset(new FreqPair);
      freq_[1].reset(new FreqPair);
      freq_[2].reset(new FreqPair);
      r00_.reset(new RangeStatus(userKey, prefixLen_, seqType));  // skip
      r10_.reset(new RangeStatus(userKey, prefixLen_, seqType));  // skip
      r20_.reset(new RangeStatus(userKey, prefixLen_, seqType));  // skip
    } else if (!r11_) {
      assert(prefixBuildInfos_.empty());
      assert(!r22_ && !r21_);
      kv_freq_.add_hist(freq_[0]->k);
      kv_freq_.add_hist(freq_[0]->v);
      freq_[1].swap(freq_[0]);
      r11_.swap(r00_);                                            // add last
      r00_.reset(new RangeStatus(userKey, prefixLen_, seqType));  // skip
      AddPrevUserKey(samePrefix, {r10_.get(), r20_.get()}, {r11_.get()});
    } else if (!r22_) {
      kv_freq_.add_hist(freq_[0]->k);
      kv_freq_.add_hist(freq_[0]->v);
      freq_[2].swap(freq_[1]);
      freq_[1].swap(freq_[0]);
      assert(prefixBuildInfos_.empty());
      r22_.swap(r11_);                                            // ignore
      r11_.reset(new RangeStatus(*r00_));                         // add last
      r21_.swap(r10_);                                            // add last
      r10_.swap(r00_);                                            // add prev
      r00_.reset(new RangeStatus(userKey, prefixLen_, seqType));  // skip
      AddPrevUserKey(samePrefix, {r10_.get(), r20_.get()},
                     {r11_.get(), r21_.get()});
    } else {
      kv_freq_.add_hist(freq_[0]->k);
      kv_freq_.add_hist(freq_[0]->v);
      if (ShouldStartBuild()) {
        auto kvs = new KeyValueStatus(std::move(*r22_), std::move(freq_[2]->v));
        prefixBuildInfos_.emplace_back(kvs);
        BuildIndex(*kvs, freq_hist_o1::estimate_size_unfinish(freq_[2]->k));
        BuildStore(*kvs, nullptr, BuildStoreInit);
        r22_.swap(r11_);  // ignore
        *r21_ = *r10_;    // add last
        r20_.swap(r10_);  // add prev
      } else {
        freq_[1]->k.add_hist(freq_[2]->k);
        freq_[1]->v.add_hist(freq_[2]->v);
        r22_.swap(r21_);  // ignore
        *r21_ = *r20_;    // add last
      }
      freq_[2].swap(freq_[1]);
      freq_[1].swap(freq_[0]);
      freq_[0]->k.clear();
      freq_[0]->v.reset1();
      *r11_ = *r00_;                                              // add last
      r10_.swap(r00_);                                            // add prev
      r00_.reset(new RangeStatus(userKey, prefixLen_, seqType));  // skip
      AddPrevUserKey(samePrefix, {r10_.get(), r20_.get()},
                     {r11_.get(), r21_.get()});
    }
    if (filePair_) {
      filePair_->key.complete_write();
      filePair_->value.complete_write();
    }
    filePair_ = NewFilePair();
    r00_->fileVec.push_back(filePair_);
    r10_->fileVec.push_back(filePair_);
    r20_->fileVec.push_back(filePair_);
    prevSamePrefix_ = 0;
    keyDataSize_ = 0;
    valueDataSize_ = 0;
    keyDataSize_ += userKey.size();
  } else if (prevUserKey != userKey) {
    assert((prevUserKey < userKey) ^ isReverseBytewiseOrder_);
    AddPrevUserKey(samePrefix, {r00_.get(), r10_.get(), r20_.get()}, {});
    keyDataSize_ += userKey.size();
  }
  prevKey_.DecodeFrom(key);
  AddValueBit();
  valueDataSize_ += value.size() + 8;
  valueBuf_.emplace_back((char*)&seqType, 8);
  valueBuf_.back_append(value.data(), value.size());
  if (!value.empty() && randomGenerator_() < sampleUpperBound_) {
    tmpSampleFile_.writer << fstringOf(value);
    sampleLenSum_ += value.size();
  }
  if (filePair_->isFullValue && second_pass_iter_ &&
      table_options_.debugLevel != 2 && valueDataSize_ > (1ull << 20) &&
      valueDataSize_ > keyDataSize_ * 2) {
    filePair_->isFullValue = false;
  }
  assert(filePair_->value.fp);
  filePair_->value.writer << seqType
                          << fstringOf(filePair_->isFullValue ? value
                                                              : Slice());

  size_t freq_size = properties_.raw_key_size + properties_.raw_value_size;
  if (freq_size >= next_freq_size_) {
    auto kv_freq_copy =
        std::unique_ptr<freq_hist_o1>(new freq_hist_o1(kv_freq_));
    kv_freq_copy->add_hist(freq_[0]->k);
    kv_freq_copy->add_hist(freq_[0]->v);
    estimateOffset_ = uint64_t(
        freq_hist_o1::estimate_size_unfinish(*kv_freq_copy) * estimateRatio_);
    next_freq_size_ = freq_size + (1ULL << 20);
  }
  NotifyCollectTableCollectorsOnAdd(key, value, estimateOffset_, collectors_,
                                    ioptions_.info_log);
  return Status::OK();
} catch (const std::exception& ex) {
  WARN_EXCEPT(ioptions_.info_log, "%s: Exception: %s", BOOST_CURRENT_FUNCTION,
              ex.what());
  return Status::Corruption(ex.what());
}

Status TerarkZipTableBuilder::AddTombstone(const Slice& key,
                                           const LazyBuffer& lazy_value) try {
  auto s = lazy_value.fetch();
  if (!s.ok()) {
    return s;
  }
  const Slice& value = lazy_value.slice();
  if (table_options_.debugLevel == 3) {
    TERARKDB_NAMESPACE::ParsedInternalKey ikey;
    TERARKDB_NAMESPACE::ParseInternalKey(key, &ikey);
    fprintf(tmpDumpFile_, "DEBUG: 1st pass => %s / %s \n",
            ikey.DebugString(true).c_str(), value.ToString(true).c_str());
  }
  properties_.raw_key_size += key.size();
  properties_.raw_value_size += value.size();

  assert(GetInternalKeyType(key) == kTypeRangeDeletion);
  range_del_block_.Add(key, value);
  ++properties_.num_range_deletions;
  size_t freq_size = properties_.raw_key_size + properties_.raw_value_size;
  if (freq_size >= next_freq_size_) {
    auto kv_freq_copy =
        std::unique_ptr<freq_hist_o1>(new freq_hist_o1(kv_freq_));
    kv_freq_copy->add_hist(freq_[0]->k);
    kv_freq_copy->add_hist(freq_[0]->v);
    estimateOffset_ = uint64_t(
        freq_hist_o1::estimate_size_unfinish(*kv_freq_copy) * estimateRatio_);
    next_freq_size_ = freq_size + (1ULL << 20);
  }
  NotifyCollectTableCollectorsOnAdd(key, value, estimateOffset_, collectors_,
                                    ioptions_.info_log);
  return Status::OK();
} catch (const std::exception& ex) {
  WARN_EXCEPT(ioptions_.info_log, "%s: Exception: %s", BOOST_CURRENT_FUNCTION,
              ex.what());
  return Status::Corruption(ex.what());
}

TerarkZipTableBuilder::WaitHandle::WaitHandle() : myWorkMem(0) {}
TerarkZipTableBuilder::WaitHandle::WaitHandle(size_t workMem)
    : myWorkMem(workMem) {}
TerarkZipTableBuilder::WaitHandle::WaitHandle(WaitHandle&& other) noexcept
    : myWorkMem(other.myWorkMem) {
  other.myWorkMem = 0;
}
TerarkZipTableBuilder::WaitHandle& TerarkZipTableBuilder::WaitHandle::operator=(
    WaitHandle&& other) noexcept {
  Release();
  myWorkMem = other.myWorkMem;
  other.myWorkMem = 0;
  return *this;
}
void TerarkZipTableBuilder::WaitHandle::Release(size_t size) {
  assert(size <= myWorkMem);
  if (myWorkMem > 0) {
    if (size == 0) {
      size = myWorkMem;
    }
    std::unique_lock<std::mutex> zipLock(zipMutex);
    assert(sumWorkingMem >= myWorkMem);
    sumWorkingMem -= size;
    zipCond.notify_all();
    myWorkMem -= size;
  }
}
TerarkZipTableBuilder::WaitHandle::~WaitHandle() { Release(myWorkMem); }

TerarkZipTableBuilder::WaitHandle TerarkZipTableBuilder::WaitForMemory(
    const char* who, size_t myWorkMem) {
  const size_t softMemLimit = table_options_.softZipWorkingMemLimit;
  const size_t hardMemLimit =
      std::max<size_t>(table_options_.hardZipWorkingMemLimit, softMemLimit);
  const size_t smallmem = table_options_.smallTaskMemory;
  const std::chrono::seconds waitForTime(10);
  long long myStartTime = 0, now;
  auto shouldWait = [&]() {
    bool w;
    if (myWorkMem < softMemLimit) {
      w = (sumWorkingMem + myWorkMem >= hardMemLimit) ||
          (sumWorkingMem + myWorkMem >= softMemLimit && myWorkMem >= smallmem);
    } else {
      w = sumWorkingMem > softMemLimit / 4;
    }
    now = g_pf.now();
    if (!w) {
      assert(!waitQueue.empty());
      if (myWorkMem < smallmem) {
        return false;  // do not wait
      }
      if (sumWaitingMem + sumWorkingMem < softMemLimit) {
        return false;  // do not wait
      }
      if (waitQueue.size() == 1) {
        assert(this == waitQueue[0].tztb);
        return false;  // do not wait
      }
      size_t minRateIdx = size_t(-1);
      double minRateVal = DBL_MAX;
      auto wq = waitQueue.data();
      for (size_t i = 0, n = waitQueue.size(); i < n; ++i) {
        double rate = myWorkMem / (0.1 + now - wq[i].startTime);
        if (rate < minRateVal) {
          minRateVal = rate;
          minRateIdx = i;
        }
      }
      if (this == wq[minRateIdx].tztb) {
        return false;  // do not wait
      }
      myStartTime = wq[minRateIdx].startTime;
    }
    return true;  // wait
  };
  if (!waitInited_) {
    std::unique_lock<std::mutex> zipLock(zipMutex);
    if (!waitInited_) {
      waitQueue.push_back({this, g_pf.now()});
      waitInited_ = true;
    }
  }
  std::unique_lock<std::mutex> zipLock(zipMutex);
  sumWaitingMem += myWorkMem;
  while (shouldWait()) {
    INFO(
        ioptions_.info_log,
        "TerarkZipTableBuilder::Finish():this=%12p:\n sumWaitingMem =%8.3f GB, "
        "sumWorkingMem =%8.3f GB, %-10s workingMem =%8.4f GB, wait...\n",
        this, sumWaitingMem / 1e9, sumWorkingMem / 1e9, who, myWorkMem / 1e9);
    zipCond.wait_for(zipLock, waitForTime);
  }
  if (myStartTime == 0) {
    auto wq = waitQueue.data();
    for (size_t i = 0, n = waitQueue.size(); i < n; ++i) {
      if (this == wq[i].tztb) {
        myStartTime = wq[i].startTime;
        break;
      }
    }
  }
  INFO(ioptions_.info_log,
       "TerarkZipTableBuilder::Finish():this=%12p:\n sumWaitingMem =%8.3f GB, "
       "sumWorkingMem =%8.3f GB, %-10s "
       "workingMem =%8.4f GB, waited %9.3f sec, Key+Value bytes =%8.3f GB\n",
       this, sumWaitingMem / 1e9, sumWorkingMem / 1e9, who, myWorkMem / 1e9,
       g_pf.sf(myStartTime, now),
       (properties_.raw_key_size + properties_.raw_value_size) / 1e9);
  sumWaitingMem -= myWorkMem;
  sumWorkingMem += myWorkMem;
  return WaitHandle{myWorkMem};
}

Status TerarkZipTableBuilder::EmptyTableFinish() {
  INFO(ioptions_.info_log, "TerarkZipTableBuilder::EmptyFinish():this=%12p\n",
       this);
  offset_ = 0;
  BlockHandle emptyTableBH, tombstoneBH(0, 0);
  Status s = WriteBlock(Slice("Empty"), file_, &offset_, &emptyTableBH);
  if (!s.ok()) {
    return s;
  }
  if (!range_del_block_.empty()) {
    s = WriteBlock(range_del_block_.Finish(), file_, &offset_, &tombstoneBH);
    if (!s.ok()) {
      return s;
    }
  }
  range_del_block_.Reset();
  offset_info_.Init(0);
  return WriteMetaData(
      std::string(), 0,
      {
          {&kTerarkEmptyTableKey, emptyTableBH},
          {!tombstoneBH.IsNull() ? &kRangeDelBlock : NULL, tombstoneBH},
      });
}

Status TerarkZipTableBuilder::Finish(
    const TablePropertyCache* prop,
    const std::vector<SequenceNumber>* snapshots,
    const std::vector<uint64_t>* inheritance_tree) try {
  assert(!closed_);
  closed_ = true;

  if (prop != nullptr) {
    properties_.purpose = prop->purpose;
    properties_.read_amp = prop->read_amp;
    properties_.dependence = prop->dependence;
  }
  if (snapshots != nullptr) {
    properties_.snapshots = *snapshots;
  }
  if (inheritance_tree != nullptr) {
    properties_.inheritance_tree = *inheritance_tree;
  }

  if (!r00_) {
    return EmptyTableFinish();
  }
  if (tmpDumpFile_) {
    tmpDumpFile_.flush();
  }
  AddPrevUserKey(0, {}, {r00_.get(), r10_.get(), r20_.get()});
  filePair_->key.complete_write();
  filePair_->value.complete_write();
  kv_freq_.add_hist(freq_[0]->k);
  kv_freq_.add_hist(freq_[0]->v);
  freq_[1]->k.add_hist(freq_[0]->k);
  freq_[1]->v.add_hist(freq_[0]->v);
  size_t freq_21_entropy_size =
      freq_hist_o1::estimate_size_unfinish(freq_[2]->k, freq_[1]->k);
  if (r22_ && !MergeRangeStatus(r22_.get(), r10_.get(), r20_.get(),
                                freq_21_entropy_size)) {
    auto kvs = new KeyValueStatus(std::move(*r22_), std::move(freq_[2]->v));
    prefixBuildInfos_.emplace_back(kvs);
    BuildIndex(*kvs, freq_hist_o1::estimate_size_unfinish(freq_[2]->k));
    BuildStore(*kvs, nullptr, BuildStoreInit);
    kvs = new KeyValueStatus(std::move(*r10_), std::move(freq_[1]->v));
    prefixBuildInfos_.emplace_back(kvs);
    BuildIndex(*kvs, freq_hist_o1::estimate_size_unfinish(freq_[1]->k));
    BuildStore(*kvs, nullptr, BuildStoreInit);
  } else {
    freq_[2]->v.add_hist(freq_[1]->v);
    auto kvs = new KeyValueStatus(std::move(*r20_), std::move(freq_[2]->v));
    prefixBuildInfos_.emplace_back(kvs);
    BuildIndex(*kvs, freq_21_entropy_size);
    BuildStore(*kvs, nullptr, BuildStoreInit);
  }
  freq_[0].reset();
  freq_[1].reset();
  freq_[2].reset();
  r00_.reset();
  r10_.reset();
  r20_.reset();
  r11_.reset();
  r21_.reset();
  r22_.reset();

  tmpSampleFile_.complete_write();
  {
    long long rawBytes = properties_.raw_key_size + properties_.raw_value_size;
    long long tt = g_pf.now();
    INFO(ioptions_.info_log,
         "TerarkZipTableBuilder::Finish():this=%12p:\n  first pass time "
         "=%8.2f's,%8.3f'MB/sec\n",
         this, g_pf.sf(t0, tt), rawBytes * 1.0 / g_pf.uf(t0, tt));
  }
  Status s = prefixBuildInfos_.size() > 1 ? ZipValueToFinishMulti()
                                          : ZipValueToFinish();
  if (!s.ok()) {
    TerarkZipTableBuilder::Abandon();
  }
  return s;
} catch (const std::exception& ex) {
  fprintf(stderr, "TerarkZipTableBuilder::Finish():this=%12p:ex.what()=%s",
          this, ex.what());
  WARN(ioptions_.info_log,
       "TerarkZipTableBuilder::Finish():this=%12p:ex.what()=%s", this,
       ex.what());
  return AbortFinish(ex);
}

std::unique_ptr<TERARKDB_NAMESPACE::AsyncTask<TERARKDB_NAMESPACE::Status>>
TerarkZipTableBuilder::Async(std::function<Status()> func, void* tag) {
  auto task = std::unique_ptr<
      TERARKDB_NAMESPACE::AsyncTask<TERARKDB_NAMESPACE::Status>>(
      new AsyncTask<Status>([func]() {
        try {
          return func();
        } catch (const std::exception& ex) {
          return Status::Corruption(ex.what());
        }
      }));
  ioptions_.env->Schedule(c_style_callback(*task), task.get(),
                          TERARKDB_NAMESPACE::Env::Priority::LOW, tag,
                          c_style_callback(*task));
  return task;
}

void TerarkZipTableBuilder::BuildIndex(KeyValueStatus& kvs, size_t entropyLen) {
  kvs.status.stat.entropyLen = entropyLen;
  assert(kvs.status.stat.keyCount > 0);
  kvs.indexWait = Async(
      [this, &kvs]() {
        auto& keyStat = kvs.status.stat;
        std::unique_ptr<TerarkKeyReader> tempKeyFileReader(
            TerarkKeyReader::MakeReader(kvs.status.fileVec, true));
        const size_t myWorkMem = TerarkIndex::Factory::MemSizeForBuild(keyStat);
        auto waitHandle = WaitForMemory("nltTrie", myWorkMem);

        MmapWholeFile mmap_file;
        std::unique_ptr<TerarkIndex> indexPtr;
        long long t1 = g_pf.now();
        try {
          indexPtr.reset(TerarkIndex::Factory::Build(tempKeyFileReader.get(),
                                                     tiopt_, keyStat, nullptr));
        } catch (const std::exception& ex) {
          WARN_EXCEPT(
              ioptions_.info_log,
              "TerarkZipTableBuilder::Finish():this=%12p:\n  index build "
              "fail , error = %s\n",
              this, ex.what());
          return Status::Corruption("TerarkZipTableBuilder index build error",
                                    ex.what());
        }
        auto verify_index_impl = [&] {
          // check index correctness
          tempKeyFileReader->rewind();
          auto it = UniquePtrOf(indexPtr->NewIterator(nullptr));
          if (fstring(kvs.status.stat.minKey) <
              fstring(kvs.status.stat.maxKey)) {
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
              if (it->key() != tempKeyFileReader->next()) {
                return false;
              }
            }
          } else {
            for (it->SeekToLast(); it->Valid(); it->Prev()) {
              if (it->key() != tempKeyFileReader->next()) {
                return false;
              }
            }
          }
          return true;
        };
        auto verify_index = [&] {
          if (verify_index_impl()) {
            return true;
          }
#ifndef NDEBUG
          assert(false);
#endif
#ifdef _MSV_VER
          BOOL IsDbgPresent = FALSE;
          CheckRemoteDebuggerPresent(GetCurrentProcess(), &IsDbgPresent);
          if (IsDbgPresent || IsDebuggerPresent()) {
            verify_index_impl();
          }
#endif
          return false;
        };
        if (table_options_.debugLevel == 2 && !verify_index()) {
          return Status::Corruption("TerarkZipTableBuilder index check fail",
                                    indexPtr->Name().data());
        }
        size_t fileSize = 0;
        {
          std::unique_lock<std::mutex> l(indexBuildMutex_);
          FileStream writer(tmpIndexFile_, "ab+");
          kvs.indexFileBegin = writer.fsize();
          indexPtr->SaveMmap(
              [&fileSize, &writer](const void* data, size_t size) {
                fileSize += size;
                writer.ensureWrite(data, size);
              });
          writer.flush();
          kvs.indexFileEnd = writer.fsize();
        }
        assert(kvs.indexFileEnd - kvs.indexFileBegin == fileSize);
        assert(fileSize % 8 == 0);
        if (table_options_.debugLevel == 2) {
          std::unique_lock<std::mutex> l(indexBuildMutex_);
          MmapWholeFile(tmpIndexFile_.fpath).swap(mmap_file);
          indexPtr.reset();
          indexPtr = TerarkIndex::LoadMemory(
              mmap_file.memory().substr(kvs.indexFileBegin, fileSize));
          if (!verify_index()) {
            return Status::Corruption(
                "TerarkZipTableBuilder index check fail after reload",
                indexPtr->Name().data());
          }
        }
        long long tt = g_pf.now();
        size_t rawKeySize = kvs.status.stat.sumKeyLen;
        size_t keyCount = kvs.status.stat.keyCount;
        INFO(ioptions_.info_log,
             "TerarkZipTableBuilder::Finish():this=%12p:\n  index pass time "
             "=%8.2f's,%8.3f'MB/sec\n"
             "    index type = %s\n"
             "    usrkeys = %zd  min-keylen = %zd  max-keylen = %zd\n"
             "    raw-key =%9.4f GB  zip-key =%9.4f GB  avg-key =%7.2f  "
             "avg-zkey "
             "=%7.2f\n",
             this, g_pf.sf(t1, tt), rawKeySize * 1.0 / g_pf.uf(t1, tt),
             indexPtr->Name().data(), keyCount, kvs.status.stat.minKeyLen,
             kvs.status.stat.maxKeyLen, rawKeySize * 1.0 / 1e9,
             fileSize * 1.0 / 1e9, rawKeySize * 1.0 / keyCount,
             fileSize * 1.0 / keyCount);
        if (--kvs.keyFileRef == 0) {
          for (auto& pair : kvs.status.fileVec) {
            pair->key.close();
          }
        }
        return Status::OK();
      },
      &indexTag);
}

Status TerarkZipTableBuilder::BuildStore(KeyValueStatus& kvs,
                                         DictZipBlobStore::ZipBuilder* zbuilder,
                                         uint64_t flag) {
  auto buildUncompressedStore = [this, &kvs]() {
    std::unique_lock<std::mutex> l(storeBuildMutex_);
    assert(tmpStoreFileSize_ == 0 ||
           tmpStoreFileSize_ == FileStream(tmpStoreFile_.fpath, "rb").fsize());
    auto& stat = kvs.status.stat;
    size_t fixedNum = kvs.status.valueHist.m_cnt_of_max_cnt_key;
    size_t variaNum = stat.keyCount - fixedNum;
    BuildStoreParams params = {kvs, 0, tmpStoreFile_, tmpStoreFileSize_};
    Status s;
    try {
      if (kvs.status.valueHist.m_total_key_len == 0) {
        s = buildZeroLengthBlobStore(params);
      } else if (stat.keyCount >= 4096 && table_options_.enableEntropyStore &&
                 kvs.status.valueHist.m_total_key_len / stat.keyCount < 32) {
        s = buildEntropyZipBlobStore(params);
      } else if (table_options_.offsetArrayBlockUnits) {
        if (variaNum * 64 < stat.keyCount) {
          s = buildMixedLenBlobStore(params);
        } else {
          s = buildZipOffsetBlobStore(params);
        }
      } else {
        s = buildMixedLenBlobStore(params);
      }
      size_t newTmpStoreFileSize =
          FileStream(tmpStoreFile_.fpath, "rb").fsize();
      if (s.ok()) {
        kvs.valueFileBegin = tmpStoreFileSize_;
        kvs.valueFileEnd = newTmpStoreFileSize;
        assert((kvs.valueFileEnd - kvs.valueFileBegin) % 8 == 0);
      }
      tmpStoreFileSize_ = newTmpStoreFileSize;
    } catch (...) {
      tmpStoreFileSize_ = FileStream(tmpStoreFile_.fpath, "rb").fsize();
      throw;
    }
    return s;
  };
  auto buildCompressedStore = [this, &kvs, zbuilder]() {
    assert(zbuilder != nullptr);
    std::unique_lock<std::mutex> l(storeBuildMutex_);
    assert(tmpZipStoreFileSize_ == 0 ||
           tmpZipStoreFileSize_ ==
               FileStream(tmpZipStoreFile_.fpath, "rb").fsize());

    zbuilder->prepare(kvs.status.stat.keyCount, tmpZipStoreFile_,
                      tmpZipStoreFileSize_);
    Status s;
    try {
      s = BuilderWriteValues(
          kvs, [&](fstring value) { zbuilder->addRecord(value); });
      size_t newTmpZipStoreFileSize = 0;
      if (s.ok()) {
        zbuilder->finish(DictZipBlobStore::ZipBuilder::FinishNone);
        kvs.valueFileBegin = tmpZipStoreFileSize_;
        newTmpZipStoreFileSize =
            FileStream(tmpZipStoreFile_.fpath, "rb").fsize();
        kvs.valueFileEnd = newTmpZipStoreFileSize;
        assert((kvs.valueFileEnd - kvs.valueFileBegin) % 8 == 0);
      } else {
        zbuilder->abandon();
        newTmpZipStoreFileSize =
            FileStream(tmpZipStoreFile_.fpath, "rb").fsize();
      }
      tmpZipStoreFileSize_ = newTmpZipStoreFileSize;
    } catch (...) {
      tmpZipStoreFileSize_ = FileStream(tmpZipStoreFile_.fpath, "rb").fsize();
      throw;
    }
    return s;
  };

  if (flag & BuildStoreInit) {
    for (auto& pair : kvs.status.fileVec) {
      pair->value.complete_write();
    }
    auto avgValueLen =
        kvs.status.valueHist.m_total_key_len / kvs.status.stat.keyCount;
    if (avgValueLen < table_options_.minDictZipValueSize) {
      if (kvs.isFullValue && table_options_.debugLevel != 2) {
        kvs.isValueBuild = true;
        if (flag & BuildStoreSync) {
          return buildUncompressedStore();
        } else {
          kvs.storeWait = Async(buildUncompressedStore, &storeTag);
        }
      }
    } else {
      kvs.isUseDictZip = true;
    }
    return Status::OK();
  }
  kvs.isValueBuild = true;
  if (kvs.isUseDictZip && zbuilder == nullptr) {
    kvs.isUseDictZip = false;
  }
  if (flag & BuildStoreSync) {
    return kvs.isUseDictZip ? buildCompressedStore() : buildUncompressedStore();
  } else {
    if (kvs.isUseDictZip) {
      kvs.storeWait = Async(buildCompressedStore, &storeTag);
    } else {
      kvs.storeWait = Async(buildUncompressedStore, &storeTag);
    }
  }
  return Status::OK();
}

std::unique_ptr<TERARKDB_NAMESPACE::AsyncTask<TERARKDB_NAMESPACE::Status>>
TerarkZipTableBuilder::CompressDict(fstring tmpDictFile, fstring dict,
                                    std::string* info, long long* td) {
  if (table_options_.disableCompressDict) {
    return Async(
        [=] {
          long long tds = g_pf.now();
          FileStream(tmpDictFile, "wb+").ensureWrite(dict.data(), dict.size());
          info->clear();
          *td = g_pf.now() - tds;
          return Status::OK();
        },
        &dictTag);
  }
  return Async(
      [=] {
        long long tds = g_pf.now();
        FileStream(tmpDictFile, "wb+").chsize(dict.size());
        MmapWholeFile dictFile(tmpDictFile, true);
        size_t zstd_size = ZSTD_compress(dictFile.base, dictFile.size,
                                         dict.data(), dict.size(), 0);
        if (ZSTD_isError(zstd_size) || zstd_size >= dict.size()) {
          memcpy(dictFile.base, dict.data(), dict.size());
          info->clear();
        } else {
          MmapWholeFile().swap(dictFile);
          FileStream(tmpDictFile, "rb+").chsize(zstd_size);
          *info = "ZSTD_";
          info->append(lcast(ZSTD_versionNumber()));
        }
        *td = g_pf.now() - tds;
        return Status::OK();
      },
      &dictTag);
}

Status TerarkZipTableBuilder::WaitBuildIndex() {
  ioptions_.env->UnSchedule(&indexTag, TERARKDB_NAMESPACE::Env::Priority::LOW);
  Status result = Status::OK();
  for (auto& kvs : prefixBuildInfos_) {
    assert(kvs);
    assert(kvs->indexWait);
    assert(kvs->indexWait->valid());
    auto s = kvs->indexWait->get();
    if (terark_unlikely(!s.ok() && result.ok())) {
      result = std::move(s);
    }
  }
  return result;
}

Status TerarkZipTableBuilder::WaitBuildStore() {
  ioptions_.env->UnSchedule(&storeTag, TERARKDB_NAMESPACE::Env::Priority::LOW);
  Status result = Status::OK();
  for (auto& kvs : prefixBuildInfos_) {
    assert(kvs);
    if (kvs->storeWait) {
      assert(kvs->storeWait->valid());
      auto s = kvs->storeWait->get();
      if (terark_unlikely(!s.ok() && result.ok())) {
        result = std::move(s);
      }
    }
  }
  return result;
}

void TerarkZipTableBuilder::BuildReorderMap(std::unique_ptr<TerarkIndex>& index,
                                            BuildReorderParams& params,
                                            KeyValueStatus& kvs,
                                            fstring mmap_memory,
                                            AbstractBlobStore* store,
                                            long long& t6) {
  bool reorder = false;
  index =
      TerarkIndex::LoadMemory(fstring(mmap_memory.data() + kvs.indexFileBegin,
                                      kvs.indexFileEnd - kvs.indexFileBegin));
  reorder = index->NeedsReorder() || isReverseBytewiseOrder_;
  char buffer[512];
  INFO(ioptions_.info_log,
       "TerarkZipTableBuilder::Finish():this=%12p:\n    index type = %-32s, "
       "store type = %-20s\n%s"
       "    raw-val =%9.4f GB  zip-val =%9.4f GB  avg-val =%7.2f  avg-zval "
       "=%7.2f\n",
       this, index->Name().data(), store->name(),
       index->Info(buffer, sizeof buffer), store->total_data_size() * 1.0 / 1e9,
       store->get_mmap().size() * 1.0 / 1e9,
       store->total_data_size() * 1.0 / store->num_records(),
       store->get_mmap().size() * 1.0 / store->num_records());
  t6 = g_pf.now();
  if (!reorder) {
    params.type.clear();
    params.tmpReorderFile.Delete();
    return;
  }
  auto& stat = kvs.status.stat;
  params.type.resize_no_init(stat.keyCount);
  ZReorderMap::Builder builder(stat.keyCount, isReverseBytewiseOrder_ ? -1 : 1,
                               params.tmpReorderFile.fpath, "wb");
  if (isReverseBytewiseOrder_) {
    size_t ho = stat.keyCount;
    size_t hn = 0;
    size_t count = index->NumKeys();
    ho -= count;
    if (index->NeedsReorder()) {
      size_t memory =
          UintVecMin0::compute_mem_size_by_max_val(count, count - 1);
      WaitHandle handle = WaitForMemory("reorder", memory);
      UintVecMin0 newToOld(index->NumKeys(), index->NumKeys() - 1);
      index->GetOrderMap(newToOld);
      for (size_t n = 0; n < count; ++n) {
        size_t o = count - newToOld[n] - 1 + ho;
        builder.push_back(o);
        params.type.set0(n + hn, kvs.type[o]);
      }
    } else {
      for (size_t n = 0, o = count - 1 + ho; n < count; ++n, --o) {
        builder.push_back(o);
        params.type.set0(n + hn, kvs.type[o]);
      }
    }
    hn += count;
    assert(ho == 0);
    assert(hn == stat.keyCount);
  } else {
    size_t h = 0;
    size_t count = index->NumKeys();
    if (index->NeedsReorder()) {
      size_t memory =
          UintVecMin0::compute_mem_size_by_max_val(count, count - 1);
      WaitHandle handle = WaitForMemory("reorder", memory);
      UintVecMin0 newToOld(index->NumKeys(), index->NumKeys() - 1);
      index->GetOrderMap(newToOld);
      for (size_t n = 0; n < count; ++n) {
        size_t o = newToOld[n] + h;
        builder.push_back(o);
        params.type.set0(n + h, kvs.type[o]);
      }
    } else {
      assert(false);
    }
    h += count;
    assert(h == stat.keyCount);
    assert(h == stat.keyCount);
  }
  builder.finish();
}

TerarkZipTableBuilder::WaitHandle TerarkZipTableBuilder::LoadSample(
    std::unique_ptr<DictZipBlobStore::ZipBuilder>& zbuilder) {
  if (compaction_load_ > 0.99) {
    INFO(ioptions_.info_log,
         "TerarkZipTableBuilder::LoadSample():this=%12p:\n"
         "sample_len = %zd, real_sample_len = 0, level = %d, compaction_load = "
         "%f\n",
         this, sampleLenSum_, level_, compaction_load_);
    zbuilder.reset();
    return WaitHandle();
  }

  size_t sampleMax =
      std::min<size_t>(INT32_MAX, table_options_.softZipWorkingMemLimit / 7);
  size_t dictWorkingMemory = std::min<size_t>(sampleMax, sampleLenSum_) * 6;
  auto waitHandle = WaitForMemory("dictZip", dictWorkingMemory);

  valvec<byte_t> sample;
  NativeDataInput<InputBuffer> sampleInput(&tmpSampleFile_.fp);
  size_t newSampleLen =
      std::size_t(std::pow(std::min(sampleLenSum_, sampleMax) / 4096.0,
                           1 - compaction_load_) *
                  4096);
  size_t realSampleLenSum = 0;

  if (newSampleLen >= sampleLenSum_) {
    for (size_t len = 0; len < sampleLenSum_;) {
      sampleInput >> sample;
      zbuilder->addSample(sample);
      len += sample.size();
    }
    realSampleLenSum = sampleLenSum_;
  } else {
    auto upperBoundSample =
        uint64_t(randomGenerator_.max() * double(newSampleLen) / sampleLenSum_);
    for (size_t len = 0; len < sampleLenSum_;) {
      sampleInput >> sample;
      if (randomGenerator_() < upperBoundSample) {
        realSampleLenSum += sample.size();
        if (realSampleLenSum < newSampleLen) {
          zbuilder->addSample(sample);
        } else {
          zbuilder->addSample(
              fstring(sample).substr(0, realSampleLenSum - newSampleLen));
          break;
        }
      }
      len += sample.size();
    }
    INFO(ioptions_.info_log,
         "TerarkZipTableBuilder::LoadSample():this=%12p:\n"
         "sample_len = %zd, real_sample_len = %zd, level = %d, compaction_load "
         "= %f\n",
         this, sampleLenSum_, realSampleLenSum, level_, compaction_load_);
  }
  tmpSampleFile_.close();
  if (realSampleLenSum == 0) {  // prevent from empty
    zbuilder->addSample(
        sample.empty()
            ? fstring("Hello World!")
            : fstring(sample).substr(0, std::min(sample.size(), newSampleLen)));
  }
  zbuilder->finishSample();
  return waitHandle;
}

Status TerarkZipTableBuilder::buildEntropyZipBlobStore(
    BuildStoreParams& params) {
  auto& kvs = params.kvs;
  auto& hist = kvs.status.valueHist;
  auto checksumType = (hist.m_total_key_len / hist.m_cnt_sum <
                       static_cast<size_t>(table_options_.checksumSmallValSize))
                          ? kCRC16C
                          : kCRC32C;
  size_t blockUnits = table_options_.offsetArrayBlockUnits != 0
                          ? table_options_.offsetArrayBlockUnits
                          : 128;
  terark::EntropyZipBlobStore::MyBuilder builder(
      kvs.valueFreq, blockUnits, params.fpath, params.offset,
      table_options_.checksumLevel, checksumType,
      !table_options_.disableCompressDict);
  auto s =
      BuilderWriteValues(kvs, [&](fstring value) { builder.addRecord(value); });
  if (s.ok()) {
    builder.finish();
  }
  return s;
}
Status TerarkZipTableBuilder::buildZeroLengthBlobStore(
    BuildStoreParams& params) {
  auto& kvs = params.kvs;
  auto store = UniquePtrOf(new terark::ZeroLengthBlobStore());
  auto s = BuilderWriteValues(kvs, [&](fstring value) {
    assert(value.empty());
    (void)value;
  });
  if (s.ok()) {
    store->finish(kvs.status.stat.keyCount);
    FileStream file(params.fpath, "ab+");
    store->save_mmap([&](const void* d, size_t s) { file.ensureWrite(d, s); });
  }
  return s;
}
Status TerarkZipTableBuilder::buildMixedLenBlobStore(BuildStoreParams& params) {
  auto& kvs = params.kvs;
  auto& hist = kvs.status.valueHist;
  size_t fixedLen = hist.m_max_cnt_key;
  size_t fixedLenCount = hist.m_cnt_of_max_cnt_key;
  size_t varDataLen = hist.m_total_key_len - fixedLen * fixedLenCount;
  size_t varDataLenCount = hist.m_cnt_sum - fixedLenCount;
  auto checksumType = (hist.m_total_key_len / hist.m_cnt_sum <
                       static_cast<size_t>(table_options_.checksumSmallValSize))
                          ? kCRC16C
                          : kCRC32C;
  std::unique_ptr<AbstractBlobStore::Builder> builder;
  if (hist.m_cnt_sum < (4ULL << 30)) {
    builder.reset(new terark::MixedLenBlobStore::MyBuilder(
        fixedLen, varDataLen, varDataLenCount, params.fpath, params.offset,
        table_options_.checksumLevel, checksumType));
  } else {
    builder.reset(new terark::MixedLenBlobStore64::MyBuilder(
        fixedLen, varDataLen, varDataLenCount, params.fpath, params.offset,
        table_options_.checksumLevel, checksumType));
  }
  auto s = BuilderWriteValues(
      kvs, [&](fstring value) { builder->addRecord(value); });
  if (s.ok()) {
    builder->finish();
  }
  return s;
}
Status TerarkZipTableBuilder::buildZipOffsetBlobStore(
    BuildStoreParams& params) {
  auto& kvs = params.kvs;
  auto& hist = kvs.status.valueHist;
  auto checksumType = (hist.m_total_key_len / hist.m_cnt_sum <
                       static_cast<size_t>(table_options_.checksumSmallValSize))
                          ? kCRC16C
                          : kCRC32C;
  terark::ZipOffsetBlobStore::Options options;
  options.block_units = table_options_.offsetArrayBlockUnits;
  options.compress_level = 0;
  options.checksum_level = table_options_.checksumLevel;
  options.checksum_type = checksumType;
  terark::ZipOffsetBlobStore::MyBuilder builder(params.fpath, params.offset,
                                                options);
  auto s =
      BuilderWriteValues(kvs, [&](fstring value) { builder.addRecord(value); });
  if (s.ok()) {
    builder.finish();
  }
  return s;
}

Status TerarkZipTableBuilder::ZipValueToFinish() {
  assert(prefixBuildInfos_.size() == 1);
  auto& kvs = *prefixBuildInfos_.front();
  AutoDeleteFile tmpDictFile{tmpSentryFile_.path + ".dict"};
  std::unique_ptr<DictZipBlobStore::ZipBuilder> zbuilder;
  WaitHandle dictWaitHandle;
  std::unique_ptr<AsyncTask<Status>> dictWait;
  DictZipBlobStore::ZipStat dzstat;
  std::string dictInfo;
  uint64_t dictHash = 0;
  long long t3, t4, td;
  t3 = g_pf.now();
  Status s = WaitBuildStore();
  if (!s.ok()) {
    return s;
  }
  if (!kvs.isValueBuild) {
    if (kvs.isUseDictZip) {
      zbuilder.reset(createZipBuilder());
      dictWaitHandle = LoadSample(zbuilder);
    }
    if (zbuilder) {
      // prepareDict() will invalid zbuilder->getDictionary().memory
      zbuilder->prepareDict();
      dictWait = CompressDict(tmpDictFile, zbuilder->getDictionary().memory,
                              &dictInfo, &td);
      dictHash = zbuilder->getDictionary().xxhash;
      dictSize_ = zbuilder->getDictionary().memory.size();
    }
    s = BuildStore(kvs, zbuilder.get(), BuildStoreSync);
    if (!s.ok()) {
      if (dictWait) {
        assert(dictWait->valid());
        dictWait->wait();
      }
      return s;
    }
    if (zbuilder) {
      dzstat = zbuilder->getZipStat();
    }
  }
  if (zbuilder) {
    zbuilder->freeDict();
    t4 = g_pf.now();
    ioptions_.env->UnSchedule(&dictTag, TERARKDB_NAMESPACE::Env::Priority::LOW);
    assert(dictWait->valid());
    s = dictWait->get();
    if (!s.ok()) {
      return s;
    }
    zbuilder.reset();
    dictWaitHandle.Release();
  } else {
    tmpDictFile.fpath.clear();
    t4 = g_pf.now();
    td = 1;
  }
  // wait for indexing complete, if indexing is slower than value compressing
  s = WaitBuildIndex();
  if (!s.ok()) {
    return s;
  }
  if (tmpDumpFile_.isOpen()) {
    tmpDumpFile_.close();
  }
  return WriteSSTFile(t3, t4, td, tmpDictFile, dictInfo, dictHash, dzstat);
}

Status TerarkZipTableBuilder::ZipValueToFinishMulti() {
  assert(prefixBuildInfos_.size() > 1);
  AutoDeleteFile tmpDictFile{tmpSentryFile_.path + ".dict"};
  std::unique_ptr<DictZipBlobStore::ZipBuilder> zbuilder;
  WaitHandle dictWaitHandle;
  std::unique_ptr<AsyncTask<Status>> dictWait;
  std::unique_ptr<AbstractBlobStore> store;
  DictZipBlobStore::ZipStat dzstat;
  std::string dictInfo;
  uint64_t dictHash = 0;
  size_t dictRefCount = 0;
  Status s;
  long long t3, t4, td = 0;
  t3 = g_pf.now();
  bool isUseDictZip = false;
  for (auto& kvs : prefixBuildInfos_) {
    if (kvs->isValueBuild) {
      continue;
    }
    if (kvs->isUseDictZip) {
      isUseDictZip = true;
    } else {
      BuildStore(*kvs, nullptr, 0);
    }
  }
  if (isUseDictZip) {
    zbuilder.reset(createZipBuilder());
    dictWaitHandle = LoadSample(zbuilder);
    if (zbuilder) {
      assert(tmpZipStoreFileSize_ == 0);
      // build dict in this thread
      zbuilder->prepareDict();
      dictWait = CompressDict(tmpDictFile, zbuilder->getDictionary().memory,
                              &dictInfo, &td);
      dictHash = zbuilder->getDictionary().xxhash;
      dictSize_ = zbuilder->getDictionary().memory.size();
    }
    for (auto& kvs : prefixBuildInfos_) {
      if (kvs->isUseDictZip) {
        s = BuildStore(*kvs, zbuilder.get(), BuildStoreSync);
        if (!s.ok()) {
          break;
        }
        if (zbuilder && dictRefCount == 0) {
          dzstat = zbuilder->getZipStat();
        }
        ++dictRefCount;
      }
    }
  }
  if (!s.ok()) {
    if (dictWait) {
      assert(dictWait->valid());
      dictWait->wait();
    }
    return s;
  }
  if (zbuilder) {
    zbuilder->freeDict();
    t4 = g_pf.now();
    assert(dictWait->valid());
    s = dictWait->get();

    if (!s.ok()) {
      return s;
    }
    zbuilder.reset();
    dictWaitHandle.Release();
  } else {
    tmpDictFile.fpath.clear();
    t4 = g_pf.now();
  }
  s = WaitBuildStore();
  if (!s.ok()) {
    return s;
  }
  s = WaitBuildIndex();
  if (!s.ok()) {
    return s;
  }
  dzstat.dictZipTime = g_pf.sf(t3, t4);
  if (tmpDumpFile_.isOpen()) {
    tmpDumpFile_.close();
  }
  return WriteSSTFileMulti(t3, t4, td, tmpDictFile, dictInfo, dictHash, dzstat);
}

class TerarkValueReader {
  const valvec<std::shared_ptr<FilePair>>& files;
  size_t index;
  NativeDataInput<InputBuffer> reader;
  valvec<byte_t> buffer;

  void checkEOF() {
    if (terark_unlikely(reader.eof())) {
      FileStream* fp = &files[++index]->value.fp;
      fp->rewind();
      reader.attach(fp);
    }
  }

 public:
  TerarkValueReader(const valvec<std::shared_ptr<FilePair>>& _files)
      : files(_files) {}

  uint64_t readUInt64() {
    checkEOF();
    return reader.load_as<uint64_t>();
  }

  void appendBuffer(valvec<byte_t>* buffer) {
    checkEOF();
    reader.load_add(*buffer);
  }

  void rewind() {
    index = 0;
    FileStream* fp = &files.front()->value.fp;
    fp->rewind();
    reader.attach(fp);
  }
};

Status TerarkZipTableBuilder::BuilderWriteValues(
    KeyValueStatus& kvs, std::function<void(fstring)> write) {
  auto& bzvType = kvs.type;
  size_t zeroSeqCount = 0;
  auto& stat = kvs.status.stat;
  bzvType.resize(kvs.status.stat.keyCount);
  auto seekSecondPassIter = [&] {
    std::string target;
    target.reserve(stat.minKey.size() + 8);
    target.assign((const char*)stat.minKey.data(), stat.minKey.size());
    target.append((const char*)&kvs.status.seqType, 8);
    second_pass_iter_->Seek(target);
  };

#define ITER_MOVE_NEXT(it)                       \
  do {                                           \
    it->Next();                                  \
    if (!it->status().ok()) return it->status(); \
  } while (0)

  valvec<byte_t> key, value;
  std::unique_ptr<TerarkKeyReader> keyInput;

  auto readKey = [&](uint64_t seqType, bool next) {
    if (!next) {
      (uint64_t&)key.end()[-8] = seqType;
      return SliceOf(key);
    }
    key.assign(keyInput->next());
    key.append((char*)&seqType, 8);
    return SliceOf(key);
  };

  TerarkValueReader input(kvs.status.fileVec);
  input.rewind();
  if (kvs.isFullValue) {
    if (--kvs.keyFileRef == 0) {
      for (auto& pair : kvs.status.fileVec) {
        pair->key.close();
      }
    }
    size_t entryId = 0;
    size_t bitPos = 0;
    for (size_t recId = 0; recId < stat.keyCount; recId++) {
      uint64_t seqType = input.readUInt64();
      uint64_t seqNum;
      ValueType vType;
      UnPackSequenceAndType(seqType, &seqNum, &vType);
      size_t oneSeqLen = kvs.status.valueBits.one_seq_len(bitPos);
      assert(oneSeqLen >= 1);
      if (1 == oneSeqLen && (kTypeDeletion == vType || kTypeValue == vType)) {
        if (0 == seqNum && kTypeValue == vType) {
          bzvType.set0(recId, size_t(ZipValueType::kZeroSeq));
          ++zeroSeqCount;
          value.erase_all();
          input.appendBuffer(&value);
        } else {
          if (kTypeValue == vType) {
            bzvType.set0(recId, size_t(ZipValueType::kValue));
          } else {
            bzvType.set0(recId, size_t(ZipValueType::kDelete));
          }
          value.erase_all();
          value.append((byte_t*)&seqNum, 7);
          input.appendBuffer(&value);
        }
      } else {
        bzvType.set0(recId, size_t(ZipValueType::kMulti));
        size_t headerSize = ZipValueMultiValue::calcHeaderSize(oneSeqLen);
        value.resize(headerSize);
        ((ZipValueMultiValue*)value.data())->offsets[0] = uint32_t(oneSeqLen);
        for (size_t j = 0; j < oneSeqLen; j++) {
          if (j > 0) {
            seqType = input.readUInt64();
          }
          value.append((byte_t*)&seqType, 8);
          input.appendBuffer(&value);
          if (j + 1 < oneSeqLen) {
            ((ZipValueMultiValue*)value.data())->offsets[j + 1] =
                uint32_t(value.size() - headerSize);
          }
        }
      }
      write(value);
      bitPos += oneSeqLen + 1;
      entryId += oneSeqLen;
    }
    // tmpSentryFile_ ignore kTypeRangeDeletion keys
    // so entryId may less than properties_.num_entries
    assert(entryId <= properties_.num_entries);
    for (auto& pair : kvs.status.fileVec) {
      pair->value.close();
    }
  } else {
    assert(second_pass_iter_ != nullptr);
    keyInput.reset(TerarkKeyReader::MakeReader(kvs.status.fileVec, false));
    keyInput->rewind();

    valvec<byte_t> ignVal;
    size_t recId = 0, entryId = 0, bitPos = 0;

    auto readInternalKey = [&](bool next) {
      auto seqType = input.readUInt64();
      input.appendBuffer(&ignVal);
      ignVal.erase_all();
      return readKey(seqType, next);
    };

    auto dumpKeyValueFunc = [&](const ParsedInternalKey& ikey,
                                const Slice& value) {
      fprintf(tmpDumpFile_.fp(), "DEBUG: 2nd pass => %s / %s \n",
              ikey.DebugString(true).c_str(), value.ToString(true).c_str());
    };

    auto& ic = ioptions_.internal_comparator;

    seekSecondPassIter();
    if (!second_pass_iter_->status().ok()) {
      return second_pass_iter_->status();
    }

    bool debugDumpKeyValue = table_options_.debugLevel == 3;

    size_t varNum;
    int cmpRet, mulCmpRet;
    ParsedInternalKey pIKey;
    Slice curKey, curVal, bufKey = readInternalKey(true);

    value.erase_all();

    while (recId < stat.keyCount && second_pass_iter_->Valid()) {
      LazyBuffer lazy_value = second_pass_iter_->value();
      auto s = lazy_value.fetch();
      if (!s.ok()) {
        return s;
      }
      curKey = second_pass_iter_->key();
      curVal = lazy_value.slice();
      TERARK_RT_assert(ParseInternalKey(curKey, &pIKey), std::logic_error);
      if (debugDumpKeyValue) {
        dumpKeyValueFunc(pIKey, curVal);
      }
      varNum = kvs.status.valueBits.one_seq_len(bitPos);
      assert(varNum >= 1);
      cmpRet = ic.Compare(curKey, bufKey);
      if (varNum ==
          1) {  // single record contains {value, del, other{sglDel, CFBI, BI}}
        if (cmpRet == 0) {  // curKey == bufKey
          if (pIKey.sequence == 0 && pIKey.type == kTypeValue) {
            bzvType.set0(recId, size_t(ZipValueType::kZeroSeq));
            ++zeroSeqCount;
            write(fstringOf(curVal));
          } else if (pIKey.type == kTypeValue) {
            bzvType.set0(recId, size_t(ZipValueType::kValue));
            value.append((byte_t*)&pIKey.sequence, 7);
            value.append(fstringOf(curVal));
            write(value);
          } else if (pIKey.type == kTypeDeletion) {
            bzvType.set0(recId, size_t(ZipValueType::kDelete));
            value.append((byte_t*)&pIKey.sequence, 7);
            value.append(fstringOf(curVal));
            write(value);
          } else {
            bzvType.set0(recId, size_t(ZipValueType::kMulti));
            size_t headerSize = ZipValueMultiValue::calcHeaderSize(1);
            value.resize(headerSize);
            ((ZipValueMultiValue*)value.data())->offsets[0] = 1;
            value.append(bufKey.data() + bufKey.size() - 8, 8);
            value.append(fstringOf(curVal));
            write(value);
          }
          value.erase_all();
          ITER_MOVE_NEXT(second_pass_iter_);
          if (++recId < stat.keyCount) bufKey = readInternalKey(true);
        } else if (cmpRet > 0) {  // curKey > bufKey
          bzvType.set0(recId, size_t(ZipValueType::kMulti));
          write(fstring());  // write nothing
          value.erase_all();
          if (++recId < stat.keyCount) bufKey = readInternalKey(true);
        } else {  // curKey < bufKey
          ITER_MOVE_NEXT(second_pass_iter_);
          continue;
        }
      } else {  // multi record contains {multi, merge, cfbi, bi}
        bzvType.set0(recId, size_t(ZipValueType::kMulti));
        size_t headerSize = ZipValueMultiValue::calcHeaderSize(varNum);
        value.resize(headerSize);
        ((ZipValueMultiValue*)value.data())->offsets[0] = uint32_t(varNum);
        size_t mulRecId = 0;
        while (mulRecId < varNum && second_pass_iter_->Valid()) {
          auto refresh_second_pass_iter_key_value = [&] {
            lazy_value = second_pass_iter_->value();
            s = lazy_value.fetch();
            if (!s.ok()) {
              return false;
            }
            curKey = second_pass_iter_->key();
            curVal = lazy_value.slice();
            TERARK_RT_assert(ParseInternalKey(curKey, &pIKey),
                             std::logic_error);
            if (debugDumpKeyValue) {
              dumpKeyValueFunc(pIKey, curVal);
            }
            return true;
          };
          if (mulRecId > 0) {
            if (!refresh_second_pass_iter_key_value()) {
              return s;
            }
          }
          mulCmpRet = ic.Compare(curKey, bufKey);
          if (mulCmpRet == 0) {  // curKey == bufKey
            value.append(bufKey.data() + bufKey.size() - 8, 8);
            value.append(fstringOf(curVal));
            ITER_MOVE_NEXT(second_pass_iter_);
            if (++mulRecId < varNum) {
              bufKey = readInternalKey(false);
              ((ZipValueMultiValue*)value.data())->offsets[mulRecId] =
                  uint32_t(value.size() - headerSize);
            }
          } else if (mulCmpRet > 0) {  // curKey > bufKey
            // write nothing
            if (++mulRecId < varNum) {
              bufKey = readInternalKey(false);
              ((ZipValueMultiValue*)value.data())->offsets[mulRecId] =
                  uint32_t(value.size() - headerSize);
            }
          } else if (mulCmpRet < 0) {  // curKey < bufKey
            ITER_MOVE_NEXT(second_pass_iter_);
            if (mulRecId == 0) {
              if (!refresh_second_pass_iter_key_value()) {
                return s;
              }
            }
          }
        }
        if (value.size() == headerSize) {
          write(fstring());  // all write nothing
        } else {
          write(value);
        }
        value.erase_all();
        if (++recId < stat.keyCount) bufKey = readInternalKey(true);
      }
      bitPos += varNum + 1;
      entryId += varNum;
    }
    value.erase_all();
    while (recId < stat.keyCount) {
      varNum = kvs.status.valueBits.one_seq_len(bitPos);
      assert(varNum >= 1);
      TERARK_RT_assert(ParseInternalKey(bufKey, &pIKey), std::logic_error);
      if (debugDumpKeyValue) {
        dumpKeyValueFunc(pIKey, Slice());
      }
      bzvType.set0(recId, size_t(ZipValueType::kMulti));
      for (size_t mulRecId = 0; mulRecId < varNum; mulRecId++) {
        if (mulRecId > 0) {
          TERARK_RT_assert(ParseInternalKey(bufKey, &pIKey), std::logic_error);
          if (debugDumpKeyValue) {
            dumpKeyValueFunc(pIKey, Slice());
          }
        }
        if (mulRecId + 1 < varNum) {
          bufKey = readInternalKey(false);
        }
      }
      write(fstring());  // write nothing
      bitPos += varNum + 1;
      entryId += varNum;
      if (++recId < stat.keyCount) bufKey = readInternalKey(true);
    }
    assert(entryId <= properties_.num_entries);
    if (--kvs.keyFileRef == 0) {
      for (auto& pair : kvs.status.fileVec) {
        pair->key.close();
      }
    }
#undef ITER_MOVE_NEXT
  }
  kvs.status.zeroSeqCount = zeroSeqCount;
  kvs.status.valueBits.clear();
  return Status::OK();
}

Status TerarkZipTableBuilder::WriteIndexStore(
    fstring indexMmap, AbstractBlobStore* store, KeyValueStatus& kvs,
    BlockHandle& /*dataBlock*/, size_t kvs_index, long long& /*t5*/,
    long long& t6, long long& t7) {
  using namespace std::placeholders;
  auto writeAppend =
      std::bind(&TerarkZipTableBuilder::DoWriteAppend, this, _1, _2);
  BuildReorderParams params;
  params.tmpReorderFile.fpath = tmpSentryFile_.path + ".reorder";
  std::unique_ptr<TerarkIndex> index;
  BuildReorderMap(index, params, kvs, indexMmap, store, t6);
  size_t indexSize;
  size_t storeSize;
  size_t typeSize;
  size_t offset;
  if (params.NeedsReorder()) {
    params.type.swap(kvs.type);  // kvs.type will be written to file
    ZReorderMap reorder(params.tmpReorderFile.fpath);
    t7 = g_pf.now();
    std::string reorder_tmp = tmpSentryFile_.path + ".reorder-tmp";
    try {
      offset = offset_;
      // Composite Index needs reorder
      index->Reorder(reorder, std::ref(writeAppend), reorder_tmp);
      indexSize = offset_ - offset;
      reorder.rewind();
      offset = offset_;
      store->reorder_zip_data(reorder, std::ref(writeAppend), reorder_tmp);
      storeSize = offset_ - offset;
    } catch (const Status& s) {
      return s;
    }
  } else {
    t7 = t6;
    try {
      offset = offset_;
      index->SaveMmap(std::ref(writeAppend));
      indexSize = offset_ - offset;
      offset = offset_;
      store->save_mmap(std::ref(writeAppend));
      storeSize = offset_ - offset;
    } catch (const Status& s) {
      return s;
    }
  }
  try {
    if (kvs.status.zeroSeqCount != kvs.type.size()) {
      DoWriteAppend(kvs.type.data(), kvs.type.mem_size());
      typeSize = kvs.type.mem_size();
    } else {
      typeSize = 0;
    }
  } catch (const Status& s) {
    return s;
  }
  offset_info_.set(kvs_index, indexSize, storeSize, typeSize);
  properties_.index_size += indexSize;
  properties_.data_size += storeSize;
  return Status::OK();
}

void TerarkZipTableBuilder::DoWriteAppend(const void* data, size_t size) {
  Status s = file_->Append(Slice((const char*)data, size));
  if (!s.ok()) {
    throw s;
  }
  offset_ += size;
}

Status TerarkZipTableBuilder::WriteSSTFile(
    long long t3, long long t4, long long td, fstring tmpDictFile,
    const std::string& dictInfo, uint64_t dictHash,
    const DictZipBlobStore::ZipStat& dzstat) {
  assert(prefixBuildInfos_.size() == 1);
  terark::MmapWholeFile dictMmap;
  AbstractBlobStore::Dictionary dict(dictSize_, dictHash);
  auto& kvs = *prefixBuildInfos_.front();
  offset_info_.Init(1);
  terark::MmapWholeFile mmapIndexFile(tmpIndexFile_.fpath);
  terark::MmapWholeFile mmapStoreFile(
      (kvs.isUseDictZip ? tmpZipStoreFile_ : tmpStoreFile_).fpath);
  assert(mmapIndexFile.base != nullptr);
  assert(mmapStoreFile.base != nullptr);
  auto store = UniquePtrOf(
      AbstractBlobStore::load_from_user_memory(mmapStoreFile.memory(), dict));
  auto& bzvType = kvs.type;
  const size_t realsampleLenSum = dict.memory.size();
  long long rawBytes = properties_.raw_key_size + properties_.raw_value_size;
  long long t5 = g_pf.now();
  Status s;
  BlockHandle dataBlock, dictBlock, offsetBlock, tombstoneBlock(0, 0);
  {
    size_t real_size =
        mmapIndexFile.size + store->mem_size() + bzvType.mem_size();
    size_t block_size, last_allocated_block;
    file_->writable_file()->GetPreallocationStatus(&block_size,
                                                   &last_allocated_block);
    INFO(ioptions_.info_log,
         "TerarkZipTableBuilder::Finish():this=%12p:\n old prealloc_size = "
         "%zd, real_size = %zd\n",
         this, block_size, real_size);
    file_->writable_file()->SetPreallocationBlockSize(1 * 1024 * 1024 +
                                                      real_size);
  }
  long long t6, t7;
  offset_ = 0;
  dataBlock.set_offset(offset_);
  s = WriteIndexStore(mmapIndexFile.memory(), store.get(), kvs, dataBlock, 0,
                      t5, t6, t7);
  dataBlock.set_size(offset_ - dataBlock.offset());
  if (!s.ok()) {
    return s;
  }
  s = WriteBlock(offset_info_.dump(), file_, &offset_, &offsetBlock);
  if (!s.ok()) {
    return s;
  }
  if (!range_del_block_.empty()) {
    s = WriteBlock(range_del_block_.Finish(), file_, &offset_, &tombstoneBlock);
    if (!s.ok()) {
      return s;
    }
  }
  range_del_block_.Reset();
  if (!dict.memory.empty()) {
    s = WriteBlock(MmapWholeFile(tmpDictFile).memory(), file_, &offset_,
                   &dictBlock);
    if (!s.ok()) {
      return s;
    }
  }
  auto& stat = kvs.status.stat;
  properties_.num_data_blocks = stat.keyCount;
  kv_freq_.finish();
  size_t entropy = freq_hist_o1::estimate_size(kv_freq_.histogram());
  WriteMetaData(
      dictInfo, entropy,
      {
          {!dict.memory.empty() ? &kTerarkZipTableValueDictBlock : NULL,
           dictBlock},
          {&kTerarkZipTableOffsetBlock, offsetBlock},
          {!tombstoneBlock.IsNull() ? &kRangeDelBlock : NULL, tombstoneBlock},
      });

  size_t sumKeyLen = stat.sumKeyLen;
  long long t8 = g_pf.now();
  {
    std::unique_lock<std::mutex> lock(g_sumMutex);
    g_sumKeyLen += properties_.raw_key_size;
    g_sumValueLen += properties_.raw_value_size;
    g_sumUserKeyLen += sumKeyLen;
    g_sumUserKeyNum += stat.keyCount;
    g_sumEntryNum += properties_.num_entries;
  }
  size_t dictBlockSize = dict.memory.empty() ? 0 : dictBlock.size();
  INFO(
      ioptions_.info_log,
      "TerarkZipTableBuilder::Finish():this=%12p:\n"
      "  second pass time =%8.2f's,%8.3f'MB/sec, value only(%4.1f%% of KV)\n"
      "   wait indexing time = %7.2f's,\n"
      "  remap KeyValue time = %7.2f's, %8.3f'MB/sec (all stages of remap)\n"
      "    Get OrderMap time = %7.2f's, %8.3f'MB/sec (index lex order gen)\n"
      "  rebuild zvType time = %7.2f's, %8.3f'MB/sec\n"
      "  write SST data time = %7.2f's, %8.3f'MB/sec\n"
      "   dict compress time = %7.2f's, %8.3f'MB/sec\n"
      "    z-dict build time = %7.2f's, sample length = %7.3f'MB, throughput = "
      "%6.3f'MB/sec\n"
      "    zip my value time = %7.2f's, unzip  length = %7.3f'GB\n"
      "    zip my value throughput = %7.3f'MB/sec\n"
      "    zip pipeline throughput = %7.3f'MB/sec\n"
      "    entries = %zd  avg-key = %.2f  avg-zkey = %.2f  avg-val = %.2f  "
      "avg-zval = %.2f\n"
      "    usrkeys = %zd  avg-key = %.2f  avg-zkey = %.2f  avg-val = %.2f  "
      "avg-zval = %.2f\n"
      "    seq expand size = %zd  multi value expand size = %zd entropy size = "
      "%.4f GB\n"
      "    UnZipSize{ index =%9.4f GB  value =%9.4f GB  dict =%7.2f MB  all "
      "=%9.4f GB }\n"
      "    __ZipSize{ index =%9.4f GB  value =%9.4f GB  dict =%7.2f MB  all "
      "=%9.4f GB }\n"
      "    UnZip/Zip{ index =%9.4f     value =%9.4f     dict =%7.2f     all "
      "=%9.4f    }\n"
      "    Zip/UnZip{ index =%9.4f     value =%9.4f     dict =%7.2f     all "
      "=%9.4f    }\n"
      "----------------------------\n"
      "    total value len =%14.6f GB     avg =%8.3f KB (by entry num)\n"
      "    total  key  len =%14.6f GB     avg =%8.3f KB\n"
      "    total ukey  len =%14.6f GB     avg =%8.3f KB\n"
      "    total ukey  num =%17.9f Billion\n"
      "    total entry num =%17.9f Billion\n"
      "    write speed all =%17.9f MB/sec (with    version num)\n"
      "    write speed all =%17.9f MB/sec (without version num)",
      this, g_pf.sf(t3, t4), properties_.raw_value_size * 1.0 / g_pf.uf(t3, t4),
      properties_.raw_value_size * 100.0 / rawBytes

      ,
      g_pf.sf(t4, t5)  // wait indexing time
      ,
      g_pf.sf(t5, t8), double(offset_) / g_pf.uf(t5, t8)

                           ,
      g_pf.sf(t5, t6),
      properties_.index_size / g_pf.uf(t5, t6)  // index lex walk

      ,
      g_pf.sf(t6, t7),
      stat.keyCount * 2 / 8 / (g_pf.uf(t6, t7) + 1.0)  // rebuild zvType

      ,
      g_pf.sf(t7, t8), double(offset_) / g_pf.uf(t7, t8)  // write SST data

      ,
      g_pf.uf(td) / 1e6, dictSize_ / (g_pf.uf(td) + 1.0)  // dict compress

      ,
      dzstat.dictBuildTime, realsampleLenSum / 1e6  // z-dict build
      ,
      realsampleLenSum / dzstat.dictBuildTime / 1e6

      ,
      dzstat.dictZipTime, properties_.raw_value_size / 1e9  // zip my value
      ,
      properties_.raw_value_size / dzstat.dictZipTime / 1e6,
      dzstat.pipelineThroughBytes / dzstat.dictZipTime / 1e6

      ,
      size_t(properties_.num_entries),
      double(properties_.raw_key_size) / properties_.num_entries,
      double(properties_.index_size) / properties_.num_entries,
      double(properties_.raw_value_size) / properties_.num_entries,
      double(properties_.data_size) / properties_.num_entries

      ,
      stat.keyCount, double(sumKeyLen) / stat.keyCount,
      double(properties_.index_size) / stat.keyCount,
      double(properties_.raw_value_size + seqExpandSize_ +
             multiValueExpandSize_) /
          stat.keyCount,
      double(properties_.data_size) / stat.keyCount

      ,
      seqExpandSize_, multiValueExpandSize_, entropy / 1e9

      ,
      sumKeyLen / 1e9, properties_.raw_value_size / 1e9, dictSize_ / 1e6,
      rawBytes / 1e9

      ,
      properties_.index_size / 1e9, properties_.data_size / 1e9,
      dictBlockSize / 1e6, offset_ / 1e9

      ,
      double(sumKeyLen) / properties_.index_size,
      double(properties_.raw_value_size) / properties_.data_size,
      double(dictSize_) / std::max<size_t>(dictBlockSize, 1),
      double(rawBytes) / offset_

      ,
      properties_.index_size / double(sumKeyLen),
      properties_.data_size / double(properties_.raw_value_size),
      dictBlockSize / std::max<double>(dictSize_, 1), offset_ / double(rawBytes)

                                                          ,
      g_sumValueLen / 1e9, g_sumValueLen / 1e3 / g_sumEntryNum,
      g_sumKeyLen / 1e9, g_sumKeyLen / 1e3 / g_sumEntryNum,
      g_sumUserKeyLen / 1e9, g_sumUserKeyLen / 1e3 / g_sumUserKeyNum,
      g_sumUserKeyNum / 1e9, g_sumEntryNum / 1e9,
      (g_sumKeyLen + g_sumValueLen) / g_pf.uf(g_lastTime, t8),
      (g_sumKeyLen + g_sumValueLen - g_sumEntryNum * 8) /
          g_pf.uf(g_lastTime, t8));
  return s;
}

Status TerarkZipTableBuilder::WriteSSTFileMulti(
    long long t3, long long t4, long long td, fstring tmpDictFile,
    const std::string& dictInfo, uint64_t dictHash,
    const DictZipBlobStore::ZipStat& dzstat) {
  assert(prefixBuildInfos_.size() > 1);
  terark::MmapWholeFile dictMmap;
  AbstractBlobStore::Dictionary dict(dictSize_, dictHash);
  const size_t realsampleLenSum = dict.memory.size();
  long long rawBytes = properties_.raw_key_size + properties_.raw_value_size;
  long long t5 = g_pf.now();
  terark::MmapWholeFile mmapIndexFile(tmpIndexFile_.fpath);
  terark::MmapWholeFile mmapStoreFile;
  terark::MmapWholeFile mmapZipStoreFile;
  assert(mmapIndexFile.base != nullptr);
  if (tmpStoreFileSize_ != 0) {
    terark::MmapWholeFile(tmpStoreFile_.fpath).swap(mmapStoreFile);
    assert(mmapStoreFile.base != nullptr);
  }
  if (tmpZipStoreFileSize_ != 0) {
    terark::MmapWholeFile(tmpZipStoreFile_.fpath).swap(mmapZipStoreFile);
    assert(mmapZipStoreFile.base != nullptr);
  }
  Status s;
  BlockHandle dataBlock, dictBlock, offsetBlock, tombstoneBlock(0, 0);
  offset_info_.Init(prefixBuildInfos_.size());
  size_t typeSize = 0;
  for (auto& kvs : prefixBuildInfos_) {
    typeSize += kvs->type.mem_size();
  }
  {
    size_t real_size =
        TerarkZipMultiOffsetInfo::calc_size(prefixBuildInfos_.size()) +
        mmapIndexFile.size + mmapStoreFile.size + typeSize;
    size_t block_size, last_allocated_block;
    file_->writable_file()->GetPreallocationStatus(&block_size,
                                                   &last_allocated_block);
    INFO(ioptions_.info_log,
         "TerarkZipTableBuilder::Finish():this=%12p:\n old prealloc_size = "
         "%zd, real_size = %zd\n",
         this, block_size, real_size);
    file_->writable_file()->SetPreallocationBlockSize((1ull << 20) + real_size);
  }
  long long t6 = t5, t7 = t5;
  offset_ = 0;
  auto getMmapPart = [](terark::MmapWholeFile& mmap, size_t beg, size_t end) {
    assert(beg <= end);
    assert(end <= mmap.size);
    auto base = (const char*)(mmap.base);
    return fstring(base + beg, base + end);
  };
  size_t sumKeyLen = 0;
  size_t numKeys = 0;
  dataBlock.set_offset(offset_);
  for (size_t i = 0; i < prefixBuildInfos_.size(); ++i) {
    size_t kvs_index =
        isReverseBytewiseOrder_ ? prefixBuildInfos_.size() - 1 - i : i;
    auto& kvs = *prefixBuildInfos_[kvs_index];
    sumKeyLen += kvs.status.stat.sumKeyLen;
    numKeys += kvs.status.stat.keyCount;
    unique_ptr<AbstractBlobStore> store(
        AbstractBlobStore::load_from_user_memory(
            getMmapPart(kvs.isUseDictZip ? mmapZipStoreFile : mmapStoreFile,
                        kvs.valueFileBegin, kvs.valueFileEnd),
            dict));
    long long t6p, t7p;
    s = WriteIndexStore(mmapIndexFile.memory(), store.get(), kvs, dataBlock, i,
                        t7, t6p, t7p);
    if (!s.ok()) {
      return s;
    }
    t6 += t6p - t7;
    t7 = t7p;
  }
  dataBlock.set_size(offset_ - dataBlock.offset());
  s = WriteBlock(offset_info_.dump(), file_, &offset_, &offsetBlock);
  if (!s.ok()) {
    return s;
  }
  if (!range_del_block_.empty()) {
    s = WriteBlock(range_del_block_.Finish(), file_, &offset_, &tombstoneBlock);
    if (!s.ok()) {
      return s;
    }
  }
  range_del_block_.Reset();
  if (!dict.memory.empty()) {
    s = WriteBlock(MmapWholeFile(tmpDictFile).memory(), file_, &offset_,
                   &dictBlock);
    if (!s.ok()) {
      return s;
    }
  }
  properties_.num_data_blocks = numKeys;
  kv_freq_.finish();
  size_t entropy = freq_hist_o1::estimate_size(kv_freq_.histogram());
  WriteMetaData(
      dictInfo, entropy,
      {
          {!dict.memory.empty() ? &kTerarkZipTableValueDictBlock : NULL,
           dictBlock},
          {&kTerarkZipTableOffsetBlock, offsetBlock},
          {!tombstoneBlock.IsNull() ? &kRangeDelBlock : NULL, tombstoneBlock},
      });
  size_t dictBlockSize = dict.memory.empty() ? 0 : dictBlock.size();
  long long t8 = g_pf.now();
  {
    std::unique_lock<std::mutex> lock(g_sumMutex);
    g_sumKeyLen += properties_.raw_key_size;
    g_sumValueLen += properties_.raw_value_size;
    g_sumUserKeyLen += sumKeyLen;
    g_sumUserKeyNum += numKeys;
    g_sumEntryNum += properties_.num_entries;
  }
  INFO(
      ioptions_.info_log,
      "TerarkZipTableBuilder::FinishMulti():this=%12p:\n"
      "  second pass time =%8.2f's,%8.3f'MB/sec, value only(%4.1f%% of KV)\n"
      "   wait indexing time = %7.2f's,\n"
      "  remap KeyValue time = %7.2f's, %8.3f'MB/sec (all stages of remap)\n"
      "    Get OrderMap time = %7.2f's, %8.3f'MB/sec (index lex order gen)\n"
      "  rebuild zvType time = %7.2f's, %8.3f'MB/sec\n"
      "  write SST data time = %7.2f's, %8.3f'MB/sec\n"
      "   dict compress time = %7.2f's, %8.3f'MB/sec\n"
      "    z-dict build time = %7.2f's, sample length = %7.3f'MB, throughput = "
      "%6.3f'MB/sec\n"
      "    zip my value time = %7.2f's, unzip  length = %7.3f'GB\n"
      "    zip my value throughput = %7.3f'MB/sec\n"
      "    zip pipeline throughput = %7.3f'MB/sec\n"
      "    entries = %zd  avg-key = %.2f  avg-zkey = %.2f  avg-val = %.2f  "
      "avg-zval = %.2f\n"
      "    usrkeys = %zd  avg-key = %.2f  avg-zkey = %.2f  avg-val = %.2f  "
      "avg-zval = %.2f\n"
      "    seq expand size = %zd  multi value expand size = %zd entropy size = "
      "%0.4f GB\n"
      "    UnZipSize{ index =%9.4f GB  value =%9.4f GB  dict =%7.2f MB  all "
      "=%9.4f GB }\n"
      "    __ZipSize{ index =%9.4f GB  value =%9.4f GB  dict =%7.2f MB  all "
      "=%9.4f GB }\n"
      "    UnZip/Zip{ index =%9.4f     value =%9.4f     dict =%7.2f     all "
      "=%9.4f    }\n"
      "    Zip/UnZip{ index =%9.4f     value =%9.4f     dict =%7.2f     all "
      "=%9.4f    }\n"
      "----------------------------\n"
      "    total value len =%14.6f GB     avg =%8.3f KB (by entry num)\n"
      "    total  key  len =%14.6f GB     avg =%8.3f KB\n"
      "    total ukey  len =%14.6f GB     avg =%8.3f KB\n"
      "    total ukey  num =%17.9f Billion\n"
      "    total entry num =%17.9f Billion\n"
      "    write speed all =%17.9f MB/sec (with    version num)\n"
      "    write speed all =%17.9f MB/sec (without version num)",
      this, g_pf.sf(t3, t4), properties_.raw_value_size * 1.0 / g_pf.uf(t3, t4),
      properties_.raw_value_size * 100.0 / rawBytes

      ,
      g_pf.sf(t4, t5)  // wait indexing time
      ,
      g_pf.sf(t5, t8), double(offset_) / g_pf.uf(t5, t8)

                           ,
      g_pf.sf(t5, t6),
      properties_.index_size / g_pf.uf(t5, t6)  // index lex walk

      ,
      g_pf.sf(t6, t7),
      numKeys * 2 / 8 / (g_pf.uf(t6, t7) + 1.0)  // rebuild zvType

      ,
      g_pf.sf(t7, t8), double(offset_) / g_pf.uf(t7, t8)  // write SST data

      ,
      g_pf.uf(td) / 1e6, dictSize_ / (g_pf.uf(td) + 1.0)  // dict compress

      ,
      dzstat.dictBuildTime, realsampleLenSum / 1e6  // z-dict build
      ,
      realsampleLenSum / dzstat.dictBuildTime / 1e6

      ,
      dzstat.dictZipTime, properties_.raw_value_size / 1e9  // zip my value
      ,
      properties_.raw_value_size / dzstat.dictZipTime / 1e6,
      dzstat.pipelineThroughBytes / dzstat.dictZipTime / 1e6

      ,
      size_t(properties_.num_entries),
      double(properties_.raw_key_size) / properties_.num_entries,
      double(properties_.index_size) / properties_.num_entries,
      double(properties_.raw_value_size) / properties_.num_entries,
      double(properties_.data_size) / properties_.num_entries

      ,
      numKeys, double(sumKeyLen) / numKeys,
      double(properties_.index_size) / numKeys,
      double(properties_.raw_value_size + seqExpandSize_ +
             multiValueExpandSize_) /
          numKeys,
      double(properties_.data_size) / numKeys

      ,
      seqExpandSize_, multiValueExpandSize_, entropy / 1e9

      ,
      sumKeyLen / 1e9, properties_.raw_value_size / 1e9, dictSize_ / 1e6,
      rawBytes / 1e9

      ,
      properties_.index_size / 1e9, properties_.data_size / 1e9,
      dictBlockSize / 1e6, offset_ / 1e9

      ,
      double(sumKeyLen) / properties_.index_size,
      double(properties_.raw_value_size) / properties_.data_size,
      double(dictSize_) / std::max<double>(dictBlockSize, 1),
      double(rawBytes) / offset_

      ,
      properties_.index_size / double(sumKeyLen),
      properties_.data_size / double(properties_.raw_value_size),
      dictBlockSize / std::max<double>(dictSize_, 1), offset_ / double(rawBytes)

                                                          ,
      g_sumValueLen / 1e9, g_sumValueLen / 1e3 / g_sumEntryNum,
      g_sumKeyLen / 1e9, g_sumKeyLen / 1e3 / g_sumEntryNum,
      g_sumUserKeyLen / 1e9, g_sumUserKeyLen / 1e3 / g_sumUserKeyNum,
      g_sumUserKeyNum / 1e9, g_sumEntryNum / 1e9,
      (g_sumKeyLen + g_sumValueLen) / g_pf.uf(g_lastTime, t8),
      (g_sumKeyLen + g_sumValueLen - g_sumEntryNum * 8) /
          g_pf.uf(g_lastTime, t8));
  return s;
}

Status TerarkZipTableBuilder::WriteMetaData(
    const std::string& dictInfo, size_t entropy,
    std::initializer_list<std::pair<const std::string*, BlockHandle>> blocks) {
  MetaIndexBuilder metaindexBuiler;
  for (const auto& block : blocks) {
    if (block.first) {
      metaindexBuiler.Add(*block.first, block.second);
    }
  }
  PropertyBlockBuilder propBlockBuilder;
  propBlockBuilder.AddTableProperty(properties_);
  UserCollectedProperties user_collected_properties;
  for (auto& collector : collectors_) {
    user_collected_properties.clear();
    Status s = collector->Finish(&user_collected_properties);
    if (!s.ok()) {
      LogPropertiesCollectionError(ioptions_.info_log, "Finish",
                                   collector->Name());
      continue;
    }
    s = MyRocksTablePropertiesCollectorHack(collector.get(), offset_info_,
                                            user_collected_properties,
                                            isReverseBytewiseOrder_);
    if (!s.ok()) {
      WARN(ioptions_.info_log, "MyRocksTablePropertiesCollectorHack fail: %s",
           s.ToString().c_str());
    }
    propBlockBuilder.Add(user_collected_properties);
  }
  propBlockBuilder.Add(kTerarkZipTableBuildTimestamp, GetTimestamp());
  if (entropy > 0) {
    propBlockBuilder.Add(kTerarkZipTableEntropy, terark::lcast(entropy));
  }
  if (!dictInfo.empty()) {
    propBlockBuilder.Add(kTerarkZipTableDictInfo, dictInfo);
  }
  BlockHandle propBlock, metaindexBlock;
  Status s = WriteBlock(propBlockBuilder.Finish(), file_, &offset_, &propBlock);
  if (!s.ok()) {
    return s;
  }
  metaindexBuiler.Add(kPropertiesBlock, propBlock);
  Slice metaindexData = metaindexBuiler.Finish();
  s = WriteBlock(metaindexData, file_, &offset_, &metaindexBlock);
  if (s.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = kNoCompression;
    void* xxh = XXH32_init(0);
    XXH32_update(xxh, metaindexData.data(),
                 static_cast<uint32_t>(metaindexData.size()));
    XXH32_update(xxh, trailer, 1);  // Extend  to cover block type
    EncodeFixed32(trailer + 1, XXH32_digest(xxh));
    s = file_->Append(Slice(trailer, kBlockTrailerSize));
    offset_ += kBlockTrailerSize;
  }
  if (!s.ok()) {
    return s;
  }
  Footer footer(kTerarkZipTableMagicNumber, 0);
  footer.set_metaindex_handle(metaindexBlock);
  footer.set_index_handle(BlockHandle::NullBlockHandle());
  footer.set_checksum(kxxHash);
  std::string footer_encoding;
  footer.EncodeTo(&footer_encoding);
  s = file_->Append(footer_encoding);
  if (s.ok()) {
    offset_ += footer_encoding.size();
  }
  return s;
}

void TerarkZipTableBuilder::Abandon() {
  closed_ = true;
  ioptions_.env->UnSchedule(&indexTag, TERARKDB_NAMESPACE::Env::Priority::LOW);
  ioptions_.env->UnSchedule(&storeTag, TERARKDB_NAMESPACE::Env::Priority::LOW);
  ioptions_.env->UnSchedule(&dictTag, TERARKDB_NAMESPACE::Env::Priority::LOW);
  for (auto& kvs : prefixBuildInfos_) {
    if (!kvs) {
      continue;
    }
    if (kvs->indexWait && kvs->indexWait->valid()) {
      kvs->indexWait->wait();
    } else {
      for (auto& pair : kvs->status.fileVec) {
        pair->key.close();
      }
    }
    if (kvs->storeWait && kvs->storeWait->valid()) {
      kvs->storeWait->wait();
    }
  }
  prefixBuildInfos_.clear();
  if (tmpSentryFile_.fp) {
    tmpSentryFile_.complete_write();
  }
  if (tmpSampleFile_.fp) {
    tmpSampleFile_.complete_write();
  }
  tmpIndexFile_.Delete();
  tmpStoreFile_.Delete();
  tmpZipDictFile_.Delete();
  tmpZipValueFile_.Delete();
}

// based on Abandon
Status TerarkZipTableBuilder::AbortFinish(const std::exception& ex) {
  TerarkZipTableBuilder::Abandon();
  return Status::Corruption("exception", ex.what());
}

void TerarkZipTableBuilder::AddPrevUserKey(
    size_t samePrefix, std::initializer_list<RangeStatus*> r,
    std::initializer_list<RangeStatus*> e) {
  uint64_t seq, seqType = *(uint64_t*)valueBuf_.strpool.data();
  ValueType type;
  UnPackSequenceAndType(seqType, &seq, &type);
  const size_t vNum = valueBuf_.size();
  size_t valueLen = 0;
  bool zeroSeq = false;
  if (vNum == 1 && (kTypeDeletion == type || kTypeValue == type)) {
    if (0 == seq && kTypeValue == type) {
      valueLen = valueBuf_.strpool.size() - 8;
      freq_[0]->v.add_record(fstring(valueBuf_.strpool.data() + 8, valueLen));
      zeroSeq = true;
    } else {
      valueLen = valueBuf_.strpool.size() - 1;
      freq_[0]->v.add_record(fstring(valueBuf_.strpool.data() + 1, valueLen));
      seqExpandSize_ += 7;
    }
  } else {
    size_t headerSize = ZipValueMultiValue::calcHeaderSize(vNum);
    valueLen = valueBuf_.strpool.size() + headerSize;
    valueTestBuf_.reserve(valueLen);
    valueTestBuf_.risk_set_size(headerSize);
    ZipValueMultiValue* zmValue = (ZipValueMultiValue*)valueTestBuf_.data();
    zmValue->offsets[0] = uint32_t(vNum);
    for (size_t i = 1; i < vNum; ++i) {
      fstring v = valueBuf_[i - 1];
      valueTestBuf_.append(v);
      zmValue->offsets[i] = uint32_t(valueTestBuf_.size() - headerSize);
    }
    valueTestBuf_.append(valueBuf_.back());
    freq_[0]->v.add_record(valueTestBuf_);
    seqExpandSize_ += vNum * 8;
    multiValueExpandSize_ += headerSize;
  }
  valueBuf_.erase_all();
  auto prevUserKey = fstringOf(prevKey_.user_key());
  if (keyDataSize_ == 0) {
    filePair_->key.writer << var_uint64_t(0) << prevUserKey;
  } else {
    filePair_->key.writer << var_uint64_t(prevSamePrefix_)
                          << prevUserKey.substr(prevSamePrefix_);
  }
  prevSamePrefix_ = samePrefix;
  for (auto ptr : r) {
    ptr->AddKey(prevUserKey, prefixLen_, samePrefix, valueLen, zeroSeq);
  }
  for (auto ptr : e) {
    ptr->AddKey(prevUserKey, prefixLen_, size_t(-1), valueLen, zeroSeq);
  }
  freq_[0]->k.add_record(prevUserKey);
}

void TerarkZipTableBuilder::AddValueBit() {
  r00_->AddValueBit();
  r10_->AddValueBit();
  r20_->AddValueBit();
}

bool TerarkZipTableBuilder::MergeRangeStatus(RangeStatus* aa, RangeStatus* bb,
                                             RangeStatus* ab,
                                             size_t entropyLen) {
  ab->stat.entropyLen = entropyLen;
  aa->stat.entropyLen = entropyLen * aa->stat.sumKeyLen / ab->stat.sumKeyLen;
  bb->stat.entropyLen = entropyLen * bb->stat.sumKeyLen / ab->stat.sumKeyLen;
  assert(aa->stat.keyCount + bb->stat.keyCount == ab->stat.keyCount);
  assert(aa->stat.sumKeyLen + bb->stat.sumKeyLen == ab->stat.sumKeyLen);
  assert(aa->stat.minKey == ab->stat.minKey);
  assert(bb->stat.maxKey == ab->stat.maxKey);
  assert((aa->stat.maxKey < bb->stat.minKey) ^ isReverseBytewiseOrder_);
  auto aai = TerarkIndex::GetPrefixBuildInfo(tiopt_, aa->stat);
  auto bbi = TerarkIndex::GetPrefixBuildInfo(tiopt_, bb->stat);
  using info_t = TerarkIndex::PrefixBuildInfo;
  if (aai.type == info_t::nest_louds_trie &&
      bbi.type == info_t::nest_louds_trie) {
    return true;
  }
  if (aai.type == info_t::crit_bit_trie && bbi.type == info_t::crit_bit_trie) {
    return true;
  }
  if (aai.type == info_t::nest_louds_trie ||
      aai.type == info_t::crit_bit_trie ||
      bbi.type == info_t::nest_louds_trie ||
      bbi.type == info_t::crit_bit_trie) {
    return false;
  }
  auto abi = TerarkIndex::GetPrefixBuildInfo(tiopt_, ab->stat);
  if (abi.type == info_t::nest_louds_trie ||
      abi.type == info_t::crit_bit_trie) {
    return false;
  }
  return aai.estimate_size + bbi.estimate_size >= aai.estimate_size * 0.9;
}

TableBuilder* createTerarkZipTableBuilder(
    const TerarkZipTableFactory* table_factory,
    const TerarkZipTableOptions& tzo, const TableBuilderOptions& tbo,
    uint32_t column_family_id, WritableFileWriter* file,
    uint32_t key_prefixLen) {
  return new TerarkZipTableBuilder(table_factory, tzo, tbo, column_family_id,
                                   file, key_prefixLen);
}

std::string ParseTerarkZipTableOption(const std::string& name,
                                      const std::string& org_value,
                                      TerarkZipTableOptions* new_options,
                                      bool input_strings_escaped = false,
                                      bool ignore_unknown_options = false) {
  const std::string& value =
      input_strings_escaped ? UnescapeOptionString(org_value) : org_value;
  const auto iter = terark_zip_table_type_info.find(name);
  if (iter == terark_zip_table_type_info.end()) {
    if (ignore_unknown_options) {
      return "";
    } else {
      return "Unrecognized option";
    }
  }
  const auto& opt_info = iter->second;
  if (opt_info.verification != OptionVerificationType::kDeprecated &&
      !ParseOptionHelper(reinterpret_cast<char*>(new_options) + opt_info.offset,
                         opt_info.type, value)) {
    return "Invalid value";
  }
  return "";
}

Status GetTerarkZipTableOptionsFromMap(
    const TerarkZipTableOptions& table_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    TerarkZipTableOptions* new_table_options, bool input_strings_escaped,
    bool ignore_unknown_options) {
  assert(new_table_options);
  *new_table_options = table_options;
  for (const auto& o : opts_map) {
    auto error_message = ParseTerarkZipTableOption(
        o.first, o.second, new_table_options, input_strings_escaped,
        ignore_unknown_options);
    if (error_message != "") {
      const auto iter = terark_zip_table_type_info.find(o.first);
      if (iter == terark_zip_table_type_info.end() ||
          !input_strings_escaped ||  // !input_strings_escaped indicates
                                     // the old API, where everything is
                                     // parsable.
          (iter->second.verification != OptionVerificationType::kByName &&
           iter->second.verification !=
               OptionVerificationType::kByNameAllowNull &&
           iter->second.verification !=
               OptionVerificationType::kByNameAllowFromNull &&
           iter->second.verification != OptionVerificationType::kDeprecated)) {
        // Restore "new_options" to the default "base_options".
        *new_table_options = table_options;
        return Status::InvalidArgument("Can't parse TerarkZipTableOption:",
                                       o.first + " " + error_message);
      }
    }
  }
  return Status::OK();
}

Status GetTerarkZipTableOptionsFromString(
    const TerarkZipTableOptions& table_options, const std::string& opts_str,
    TerarkZipTableOptions* new_table_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    return s;
  }

  return GetTerarkZipTableOptionsFromMap(table_options, opts_map,
                                         new_table_options);
}

}  // namespace TERARKDB_NAMESPACE
