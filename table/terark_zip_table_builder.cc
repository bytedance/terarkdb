// project headers
#include "terark_zip_table_builder.h"
// std headers
#include <future>
#include <cfloat>
// boost headers
// rocksdb headers
#include <rocksdb/merge_operator.h>
#include <table/meta_blocks.h>
// terark headers
#include <terark/util/sortable_strvec.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/lcast.hpp>
#include <terark/num_to_str.hpp>
#include <terark/zbs/zero_length_blob_store.hpp>
#include <terark/zbs/plain_blob_store.hpp>
#include <terark/zbs/mixed_len_blob_store.hpp>
#include <terark/zbs/zip_offset_blob_store.hpp>
// third party
#include <zstd/zstd.h>

namespace rocksdb {

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
}
static std::mutex zipMutex;
static std::condition_variable zipCond;
static valvec<PendingTask> waitQueue;
static size_t sumWaitingMem = 0;
static size_t sumWorkingMem = 0;

template<class ByteArray>
static
Status WriteBlock(const ByteArray& blockData, WritableFileWriter* file,
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
    uint64_t timestamp = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    return terark::lcast(timestamp);
}

///////////////////////////////////////////////////////////////////
// hack MyRocks Rdb_tbl_prop_coll
rocksdb::Status MyRocksTablePropertiesCollectorHack(IntTblPropCollector* collector,
                                                    const TerarkZipMultiOffsetInfo& offset_info,
                                                    UserCollectedProperties &user_collected_properties,
                                                    bool is_reverse_bytewise_order) {
  if (fstring(collector->Name()) != "Rdb_tbl_prop_coll") {
    return Status::OK();
  }
  if (offset_info.prefixLen_ != 4) { // prefix mismatch, can't hack ...
    return Status::OK();
  }
  auto find = user_collected_properties.find("__indexstats__");
  assert(find != user_collected_properties.end());
  if (find == user_collected_properties.end()) {
    return Status::OK();
  }
  std::string Rdb_index_stats = find->second;
  terark::BigEndianDataInput <terark::MemIO> input;
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
  auto getActualSize = [&offset_info](size_t i)->uint64_t {
    auto &info = offset_info.offset_[i];
    if (i == 0)
      return info.key + info.value;
    auto &last = offset_info.offset_[i - 1];
    return info.key + info.value - last.key - last.value;
  };
  for (size_t i = 0; i < offset_info.partCount_; ++i) {
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
    if (version >= 2) { // INDEX_STATS_VERSION_ENTRY_TYPES
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
  return Status::OK();
}
///////////////////////////////////////////////////////////////////

class TerarkZipTableBuilderTask : public PipelineTask {
public:
  std::promise<Status> promise;
  std::function<Status()> func;
};

class TerarkZipTableBuilderStage : public PipelineStage
{
protected:
  void process(int threadno, PipelineQueueItem* item) {
    auto task = static_cast<TerarkZipTableBuilderTask*>(item->task);
    Status s;
    try {
      s = task->func();
    }
    catch (const std::exception& ex) {
      s = Status::Aborted("exception", ex.what());
    }
    task->promise.set_value(s);
  }

public:
  TerarkZipTableBuilderStage()
    : PipelineStage(3)
  {
    m_step_name = "build";
  }
};

TerarkZipTableBuilder::TerarkZipTableBuilder(const TerarkZipTableFactory* table_factory,
                                             const TerarkZipTableOptions& tzto,
                                             const TableBuilderOptions& tbo,
                                             uint32_t column_family_id,
                                             WritableFileWriter* file,
                                             size_t key_prefixLen)
  : table_options_(tzto)
  , table_factory_(table_factory)
  , ioptions_(tbo.ioptions)
  , range_del_block_(1)
  , key_prefixLen_(key_prefixLen)
{
try {
  singleIndexMemLimit = std::min(table_options_.softZipWorkingMemLimit,
    table_options_.singleIndexMemLimit);

  estimateRatio_ = table_factory_->GetCollect().estimate(table_options_.estimateCompressionRatio);

  properties_.fixed_key_len = 0;
  properties_.num_data_blocks = 1;
  properties_.column_family_id = column_family_id;
  properties_.column_family_name = tbo.column_family_name;
  properties_.comparator_name = ioptions_.user_comparator ?
    ioptions_.user_comparator->Name() : "nullptr";
  properties_.merge_operator_name = ioptions_.merge_operator ?
    ioptions_.merge_operator->Name() : "nullptr";
  properties_.compression_name = CompressionTypeToString(tbo.compression_type);
  properties_.prefix_extractor_name = ioptions_.prefix_extractor ?
    ioptions_.prefix_extractor->Name() : "nullptr";

  isReverseBytewiseOrder_ =
    fstring(properties_.comparator_name).startsWith("rev:");
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
  isUint64Comparator_ =
    fstring(properties_.comparator_name) == "rocksdb.Uint64Comparator";
#endif

  if (tbo.int_tbl_prop_collector_factories) {
    const auto& factories = *tbo.int_tbl_prop_collector_factories;
    collectors_.resize(factories.size());
    auto cfId = properties_.column_family_id;
    for (size_t i = 0; i < collectors_.size(); ++i) {
      collectors_[i].reset(factories[i]->CreateIntTblPropCollector(cfId));
    }
  }

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
  sampleUpperBound_ = randomGenerator_.max() * table_options_.sampleRatio;
  tmpSentryFile_.path = tzto.localTempDir + "/Terark-XXXXXX";
  tmpSentryFile_.open_temp();
  tmpSampleFile_.path = tmpSentryFile_.path + ".sample";
  tmpSampleFile_.open();
  tmpIndexFile_.fpath = tmpSentryFile_.path + ".index";
  tmpStoreFile_.fpath = tmpSentryFile_.path + ".bs";
  tmpZipStoreFile_.fpath = tmpSentryFile_.path + ".zbs";
  if (table_options_.debugLevel == 3) {
    tmpDumpFile_.open(tmpSentryFile_.path + ".dump", "wb+");
  }

  if (tzto.isOfflineBuild) {
    if (tbo.compression_dict && tbo.compression_dict->size()) {
      auto data = (byte_t*)tbo.compression_dict->data();
      auto size = tbo.compression_dict->size();
      tmpZipValueFile_.fpath = tmpSentryFile_.path + ".zbs";
      tmpZipDictFile_.fpath = tmpSentryFile_.path + ".zbs-dict";
      valvec<byte_t> strDict(data, size);
#if defined(MADV_DONTNEED)
      madvise(data, size, MADV_DONTNEED);
#endif
      zbuilder_.reset(this->createZipBuilder());
      zbuilder_->useSample(strDict); // take ownership of strDict
                                      //zbuilder_->finishSample(); // do not call finishSample here
      zbuilder_->prepare(1024, tmpZipValueFile_.fpath);
    }
  }
  pipeline_.m_silent = true;
  pipeline_ >> new TerarkZipTableBuilderStage;
  pipeline_.setQueueSize(50); // we thought it's enough ...
  pipeline_.compile();
}
catch (const std::exception& ex) {
  WARN_EXCEPT(tbo.ioptions.info_log
      , "%s: Exception: %s", BOOST_CURRENT_FUNCTION, ex.what());
  throw;
}
}

DictZipBlobStore::ZipBuilder*
TerarkZipTableBuilder::createZipBuilder() const {
  DictZipBlobStore::Options dzopt;
  dzopt.entropyAlgo = DictZipBlobStore::Options::EntropyAlgo(table_options_.entropyAlgo);
  dzopt.checksumLevel = table_options_.checksumLevel;
  dzopt.offsetArrayBlockUnits = table_options_.offsetArrayBlockUnits;
  dzopt.useSuffixArrayLocalMatch = table_options_.useSuffixArrayLocalMatch;
  return DictZipBlobStore::createZipBuilder(dzopt);
}

TerarkZipTableBuilder::~TerarkZipTableBuilder() {
  pipeline_.stop();
  pipeline_.wait();
  std::unique_lock<std::mutex> zipLock(zipMutex);
  waitQueue.trim(std::remove_if(waitQueue.begin(), waitQueue.end(),
    [this](PendingTask x) {return this == x.tztb; }));
}

uint64_t TerarkZipTableBuilder::FileSize() const {
  if (0 == offset_) {
    // for compaction caller to split file by increasing size
#if defined(TerocksPrivateCode)
    return estimateOffset_;
#endif // TerocksPrivateCode
    size_t nltTrieMemSize = 0;
    for (auto& item : histogram_) {
      for (auto& ptr : item->build) {
        auto &stat = ptr->stat;
        size_t indexSize = UintVecMin0::compute_mem_size_by_max_val(stat.sumKeyLen, stat.numKeys);
        nltTrieMemSize = std::max(nltTrieMemSize, stat.sumKeyLen + indexSize);
      }
    }
    nltTrieMemSize = nltTrieMemSize * 3 / 2;
    if (nltTrieMemSize < table_options_.softZipWorkingMemLimit) {
      return estimateOffset_;
    }
    else {
      return 1ULL << 60; // notify rocksdb to `Finish()` this table asap.
    }
  }
  else {
    return offset_;
  }
}

TableProperties TerarkZipTableBuilder::GetTableProperties() const {
  TableProperties ret = properties_;
  for (const auto& collector : collectors_) {
    for (const auto& prop : collector->GetReadableProperties()) {
      ret.readable_properties.insert(prop);
    }
    collector->Finish(&ret.user_collected_properties);
    MyRocksTablePropertiesCollectorHack(collector.get(), offset_info_,
                                        ret.user_collected_properties, isReverseBytewiseOrder_);
  }
  return ret;
}

void TerarkZipTableBuilder::Add(const Slice& key, const Slice& value)
try {
  if (table_options_.debugLevel == 3) {
    rocksdb::ParsedInternalKey ikey;
    rocksdb::ParseInternalKey(key, &ikey);
    fprintf(tmpDumpFile_, "DEBUG: 1st pass => %s / %s \n",
        ikey.DebugString(true).c_str(), value.ToString(true).c_str());
  }

  ++properties_.num_entries;
  properties_.raw_key_size += key.size();
  properties_.raw_value_size += value.size();
  uint64_t offset = uint64_t((properties_.raw_key_size + properties_.raw_value_size)
                             * estimateRatio_);
  assert(offset >= estimateOffset_);
  NotifyCollectTableCollectorsOnAdd(key, value, offset,
                                    collectors_, ioptions_.info_log);
  estimateOffset_ = offset;

  uint64_t seqType = DecodeFixed64(key.data() + key.size() - 8);
  ValueType value_type = ValueType(seqType & 255);
  if (IsValueType(value_type)) {
    assert(key.size() >= 8);
    fstring userKey(key.data(), key.size() - 8);
    assert(userKey.size() >= key_prefixLen_);
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
    uint64_t u64_key;
    if (isUint64Comparator_) {
      assert(userKey.size() == 8);
      u64_key = byte_swap(*reinterpret_cast<const uint64_t*>(userKey.data()));
      userKey = fstring(reinterpret_cast<const char*>(&u64_key), 8);
    }
#endif
    auto newBuildIndex = [&] {
      auto newParams = new BuildIndexParams;
      char buffer[32];
      snprintf(buffer, sizeof buffer, ".key.%06zd", nameSeed_++);
      newParams->data.path = tmpSentryFile_.path + buffer;
      newParams->data.open();
      currentStat_ = &newParams->stat;
      return newParams;
    };
    if (terark_likely(!histogram_.empty()
      && histogram_.back()->prefix == userKey.substr(0, key_prefixLen_))) {
      userKey = userKey.substr(key_prefixLen_);
      if (prevUserKey_ != userKey) {
        assert((prevUserKey_ < userKey) ^ isReverseBytewiseOrder_);
#if defined(TerocksPrivateCode)
        size_t indexSize = UintVecMin0::compute_mem_size_by_max_val(
          currentStat_->sumKeyLen, currentStat_->numKeys);
        size_t nltTrieMemSize = currentStat_->sumKeyLen + indexSize;
        if (terark_unlikely(nltTrieMemSize > singleIndexMemLimit)) {
          AddLastUserKey();
          ++histogram_.back()->split;
          BuildIndex(*histogram_.back()->build.back(), *histogram_.back());
          histogram_.back()->build.emplace_back(newBuildIndex());
          currentStat_->minKey.assign(userKey);
        }
        else
#endif // TerocksPrivateCode
        {
          AddPrevUserKey();
        }
        currentStat_->minKeyLen = std::min(userKey.size(), currentStat_->minKeyLen);
        currentStat_->maxKeyLen = std::max(userKey.size(), currentStat_->maxKeyLen);
        prevUserKey_.assign(userKey);
      }
    }
    else {
      if (terark_unlikely(histogram_.empty())) {
        t0 = g_pf.now();
      }
      else {
        AddLastUserKey();
        BuildIndex(*histogram_.back()->build.back(), *histogram_.back());
        BuildStore(*histogram_.back(), nullptr, BuildStoreInit);
      }
      histogram_.emplace_back(new KeyValueStatus);
      auto& currentHistogram = *histogram_.back();
      currentHistogram.build.emplace_back(newBuildIndex());
      currentHistogram.prefix.assign(userKey.data(), key_prefixLen_);
      userKey = userKey.substr(key_prefixLen_);
      currentStat_->minKeyLen = userKey.size();
      currentStat_->maxKeyLen = userKey.size();
      currentStat_->minKey.assign(userKey);
      prevUserKey_.assign(userKey);
      currentHistogram.seqType = seqType;
      char buffer[32];
      snprintf(buffer, sizeof buffer, ".value.%06zd", nameSeed_++);
      currentHistogram.valueFile.path = tmpSentryFile_.path + buffer;
      currentHistogram.valueFile.open();
      keyDataSize_ = 0;
      valueDataSize_ = 0;
    }
    auto& currentHistogram = *histogram_.back();
    keyDataSize_ += userKey.size();
    valueDataSize_ += value.size() + 8;
    currentHistogram.valueBits.push_back(true);
    valueBuf_.emplace_back((char*)&seqType, 8);
    valueBuf_.back_append(value.data(), value.size());
    if (!zbuilder_) {
      if (!value.empty() && randomGenerator_() < sampleUpperBound_) {
        tmpSampleFile_.writer << fstringOf(value);
        sampleLenSum_ += value.size();
      }
      if (second_pass_iter_
        && currentHistogram.valueFile.fp
        && table_options_.debugLevel != 2
        && valueDataSize_ > (1ull << 20)
        && valueDataSize_ > keyDataSize_ * 2) {
        currentHistogram.valueFile.close();
      }
      if (!second_pass_iter_ || currentHistogram.valueFile.fp) {
        currentHistogram.valueFile.writer << seqType;
        currentHistogram.valueFile.writer << fstringOf(value);
      }
    }
  }
  else if (value_type == kTypeRangeDeletion) {
    range_del_block_.Add(key, value);
  }
  else {
    assert(false);
  }
}
catch (const std::exception& ex) {
  WARN_EXCEPT(ioptions_.info_log
      , "%s: Exception: %s", BOOST_CURRENT_FUNCTION, ex.what());
  throw;
}

TerarkZipTableBuilder::WaitHandle::WaitHandle() : myWorkMem(0) {
}
TerarkZipTableBuilder::WaitHandle::WaitHandle(size_t workMem)
  : myWorkMem(workMem) {
}
TerarkZipTableBuilder::WaitHandle::WaitHandle(WaitHandle&& other)
  : myWorkMem(other.myWorkMem) {
  other.myWorkMem = 0;
}
TerarkZipTableBuilder::WaitHandle&
TerarkZipTableBuilder::WaitHandle::operator=(WaitHandle&& other) {
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
TerarkZipTableBuilder::WaitHandle::~WaitHandle() {
  Release(myWorkMem);
}

TerarkZipTableBuilder::WaitHandle
TerarkZipTableBuilder::WaitForMemory(const char* who, size_t myWorkMem) {
  const size_t softMemLimit = table_options_.softZipWorkingMemLimit;
  const size_t hardMemLimit = std::max(table_options_.hardZipWorkingMemLimit, softMemLimit);
  const size_t smallmem = table_options_.smallTaskMemory;
  const std::chrono::seconds waitForTime(10);
  long long myStartTime = 0, now;
  auto shouldWait = [&]() {
    bool w;
    if (myWorkMem < softMemLimit) {
      w = (sumWorkingMem + myWorkMem >= hardMemLimit) ||
        (sumWorkingMem + myWorkMem >= softMemLimit && myWorkMem >= smallmem);
    }
    else {
      w = sumWorkingMem > softMemLimit / 4;
    }
    now = g_pf.now();
    if (!w) {
      assert(!waitQueue.empty());
      if (myWorkMem < smallmem) {
        return false; // do not wait
      }
      if (sumWaitingMem + sumWorkingMem < softMemLimit) {
        return false; // do not wait
      }
      if (waitQueue.size() == 1) {
        assert(this == waitQueue[0].tztb);
        return false; // do not wait
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
        return false; // do not wait
      }
      myStartTime = wq[minRateIdx].startTime;
    }
    return true; // wait
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
    INFO(ioptions_.info_log
      , "TerarkZipTableBuilder::Finish():this=%012p: sumWaitingMem =%8.3f GB, sumWorkingMem =%8.3f GB, %-10s workingMem =%8.4f GB, wait...\n"
      , this, sumWaitingMem / 1e9, sumWorkingMem / 1e9, who, myWorkMem / 1e9
    );
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
  INFO(ioptions_.info_log
    , "TerarkZipTableBuilder::Finish():this=%012p: sumWaitingMem =%8.3f GB, sumWorkingMem =%8.3f GB, %-10s workingMem =%8.4f GB, waited %9.3f sec, Key+Value bytes =%8.3f GB\n"
    , this, sumWaitingMem / 1e9, sumWorkingMem / 1e9, who, myWorkMem / 1e9
    , g_pf.sf(myStartTime, now)
    , (properties_.raw_key_size + properties_.raw_value_size) / 1e9
  );
  sumWaitingMem -= myWorkMem;
  sumWorkingMem += myWorkMem;
  return WaitHandle{myWorkMem};
}

Status TerarkZipTableBuilder::EmptyTableFinish() {
  INFO(ioptions_.info_log
    , "TerarkZipTableBuilder::EmptyFinish():this=%012p\n", this);
  offset_ = 0;
  BlockHandle emptyTableBH, tombstoneBH(0, 0);
  Status s = WriteBlock(Slice("Empty"), file_, &offset_, &emptyTableBH);
  if (!s.ok()) {
    return s;
  }
#if defined(TerocksPrivateCode)
  auto& license = table_factory_->GetLicense();
  BlockHandle licenseHandle;
  s = WriteBlock(SliceOf(license.dump()), file_, &offset_, &licenseHandle);
  if (!s.ok()) {
    return s;
  }
#endif // TerocksPrivateCode
  if (!range_del_block_.empty()) {
    s = WriteBlock(range_del_block_.Finish(), file_, &offset_, &tombstoneBH);
    if (!s.ok()) {
      return s;
    }
  }
  range_del_block_.Reset();
  offset_info_.Init(0, 0);
  return WriteMetaData(std::string(), {
#if defined(TerocksPrivateCode)
    { &kTerarkZipTableExtendedBlock                 , licenseHandle },
#endif // TerocksPrivateCode
    { &kTerarkEmptyTableKey                         , emptyTableBH  },
    { !tombstoneBH.IsNull() ? &kRangeDelBlock : NULL, tombstoneBH   },
  });
}


Status TerarkZipTableBuilder::Finish() try {
  assert(!closed_);
  closed_ = true;

  if (histogram_.empty()) {
    return EmptyTableFinish();
  }
  if (tmpDumpFile_) {
    tmpDumpFile_.flush();
  }

  AddLastUserKey();
  BuildIndex(*histogram_.back()->build.back(), *histogram_.back());
  BuildStore(*histogram_.back(), nullptr, BuildStoreInit);
  if (zbuilder_) {
    return OfflineFinish();
  }

  tmpSampleFile_.complete_write();
  {
    long long rawBytes = properties_.raw_key_size + properties_.raw_value_size;
    long long tt = g_pf.now();
    INFO(ioptions_.info_log
      , "TerarkZipTableBuilder::Finish():this=%012p:  first pass time =%8.2f's,%8.3f'MB/sec\n"
      , this, g_pf.sf(t0, tt), rawBytes*1.0 / g_pf.uf(t0, tt)
    );
  }
  if (histogram_.size() > 1) {
    return ZipValueToFinishMulti();
  }
  return ZipValueToFinish();
}
catch (const std::exception& ex) {
  return AbortFinish(ex);
}

std::future<Status> TerarkZipTableBuilder::Async(std::function<Status()> func) {
  auto task = new TerarkZipTableBuilderTask;
  task->func = std::move(func);
  auto future = task->promise.get_future();
  pipeline_.inqueue(task);
  return future;
}

void TerarkZipTableBuilder::BuildIndex(BuildIndexParams& param, KeyValueStatus& kvs) {
  assert(param.stat.numKeys > 0);
#if defined(TerocksPrivateCode)
  if (kvs.split == 0) {
    param.stat.commonPrefixLen = commonPrefixLen(param.stat.minKey, param.stat.maxKey);
  }
  else if (0)
#endif // TerocksPrivateCode
  {
    // nlt don't support only one key which is empty ...
    if (kvs.split == 0 && param.stat.numKeys > 1) {
      param.stat.commonPrefixLen = commonPrefixLen(param.stat.minKey, param.stat.maxKey);
    }
  }
  param.data.complete_write();
  param.wait = Async([this, &param, &kvs]() {
    auto& keyStat = param.stat;
    const TerarkIndex::Factory* factory;
#if defined(TerocksPrivateCode)
    if (kvs.split != 0) {
      factory = TerarkIndex::GetFactory(table_options_.indexType);
    }
    else
#endif // TerocksPrivateCode
    {
      factory = TerarkIndex::SelectFactory(keyStat, table_options_.indexType);
    }
    if (!factory) {
      THROW_STD(invalid_argument,
        "invalid indexType: %s", table_options_.indexType.c_str());
    }
    NativeDataInput<InputBuffer> tempKeyFileReader(&param.data.fp);
    const size_t myWorkMem = factory->MemSizeForBuild(keyStat);
    auto waitHandle = WaitForMemory("nltTrie", myWorkMem);

    long long t1 = g_pf.now();
    std::unique_ptr<TerarkIndex> indexPtr;
    try {
      indexPtr.reset(factory->Build(tempKeyFileReader,
        table_options_, keyStat, &ioptions_));
    }
    catch (const std::exception& ex) {
      INFO(ioptions_.info_log
        , "TerarkZipTableBuilder::Finish():this=%012p:  index build fail , type = %s , error = %s\n"
        , this, factory->WireName(), ex.what()
      );
      return Status::Corruption("TerarkZipTableBuilder index build error", ex.what());
    }
    if (table_options_.debugLevel == 2) {
        // check index correctness
        tempKeyFileReader.resetbuf();
        param.data.fp.rewind();
        auto it = UniquePtrOf(indexPtr->NewIterator());
        auto commonPrefixLen = param.stat.commonPrefixLen;
        valvec<byte_t> value;
        if (fstring(param.stat.minKey) < fstring(param.stat.maxKey)) {
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
                tempKeyFileReader >> value;
                if (it->key() != fstring(value).substr(commonPrefixLen)) {
                    return Status::Corruption("TerarkZipTableBuilder index build check fail",
                        indexPtr->Name());
                }
            }
        }
        else {
            for (it->SeekToLast(); it->Valid(); it->Prev()) {
                tempKeyFileReader >> value;
                if (it->key() != fstring(value).substr(commonPrefixLen)) {
                    return Status::Corruption("TerarkZipTableBuilder index build check fail",
                        indexPtr->Name());
                }
            }
        }
    }
    size_t fileSize = 0;
    {
      std::unique_lock<std::mutex> l(indexBuildMutex_);
      FileStream writer(tmpIndexFile_, "ab+");
      param.indexFileBegin = writer.fsize();
      indexPtr->SaveMmap([&fileSize, &writer](const void* data, size_t size) {
        fileSize += size;
        writer.ensureWrite(data, size);
      });
      writer.flush();
      param.indexFileEnd = writer.fsize();
    }
    assert(param.indexFileEnd - param.indexFileBegin == fileSize);
    assert(fileSize % 8 == 0);
    long long tt = g_pf.now();
    size_t prefixLen = param.stat.commonPrefixLen + kvs.prefix.size();
    size_t rawKeySize = param.stat.sumKeyLen - param.stat.numKeys * param.stat.commonPrefixLen;
    INFO(ioptions_.info_log,
      "TerarkZipTableBuilder::Finish():this=%012p:  index pass time =%8.2f's,%8.3f'MB/sec\n"
      "    index type = %s\n"
      "    usrkeys = %zd  min-keylen = %zd  max-keylen = %zd  prefix = %zd\n"
      "    raw-key =%9.4f GB  zip-key =%9.4f GB  avg-key =%7.2f  avg-zkey =%7.2f\n"
      , this, g_pf.sf(t1, tt), rawKeySize*1.0 / g_pf.uf(t1, tt)
      , indexPtr->Name()
      , param.stat.numKeys, param.stat.minKeyLen, param.stat.maxKeyLen, prefixLen
      , rawKeySize*1.0 / 1e9, fileSize*1.0 / 1e9
      , rawKeySize*1.0 / param.stat.numKeys, fileSize*1.0 / param.stat.numKeys
    );
    if (table_options_.debugLevel != 2 || second_pass_iter_ == nullptr) {
      param.data.close();
    }
    return Status::OK();
  });
}

Status TerarkZipTableBuilder::BuildStore(KeyValueStatus& kvs,
                                         DictZipBlobStore::ZipBuilder* zbuilder,
                                         uint64_t flag) {
  auto buildUncompressedStore = [this, &kvs]() {
    std::unique_lock<std::mutex> l(storeBuildMutex_);
    assert(tmpStoreFileSize_ == 0
      || tmpStoreFileSize_ == FileStream(tmpStoreFile_.fpath, "rb").fsize());

    size_t fixedNum = kvs.value.m_cnt_of_max_cnt_key;
    size_t variaNum = kvs.key.m_cnt_sum - fixedNum;
    BuildStoreParams params = { kvs, 0, tmpStoreFile_, tmpStoreFileSize_ };
    Status s;
    try {
      if (kvs.value.m_total_key_len == 0) {
        s = buildZeroLengthBlobStore(params);
      }
      else if (table_options_.offsetArrayBlockUnits) {
        if (variaNum * 64 < kvs.key.m_cnt_sum) {
          s = buildMixedLenBlobStore(params);
        }
        else {
          s = buildZipOffsetBlobStore(params);
        }
      }
      else {
        if (4 * variaNum + kvs.key.m_cnt_sum * 5 / 4 < 4 * kvs.key.m_cnt_sum) {
          s = buildMixedLenBlobStore(params);
        }
        else {
          s = buildPlainBlobStore(params);
        }
      }
      size_t newTmpStoreFileSize = FileStream(tmpStoreFile_.fpath, "rb").fsize();
      if (s.ok()) {
        kvs.valueFileBegin = tmpStoreFileSize_;
        kvs.valueFileEnd = newTmpStoreFileSize;
        assert((kvs.valueFileEnd - kvs.valueFileBegin) % 8 == 0);
      }
      tmpStoreFileSize_ = newTmpStoreFileSize;
    }
    catch(...) {
      tmpStoreFileSize_ = FileStream(tmpStoreFile_.fpath, "rb").fsize();
      throw;
    }
    return s;
  };
  auto buildCompressedStore = [this, &kvs, zbuilder]() {
    assert(zbuilder != nullptr);
    std::unique_lock<std::mutex> l(storeBuildMutex_);
    assert(tmpZipStoreFileSize_ == 0
      || tmpZipStoreFileSize_ == FileStream(tmpZipStoreFile_.fpath, "rb").fsize());

    zbuilder->prepare(kvs.key.m_cnt_sum, tmpZipStoreFile_, tmpZipStoreFileSize_);
    Status s;
    try {
      s = BuilderWriteValues(kvs, [&](fstring value) {zbuilder->addRecord(value); });
      size_t newTmpZipStoreFileSize = 0;
      if (s.ok()) {
        zbuilder->finish(DictZipBlobStore::ZipBuilder::FinishNone);
        kvs.valueFileBegin = tmpZipStoreFileSize_;
        newTmpZipStoreFileSize = FileStream(tmpZipStoreFile_.fpath, "rb").fsize();
        kvs.valueFileEnd = newTmpZipStoreFileSize;
        assert((kvs.valueFileEnd - kvs.valueFileBegin) % 8 == 0);
      } else {
        zbuilder->abandon();
        newTmpZipStoreFileSize = FileStream(tmpZipStoreFile_.fpath, "rb").fsize();
      }
      tmpZipStoreFileSize_ = newTmpZipStoreFileSize;
    }
    catch(...) {
      tmpZipStoreFileSize_ = FileStream(tmpZipStoreFile_.fpath, "rb").fsize();
      throw;
    }
    return s;
  };

  if (flag & BuildStoreInit) {
    if (kvs.valueFile.fp) {
      kvs.valueFile.complete_write();
    }
    kvs.key.finish();
    kvs.value.finish();
    auto avgValueLen = kvs.value.m_total_key_len / kvs.key.m_cnt_sum;
    if (avgValueLen < table_options_.minDictZipValueSize) {
      if (kvs.valueFile.fp && table_options_.debugLevel != 2) {
        kvs.isValueBuild = true;
        if (flag & BuildStoreSync) {
          return buildUncompressedStore();
        }
        else {
          kvs.wait = Async(buildUncompressedStore);
        }
      }
    }
    else {
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
  }
  else {
    if (kvs.isUseDictZip) {
      kvs.wait = Async(buildCompressedStore);
    }
    else {
      kvs.wait = Async(buildUncompressedStore);
    }
  }
  return Status::OK();
}

std::future<Status>
TerarkZipTableBuilder::SaveDict(fstring tmpDictFile, fstring dict, std::string* info) {
  if (table_options_.disableZipDict) {
    return Async([=] {
      FileStream(tmpDictFile, "wb+").ensureWrite(dict.data(), dict.size());
      info->clear();
      return Status::OK();
    });
  }
  return Async([=] {
    FileStream(tmpDictFile, "wb+").chsize(dict.size());
    MmapWholeFile dictFile(tmpDictFile, true);
    size_t zstd_size = ZSTD_compress(dictFile.base, dictFile.size, dict.data(), dict.size(), 0);
    if (ZSTD_isError(zstd_size) || CollectInfo::hard(dict.size(), zstd_size)) {
      memcpy(dictFile.base, dict.data(), dict.size());
      info->clear();
    }
    else {
      MmapWholeFile().swap(dictFile);
      FileStream(tmpDictFile, "rb+").chsize(zstd_size);
      *info = "ZSTD_";
      info->append(lcast(ZSTD_versionNumber()));
    }
    return Status::OK();
  });
}

Status TerarkZipTableBuilder::WaitBuildIndex() {
  Status result = Status::OK();
  size_t offset = 0;
  for (auto& kvs : histogram_) {
    size_t commonPrefixLength = size_t(-1);
    kvs->indexFileBegin = offset;
    for (auto& ptr : kvs->build) {
      auto& param = *ptr;
      assert(param.wait.valid());
      auto s = param.wait.get();
      offset += param.indexFileEnd - param.indexFileBegin;
      commonPrefixLength = std::min(commonPrefixLength, param.stat.commonPrefixLen);
      if (terark_unlikely(!s.ok() && result.ok())) {
        result = std::move(s);
      }
      if (param.data.fp) {
        param.data.close();
      }
    }
    auto minKey = fstring(kvs->build.front()->stat.minKey);
    kvs->commonPrefix.assign(minKey.substr(0, commonPrefixLength));
    kvs->indexFileEnd = offset;
  }
  return result;
}

Status TerarkZipTableBuilder::WaitBuildStore() {
  Status result = Status::OK();
  for (auto& kvs : histogram_) {
    if (kvs->wait.valid()) {
      auto s = kvs->wait.get();
      if (terark_unlikely(!s.ok() && result.ok())) {
        result = std::move(s);
      }
    }
  }
  return result;
}

void TerarkZipTableBuilder::BuildReorderMap(BuildReorderParams& params,
                                            KeyValueStatus& kvs,
                                            fstring mmap_memory,
                                            AbstractBlobStore* store,
                                            long long& t6) {
  valvec<std::unique_ptr<TerarkIndex>> indexes;
  size_t rawKeySize = 0;
  size_t zipKeySize = 0;
  size_t reoder = 0;
  for (auto& ptr : kvs.build) {
    auto &param = *ptr;
    rawKeySize += param.stat.sumKeyLen;
    zipKeySize += param.indexFileEnd - param.indexFileBegin;
    indexes.emplace_back(
      TerarkIndex::LoadMemory(
        fstring(
          mmap_memory.data() + param.indexFileBegin,
          param.indexFileEnd - param.indexFileBegin
        )
      ).release()
    );
    if (indexes.back()->NeedsReorder() || isReverseBytewiseOrder_) {
      ++reoder;
    }
  }
  INFO(ioptions_.info_log,
    "TerarkZipTableBuilder::Finish():this=%012p:  index type = %-32s, store type = %-20s\n"
    "    usrkeys = %zd  key-segment = %zd  prefix = %zd\n"
    "    raw-key =%9.4f GB  zip-key =%9.4f GB  avg-key =%7.2f  avg-zkey =%7.2f\n"
    "    raw-val =%9.4f GB  zip-val =%9.4f GB  avg-val =%7.2f  avg-zval =%7.2f\n"
    , this, indexes.size() == 1 ? indexes.front()->Name() : "LazyUnionDFA", store->name()
    , store->num_records(), indexes.size(), kvs.prefix.size()

    , rawKeySize*1.0 / 1e9, zipKeySize*1.0 / 1e9
    , rawKeySize*1.0 / store->num_records(), zipKeySize*1.0 / store->num_records()

    , store->total_data_size()*1.0 / 1e9, store->get_mmap().size()*1.0 / 1e9
    , store->total_data_size()*1.0 / store->num_records(), store->get_mmap().size()*1.0 / store->num_records()
  );
  t6 = g_pf.now();
  if (reoder == 0) {
    params.type.clear();
    params.tmpReorderFile.Delete();
    return;
  }
  params.type.resize_no_init(kvs.key.m_cnt_sum);
  ZReorderMap::Builder builder(kvs.key.m_cnt_sum,
    isReverseBytewiseOrder_ ? -1 : 1, params.tmpReorderFile.fpath, "wb");
  if (isReverseBytewiseOrder_) {
    size_t ho = kvs.key.m_cnt_sum;
    size_t hn = 0;
    for (auto it = indexes.rbegin(); it != indexes.rend(); ++it) {
      auto index = it->get();
      size_t count = index->NumKeys();
      ho -= count;
      if (index->NeedsReorder()) {
        size_t memory = UintVecMin0::compute_mem_size_by_max_val(count, count - 1);
        WaitHandle handle = std::move(WaitForMemory("reorder", memory));
        UintVecMin0 newToOld(index->NumKeys(), index->NumKeys() - 1);
        index->GetOrderMap(newToOld);
        for (size_t n = 0; n < count; ++n) {
          size_t o = count - newToOld[n] - 1 + ho;
          builder.push_back(o);
          params.type.set0(n + hn, kvs.type[o]);
        }
      }
      else {
        for (size_t n = 0, o = count - 1 + ho; n < count; ++n, --o) {
          builder.push_back(o);
          params.type.set0(n + hn, kvs.type[o]);
        }
      }
      hn += count;
      it->reset();
    }
    assert(ho == 0);
    assert(hn == kvs.key.m_cnt_sum);
  }
  else {
    size_t h = 0;
    for (auto& ptr : indexes) {
      auto index = ptr.get();
      size_t count = index->NumKeys();
      if (index->NeedsReorder()) {
        size_t memory = UintVecMin0::compute_mem_size_by_max_val(count, count - 1);
        WaitHandle handle = std::move(WaitForMemory("reorder", memory));
        UintVecMin0 newToOld(index->NumKeys(), index->NumKeys() - 1);
        index->GetOrderMap(newToOld);
        for (size_t n = 0; n < count; ++n) {
          size_t o = newToOld[n] + h;
          builder.push_back(o);
          params.type.set0(n + h, kvs.type[o]);
        }
      }
      else {
        for (size_t n = 0; n < count; ++n) {
          size_t o = n + h;
          builder.push_back(o);
          params.type.set0(n + h, kvs.type[o]);
        }
      }
      h += count;
      ptr.reset();
    }
    assert(h == kvs.key.m_cnt_sum);
  }
  builder.finish();
}

TerarkZipTableBuilder::WaitHandle
TerarkZipTableBuilder::
LoadSample(std::unique_ptr<DictZipBlobStore::ZipBuilder>& zbuilder) {
  size_t sampleMax = std::min<size_t>(INT32_MAX, table_options_.softZipWorkingMemLimit / 7);
  size_t dictWorkingMemory = std::min<size_t>(sampleMax, sampleLenSum_) * 6;
  auto waitHandle = WaitForMemory("dictZip", dictWorkingMemory);

  valvec<byte_t> sample;
  valvec<byte_t> test;
  const size_t test_size = 32ull << 20;
  auto hard_test = table_options_.enableCompressionProbe && table_factory_->GetCollect().hard();
  NativeDataInput<InputBuffer> sampleInput(&tmpSampleFile_.fp);
  size_t realsampleLenSum = 0;
  if (hard_test && sampleLenSum_ < test_size) {
    for (size_t len = 0; len < sampleLenSum_; ) {
      sampleInput >> sample;
      zbuilder->addSample(sample);
      test.append(sample);
      len += sample.size();
    }
    realsampleLenSum = sampleLenSum_;
  }
  else {
    if (sampleLenSum_ < sampleMax) {
      uint64_t upperBoundTest = uint64_t(
        randomGenerator_.max() * double(test_size) / sampleLenSum_);
      for (size_t len = 0; len < sampleLenSum_; ) {
        sampleInput >> sample;
        zbuilder->addSample(sample);
        len += sample.size();
        if (hard_test && randomGenerator_() < upperBoundTest) {
          test.append(sample);
        }
      }
      realsampleLenSum = sampleLenSum_;
    }
    else {
      uint64_t upperBoundSample = uint64_t(
        randomGenerator_.max() * double(sampleMax) / sampleLenSum_);
      uint64_t upperBoundTest = uint64_t(
        randomGenerator_.max() * double(test_size) / sampleLenSum_);
      for (size_t len = 0; len < sampleLenSum_; ) {
        sampleInput >> sample;
        if (randomGenerator_() < upperBoundSample) {
          zbuilder->addSample(sample);
          realsampleLenSum += sample.size();
        }
        if (hard_test && randomGenerator_() < upperBoundTest) {
          test.append(sample);
        }
        len += sample.size();
      }
    }
  }
  tmpSampleFile_.close();
  if (hard_test) {
    valvec<byte_t> output(test.size(), valvec_no_init());
    size_t zstd_size = ZSTD_compress(output.data(), test.size(), test.data(), test.size(), 0);
    if (ZSTD_isError(zstd_size) || CollectInfo::hard(test.size(), zstd_size)) {
      // hard to compress ...
      zbuilder.reset();
      waitHandle.Release();
      return waitHandle;
    }
    test.clear();
  }
  if (0 == realsampleLenSum) { // prevent from empty
    zbuilder->addSample("Hello World!");
  }
  zbuilder->finishSample();
  return waitHandle;
}

Status TerarkZipTableBuilder::buildZeroLengthBlobStore(BuildStoreParams &params) {
  auto& kvs = params.kvs;
  auto store = UniquePtrOf(new terark::ZeroLengthBlobStore());
  auto s = BuilderWriteValues(kvs, [&](fstring value) { assert(value.empty()); });
  if (s.ok()) {
    store->finish(kvs.key.m_cnt_sum);
    FileStream file(params.fpath, "ab+");
    store->save_mmap([&](const void* d, size_t s) {
      file.ensureWrite(d, s);
    });
  }
  return s;
}
Status TerarkZipTableBuilder::buildPlainBlobStore(BuildStoreParams &params) {
  auto& kvs = params.kvs;
  terark::PlainBlobStore::MyBuilder builder(kvs.value.m_total_key_len, params.fpath, params.offset);
  auto s = BuilderWriteValues(kvs, [&](fstring value) { builder.addRecord(value); });
  if (s.ok()) {
    builder.finish();
  }
  return s;
}
Status TerarkZipTableBuilder::buildMixedLenBlobStore(BuildStoreParams &params) {
  auto& kvs = params.kvs;
  size_t fixedLen = kvs.value.m_max_cnt_key;
  size_t fixedLenCount = kvs.value.m_cnt_of_max_cnt_key;
  size_t varDataLen = kvs.value.m_total_key_len - fixedLen * fixedLenCount;
  std::unique_ptr<AbstractBlobStore::Builder> builder;
  if (kvs.value.m_cnt_sum < (4ULL << 30)) {
    builder.reset(new terark::MixedLenBlobStore::MyBuilder(
        fixedLen, varDataLen, params.fpath, params.offset));
  }
  else {
    builder.reset(new terark::MixedLenBlobStore64::MyBuilder(
        fixedLen, varDataLen, params.fpath, params.offset));
  }
  auto s = BuilderWriteValues(kvs, [&](fstring value) { builder->addRecord(value); });
  if (s.ok()) {
    builder->finish();
  }
  return s;
}
Status TerarkZipTableBuilder::buildZipOffsetBlobStore(BuildStoreParams &params) {
  auto& kvs = params.kvs;
  size_t blockUnits = table_options_.offsetArrayBlockUnits;
  terark::ZipOffsetBlobStore::MyBuilder builder(blockUnits, params.fpath, params.offset);
  auto s = BuilderWriteValues(kvs, [&](fstring value) { builder.addRecord(value); });
  if (s.ok()) {
    builder.finish();
  }
  return s;
}

Status TerarkZipTableBuilder::ZipValueToFinish() {
  assert(histogram_.size() == 1);
  auto& kvs = *histogram_.front();
  AutoDeleteFile tmpDictFile{ tmpSentryFile_.path + ".dict" };
  std::unique_ptr<DictZipBlobStore::ZipBuilder> zbuilder;
  WaitHandle dictWaitHandle;
  std::future<Status> dictWait;
  DictZipBlobStore::ZipStat dzstat;
  std::string dictInfo;
  size_t dictSize = 0;
  uint64_t dictHash = 0;
  long long t3, t4;
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
      // prepareDict() will invalid zbuilder->getDictionary().memory()
      zbuilder->prepareDict();
      dictWait = SaveDict(tmpDictFile, zbuilder->getDictionary().memory, &dictInfo);
      dictSize = zbuilder->getDictionary().memory.size();
      dictHash = zbuilder->getDictionary().xxhash;
    }
    s = BuildStore(kvs, zbuilder.get(), BuildStoreSync);
    if (!s.ok()) {
      return s;
    }
    if (zbuilder) {
      dzstat = zbuilder->getZipStat();
    }
  }
  if (zbuilder) {
    zbuilder->freeDict();
    t4 = g_pf.now();
    assert(dictWait.valid());
    s = dictWait.get();
    if (!s.ok()) {
      return s;
    }
    zbuilder.reset();
    dictWaitHandle.Release();
  }
  else {
    tmpDictFile.fpath.clear();
    t4 = g_pf.now();
  }
  // wait for indexing complete, if indexing is slower than value compressing
  s = WaitBuildIndex();
  if (!s.ok()) {
    return s;
  }
  if (tmpDumpFile_.isOpen()) {
    tmpDumpFile_.close();
  }
  return WriteSSTFile(t3, t4, tmpDictFile, dictInfo, dictSize, dictHash, dzstat);
}

Status TerarkZipTableBuilder::ZipValueToFinishMulti() {
  assert(histogram_.size() > 1);
  AutoDeleteFile tmpDictFile{ tmpSentryFile_.path + ".dict"};
  std::unique_ptr<DictZipBlobStore::ZipBuilder> zbuilder;
  WaitHandle dictWaitHandle;
  std::future<Status> dictWait;
  std::unique_ptr<AbstractBlobStore> store;
  DictZipBlobStore::ZipStat dzstat;
  std::string dictInfo;
  size_t dictSize = 0;
  uint64_t dictHash = 0;
  size_t dictRefCount = 0;
  Status s;
  long long t3, t4;
  t3 = g_pf.now();
  bool isUseDictZip = false;
  for (auto& kvs : histogram_) {
    if (kvs->isValueBuild) {
      continue;
    }
    if (kvs->isUseDictZip) {
      isUseDictZip = true;
    }
    else {
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
      dictWait = SaveDict(tmpDictFile, zbuilder->getDictionary().memory, &dictInfo);
      dictSize = zbuilder->getDictionary().memory.size();
      dictHash = zbuilder->getDictionary().xxhash;
    }
    for (auto& kvs : histogram_) {
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
    return s;
  }
  if (zbuilder) {
    zbuilder->freeDict();
    t4 = g_pf.now();
    assert(dictWait.valid());
    s = dictWait.get();
    if (!s.ok()) {
      return s;
    }
    zbuilder.reset();
    dictWaitHandle.Release();
  }
  else {
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
  return WriteSSTFileMulti(t3, t4, tmpDictFile, dictInfo, dictSize, dictHash, dzstat);
}

Status
TerarkZipTableBuilder::BuilderWriteValues(KeyValueStatus& kvs, std::function<void(fstring)> write) {
  auto& bzvType = kvs.type;
  bzvType.resize(kvs.key.m_cnt_sum);
  auto seekSecondPassIter = [&] {
    auto& stat = kvs.build.front()->stat;
    std::string target;
    target.resize(stat.minKey.size() + kvs.prefix.size() + 8);
    target.assign(kvs.prefix.data(), kvs.prefix.size());
    target.append((const char*)stat.minKey.data(), stat.minKey.size());
    target.append((const char*)&kvs.seqType, 8);
    second_pass_iter_->Seek(target);
  };
#define ITER_MOVE_NEXT(it)                    \
    do {                                      \
      it->Next();                             \
      if (!it->status().ok())                 \
        return it->status();                  \
    } while(0)
  if (kvs.valueFile.fp)
  {
    NativeDataInput<InputBuffer> input(&kvs.valueFile.fp);
    bool veriftIter = table_options_.debugLevel == 2 && second_pass_iter_ != nullptr;
    size_t keyFileIndex = 0;
    size_t keyFileCount = 0;
    FileStream keyFile;
    valvec<byte_t> key, value;
    NativeDataInput<InputBuffer> keyInput;
    auto readKey = [&](uint64_t seqType, bool next) {
      if (!next) {
        (uint64_t&)key.end()[-8] = seqType;
        return SliceOf(key);
      }
      TERARK_RT_assert(keyFileIndex != kvs.build.size(), std::logic_error);
      key.resize(kvs.prefix.size());
      keyInput.load_add(key);
      key.append((char*)&seqType, 8);
      if (++keyFileCount == kvs.build[keyFileIndex]->stat.numKeys) {
        keyFileCount = 0;
        keyInput.resetbuf();
        keyFile.close();
        if (++keyFileIndex < kvs.build.size()) {
          keyFile.open(kvs.build[keyFileIndex]->data.path, "rb");
          keyFile.disbuf();
          keyInput.attach(&keyFile);
        }
      }
      return SliceOf(key);
    };
    if (veriftIter) {
      key.assign(kvs.prefix);
      keyFile.open(kvs.build.front()->data.path, "rb");
      keyFile.disbuf();
      keyInput.attach(&keyFile);
      seekSecondPassIter();
    }
    size_t entryId = 0;
    size_t bitPos = 0;
    for (size_t recId = 0; recId < kvs.key.m_cnt_sum ; recId++) {
      uint64_t seqType = input.load_as<uint64_t>();
      uint64_t seqNum;
      ValueType vType;
      UnPackSequenceAndType(seqType, &seqNum, &vType);
      size_t oneSeqLen = kvs.valueBits.one_seq_len(bitPos);
      assert(oneSeqLen >= 1);
      if (1 == oneSeqLen && (kTypeDeletion == vType || kTypeValue == vType)) {
        if (0 == seqNum && kTypeValue == vType) {
          bzvType.set0(recId, size_t(ZipValueType::kZeroSeq));
          input >> value;
          if (veriftIter) {
            TERARK_RT_assert(readKey(seqType, true) == second_pass_iter_->key(), std::logic_error);
            TERARK_RT_assert(SliceOf(value) == second_pass_iter_->value(), std::logic_error);
            ITER_MOVE_NEXT(second_pass_iter_);
          }
        }
        else {
          if (kTypeValue == vType) {
            bzvType.set0(recId, size_t(ZipValueType::kValue));
          }
          else {
            bzvType.set0(recId, size_t(ZipValueType::kDelete));
          }
          value.erase_all();
          value.append((byte_t*)&seqNum, 7);
          input.load_add(value);
          if (veriftIter) {
            TERARK_RT_assert(readKey(seqType, true) == second_pass_iter_->key(), std::logic_error);
            TERARK_RT_assert(Slice((char*)value.data() + 7, value.size() - 7) ==
              second_pass_iter_->value(), std::logic_error);
            ITER_MOVE_NEXT(second_pass_iter_);
          }
        }
      }
      else {
        bzvType.set0(recId, size_t(ZipValueType::kMulti));
        size_t headerSize = ZipValueMultiValue::calcHeaderSize(oneSeqLen);
        value.resize(headerSize);
        ((ZipValueMultiValue*)value.data())->offsets[0] = uint32_t(oneSeqLen);
        for (size_t j = 0; j < oneSeqLen; j++) {
          if (j > 0) {
            seqType = input.load_as<uint64_t>();
          }
          value.append((byte_t*)&seqType, 8);
          size_t oldSize = value.size();
          input.load_add(value);
          if (veriftIter) {
            TERARK_RT_assert(readKey(seqType, j == 0) == second_pass_iter_->key(), std::logic_error);
            TERARK_RT_assert(Slice((char*)value.data() + oldSize, value.size() - oldSize) ==
              second_pass_iter_->value(), std::logic_error);
            ITER_MOVE_NEXT(second_pass_iter_);
          }
          if (j + 1 < oneSeqLen) {
            ((ZipValueMultiValue*)value.data())->offsets[j + 1] = value.size() - headerSize;
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
    kvs.valueFile.close();
  }
  else
  {
    assert(second_pass_iter_ != nullptr);
    seekSecondPassIter();
    valvec<byte_t> value;
    size_t entryId = 0;
    size_t bitPos = 0;
    bool dumpKeyValue = table_options_.debugLevel == 3;
    auto dumpKeyValueFunc = [&](const ParsedInternalKey& ikey, const Slice& value) {
      fprintf(tmpDumpFile_.fp(), "DEBUG: 2nd pass => %s / %s \n", ikey.DebugString(true).c_str(), value.ToString(true).c_str());
    };

    for (size_t recId = 0; recId < kvs.key.m_cnt_sum ; recId++) {
      value.erase_all();
      assert(second_pass_iter_->Valid());
      ParsedInternalKey pikey;
      Slice curKey = second_pass_iter_->key();
      TERARK_RT_assert(ParseInternalKey(curKey, &pikey), std::logic_error);
      if (dumpKeyValue) {
        dumpKeyValueFunc(pikey, second_pass_iter_->value());
      }
      while (kTypeRangeDeletion == pikey.type) {
        ITER_MOVE_NEXT(second_pass_iter_);
        assert(second_pass_iter_->Valid());
        curKey = second_pass_iter_->key();
        TERARK_RT_assert(ParseInternalKey(curKey, &pikey), std::logic_error);
        if (dumpKeyValue) {
          dumpKeyValueFunc(pikey, second_pass_iter_->value());
        }
        entryId += 1;
      }
      Slice curVal = second_pass_iter_->value();
      size_t oneSeqLen = kvs.valueBits.one_seq_len(bitPos);
      assert(oneSeqLen >= 1);
      if (1 == oneSeqLen && (kTypeDeletion == pikey.type || kTypeValue == pikey.type)) {
        if (0 == pikey.sequence && kTypeValue == pikey.type) {
          bzvType.set0(recId, size_t(ZipValueType::kZeroSeq));
          write(fstringOf(curVal));
        }
        else {
          if (kTypeValue == pikey.type) {
            bzvType.set0(recId, size_t(ZipValueType::kValue));
          }
          else {
            bzvType.set0(recId, size_t(ZipValueType::kDelete));
          }
          value.append((byte_t*)&pikey.sequence, 7);
          value.append(fstringOf(curVal));
          write(value);
        }
        ITER_MOVE_NEXT(second_pass_iter_);
      }
      else {
        bzvType.set0(recId, size_t(ZipValueType::kMulti));
        size_t headerSize = ZipValueMultiValue::calcHeaderSize(oneSeqLen);
        value.resize(headerSize);
        ((ZipValueMultiValue*)value.data())->offsets[0] = uint32_t(oneSeqLen);
        for (size_t j = 0; j < oneSeqLen; j++) {
          if (j > 0) {
            assert(second_pass_iter_->Valid());
            curKey = second_pass_iter_->key();
            TERARK_RT_assert(ParseInternalKey(curKey, &pikey), std::logic_error);
            if (dumpKeyValue) {
              dumpKeyValueFunc(pikey, second_pass_iter_->value());
            }
            while (kTypeRangeDeletion == pikey.type) {
              ITER_MOVE_NEXT(second_pass_iter_);
              assert(second_pass_iter_->Valid());
              curKey = second_pass_iter_->key();
              TERARK_RT_assert(ParseInternalKey(curKey, &pikey), std::logic_error);
              if (dumpKeyValue) {
                dumpKeyValueFunc(pikey, second_pass_iter_->value());
              }
              entryId += 1;
            }
            curVal = second_pass_iter_->value();
          }
          else {
            assert(kTypeRangeDeletion != pikey.type);
          }
          uint64_t seqType = PackSequenceAndType(pikey.sequence, pikey.type);
          value.append((byte_t*)&seqType, 8);
          value.append(fstringOf(curVal));
          if (j + 1 < oneSeqLen) {
            ((ZipValueMultiValue*)value.data())->offsets[j + 1] = value.size() - headerSize;
          }
          ITER_MOVE_NEXT(second_pass_iter_);
        }
        write(value);
      }
      bitPos += oneSeqLen + 1;
      entryId += oneSeqLen;
    }
    // second pass no range deletion ...
    //assert(entryId <= properties_.num_entries);

#undef ITER_MOVE_NEXT
  }
  kvs.valueBits.clear();
  return Status::OK();
}

Status TerarkZipTableBuilder::WriteStore(fstring indexMmap, AbstractBlobStore* store
  , KeyValueStatus& kvs
  , BlockHandle& dataBlock
  , long long& t5, long long& t6, long long& t7) {
  using namespace std::placeholders;
  auto writeAppend = std::bind(&TerarkZipTableBuilder::DoWriteAppend, this, _1, _2);
  BuildReorderParams params;
  params.tmpReorderFile.fpath = tmpSentryFile_.path + ".reorder";
  BuildReorderMap(params, kvs, indexMmap, store, t6);
  if (params.type.size() != 0) {
    params.type.swap(kvs.type);
    ZReorderMap reorder(params.tmpReorderFile.fpath);
    t7 = g_pf.now();
    try {
      dataBlock.set_offset(offset_);
      store->reorder_zip_data(reorder, std::ref(writeAppend), tmpSentryFile_.path + ".reorder-tmp");
      dataBlock.set_size(offset_ - dataBlock.offset());
    }
    catch (const Status& s) {
      return s;
    }
  }
  else {
    t7 = t6;
    try {
      dataBlock.set_offset(offset_);
      store->save_mmap(std::ref(writeAppend));
      dataBlock.set_size(offset_ - dataBlock.offset());
    }
    catch (const Status& s) {
      return s;
    }
  }
  return Status::OK();
}

void TerarkZipTableBuilder::DoWriteAppend(const void* data, size_t size) {
  Status s = file_->Append(Slice((const char*)data, size));
  if (!s.ok()) {
    throw s;
  }
  offset_ += size;
}

Status TerarkZipTableBuilder::WriteSSTFile(long long t3, long long t4
  , fstring tmpDictFile, const std::string& dictInfo, size_t dictSize, uint64_t dictHash
  , const DictZipBlobStore::ZipStat& dzstat)
{
  assert(histogram_.size() == 1);
  terark::MmapWholeFile dictMmap;
  AbstractBlobStore::Dictionary dict(dictSize, dictHash);
  auto& kvs = *histogram_.front();
  terark::MmapWholeFile mmapIndexFile(tmpIndexFile_.fpath);
  terark::MmapWholeFile mmapStoreFile((kvs.isUseDictZip ? tmpZipStoreFile_ : tmpStoreFile_).fpath);
  assert(mmapIndexFile.base != nullptr);
  assert(mmapStoreFile.base != nullptr);
  auto store = UniquePtrOf(AbstractBlobStore::load_from_user_memory(mmapStoreFile.memory(), dict));
  auto& bzvType = kvs.type;
  const size_t realsampleLenSum = dict.memory.size();
  long long rawBytes = properties_.raw_key_size + properties_.raw_value_size;
  long long t5 = g_pf.now();
  Status s;
  BlockHandle dataBlock, dictBlock, indexBlock, zvTypeBlock(0, 0), tombstoneBlock(0, 0);
  BlockHandle commonPrefixBlock;
  {
    size_t real_size = mmapIndexFile.size + store->mem_size() + bzvType.mem_size();
    size_t block_size, last_allocated_block;
    file_->writable_file()->GetPreallocationStatus(&block_size, &last_allocated_block);
    INFO(ioptions_.info_log
      , "TerarkZipTableBuilder::Finish():this=%012p: old prealloc_size = %zd, real_size = %zd\n"
      , this, block_size, real_size
    );
    file_->writable_file()->SetPreallocationBlockSize(1 * 1024 * 1024 + real_size);
  }
  long long t6, t7;
  offset_ = 0;
  s = WriteStore(mmapIndexFile.memory(), store.get(), kvs, dataBlock, t5, t6, t7);
  if (!s.ok()) {
    return s;
  }
  properties_.data_size = dataBlock.size();
  indexBlock.set_offset(offset_);
  indexBlock.set_size(mmapIndexFile.size);
  if (isReverseBytewiseOrder_) {
    for (size_t j = kvs.build.size(); j > 0; ) {
      auto& param = *kvs.build[--j];
      DoWriteAppend((const char*)mmapIndexFile.base + param.indexFileBegin,
        param.indexFileEnd - param.indexFileBegin);
    }
  }
  else {
    for (auto& ptr : kvs.build) {
      auto& param = *ptr;
      DoWriteAppend((const char*)mmapIndexFile.base + param.indexFileBegin,
        param.indexFileEnd - param.indexFileBegin);
    }
  }
  assert(offset_ == indexBlock.offset() + indexBlock.size());
  properties_.index_size = indexBlock.size();
  if (zeroSeqCount_ != bzvType.size()) {
    assert(zeroSeqCount_ < bzvType.size());
    fstring zvTypeMem(bzvType.data(), bzvType.mem_size());
    s = WriteBlock(zvTypeMem, file_, &offset_, &zvTypeBlock);
    if (!s.ok()) {
      return s;
    }
  }
#if defined(TerocksPrivateCode)
  auto& license = table_factory_->GetLicense();
  BlockHandle licenseHandle;
  s = WriteBlock(SliceOf(license.dump()), file_, &offset_, &licenseHandle);
  if (!s.ok()) {
    return s;
  }
#endif // TerocksPrivateCode
  if (!range_del_block_.empty()) {
    s = WriteBlock(range_del_block_.Finish(), file_, &offset_, &tombstoneBlock);
    if (!s.ok()) {
      return s;
    }
  }
  range_del_block_.Reset();
  std::string commonPrefix;
  commonPrefix.reserve(kvs.prefix.size() + kvs.commonPrefix.size());
  commonPrefix.append(kvs.prefix.data(), kvs.prefix.size());
  commonPrefix.append(kvs.commonPrefix.data(), kvs.commonPrefix.size());
  WriteBlock(commonPrefix, file_, &offset_, &commonPrefixBlock);
  if (!dict.memory.empty()) {
    s = WriteBlock(MmapWholeFile(tmpDictFile).memory(), file_, &offset_, &dictBlock);
    if (!s.ok()) {
      return s;
    }
  }
  properties_.num_data_blocks = kvs.key.m_cnt_sum;
  offset_info_.Init(kvs.prefix.size(), 1);
  offset_info_.set(0,
                   kvs.prefix,
                   indexBlock.size(),
                   properties_.data_size,
                   zvTypeBlock.size(),
                   commonPrefixBlock.size());
  WriteMetaData(dictInfo, {
#if defined(TerocksPrivateCode)
    { &kTerarkZipTableExtendedBlock                                , licenseHandle     },
#endif // TerocksPrivateCode
    { dict.memory.size() ? &kTerarkZipTableValueDictBlock : NULL   , dictBlock         },
    { &kTerarkZipTableIndexBlock                                   , indexBlock        },
    { !zvTypeBlock.IsNull() ? &kTerarkZipTableValueTypeBlock : NULL, zvTypeBlock       },
    { &kTerarkZipTableCommonPrefixBlock                            , commonPrefixBlock },
    { !tombstoneBlock.IsNull() ? &kRangeDelBlock : NULL            , tombstoneBlock    },
  });
  long long t8 = g_pf.now();
  {
    std::unique_lock<std::mutex> lock(g_sumMutex);
    g_sumKeyLen += properties_.raw_key_size;
    g_sumValueLen += properties_.raw_value_size;
    g_sumUserKeyLen += kvs.key.m_total_key_len;
    g_sumUserKeyNum += kvs.key.m_cnt_sum;
    g_sumEntryNum += properties_.num_entries;
  }
  INFO(ioptions_.info_log,
    "TerarkZipTableBuilder::Finish():this=%012p: second pass time =%8.2f's,%8.3f'MB/sec, value only(%4.1f%% of KV)\n"
    "   wait indexing time = %7.2f's,\n"
    "  remap KeyValue time = %7.2f's, %8.3f'MB/sec (all stages of remap)\n"
    "    Get OrderMap time = %7.2f's, %8.3f'MB/sec (index lex order gen)\n"
    "  rebuild zvType time = %7.2f's, %8.3f'MB/sec\n"
    "  write SST data time = %7.2f's, %8.3f'MB/sec\n"
    "    z-dict build time = %7.2f's, sample length = %7.3f'MB, throughput = %6.3f'MB/sec\n"
    "    zip my value time = %7.2f's, unzip  length = %7.3f'GB\n"
    "    zip my value throughput = %7.3f'MB/sec\n"
    "    zip pipeline throughput = %7.3f'MB/sec\n"
    "    entries = %zd  avg-key = %.2f  avg-zkey = %.2f  avg-val = %.2f  avg-zval = %.2f\n"
    "    usrkeys = %zd  avg-key = %.2f  avg-zkey = %.2f  avg-val = %.2f  avg-zval = %.2f\n"
    "    seq expand size = %zd  multi value expand size = %zd\n"
    "    UnZipSize{ index =%9.4f GB  value =%9.4f GB   all =%9.4f GB }\n"
    "    __ZipSize{ index =%9.4f GB  value =%9.4f GB   all =%9.4f GB }\n"
    "    UnZip/Zip{ index =%9.4f     value =%9.4f      all =%9.4f    }\n"
    "    Zip/UnZip{ index =%9.4f     value =%9.4f      all =%9.4f    }\n"
    "----------------------------\n"
    "    total value len =%14.6f GB     avg =%8.3f KB (by entry num)\n"
    "    total  key  len =%14.6f GB     avg =%8.3f KB\n"
    "    total ukey  len =%14.6f GB     avg =%8.3f KB\n"
    "    total ukey  num =%17.9f Billion\n"
    "    total entry num =%17.9f Billion\n"
    "    write speed all =%17.9f MB/sec (with    version num)\n"
    "    write speed all =%17.9f MB/sec (without version num)"
, this, g_pf.sf(t3, t4)
, properties_.raw_value_size*1.0 / g_pf.uf(t3, t4)
, properties_.raw_value_size*100.0 / rawBytes

, g_pf.sf(t4, t5) // wait indexing time
, g_pf.sf(t5, t8), double(offset_) / g_pf.uf(t5, t8)

, g_pf.sf(t5, t6), properties_.index_size / g_pf.uf(t5, t6) // index lex walk

, g_pf.sf(t6, t7), kvs.key.m_cnt_sum * 2 / 8 / (g_pf.uf(t6, t7) + 1.0) // rebuild zvType

, g_pf.sf(t7, t8), double(offset_) / g_pf.uf(t7, t8) // write SST data

, dzstat.dictBuildTime, realsampleLenSum / 1e6
, realsampleLenSum / dzstat.dictBuildTime / 1e6

, dzstat.dictZipTime, properties_.raw_value_size / 1e9
, properties_.raw_value_size / dzstat.dictZipTime / 1e6
, dzstat.pipelineThroughBytes / dzstat.dictZipTime / 1e6

, size_t(properties_.num_entries)
, double(properties_.raw_key_size)   / properties_.num_entries
, double(properties_.index_size)     / properties_.num_entries
, double(properties_.raw_value_size) / properties_.num_entries
, double(properties_.data_size)      / properties_.num_entries

, kvs.key.m_cnt_sum
, double(kvs.key.m_total_key_len)    / kvs.key.m_cnt_sum
, double(properties_.index_size)     / kvs.key.m_cnt_sum
, double(properties_.raw_value_size + seqExpandSize_ + multiValueExpandSize_) / kvs.key.m_cnt_sum
, double(properties_.data_size)      / kvs.key.m_cnt_sum

, seqExpandSize_, multiValueExpandSize_

, kvs.key.m_total_key_len / 1e9, properties_.raw_value_size / 1e9, rawBytes / 1e9

, properties_.index_size / 1e9, properties_.data_size / 1e9, offset_ / 1e9

, double(kvs.key.m_total_key_len) / properties_.index_size
, double(properties_.raw_value_size) / properties_.data_size
, double(rawBytes) / offset_

, properties_.index_size / double(kvs.key.m_total_key_len)
, properties_.data_size / double(properties_.raw_value_size)
, offset_ / double(rawBytes)

, g_sumValueLen / 1e9, g_sumValueLen / 1e3 / g_sumEntryNum
, g_sumKeyLen / 1e9, g_sumKeyLen / 1e3 / g_sumEntryNum
, g_sumUserKeyLen / 1e9, g_sumUserKeyLen / 1e3 / g_sumUserKeyNum
, g_sumUserKeyNum / 1e9
, g_sumEntryNum / 1e9
, (g_sumKeyLen + g_sumValueLen) / g_pf.uf(g_lastTime, t8)
, (g_sumKeyLen + g_sumValueLen - g_sumEntryNum * 8) / g_pf.uf(g_lastTime, t8)
);
  return s;
}

Status TerarkZipTableBuilder::WriteSSTFileMulti(long long t3, long long t4
  , fstring tmpDictFile, const std::string& dictInfo, size_t dictSize, uint64_t dictHash
  , const DictZipBlobStore::ZipStat& dzstat) {
  assert(histogram_.size() > 1);
  terark::MmapWholeFile dictMmap;
  AbstractBlobStore::Dictionary dict(dictSize, dictHash);
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
  BlockHandle dataBlock, dictBlock, indexBlock, zvTypeBlock, offsetBlock, tombstoneBlock(0, 0);
  BlockHandle commonPrefixBlock;
  offset_info_.Init(key_prefixLen_, histogram_.size());
  size_t typeSize = 0;
  size_t commonPrefixLenSize = 0;
  for (size_t i = 0; i < histogram_.size(); ++i) {
    auto& kvs = *histogram_[i];
    typeSize += kvs.type.mem_size();
    commonPrefixLenSize += kvs.commonPrefix.size();
  }
  {
    size_t real_size = TerarkZipMultiOffsetInfo::calc_size(key_prefixLen_, histogram_.size())
      + mmapIndexFile.size
      + mmapStoreFile.size
      + typeSize
      + terark::align_up(commonPrefixLenSize, 16);
    size_t block_size, last_allocated_block;
    file_->writable_file()->GetPreallocationStatus(&block_size, &last_allocated_block);
    INFO(ioptions_.info_log
      , "TerarkZipTableBuilder::Finish():this=%012p: old prealloc_size = %zd, real_size = %zd\n"
      , this, block_size, real_size
    );
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
  valvec<byte_t> commonPrefix;
  size_t sumKeyLen = 0;
  size_t numKeys = 0;
  size_t keyOffset = 0;
  size_t valueOffset = 0;
  typeSize = 0;
  commonPrefix.reserve(terark::align_up(commonPrefixLenSize, 16));
  for (size_t i = 0; i < histogram_.size(); ++i) {
    auto& kvs = *histogram_[isReverseBytewiseOrder_ ? histogram_.size() - 1 - i : i];
    sumKeyLen += kvs.key.m_total_key_len + kvs.prefix.size() * kvs.key.m_cnt_sum;
    numKeys += kvs.key.m_cnt_sum ;
    commonPrefix.append(kvs.commonPrefix);
    unique_ptr<AbstractBlobStore> store(AbstractBlobStore::load_from_user_memory(
        getMmapPart(kvs.isUseDictZip ? mmapZipStoreFile : mmapStoreFile,
          kvs.valueFileBegin, kvs.valueFileEnd
        ), dict));
    long long t6p, t7p;
    s = WriteStore(mmapIndexFile.memory(), store.get(), kvs, dataBlock, t7, t6p, t7p);
    if (!s.ok()) {
      return s;
    }
    t6 += t6p - t7;
    t7 = t7p;
    keyOffset += kvs.indexFileEnd - kvs.indexFileBegin;
    valueOffset = offset_;
    typeSize += kvs.type.mem_size();
    offset_info_.set(i, kvs.prefix, keyOffset, valueOffset, typeSize, commonPrefix.size());
  }
  properties_.data_size = offset_;
  try {
    indexBlock.set_offset(offset_);
    indexBlock.set_size(mmapIndexFile.size);
    if (isReverseBytewiseOrder_) {
      for (size_t i = histogram_.size(); i > 0; ) {
        auto& kvs = *histogram_[--i];
        for (size_t j = kvs.build.size(); j > 0; ) {
          auto& param = *kvs.build[--j];
          DoWriteAppend((const char*)mmapIndexFile.base + param.indexFileBegin,
            param.indexFileEnd - param.indexFileBegin);
        }
      }
    }
    else {
      for (auto& kvs : histogram_) {
        for (auto& ptr : kvs->build) {
          auto& param = *ptr;
          DoWriteAppend((const char*)mmapIndexFile.base + param.indexFileBegin,
            param.indexFileEnd - param.indexFileBegin);
        }
      }
    }
    assert(offset_ == indexBlock.offset() + indexBlock.size());
    zvTypeBlock.set_offset(offset_);
    zvTypeBlock.set_size(typeSize);
    if (isReverseBytewiseOrder_) {
      for (size_t i = histogram_.size(); i > 0; ) {
        auto& kvs = *histogram_[--i];
        DoWriteAppend(kvs.type.data(), kvs.type.mem_size());
      }
    }
    else {
      for (size_t i = 0; i < histogram_.size(); ++i) {
        auto& kvs = *histogram_[i];
        DoWriteAppend(kvs.type.data(), kvs.type.mem_size());
      }
    }
    assert(offset_ == zvTypeBlock.offset() + zvTypeBlock.size());
  }
  catch (const Status& s) {
    return s;
  }
  s = WriteBlock(offset_info_.dump(), file_, &offset_, &offsetBlock);
  if (!s.ok()) {
    return s;
  }
#if defined(TerocksPrivateCode)
  auto& license = table_factory_->GetLicense();
  BlockHandle licenseHandle;
  s = WriteBlock(SliceOf(license.dump()), file_, &offset_, &licenseHandle);
  if (!s.ok()) {
      return s;
  }
#endif // TerocksPrivateCode
  if (!range_del_block_.empty()) {
    s = WriteBlock(range_del_block_.Finish(), file_, &offset_, &tombstoneBlock);
    if (!s.ok()) {
      return s;
    }
  }
  range_del_block_.Reset();
  commonPrefix.resize(terark::align_up(commonPrefix.size(), 16), 0);
  WriteBlock(commonPrefix, file_, &offset_, &commonPrefixBlock);
  if (!dict.memory.empty()) {
      s = WriteBlock(MmapWholeFile(tmpDictFile).memory(), file_, &offset_, &dictBlock);
      if (!s.ok()) {
          return s;
      }
  }
  properties_.index_size = indexBlock.size();
  properties_.num_data_blocks = numKeys;
  WriteMetaData(dictInfo, {
#if defined(TerocksPrivateCode)
    {&kTerarkZipTableExtendedBlock                                , licenseHandle     },
#endif // TerocksPrivateCode
    {!dict.memory.empty() ? &kTerarkZipTableValueDictBlock : NULL , dictBlock         },
    {&kTerarkZipTableIndexBlock                                   , indexBlock        },
    {!zvTypeBlock.IsNull() ? &kTerarkZipTableValueTypeBlock : NULL, zvTypeBlock       },
    {&kTerarkZipTableOffsetBlock                                  , offsetBlock       },
    {&kTerarkZipTableCommonPrefixBlock                            , commonPrefixBlock },
    {!tombstoneBlock.IsNull() ? &kRangeDelBlock : NULL            , tombstoneBlock    },
  });
  long long t8 = g_pf.now();
  {
    std::unique_lock<std::mutex> lock(g_sumMutex);
    g_sumKeyLen += properties_.raw_key_size;
    g_sumValueLen += properties_.raw_value_size;
    g_sumUserKeyLen += sumKeyLen;
    g_sumUserKeyNum += numKeys;
    g_sumEntryNum += properties_.num_entries;
  }
  INFO(ioptions_.info_log,
    "TerarkZipTableBuilder::FinishMulti():this=%012p: second pass time =%8.2f's,%8.3f'MB/sec, value only(%4.1f%% of KV)\n"
    "   wait indexing time = %7.2f's,\n"
    "  remap KeyValue time = %7.2f's, %8.3f'MB/sec (all stages of remap)\n"
    "    Get OrderMap time = %7.2f's, %8.3f'MB/sec (index lex order gen)\n"
    "  rebuild zvType time = %7.2f's, %8.3f'MB/sec\n"
    "  write SST data time = %7.2f's, %8.3f'MB/sec\n"
    "    z-dict build time = %7.2f's, sample length = %7.3f'MB, throughput = %6.3f'MB/sec\n"
    "    zip my value time = %7.2f's, unzip  length = %7.3f'GB\n"
    "    zip my value throughput = %7.3f'MB/sec\n"
    "    zip pipeline throughput = %7.3f'MB/sec\n"
    "    entries = %zd  avg-key = %.2f  avg-zkey = %.2f  avg-val = %.2f  avg-zval = %.2f\n"
    "    usrkeys = %zd  avg-key = %.2f  avg-zkey = %.2f  avg-val = %.2f  avg-zval = %.2f\n"
    "    seq expand size = %zd  multi value expand size = %zd\n"
    "    UnZipSize{ index =%9.4f GB  value =%9.4f GB   all =%9.4f GB }\n"
    "    __ZipSize{ index =%9.4f GB  value =%9.4f GB   all =%9.4f GB }\n"
    "    UnZip/Zip{ index =%9.4f     value =%9.4f      all =%9.4f    }\n"
    "    Zip/UnZip{ index =%9.4f     value =%9.4f      all =%9.4f    }\n"
    "----------------------------\n"
    "    total value len =%14.6f GB     avg =%8.3f KB (by entry num)\n"
    "    total  key  len =%14.6f GB     avg =%8.3f KB\n"
    "    total ukey  len =%14.6f GB     avg =%8.3f KB\n"
    "    total ukey  num =%17.9f Billion\n"
    "    total entry num =%17.9f Billion\n"
    "    write speed all =%17.9f MB/sec (with    version num)\n"
    "    write speed all =%17.9f MB/sec (without version num)"
, this, g_pf.sf(t3, t4)
, properties_.raw_value_size*1.0 / g_pf.uf(t3, t4)
, properties_.raw_value_size*100.0 / rawBytes

, g_pf.sf(t4, t5) // wait indexing time
, g_pf.sf(t5, t8), double(offset_) / g_pf.uf(t5, t8)

, g_pf.sf(t5, t6), properties_.index_size / g_pf.uf(t5, t6) // index lex walk

, g_pf.sf(t6, t7), numKeys * 2 / 8 / (g_pf.uf(t6, t7) + 1.0) // rebuild zvType

, g_pf.sf(t7, t8), double(offset_) / g_pf.uf(t7, t8) // write SST data

, dzstat.dictBuildTime, realsampleLenSum / 1e6
, realsampleLenSum / dzstat.dictBuildTime / 1e6

, dzstat.dictZipTime, properties_.raw_value_size / 1e9
, properties_.raw_value_size / dzstat.dictZipTime / 1e6
, dzstat.pipelineThroughBytes / dzstat.dictZipTime / 1e6

, size_t(properties_.num_entries)
, double(properties_.raw_key_size)   / properties_.num_entries
, double(properties_.index_size)     / properties_.num_entries
, double(properties_.raw_value_size) / properties_.num_entries
, double(properties_.data_size)      / properties_.num_entries

, numKeys
, double(sumKeyLen)                  / numKeys
, double(properties_.index_size)     / numKeys
, double(properties_.raw_value_size + seqExpandSize_ + multiValueExpandSize_) / numKeys
, double(properties_.data_size)      / numKeys

, seqExpandSize_, multiValueExpandSize_

, sumKeyLen / 1e9, properties_.raw_value_size / 1e9, rawBytes / 1e9

, properties_.index_size / 1e9, properties_.data_size / 1e9, offset_ / 1e9

, double(sumKeyLen) / properties_.index_size
, double(properties_.raw_value_size) / properties_.data_size
, double(rawBytes) / offset_

, properties_.index_size / double(sumKeyLen)
, properties_.data_size / double(properties_.raw_value_size)
, offset_ / double(rawBytes)

, g_sumValueLen / 1e9, g_sumValueLen / 1e3 / g_sumEntryNum
, g_sumKeyLen / 1e9, g_sumKeyLen / 1e3 / g_sumEntryNum
, g_sumUserKeyLen / 1e9, g_sumUserKeyLen / 1e3 / g_sumUserKeyNum
, g_sumUserKeyNum / 1e9
, g_sumEntryNum / 1e9
, (g_sumKeyLen + g_sumValueLen) / g_pf.uf(g_lastTime, t8)
, (g_sumKeyLen + g_sumValueLen - g_sumEntryNum * 8) / g_pf.uf(g_lastTime, t8)
);
  return s;
}

Status TerarkZipTableBuilder::WriteMetaData(const std::string& dictInfo,
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
      LogPropertiesCollectionError(ioptions_.info_log, "Finish", collector->Name());
      continue;
    }
    s = MyRocksTablePropertiesCollectorHack(collector.get(), offset_info_,
                                            user_collected_properties, isReverseBytewiseOrder_);
    if (!s.ok()) {
      WARN(ioptions_.info_log, "MyRocksTablePropertiesCollectorHack fail: %s", s.ToString().c_str());
    }
    propBlockBuilder.Add(user_collected_properties);
  }
  propBlockBuilder.Add(kTerarkZipTableBuildTimestamp, GetTimestamp());
  if (!dictInfo.empty()) {
    propBlockBuilder.Add(kTerarkZipTableDictInfo, dictInfo);
  }
  BlockHandle propBlock, metaindexBlock;
  Status s = WriteBlock(propBlockBuilder.Finish(), file_, &offset_, &propBlock);
  if (!s.ok()) {
    return s;
  }
  metaindexBuiler.Add(kPropertiesBlock, propBlock);
  s = WriteBlock(metaindexBuiler.Finish(), file_, &offset_, &metaindexBlock);
  if (!s.ok()) {
    return s;
  }
  Footer footer(kTerarkZipTableMagicNumber, 0);
  footer.set_metaindex_handle(metaindexBlock);
  footer.set_index_handle(BlockHandle::NullBlockHandle());
  std::string footer_encoding;
  footer.EncodeTo(&footer_encoding);
  s = file_->Append(footer_encoding);
  if (s.ok()) {
    offset_ += footer_encoding.size();
  }
  return s;
}

Status TerarkZipTableBuilder::OfflineFinish() {
  //std::unique_ptr<DictZipBlobStore> zstore(zbuilder_->finish(
  //  DictZipBlobStore::ZipBuilder::FinishFreeDict));
  //fstring fpath = zstore->get_fpath();
  //std::string tmpStoreFile(fpath.data(), fpath.size());
  //auto& kvs = histogram_[0];
  //auto dzstat = zbuilder_->getZipStat();
  //valvec<byte_t> commonPrefix(prevUserKey_.data(), kvs.stat.commonPrefixLen);
  //AutoDeleteFile tmpIndexFile{tmpSentryFile_.path + ".index"};
  //AutoDeleteFile tmpDictFile{tmpSentryFile_.path + ".dict"};
  //fstring dict = zstore->get_dict().memory;
  //FileStream(tmpDictFile, "wb+").ensureWrite(dict.data(), dict.size());
  //zstore.reset();
  //zbuilder_.reset();
  //long long t1 = g_pf.now();
  //{
  //  auto factory = TerarkIndex::GetFactory(table_options_.indexType);
  //  if (!factory) {
  //    THROW_STD(invalid_argument,
  //      "invalid indexType: %s", table_options_.indexType.c_str());
  //  }
  //  NativeDataInput<InputBuffer> tempKeyFileReader(&tmpKeyFile_.fp);
  //  FileStream writer(tmpIndexFile, "wb+");
  //  factory->Build(tempKeyFileReader, table_options_,
  //    [&writer](const void* data, size_t size) {
  //      writer.ensureWrite(data, size);
  //    },
  //    kvs.stat);
  //}
  //long long tt = g_pf.now();
  //INFO(ioptions_.info_log
  //  , "TerarkZipTableBuilder::Finish():this=%012p:  index pass time =%8.2f's,%8.3f'MB/sec\n"
  //  , this, g_pf.sf(t1, tt), properties_.raw_key_size*1.0 / g_pf.uf(t1, tt)
  //);
  //return WriteSSTFile(t1, tt, tmpIndexFile, tmpStoreFile, tmpDictFile, dzstat);
  return Status::OK();
}

void TerarkZipTableBuilder::Abandon() {
  assert(!closed_);
  closed_ = true;
  for (auto& kvs : histogram_) {
    for (auto& ptr : kvs->build) {
      auto& param = *ptr;
      if (param.wait.valid()) {
        param.wait.get();
      }
      else {
        param.data.complete_write();
      }
    }
    if (kvs->wait.valid()) {
      kvs->wait.get();
    }
  }
  histogram_.clear();
  tmpSentryFile_.complete_write();
  tmpSampleFile_.complete_write();
  zbuilder_.reset();
  tmpIndexFile_.Delete();
  tmpStoreFile_.Delete();
  tmpZipDictFile_.Delete();
  tmpZipValueFile_.Delete();
}

// based on Abandon
Status TerarkZipTableBuilder::AbortFinish(const std::exception& ex) {
  closed_ = true;
  for (auto& kvs : histogram_) {
    for (auto& ptr : kvs->build) {
      auto& param = *ptr;
      if (param.wait.valid()) {
        param.wait.get();
      }
      else if (param.data.fp) {
        param.data.complete_write();
      }
    }
    if (kvs->wait.valid()) {
      kvs->wait.get();
    }
  }
  histogram_.clear();
  if (tmpSentryFile_.fp)  tmpSentryFile_.complete_write();
  if (tmpSampleFile_.fp) tmpSampleFile_.complete_write();
  zbuilder_.reset();
  tmpIndexFile_.Delete();
  tmpStoreFile_.Delete();
  tmpZipDictFile_.Delete();
  tmpZipValueFile_.Delete();
  return Status::Aborted("exception", ex.what());
}

void TerarkZipTableBuilder::AddPrevUserKey() {
  UpdateValueLenHistogram(); // will use valueBuf_
  if (zbuilder_) {
    OfflineZipValueData(); // will change valueBuf_
  }
  valueBuf_.erase_all();
  auto& currentHistogram = *histogram_.back();
  currentHistogram.key[prevUserKey_.size()]++;
  currentHistogram.build.back()->data.writer << prevUserKey_;
  currentHistogram.valueBits.push_back(false);
  currentStat_->sumKeyLen += prevUserKey_.size();
  currentStat_->numKeys++;
}

void TerarkZipTableBuilder::AddLastUserKey() {
  AddPrevUserKey();
  currentStat_->maxKey.assign(prevUserKey_);
}

void TerarkZipTableBuilder::OfflineZipValueData() {
  uint64_t seq, seqType = *(uint64_t*)valueBuf_.strpool.data();
  auto& bzvType = histogram_[0]->type;
  ValueType type;
  UnPackSequenceAndType(seqType, &seq, &type);
  const size_t vNum = valueBuf_.size();
  if (vNum == 1 && (kTypeDeletion == type || kTypeValue == type)) {
    if (0 == seq && kTypeValue == type) {
      bzvType.push_back(byte_t(ZipValueType::kZeroSeq));
      zbuilder_->addRecord(fstring(valueBuf_.strpool).substr(8));
    }
    else {
      if (kTypeValue == type) {
        bzvType.push_back(byte_t(ZipValueType::kValue));
      }
      else {
        bzvType.push_back(byte_t(ZipValueType::kDelete));
      }
      // use Little Endian upper 7 bytes
      *(uint64_t*)valueBuf_.strpool.data() <<= 8;
      zbuilder_->addRecord(fstring(valueBuf_.strpool).substr(1));
    }
  }
  else {
    bzvType.push_back(byte_t(ZipValueType::kMulti));
    size_t valueBytes = valueBuf_.strpool.size();
    size_t headerSize = ZipValueMultiValue::calcHeaderSize(vNum);
    valueBuf_.strpool.grow_no_init(headerSize);
    char* pData = valueBuf_.strpool.data();
    memmove(pData + headerSize, pData, valueBytes);
    auto multiVals = (ZipValueMultiValue*)pData;
    multiVals->offsets[0] = uint32_t(vNum);
    for (size_t i = 1; i < vNum; ++i) {
      multiVals->offsets[i] = uint32_t(valueBuf_.offsets[i]);
    }
    zbuilder_->addRecord(valueBuf_.strpool);
  }
}

void TerarkZipTableBuilder::UpdateValueLenHistogram() {
  uint64_t seq, seqType = *(uint64_t*)valueBuf_.strpool.data();
  ValueType type;
  UnPackSequenceAndType(seqType, &seq, &type);
  const size_t vNum = valueBuf_.size();
  size_t valueLen = 0;
  if (vNum == 1 && (kTypeDeletion == type || kTypeValue == type)) {
    if (0 == seq && kTypeValue == type) {
      valueLen = valueBuf_.strpool.size() - 8;
      ++zeroSeqCount_;
    }
    else {
      valueLen = valueBuf_.strpool.size() - 1;
      seqExpandSize_ += 7;
    }
  }
  else {
    seqExpandSize_ += vNum * 8;
    multiValueExpandSize_ += vNum * 4;
    valueLen = valueBuf_.strpool.size() + sizeof(uint32_t)*vNum;
  }
  histogram_.back()->value[valueLen]++;
}


TableBuilder*
createTerarkZipTableBuilder(const TerarkZipTableFactory* table_factory,
                            const TerarkZipTableOptions& tzo,
                            const TableBuilderOptions&   tbo,
                            uint32_t                     column_family_id,
                            WritableFileWriter*          file,
                            size_t                       key_prefixLen)
{
    return new TerarkZipTableBuilder(
        table_factory, tzo, tbo, column_family_id, file, key_prefixLen);
}

} // namespace
