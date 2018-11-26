// project headers
#include "terark_zip_table_reader.h"
#include "terark_zip_common.h"
// rocksdb headers
#include <table/internal_iterator.h>
#include <table/sst_file_writer_collectors.h>
#include <table/meta_blocks.h>
#include <table/get_context.h>
// terark headers
#include <terark/lcast.hpp>
#include <terark/util/crc.hpp>
#include <terark/util/function.hpp>
#include <terark/util/hugepage.hpp>

#ifndef _MSC_VER
# include <sys/mman.h>
# include <sys/unistd.h>
# include <fcntl.h>
#endif

// for isChecksumVerifyEnabled()
#include <terark/zbs/blob_store_file_header.hpp>
// third party
#include <zstd/zstd.h>

namespace {
using namespace rocksdb;

// copy & modify from block_based_table_reader.cc
SequenceNumber GetGlobalSequenceNumber(const TableProperties& table_properties,
  Logger* info_log) {
  auto& props = table_properties.user_collected_properties;

  auto version_pos = props.find(ExternalSstFilePropertyNames::kVersion);
  auto seqno_pos = props.find(ExternalSstFilePropertyNames::kGlobalSeqno);

  if (version_pos == props.end()) {
    if (seqno_pos != props.end()) {
      // This is not an external sst file, global_seqno is not supported.
      assert(false);
      fprintf(stderr,
        "A non-external sst file have global seqno property with value %s\n",
        seqno_pos->second.c_str());
    }
    return kDisableGlobalSequenceNumber;
  }

  uint32_t version = DecodeFixed32(version_pos->second.c_str());
  if (version < 2) {
    if (seqno_pos != props.end() || version != 1) {
      // This is a v1 external sst file, global_seqno is not supported.
      assert(false);
      fprintf(stderr,
        "An external sst file with version %u have global seqno property "
        "with value %s\n",
        version, seqno_pos->second.c_str());
    }
    return kDisableGlobalSequenceNumber;
  }

  SequenceNumber global_seqno = DecodeFixed64(seqno_pos->second.c_str());

  if (global_seqno > kMaxSequenceNumber) {
    assert(false);
    fprintf(stderr,
      "An external sst file with version %u have global seqno property "
      "with value %llu, which is greater than kMaxSequenceNumber\n",
      version, (long long)global_seqno);
  }

  return global_seqno;
}

Block* DetachBlockContents(BlockContents &tombstoneBlock, SequenceNumber global_seqno)
{
  std::unique_ptr<char[]> tombstoneBuf(new char[tombstoneBlock.data.size()]);
  memcpy(tombstoneBuf.get(), tombstoneBlock.data.data(), tombstoneBlock.data.size());
#ifndef _MSC_VER
  uintptr_t ptr = (uintptr_t)tombstoneBlock.data.data();
  uintptr_t aligned_ptr = terark::align_up(ptr, 4096);
  if (aligned_ptr - ptr < tombstoneBlock.data.size()) {
    size_t sz = terark::align_down(
      tombstoneBlock.data.size() - (aligned_ptr - ptr), 4096);
    if (sz > 0) {
        posix_madvise((void*)aligned_ptr, sz, POSIX_MADV_DONTNEED);
    }
  }
#endif
  return new Block(
    BlockContents(std::move(tombstoneBuf), tombstoneBlock.data.size(), false, kNoCompression),
    global_seqno);
}

void SharedBlockCleanupFunction(void* arg1, void* arg2) {
  delete reinterpret_cast<std::shared_ptr<Block>*>(arg1);
}


static void MmapWarmUpBytes(const void* addr, size_t len) {
  auto base = (const byte_t*)(uintptr_t(addr) & uintptr_t(~4095));
  auto size = terark::align_up((size_t(addr) & 4095) + len, 4096);
#ifdef POSIX_MADV_WILLNEED
  posix_madvise((void*)base, size, POSIX_MADV_WILLNEED);
#endif
  size_t sum_unused = 0;
  for (size_t i = 0; i < size; i += 4096) {
    byte_t unused = ((const volatile byte_t*)base)[i];
	sum_unused += unused;
  }
  TERARK_UNUSED_VAR(sum_unused);
}
template<class T>
static void MmapWarmUp(const T* addr, size_t len) {
  MmapWarmUpBytes(addr, sizeof(T)*len);
}
static void MmapWarmUp(fstring mem) {
  MmapWarmUpBytes(mem.data(), mem.size());
}
template<class Vec>
static void MmapWarmUp(const Vec& uv) {
  MmapWarmUpBytes(uv.data(), uv.mem_size());
}

static void MmapColdizeBytes(const void* addr, size_t len) {
  size_t low = terark::align_up(size_t(addr), 4096);
  size_t hig = terark::align_down(size_t(addr) + len, 4096);
  if (low < hig) {
    size_t size = hig - low;
#ifdef POSIX_MADV_DONTNEED
    posix_madvise((void*)low, size, POSIX_MADV_DONTNEED);
#elif defined(_MSC_VER) // defined(_WIN32) || defined(_WIN64)
    VirtualFree((void*)low, size, MEM_DECOMMIT);
#endif
  }
}
static void MmapColdize(fstring mem) {
  MmapColdizeBytes(mem.data(), mem.size());
}
//static void MmapColdize(Slice mem) {
//  MmapColdizeBytes(mem.data(), mem.size());
//}
template<class Vec>
static void MmapColdize(const Vec& uv) {
  MmapColdizeBytes(uv.data(), uv.mem_size());
}

Status DecompressDict(const TableProperties& table_properties, fstring dict, valvec<byte_t>* output_dict) {
  auto find = table_properties.user_collected_properties.find(kTerarkZipTableDictInfo);
  if (find == table_properties.user_collected_properties.end()) {
    return Status::OK();
  }
  const std::string& dictInfo = find->second;
  output_dict->clear();
  if (dictInfo.empty()) {
    return Status::OK();
  }
  if (!fstring(dictInfo).startsWith("ZSTD_")) {
    return Status::Corruption("Load global dict error", "unsupported dict format");
  }
  unsigned long long raw_size = ZSTD_getDecompressedSize(dict.data(), dict.size());
  if (raw_size == 0) {
    return Status::Corruption("Load global dict error", "zstd get raw size fail");
  }
  use_hugepage_resize_no_init(output_dict, raw_size);
  size_t size = ZSTD_decompress(output_dict->data(), raw_size, dict.data(), dict.size());
  if (ZSTD_isError(size)) {
    return Status::Corruption("Load global dict ZSTD error", ZSTD_getErrorName(size));
  }
  assert(size == raw_size);
  MmapColdize(dict);
  return Status::OK();
}


static void MmapAdviseRandom(const void* addr, size_t len) {
  size_t low = terark::align_up(size_t(addr), 4096);
  size_t hig = terark::align_down(size_t(addr) + len, 4096);
  if (low < hig) {
    size_t size = hig - low;
#ifdef POSIX_MADV_RANDOM
    posix_madvise((void*)low, size, POSIX_MADV_RANDOM);
#elif defined(_MSC_VER) // defined(_WIN32) || defined(_WIN64)
#endif
  }
}
static void MmapAdviseRandom(fstring mem) {
  MmapAdviseRandom(mem.data(), mem.size());
}

#if defined(TerocksPrivateCode)
Status UpdateLicenseInfo(const TerarkZipTableFactory* table_factory,
                         Logger* info_log,
                         const BlockContents& licenseBlock) {
  auto& license = table_factory->GetLicense();
  auto res = license.merge(licenseBlock.data.data(), licenseBlock.data.size());
  assert(res == LicenseInfo::Result::OK);
  TERARK_UNUSED_VAR(res);
  if (!license.check()) {
    license.print_error(nullptr, false, info_log);
    return Status::Corruption("License expired", "contact@terark.com");
  }
  return Status::OK();
}
#endif // TerocksPrivateCode

void UpdateCollectInfo(const TerarkZipTableFactory* table_factory,
                       const TerarkZipTableOptions* tzopt,
                       TableProperties *props,
                       size_t file_size) {
  if (!tzopt->enableCompressionProbe) {
    return;
  }
  auto find = props->user_collected_properties.find(kTerarkZipTableBuildTimestamp);
  if (find == props->user_collected_properties.end()) {
    return;
  }
  auto& collect = table_factory->GetCollect();
  uint64_t timestamp = terark::lcast(find->second);
  collect.update(timestamp
      , props->raw_value_size, props->data_size
      , props->raw_key_size + props->raw_value_size, file_size);
}

static bool g_useOldOffsetOf =
  terark::getEnvBool("TerarkZipTable_oldOffsetOf", true);

}

namespace rocksdb {

Status ReadMetaBlockAdapte(RandomAccessFileReader* file,
                           uint64_t file_size,
                           uint64_t table_magic_number,
                           const ImmutableCFOptions& ioptions,
                           const std::string& meta_block_name,
                           BlockContents* contents) {
    return ReadMetaBlock(file, TERARK_ROCKSDB_5007(nullptr,)
        file_size, table_magic_number, ioptions,
        meta_block_name, contents);
}

using terark::BadCrc32cException;
using terark::byte_swap;
using terark::lcast;
using terark::AbstractBlobStore;

class TerarkZipTableIndexIterator : public InternalIterator {
protected:
  const TerarkZipSubReader*         subReader_;
  unique_ptr<TerarkIndex::Iterator> iter_;

public:
  const TerarkIndex::Iterator* GetIndexIterator() const {
    return iter_.get();
  }
  const TerarkZipSubReader* GetSubReader() const {
    return subReader_;
  }
};

template<bool reverse>
class TerarkZipTableIterator : public TerarkZipTableIndexIterator {
protected:
  const TableReaderOptions* table_reader_options_;
  SequenceNumber          global_seqno_;
  uint64_t                key_tag_;
  byte_t*                 key_ptr_;
  size_t                  key_length_;
  size_t                  value_length_;
  terark::BlobStore::CacheOffsets* cache_offsets_;
  Slice                   user_value_;
  ZipValueType            zip_value_type_;
  size_t                  value_count_;
  size_t                  value_index_;
  Status                  status_;
  PinnedIteratorsManager* pinned_iters_mgr_;

  using TerarkZipTableIndexIterator::subReader_;
  using TerarkZipTableIndexIterator::iter_;

  valvec<byte_t>& ValueBuf() const { return cache_offsets_->recData; }

public:
  TerarkZipTableIterator(const TableReaderOptions& tro
                       , const TerarkZipSubReader* subReader
                       , const ReadOptions& ro
                       , SequenceNumber global_seqno)
    : table_reader_options_(&tro)
    , global_seqno_(global_seqno)
  {
    subReader_ = subReader;
    if (subReader_ != nullptr) {
      iter_.reset(subReader_->index_->NewIterator());
      iter_->SetInvalid();
    }
    pinned_iters_mgr_ = NULL;
    key_tag_ = 0;
    key_ptr_ = nullptr;
    key_length_ = 0;
    value_length_ = 0;
    cache_offsets_ = NULL;
    value_index_ = 0;
    value_count_ = 0;
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }

  bool Valid() const override {
    return iter_->Valid();
  }

  void SeekToFirst() override {
    if (UnzipIterRecord(IndexIterSeekToFirst())) {
      DecodeCurrKeyValue();
    }
    if (key_tag_ == port::kMaxUint64) {
      Next();
    }
  }

  void SeekToLast() override {
    if (UnzipIterRecord(IndexIterSeekToLast())) {
      value_index_ = value_count_ - 1;
      DecodeCurrKeyValue();
    }
    if (key_tag_ == port::kMaxUint64) {
      Prev();
    }
  }

  void Seek(const Slice& target) override {
    ParsedInternalKey pikey;
    if (!ParseInternalKey(target, &pikey)) {
      status_ = Status::InvalidArgument("TerarkZipTableIterator::Seek()",
        "param target.size() < 8");
      SetIterInvalid();
      return;
    }
    SeekInternal(pikey);
    if (key_tag_ == port::kMaxUint64) {
      Next();
    }
  }

  void SeekForPrev(const Slice& target) override {
    SeekForPrevImpl(target, &table_reader_options_->internal_comparator);
  }

  void Next() override {
    do {
      assert(iter_->Valid());
      value_index_++;
      if (value_index_ < value_count_) {
        DecodeCurrKeyValue();
      }
      else {
        if (UnzipIterRecord(IndexIterNext())) {
          DecodeCurrKeyValue();
        }
      }
    } while (key_tag_ == port::kMaxUint64);
  }

  void Prev() override {
    do {
      assert(iter_->Valid());
      if (value_index_ > 0) {
        value_index_--;
        DecodeCurrKeyValue();
      }
      else {
        if (UnzipIterRecord(IndexIterPrev())) {
          value_index_ = value_count_ - 1;
          DecodeCurrKeyValue();
        }
      }
    } while (key_tag_ == port::kMaxUint64);
  }

  Slice key() const override {
    assert(iter_->Valid());
    assert(key_tag_ != port::kMaxUint64);
    return Slice((const char*)key_ptr_, key_length_);
  }

  Slice value() const override {
    assert(iter_->Valid());
    return user_value_;
  }

  Status status() const override {
    return status_;
  }

  uint64_t FileNumber() const override {
    return table_reader_options_->file_number;
  }

  bool IsKeyPinned() const {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled();
  }
  bool IsValuePinned() const {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled();
  }

protected:
  void SeekToAscendingFirst() {
    if (UnzipIterRecord(iter_->SeekToFirst())) {
      if (reverse)
        value_index_ = value_count_ - 1;
      DecodeCurrKeyValue();
    }
  }
  void SeekToAscendingLast() {
    if (UnzipIterRecord(iter_->SeekToLast())) {
      if (!reverse)
        value_index_ = value_count_ - 1;
      DecodeCurrKeyValue();
    }
  }
  void SeekInternal(const ParsedInternalKey& pikey) {
    // Damn MySQL-rocksdb may use "rev:" comparator
    size_t cplen = fstringOf(pikey.user_key).commonPrefixLen(subReader_->commonPrefix_);
    if (subReader_->commonPrefix_.size() != cplen) {
      if (pikey.user_key.size() == cplen) {
        assert(pikey.user_key.size() < subReader_->commonPrefix_.size());
        if (reverse)
          SetIterInvalid();
        else
          SeekToAscendingFirst();
      }
      else {
        assert(pikey.user_key.size() > cplen);
        assert(pikey.user_key[cplen] != subReader_->commonPrefix_[cplen]);
        if ((byte_t(pikey.user_key[cplen]) < byte_t(subReader_->commonPrefix_[cplen])) ^ reverse) {
          if (reverse)
            SeekToAscendingLast();
          else
            SeekToAscendingFirst();
        }
        else {
          SetIterInvalid();
        }
      }
    }
    else {
      bool ok;
      int cmp; // compare(iterKey, searchKey)
      auto seek_key = fstringOf(pikey.user_key).substr(cplen);
      ok = iter_->Seek(seek_key);
      if (reverse) {
        if (!ok) {
          // searchKey is reverse_bytewise less than all keys in database
          iter_->SeekToLast();
          assert(iter_->Valid()); // TerarkIndex should not empty
          ok = true;
          cmp = -1;
        }
        else {
          cmp = terark::fstring_func::compare3()(iter_->key(), seek_key);
          if (cmp != 0) {
            assert(cmp > 0);
            iter_->Prev();
            ok = iter_->Valid();
          }
        }
      }
      else {
        cmp = 0;
        if (ok)
          cmp = terark::fstring_func::compare3()(iter_->key(), seek_key);
      }
      if (UnzipIterRecord(ok)) {
        if (0 == cmp) {
          value_index_ = size_t(-1);
          do {
            value_index_++;
            DecodeCurrKeyValue();
            if (key_tag_ == port::kMaxUint64) {
              continue;
            }
            if ((key_tag_ >> 8) <= pikey.sequence) {
              return; // done
            }
          } while (value_index_ + 1 < value_count_);
          // no visible version/sequence for target, use Next();
          // if using Next(), version check is not needed
          Next();
        }
        else {
          DecodeCurrKeyValue();
        }
      }
    }
  }
  void SetIterInvalid() {
    if (iter_)
      iter_->SetInvalid();
    key_tag_ = 0;
    key_ptr_ = nullptr;
    key_length_ = 0;
    value_length_ = 0;
    invalidate_offsets_cache();
    value_index_ = 0;
    value_count_ = 0;
  }
  virtual void invalidate_offsets_cache() = 0;
  virtual bool IndexIterSeekToFirst() {
    if (reverse)
      return iter_->SeekToLast();
    else
      return iter_->SeekToFirst();
  }
  virtual bool IndexIterSeekToLast() {
    if (reverse)
      return iter_->SeekToFirst();
    else
      return iter_->SeekToLast();
  }
  virtual bool IndexIterPrev() {
    if (reverse)
      return iter_->Next();
    else
      return iter_->Prev();
  }
  virtual bool IndexIterNext() {
    if (reverse)
      return iter_->Prev();
    else
      return iter_->Next();
  }
  void TryPinBuffer(valvec<byte_t>& buf) {
    if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
      pinned_iters_mgr_->PinPtr(buf.data(), free);
      buf.risk_release_ownership();
    }
  }
  bool UnzipIterRecord(bool hasRecord) {
    if (hasRecord) {
      auto& value_buf = ValueBuf();
      fstring user_key = iter_->key();
      try {
        TryPinBuffer(ValueBuf());
        size_t recId = iter_->id();
        zip_value_type_ = subReader_->type_.size()
          ? ZipValueType(subReader_->type_[recId])
          : ZipValueType::kZeroSeq;
        key_length_ = subReader_->prefix_.size() + subReader_->commonPrefix_.size() +
                      user_key.size() + sizeof key_tag_;
        size_t mulnum_size = 0;
        if (ZipValueType::kMulti == zip_value_type_) {
          mulnum_size = sizeof(uint32_t); // for offsets[valnum_]
        }
        value_buf.ensure_capacity(key_length_ + subReader_->estimateUnzipCap_ + mulnum_size);
        value_buf.resize_no_init(mulnum_size);
        subReader_->GetRecordAppend(recId, cache_offsets_);
      }
      catch (const std::exception& ex) { // crc checksum error
        SetIterInvalid();
        status_ = Status::Corruption(
          "TerarkZipTableIterator::UnzipIterRecord()", ex.what());
        return false;
      }
      if (ZipValueType::kMulti == zip_value_type_ && value_buf.size() != sizeof(uint32_t)) {
        ZipValueMultiValue::decode(value_buf, &value_count_);
        uint32_t* offsets = (uint32_t*)value_buf.data();
        size_t pos = 0;
        char* base = (char*)(offsets + value_count_ + 1);
        for (size_t i = 0; i < value_count_; ++i) {
          size_t q = offsets[i + 0];
          size_t r = offsets[i + 1];
          size_t l = r - q;
          offsets[i] = pos;
          memmove(base + pos, base + q, l);
          pos += l;
        }
        offsets[value_count_] = pos;
      }
      else {
        value_count_ = 1;
      }
      value_index_ = 0;
      value_length_ = value_buf.size();
      value_buf.resize_no_init(value_length_ + key_length_ * value_count_);
      return true;
    }
    else {
      SetIterInvalid();
      return false;
    }
  }
  virtual void DecodeCurrKeyValue() {
    assert(status_.ok());
    assert(iter_->id() < subReader_->index_->NumKeys());
    auto& value_buf = ValueBuf();
    switch (zip_value_type_) {
    default:
      status_ = Status::Aborted("TerarkZipTableIterator::DecodeCurrKeyValue()",
        "Bad ZipValueType");
      abort(); // must not goes here, if it does, it should be a bug!!
      break;
    case ZipValueType::kZeroSeq:
      assert(0 == value_index_);
      assert(1 == value_count_);
      key_ptr_ = value_buf.data() + value_length_;
      key_tag_ = PackSequenceAndType(global_seqno_, kTypeValue);
      user_value_ = SliceOf(fstring(value_buf).substr(0, value_length_));
      break;
    case ZipValueType::kValue: // should be a kTypeValue, the normal case
      assert(0 == value_index_);
      assert(1 == value_count_);
      key_ptr_ = value_buf.data() + value_length_;
      // little endian uint64_t
      key_tag_ = PackSequenceAndType(*(uint64_t*)value_buf.data() & kMaxSequenceNumber,
                                     kTypeValue);
      user_value_ = SliceOf(fstring(value_buf).substr(7, value_length_ - 7));
      break;
    case ZipValueType::kDelete:
      assert(0 == value_index_);
      assert(1 == value_count_);
      key_ptr_ = value_buf.data() + value_length_;
      // little endian uint64_t
      key_tag_ = PackSequenceAndType(*(uint64_t*)value_buf.data() & kMaxSequenceNumber,
                                     kTypeDeletion);
      user_value_ = SliceOf(fstring(value_buf).substr(7, value_length_ - 7));
      break;
    case ZipValueType::kMulti: { // more than one value
      auto zmValue = (const ZipValueMultiValue*)value_buf.data();
      assert(0 != value_count_);
      assert(value_index_ < value_count_);
      key_ptr_ = value_buf.data() + value_length_ + key_length_ * value_index_;
      Slice d;
      if (value_length_ == sizeof(uint32_t) || (d = zmValue->getValueData(value_index_, value_count_), d.empty())) {
        key_tag_ = port::kMaxUint64;
        user_value_.clear();
      }
      else {
        key_tag_ = unaligned_load<SequenceNumber>(d.data());
        d.remove_prefix(sizeof(SequenceNumber));
        user_value_ = d;
      }
      break; }
    }
    byte_t* key_ptr = key_ptr_;
    fstring user_key = iter_->key();
    memcpy(key_ptr, subReader_->prefix_.data(), subReader_->prefix_.size());
    key_ptr += subReader_->prefix_.size();
    memcpy(key_ptr, subReader_->commonPrefix_.data(), subReader_->commonPrefix_.size());
    key_ptr += subReader_->commonPrefix_.size();
    memcpy(key_ptr, user_key.data(), user_key.size());
    key_ptr += user_key.size();
    EncodeFixed64((char*)key_ptr, key_tag_);
  }
};

template<class Base, bool ZipOffset>
class IterZO : public Base {
  terark::BlobStoreRecBuffer<ZipOffset> rb_;
public:
  template<class... Args>
  IterZO(Args&&... a) : Base(std::forward<Args>(a)...) {
    // it is safe to reinterpret_cast here
    using CacheOffsets = terark::BlobStore::CacheOffsets;
    this->cache_offsets_ = reinterpret_cast<CacheOffsets*>(&rb_);
  }
  virtual ~IterZO() {}
  virtual void invalidate_offsets_cache() override {
    rb_.invalidate_offsets_cache();
  }
};

#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
class TerarkZipTableUint64Iterator : public TerarkZipTableIterator<false> {
public:
  TerarkZipTableUint64Iterator(const TableReaderOptions& tro
                             , const TerarkZipSubReader *subReader
                             , const ReadOptions& ro
                             , SequenceNumber global_seqno)
    : TerarkZipTableIterator<false>(tro, subReader, ro, global_seqno) {
  }
protected:
  typedef TerarkZipTableIterator<false> base_t;
  using base_t::subReader_;
  using base_t::key_length_;
  using base_t::key_tag_;
  using base_t::status_;

  using base_t::Next;
  using base_t::SeekInternal;
  using base_t::DecodeCurrKeyValue;

public:
  void Seek(const Slice& target) override {
    ParsedInternalKey pikey;
    if (!ParseInternalKey(target, &pikey)) {
      status_ = Status::InvalidArgument("TerarkZipTableUint64Iterator::Seek()",
        "param target.size() < 8");
      SetIterInvalid();
      return;
    }
    uint64_t u64_target;
    assert(pikey.user_key.size() == 8);
    u64_target = byte_swap(*reinterpret_cast<const uint64_t*>(pikey.user_key.data()));
    pikey.user_key = Slice(reinterpret_cast<const char*>(&u64_target), 8);
    SeekInternal(pikey);
    if (key_tag_ == port::kMaxUint64) {
      Next();
    }
  }
  void DecodeCurrKeyValue() override {
    base_t::DecodeCurrKeyValue();
    assert(key_length_ == 16);
    uint64_t& u64_key = *(uint64_t*)key_ptr_;
    u64_key = terark::byte_swap(u64_key);
  }
};
#endif

template<bool reverse>
class TerarkZipTableMultiIterator : public TerarkZipTableIterator<reverse> {
public:
  TerarkZipTableMultiIterator(const TableReaderOptions& tro
                            , const TerarkZipTableMultiReader::SubIndex& subIndex
                            , const ReadOptions& ro
                            , SequenceNumber global_seqno)
    : TerarkZipTableIterator<reverse>(tro, nullptr, ro, global_seqno)
    , subIndex_(&subIndex)
  {}
protected:
  const TerarkZipTableMultiReader::SubIndex* subIndex_;

  typedef TerarkZipTableIterator<reverse> base_t;
  using base_t::subReader_;
  using base_t::iter_;
  using base_t::key_tag_;
  using base_t::status_;
  using base_t::invalidate_offsets_cache;

  using base_t::Next;
  using base_t::SeekToAscendingFirst;
  using base_t::SeekToAscendingLast;
  using base_t::SetIterInvalid;
  using base_t::SeekInternal;

public:
  bool Valid() const override {
    return iter_ && iter_->Valid();
  }

  void Seek(const Slice& target) override {
    ParsedInternalKey pikey;
    if (!ParseInternalKey(target, &pikey)) {
      status_ = Status::InvalidArgument("TerarkZipTableMultiIterator::Seek()",
        "param target.size() < 8");
      SetIterInvalid();
      return;
    }
    auto subReader = subIndex_->LowerBoundSubReader(fstringOf(pikey.user_key));
    if (subReader == nullptr) {
      SetIterInvalid();
      return;
    }
    if (subReader_ != subReader) {
      ResetIter(subReader);
    }
    if (!pikey.user_key.starts_with(SliceOf(subReader->prefix_))) {
      if (reverse)
        SeekToAscendingLast();
      else
        SeekToAscendingFirst();
    }
    else {
      pikey.user_key.remove_prefix(subReader->prefix_.size());
      SeekInternal(pikey);
      if (!Valid()) {
        if (reverse) {
          if (subReader->subIndex_ != 0) {
            ResetIter(subIndex_->GetSubReader(subReader->subIndex_ - 1));
            SeekToAscendingLast();
          }
        }
        else {
          if (subReader->subIndex_ != subIndex_->GetSubCount() - 1) {
            ResetIter(subIndex_->GetSubReader(subReader->subIndex_ + 1));
            SeekToAscendingFirst();
          }
        }
      }
    }
    if (key_tag_ == port::kMaxUint64) {
      Next();
    }
  }

protected:
  void ResetIter(const TerarkZipSubReader* subReader) {
    subReader_ = subReader;
    iter_.reset(subReader->index_->NewIterator());
    invalidate_offsets_cache();
  }
  bool IndexIterSeekToFirst() override {
    if (reverse) {
      ResetIter(subIndex_->GetSubReader(subIndex_->GetSubCount() - 1));
      return iter_->SeekToLast();
    }
    else {
      ResetIter(subIndex_->GetSubReader(0));
      return iter_->SeekToFirst();
    }
  }
  bool IndexIterSeekToLast() override {
    if (reverse) {
      ResetIter(subIndex_->GetSubReader(0));
      return iter_->SeekToFirst();
    }
    else {
      ResetIter(subIndex_->GetSubReader(subIndex_->GetSubCount() - 1));
      return iter_->SeekToLast();
    }
  }
  bool IndexIterPrev() override {
    if (reverse) {
      if (iter_->Next())
        return true;
      if (subReader_->subIndex_ == subIndex_->GetSubCount() - 1)
        return false;
      ResetIter(subIndex_->GetSubReader(subReader_->subIndex_ + 1));
      return iter_->SeekToFirst();
    }
    else {
      if (iter_->Prev())
        return true;
      if (subReader_->subIndex_ == 0)
        return false;
      ResetIter(subIndex_->GetSubReader(subReader_->subIndex_ - 1));
      return iter_->SeekToLast();
    }
  }
  bool IndexIterNext() override {
    if (reverse) {
      if (iter_->Prev())
        return true;
      if (subReader_->subIndex_ == 0)
        return false;
      ResetIter(subIndex_->GetSubReader(subReader_->subIndex_ - 1));
      return iter_->SeekToLast();
    }
    else {
      if (iter_->Next())
        return true;
      if (subReader_->subIndex_ == subIndex_->GetSubCount() - 1)
        return false;
      ResetIter(subIndex_->GetSubReader(subReader_->subIndex_ + 1));
      return iter_->SeekToFirst();
    }
  }
};

Status TerarkZipTableReaderBase::
LoadTombstone(RandomAccessFileReader * file, uint64_t file_size) {
  BlockContents tombstoneBlock;
  Status s = ReadMetaBlockAdapte(file, file_size, kTerarkZipTableMagicNumber,
    GetTableReaderOptions().ioptions,  kRangeDelBlock, &tombstoneBlock);
  if (s.ok()) {
    tombstone_.reset(DetachBlockContents(tombstoneBlock, GetSequenceNumber()));
  }
  return s;
}

InternalIterator* TerarkZipTableReaderBase::
NewRangeTombstoneIterator(const ReadOptions & read_options) {
  if (tombstone_) {
    auto icomp = &GetTableReaderOptions().internal_comparator;
    auto iter = tombstone_->NewIterator<DataBlockIter>(
      icomp, icomp->user_comparator(), nullptr,
      GetTableReaderOptions().ioptions.statistics);
    iter->RegisterCleanup(SharedBlockCleanupFunction,
      new std::shared_ptr<Block>(tombstone_), nullptr);
    return iter;
  }
  return nullptr;
}

void TerarkZipSubReader::InitUsePread(int minPreadLen) {
  if (minPreadLen < 0) {
    storeUsePread_ = false;
  }
  else if (minPreadLen == 0) {
    storeUsePread_ = true;
  }
  else {
    size_t numRecords = store_->num_records();
    size_t memSize = store_->get_mmap().size();
    storeUsePread_ = memSize > minPreadLen * numRecords;
  }
  double sumUnzipSize = store_->total_data_size();
  double avgUnzipSize = sumUnzipSize / store_->num_records();
  estimateUnzipCap_ = size_t(avgUnzipSize * 1.62); // a bit larger than 1.618
}

static const byte_t*
FsPread(void* vself, uint64_t offset, size_t len, valvec<byte_t>* buf) {
  TerarkZipSubReader* self = (TerarkZipSubReader*)vself;
  buf->resize_no_init(len);
  Status s = self->storeFileObj_->FsRead(offset, len, buf->data());
  if (terark_unlikely(!s.ok())) {
    // to be catched by TerarkZipSubReader::Get()
    throw std::logic_error(s.ToString());
  }
  return buf->data();
}
void TerarkZipSubReader::GetRecordAppend(size_t recId, valvec<byte_t>* tbuf)
const {
  if (storeUsePread_) {
    auto cache = cache_;
    if (cache)
      store_->pread_record_append(cache, storeFD_, storeOffset_, recId, tbuf);
    else
      store_->fspread_record_append(&FsPread, (void*)this, storeOffset_, recId, tbuf);
  }
  else
    store_->get_record_append(recId, tbuf);
}

void TerarkZipSubReader::GetRecordAppend(
        size_t recId, terark::BlobStore::CacheOffsets* co)
const {
  if (storeUsePread_) {
    auto cache = cache_;
    if (cache)
      store_->pread_record_append(cache, storeFD_, storeOffset_, recId, &co->recData);
    else
      store_->fspread_record_append(&FsPread, (void*)this, storeOffset_, recId, &co->recData);
  }
  else
    store_->get_record_append(recId, co);
}

Status TerarkZipSubReader::Get(SequenceNumber global_seqno,
                               const ReadOptions& ro, const Slice& ikey,
                               GetContext* get_context, int flag)
const {
  TERARK_UNUSED_VAR(flag);
  MY_THREAD_LOCAL(valvec<byte_t>, g_tbuf);
  ParsedInternalKey pikey;
  if (!ParseInternalKey(ikey, &pikey)) {
    return Status::InvalidArgument("TerarkZipTableReader::Get()",
      "bad internal key causing ParseInternalKey() failed");
  }
  Slice user_key = pikey.user_key;
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
  uint64_t u64_target;
  if (flag & FlagUint64Comparator) {
    assert(pikey.user_key.size() == 8);
    u64_target = byte_swap(*reinterpret_cast<const uint64_t*>(pikey.user_key.data()));
    user_key = Slice(reinterpret_cast<const char*>(&u64_target), 8);
  }
#endif
  assert(user_key.starts_with(prefix_));
  user_key.remove_prefix(prefix_.size());
  size_t cplen = user_key.difference_offset(commonPrefix_);
  if (commonPrefix_.size() != cplen) {
    return Status::OK();
  }
  size_t recId = index_->Find(fstringOf(user_key).substr(cplen));
  if (size_t(-1) == recId) {
    return Status::OK();
  }
  auto zvType = type_.size()
    ? ZipValueType(type_[recId])
    : ZipValueType::kZeroSeq;
  bool matched;
  switch (zvType) {
  default:
    return Status::Aborted("TerarkZipTableReader::Get()", "Bad ZipValueType");
  case ZipValueType::kZeroSeq:
    g_tbuf.erase_all();
    try {
      GetRecordAppend(recId, &g_tbuf);
    }
    catch (const std::exception& ex) {
      return Status::Corruption("TerarkZipTableReader::Get()", ex.what());
    }
    get_context->SaveValue(ParsedInternalKey(pikey.user_key, global_seqno, kTypeValue),
      Slice((char*)g_tbuf.data(), g_tbuf.size()), &matched);
    break;
  case ZipValueType::kValue: { // should be a kTypeValue, the normal case
    g_tbuf.erase_all();
    try {
      GetRecordAppend(recId, &g_tbuf);
    }
    catch (const std::exception& ex) {
      return Status::Corruption("TerarkZipTableReader::Get()", ex.what());
    }
                               // little endian uint64_t
    uint64_t seq = *(uint64_t*)g_tbuf.data() & kMaxSequenceNumber;
    if (seq <= pikey.sequence) {
      get_context->SaveValue(ParsedInternalKey(pikey.user_key, seq, kTypeValue),
        SliceOf(fstring(g_tbuf).substr(7)), &matched);
    }
    break; }
  case ZipValueType::kDelete: {
    g_tbuf.erase_all();
    try {
      g_tbuf.reserve(sizeof(SequenceNumber));
      GetRecordAppend(recId, &g_tbuf);
    }
    catch (const std::exception& ex) {
      return Status::Corruption("TerarkZipTableReader::Get()", ex.what());
    }
    uint64_t seq = *(uint64_t*)g_tbuf.data() & kMaxSequenceNumber;
    if (seq <= pikey.sequence) {
      get_context->SaveValue(ParsedInternalKey(pikey.user_key, seq, kTypeDeletion),
        SliceOf(fstring(g_tbuf).substr(7)), &matched);
    }
    break; }
  case ZipValueType::kMulti: { // more than one value
    g_tbuf.resize_no_init(sizeof(uint32_t));
    try {
      GetRecordAppend(recId, &g_tbuf);
    }
    catch (const std::exception& ex) {
      return Status::Corruption("TerarkZipTableReader::Get()", ex.what());
    }
    if (g_tbuf.size() == sizeof(uint32_t)) {
      break;
    }
    size_t num = 0;
    auto mVal = ZipValueMultiValue::decode(g_tbuf, &num);
    for (size_t i = 0; i < num; ++i) {
      Slice val = mVal->getValueData(i, num);
      if (val.empty()) {
        continue;
      }
      auto tag = unaligned_load<SequenceNumber>(val.data());
      SequenceNumber sn;
      ValueType valtype;
      UnPackSequenceAndType(tag, &sn, &valtype);
      if (sn <= pikey.sequence) {
        val.remove_prefix(sizeof(SequenceNumber));
        bool hasMoreValue = get_context->SaveValue(
          ParsedInternalKey(pikey.user_key, sn, valtype), val, &matched);
        if (!hasMoreValue) {
          break;
        }
      }
    }
    break; }
  }
  if (g_tbuf.capacity() > 512 * 1024) {
    g_tbuf.clear(); // free large thread local memory
  }
  return Status::OK();
}

size_t TerarkZipSubReader::DictRank(fstring key) const {
  auto& cp = commonPrefix_;
  size_t cplen = key.commonPrefixLen(cp);
  if (cp.size() != cplen) {
    if (key.size() == cplen) {
      assert(key.size() < cp.size());
      return 0;
    }
    else {
      assert(key.size() > cplen);
      assert(key[cplen] != cp[cplen]);
      if ((byte_t(key[cplen]) < byte_t(cp[cplen])))
        return 0;
      else
        return index_->NumKeys();
    }
  }
  else {
    return index_->DictRank(key.substr(cplen));
  }
}

TerarkZipSubReader::~TerarkZipSubReader() {
  type_.risk_release_ownership();
}

Status
TerarkEmptyTableReader::Open(RandomAccessFileReader* file, uint64_t file_size) {
  file_.reset(file); // take ownership
  const auto& ioptions = table_reader_options_.ioptions;
  TableProperties* props = nullptr;
  Status s = ReadTableProperties(file, file_size,
    kTerarkZipTableMagicNumber, ioptions, &props);
  if (!s.ok()) {
    return s;
  }
  assert(nullptr != props);
  table_properties_.reset(props);
  Slice file_data;
  if (table_reader_options_.env_options.use_mmap_reads) {
    s = file->Read(0, file_size, &file_data, nullptr);
    if (!s.ok())
      return s;
  }
  else {
    return Status::InvalidArgument("TerarkZipTableReader::Open()",
      "EnvOptions::use_mmap_reads must be true");
  }
  if (props->comparator_name != fstring(ioptions.user_comparator->Name()) && 0) {
    return Status::InvalidArgument("TerarkZipTableReader::Open()",
      "Invalid user_comparator , need " + props->comparator_name
      + ", but provid " + ioptions.user_comparator->Name());
  }
  file_data_ = file_data;
  global_seqno_ = GetGlobalSequenceNumber(*props, ioptions.info_log);
#if defined(TerocksPrivateCode)
  BlockContents licenseBlock;
  s = ReadMetaBlockAdapte(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableExtendedBlock, &licenseBlock);
  if (s.ok()) {
    s = UpdateLicenseInfo(table_factory_, ioptions.info_log, licenseBlock);
    if (!s.ok()) {
      return s;
    }
  }
#endif // TerocksPrivateCode
  s = LoadTombstone(file, file_size);
  if (global_seqno_ == kDisableGlobalSequenceNumber) {
    global_seqno_ = 0;
  }
  INFO(ioptions.info_log
    , "TerarkZipTableReader::Open(): fsize = %zd, entries = %zd keys = 0 indexSize = 0 valueSize = 0, warm up time =      0.000'sec, build cache time =      0.000'sec\n"
    , size_t(file_size), size_t(props->num_entries)
  );
  return Status::OK();
}

AbstractBlobStore::Dictionary
getVerifyDict(Slice dictData) {
  if (terark::isChecksumVerifyEnabled()) {
    return AbstractBlobStore::Dictionary(fstringOf(dictData));
  } else {
    return AbstractBlobStore::Dictionary(fstringOf(dictData), 0);
  }
}

Status
TerarkZipTableReader::Open(RandomAccessFileReader* file, uint64_t file_size) {
  file_.reset(file); // take ownership
  const auto& ioptions = table_reader_options_.ioptions;
  TableProperties* props = nullptr;
  Status s = ReadTableProperties(file, file_size,
    kTerarkZipTableMagicNumber, ioptions, &props);
  if (!s.ok()) {
    return s;
  }
  assert(nullptr != props);
  table_properties_.reset(props);
  Slice file_data;
  if (table_reader_options_.env_options.use_mmap_reads) {
    s = file->Read(0, file_size, &file_data, nullptr);
    if (!s.ok())
      return s;
  }
  else {
    return Status::InvalidArgument("TerarkZipTableReader::Open()",
      "EnvOptions::use_mmap_reads must be true");
  }
  if (props->comparator_name != fstring(ioptions.user_comparator->Name()) && 0) {
    return Status::InvalidArgument("TerarkZipTableReader::Open()",
      "Invalid user_comparator , need " + props->comparator_name
      + ", but provid " + ioptions.user_comparator->Name());
  }
  file_data_ = file_data;
  global_seqno_ = GetGlobalSequenceNumber(*props, ioptions.info_log);
  isReverseBytewiseOrder_ =
    fstring(ioptions.user_comparator->Name()).startsWith("rev:");
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
  isUint64Comparator_ =
    fstring(ioptions.user_comparator->Name()) == "rocksdb.Uint64Comparator";
#endif
  BlockContents valueDictBlock, indexBlock, zValueTypeBlock, commonPrefixBlock;
#if defined(TerocksPrivateCode)
  BlockContents licenseBlock;
  s = ReadMetaBlockAdapte(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableExtendedBlock, &licenseBlock);
  if (s.ok()) {
    s = UpdateLicenseInfo(table_factory_, ioptions.info_log, licenseBlock);
    if (!s.ok()) {
      return s;
    }
  }
#endif // TerocksPrivateCode
  UpdateCollectInfo(table_factory_, &tzto_, props, file_size);
  s = ReadMetaBlockAdapte(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableValueDictBlock, &valueDictBlock);
  Slice dict = valueDictBlock.data;
  if (s.ok()) {
    s = DecompressDict(*props, fstringOf(valueDictBlock.data), &dict_);
    if (!s.ok()) {
      return s;
    }
    dict = dict_.empty() ? valueDictBlock.data : SliceOf(dict_);
  }
  props->user_collected_properties.emplace(kTerarkZipTableDictSize, lcast(dict.size()));
  // PlainBlobStore & MixedLenBlobStore no dict
  s = ReadMetaBlockAdapte(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableIndexBlock, &indexBlock);
  if (!s.ok()) {
    return s;
  }
  s = LoadTombstone(file, file_size);
  if (global_seqno_ == kDisableGlobalSequenceNumber) {
    global_seqno_ = 0;
  }
  s = ReadMetaBlockAdapte(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableCommonPrefixBlock, &commonPrefixBlock);
  if (s.ok()) {
    subReader_.commonPrefix_.assign(commonPrefixBlock.data.data(),
      commonPrefixBlock.data.size());
  }
  else {
    // some error, usually is
    // Status::Corruption("Cannot find the meta block", meta_block_name)
    WARN(ioptions.info_log
      , "Read %s block failed, treat as old SST version, error: %s\n"
      , kTerarkZipTableCommonPrefixBlock.c_str()
      , s.ToString().c_str());
  }
  try {
    subReader_.store_.reset(AbstractBlobStore::load_from_user_memory(
      fstring(file_data.data(), props->data_size),
      getVerifyDict(dict)
    ));
  }
  catch (const BadCrc32cException& ex) {
    return Status::Corruption("TerarkZipTableReader::Open()", ex.what());
  }
  s = LoadIndex(indexBlock.data);
  if (!s.ok()) {
    return s;
  }
  size_t recNum = subReader_.index_->NumKeys();
  s = ReadMetaBlockAdapte(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableValueTypeBlock, &zValueTypeBlock);
  if (s.ok()) {
    subReader_.type_.risk_set_data((byte_t*)zValueTypeBlock.data.data(), recNum);
  }
  subReader_.subIndex_ = 0;
  subReader_.storeFD_ = file_->file()->FileDescriptor();
  subReader_.storeFileObj_ = file_->file();
  subReader_.storeOffset_ = 0;
  subReader_.InitUsePread(tzto_.minPreadLen);
  subReader_.rawReaderOffset_ = 0;
  subReader_.rawReaderSize_ = indexBlock.data.size() + props->data_size;
  if (subReader_.storeUsePread_) {
    subReader_.cache_ = table_factory_->cache();
    if (subReader_.cache_) {
      subReader_.storeFD_ = subReader_.cache_->open(subReader_.storeFD_);
    }
  }
  long long t0 = g_pf.now();
  if (tzto_.warmUpIndexOnOpen) {
    MmapWarmUp(fstringOf(indexBlock.data));
    if (!tzto_.warmUpValueOnOpen) {
      for (fstring block : subReader_.store_->get_meta_blocks()) {
        MmapWarmUp(block);
      }
    }
  }
  if (tzto_.warmUpValueOnOpen && !subReader_.storeUsePread_) {
    for (fstring block : subReader_.store_->get_data_blocks()) {
      MmapWarmUp(block);
    }
  } else {
    //MmapColdize(subReader_.store_->get_mmap());
    if (ioptions.advise_random_on_open) {
      for (fstring block : subReader_.store_->get_data_blocks()) {
        MmapAdviseRandom(block);
      }
    }
  }
  long long t1 = g_pf.now();
  subReader_.index_->BuildCache(tzto_.indexCacheRatio);
  long long t2 = g_pf.now();
  INFO(ioptions.info_log
    , "TerarkZipTableReader::Open(): fsize = %zd, entries = %zd keys = %zd indexSize = %zd valueSize=%zd, warm up time = %6.3f'sec, build cache time = %6.3f'sec\n"
    , size_t(file_size), size_t(props->num_entries)
    , subReader_.index_->NumKeys()
    , size_t(props->index_size)
    , size_t(props->data_size)
    , g_pf.sf(t0, t1)
    , g_pf.sf(t1, t2)
  );
  return Status::OK();
}



Status TerarkZipTableReader::LoadIndex(Slice mem) {
  auto func = "TerarkZipTableReader::LoadIndex()";
  try {
    subReader_.index_ = TerarkIndex::LoadMemory(fstringOf(mem));
  }
  catch (const BadCrc32cException& ex) {
    return Status::Corruption(func, ex.what());
  }
  catch (const std::exception& ex) {
    return Status::InvalidArgument(func, ex.what());
  }
  return Status::OK();
}

InternalIterator*
TerarkZipTableReader::
NewIterator(const ReadOptions& ro, const SliceTransform* prefix_extractor,
            Arena* arena, bool skip_filters, bool for_compaction) {
  TERARK_UNUSED_VAR(skip_filters); // unused
  const bool ZipOffset = !!subReader_.store_->is_offsets_zipped();
#define ForTemplateArg(a, b) \
  if (a == !!isReverseBytewiseOrder_ && b == ZipOffset) \
    return NewIteratorImp<a,b>(ro, arena);
  ForTemplateArg(0,0);
  ForTemplateArg(0,1);
  ForTemplateArg(1,0);
  ForTemplateArg(1,1);
  assert(false);
  return NULL;
}

template<bool reverse, bool ZipOffset>
InternalIterator*
TerarkZipTableReader::
NewIteratorImp(const ReadOptions& ro, Arena* arena) {
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
  if (isUint64Comparator_) {
    typedef IterZO<TerarkZipTableUint64Iterator, ZipOffset> IterType;
    if (arena) {
      return new(arena->AllocateAligned(sizeof(IterType)))
        IterType(table_reader_options_, &subReader_, ro, global_seqno_);
    }
    else {
      return new IterType(table_reader_options_, &subReader_, ro, global_seqno_);
    }
  }
#endif
  typedef IterZO<TerarkZipTableIterator<reverse>, ZipOffset> IterType;
  if (arena) {
    return new(arena->AllocateAligned(sizeof(IterType)))
      IterType(table_reader_options_, &subReader_, ro, global_seqno_);
  }
  else {
    return new IterType(table_reader_options_, &subReader_, ro, global_seqno_);
  }
}

Status
TerarkZipTableReader::Get(const ReadOptions& ro, const Slice& ikey,
                          GetContext* get_context, const SliceTransform* prefix_extractor,
                          bool skip_filters) {
  int flag = skip_filters ? TerarkZipSubReader::FlagSkipFilter : TerarkZipSubReader::FlagNone;
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
  if (isUint64Comparator_) {
    flag |= TerarkZipSubReader::FlagUint64Comparator;
  }
#endif
  return subReader_.Get(global_seqno_, ro, ikey, get_context, flag);
}

uint64_t TerarkZipTableReader::ApproximateOffsetOf_old(const Slice& ikey) {
  auto iter = UniquePtrOf(NewIterator(ReadOptions(), nullptr, nullptr, true, false));
  iter->Seek(ikey);
  auto indexIter = static_cast<TerarkZipTableIndexIterator*>(iter.get())->GetIndexIterator();
  assert(indexIter != nullptr);
  assert(&subReader_ == static_cast<TerarkZipTableIndexIterator*>(iter.get())->GetSubReader());
  size_t numRecords = subReader_.index_->NumKeys();
  size_t rank;
  if (iter->Valid()) {
    assert(indexIter->Valid());
    rank = indexIter->DictRank();
  }
  else {
    rank = numRecords;
  }
  auto offset = uint64_t(subReader_.rawReaderSize_ * 1.0 * rank / numRecords);
  if (isReverseBytewiseOrder_)
    return subReader_.rawReaderSize_ - offset;
  return offset;
}


void TerarkZipTableReader::RangeScan(const Slice* begin, const SliceTransform* prefix_extractor, void* arg,
                                     bool(*callback_func)(void* arg, const Slice& ikey,
                                                          const Slice& value)) {
  // TODO
  TableReader::RangeScan(begin, prefix_extractor, arg, callback_func);
}

uint64_t TerarkZipTableReader::ApproximateOffsetOf_new(const Slice& ikey) {
#if defined(TerocksPrivateCode)
  size_t numRecords = subReader_.index_->NumKeys();
  size_t rank = subReader_.DictRank(fstringOf(ExtractUserKey(ikey)));
  auto offset = uint64_t(subReader_.rawReaderSize_ * 1.0 * rank / numRecords);
  if (isReverseBytewiseOrder_)
    return subReader_.rawReaderSize_ - offset;
  return offset;
#endif // TerocksPrivateCode
  return 0;
}

uint64_t TerarkZipTableReader::ApproximateOffsetOf(const Slice& ikey) {
  if (g_useOldOffsetOf)
    return ApproximateOffsetOf_old(ikey);
  else
    return ApproximateOffsetOf_new(ikey);
}

TerarkZipTableReader::~TerarkZipTableReader() {
  if (subReader_.storeUsePread_) {
    if (subReader_.cache_) {
      subReader_.cache_->close(subReader_.storeFD_);
    }
  }
}

TerarkZipTableReader::TerarkZipTableReader(const TerarkZipTableFactory* table_factory,
                                           const TableReaderOptions& tro,
                                           const TerarkZipTableOptions& tzto)
  : table_reader_options_(tro)
  , table_factory_(table_factory)
  , global_seqno_(kDisableGlobalSequenceNumber)
  , tzto_(tzto)
{
  isReverseBytewiseOrder_ = false;
}

fstring TerarkZipTableMultiReader::SubIndex::PartIndexOperator::operator[](size_t i) const {
  return fstring(p->prefixSet_.data() + i * p->alignedPrefixLen_, p->prefixLen_);
};

const TerarkZipSubReader*
TerarkZipTableMultiReader::SubIndex::LowerBoundSubReaderU64Sequential(fstring key) const {
  byte_t targetBuffer[8] = {};
  memcpy(targetBuffer + (8 - prefixLen_), key.data(), std::min<size_t>(prefixLen_, key.size()));
  uint64_t targetValue = ReadBigEndianUint64Aligned(targetBuffer, targetBuffer + 8);
  auto ptr = (const uint64_t*)prefixSet_.data();
  size_t count = partCount_;
  for (size_t i = 0; i < count; ++i) {
    if (ptr[i] >= targetValue) {
      return &subReader_[i];
    }
  }
  return nullptr;
}

const TerarkZipSubReader*
TerarkZipTableMultiReader::SubIndex::LowerBoundSubReaderU64Binary(fstring key) const {
  byte_t targetBuffer[8] = {};
  memcpy(targetBuffer + (8 - prefixLen_), key.data(), std::min<size_t>(prefixLen_, key.size()));
  uint64_t targetValue = ReadBigEndianUint64Aligned(targetBuffer, targetBuffer + 8);
  auto ptr = (const uint64_t*)prefixSet_.data();
  auto index = terark::lower_bound_n(ptr, 0, partCount_, targetValue);
  if (index == partCount_) {
    return nullptr;
  }
  return &subReader_[index];
}

const TerarkZipSubReader*
TerarkZipTableMultiReader::SubIndex::LowerBoundSubReaderU64BinaryReverse(fstring key)
const {
  byte_t targetBuffer[8] = {};
  memcpy(targetBuffer + (8 - prefixLen_), key.data(), std::min(prefixLen_, key.size()));
  uint64_t targetValue = ReadBigEndianUint64Aligned(targetBuffer, targetBuffer + 8);
  auto ptr = (const uint64_t*)prefixSet_.data();
  auto index = terark::upper_bound_n(ptr, 0, partCount_, targetValue);
  if (index == 0) {
    return nullptr;
  }
  return &subReader_[index - 1];
}

const TerarkZipSubReader*
TerarkZipTableMultiReader::SubIndex::LowerBoundSubReaderBytewise(fstring key)
const {
  if (key.size() > prefixLen_) {
    key = fstring(key.data(), prefixLen_);
  }
  PartIndexOperator ptr = {this};
  auto index = terark::lower_bound_n(ptr, 0, partCount_, key);
  if (index == partCount_) {
    return nullptr;
  }
  return &subReader_[index];
}

const TerarkZipSubReader*
TerarkZipTableMultiReader::SubIndex::LowerBoundSubReaderBytewiseReverse(fstring key)
const {
  if (key.size() > prefixLen_) {
    key = fstring(key.data(), prefixLen_);
  }
  PartIndexOperator ptr = {this};
  auto index = terark::upper_bound_n(ptr, 0, partCount_, key);
  if (index == 0) {
    return nullptr;
  }
  return &subReader_[index - 1];
}

TerarkZipTableMultiReader::SubIndex::~SubIndex() {
  if (cache_fi_ >= 0) {
    assert(nullptr != cache_);
    cache_->close(cache_fi_);
  }
}

Status TerarkZipTableMultiReader::SubIndex::Init(
                      fstring offsetMemory,
                      fstring indexMemory,
                      fstring storeMemory,
                      AbstractBlobStore::Dictionary dict,
                      fstring typeMemory,
                      fstring commonPrefixMemory,
                      int minPreadLen,
                      RandomAccessFile* fileObj,
                      LruReadonlyCache* cache,
                      bool reverse)
{
  TerarkZipMultiOffsetInfo offset;
  if (!offset.risk_set_memory(offsetMemory.data(), offsetMemory.size())) {
    return Status::Corruption("bad offset block");
  }
  TERARK_SCOPE_EXIT(offset.risk_release_ownership());
  subReader_.reserve(offset.partCount_);

  cache_ = cache;
  partCount_ = offset.partCount_;
  prefixLen_ = offset.prefixLen_;
  alignedPrefixLen_ = terark::align_up(prefixLen_, 8);
  prefixSet_.resize(alignedPrefixLen_ * partCount_);

  if (prefixLen_ <= 8) {
    for (size_t i = 0; i < partCount_; ++i) {
      auto u64p = (uint64_t*)(prefixSet_.data() + i * alignedPrefixLen_);
      auto src = (const byte_t *)offset.prefixSet_.data() + i * prefixLen_;
      *u64p = ReadBigEndianUint64(src, src + prefixLen_);
    }
    if (reverse) {
        LowerBoundSubReaderFunc = &SubIndex::LowerBoundSubReaderU64BinaryReverse;
    }
    else {
        LowerBoundSubReaderFunc = partCount_ < 32 ? &SubIndex::LowerBoundSubReaderU64Sequential
                                          : &SubIndex::LowerBoundSubReaderU64Binary;
    }
  }
  else {
    for (size_t i = 0; i < partCount_; ++i) {
      memcpy(prefixSet_.data() + i * alignedPrefixLen_,
        offset.prefixSet_.data() + i * prefixLen_, prefixLen_);
    }
    LowerBoundSubReaderFunc = reverse ? &SubIndex::LowerBoundSubReaderBytewiseReverse
                              : &SubIndex::LowerBoundSubReaderBytewise;
  }

  TerarkZipMultiOffsetInfo::KeyValueOffset last = {0, 0, 0, 0};
  size_t rawSize = 0;
  intptr_t fileFD = fileObj->FileDescriptor();
  hasAnyZipOffset_ = false;
  try {
    cache_fi_ = -1;
    for (size_t i = 0; i < partCount_; ++i) {
      subReader_.push_back();
      auto& part = subReader_.back();
      auto& curr = offset.offset_[i];
      part.subIndex_ = i;
      part.storeFileObj_ = fileObj;
      part.storeFD_ = fileFD;
      part.storeOffset_ = last.value;
      part.prefix_.assign(offset.prefixSet_.data() + i * prefixLen_, prefixLen_);
      part.index_ = TerarkIndex::LoadMemory({indexMemory.data() + last.key,
                                             ptrdiff_t(curr.key - last.key)});
      fstring dataMem(storeMemory.data() + last.value, curr.value - last.value);
      part.store_.reset(AbstractBlobStore::load_from_user_memory(dataMem, dict));
      if (part.store_->is_offsets_zipped()) {
          hasAnyZipOffset_ = true;
      }
      part.InitUsePread(minPreadLen);
      assert(bitfield_array<2>::compute_mem_size(part.index_->NumKeys()) == curr.type - last.type);
      part.type_.risk_set_data((byte_t*)(typeMemory.data() + last.type), part.index_->NumKeys());
      part.commonPrefix_.assign(commonPrefixMemory.data() + last.commonPrefix,
                                curr.commonPrefix - last.commonPrefix);
      part.rawReaderOffset_ = rawSize;
      part.rawReaderSize_ = (curr.key - last.key) + dataMem.size();
      if (part.storeUsePread_ && cache) {
        if (cache_fi_ < 0) {
          cache_fi_ = cache->open(fileFD);
        }
        part.cache_ = cache;
        part.storeFD_ = cache_fi_;
      }
      rawSize += part.rawReaderSize_;
      last = curr;
    }
#ifndef _MSC_VER
    if (cache_fi_ >= 0) {
      assert(nullptr != cache_);
#ifdef OS_MACOSX
      if (fcntl(fd, F_NOCACHE, 1) == -1) {
        return IOError("While fcntl NoCache"
            , "O_DIRECT is required for terark user space cache");
      }
#else
      if (fcntl(fileFD, F_SETFL, fcntl(fileFD, F_GETFD) | O_DIRECT) == -1) {
        return Status::IOError("While fcntl NoCache"
            , "O_DIRECT is required for terark user space cache");
      }
#endif
    }
#endif
  }
  catch (const std::exception& ex) {
    subReader_.clear();
    return Status::Corruption("TerarkZipTableReader::Open()", ex.what());
  }
  return Status::OK();
}

size_t TerarkZipTableMultiReader::SubIndex::GetPrefixLen() const {
  return prefixLen_;
}

size_t TerarkZipTableMultiReader::SubIndex::GetSubCount() const {
  return partCount_;
}

const TerarkZipSubReader*
TerarkZipTableMultiReader::SubIndex::GetSubReader(size_t i) const {
  return &subReader_[i];
}

const TerarkZipSubReader*
TerarkZipTableMultiReader::SubIndex::LowerBoundSubReader(fstring key) const {
  return (this->*LowerBoundSubReaderFunc)(key);
}

InternalIterator*
TerarkZipTableMultiReader::
NewIterator(const ReadOptions& ro, const SliceTransform* prefix_extractor,
            Arena* arena, bool skip_filters, bool for_compaction) {
  TERARK_UNUSED_VAR(skip_filters); // unused
  const bool ZipOffset = this->subIndex_.HasAnyZipOffset();
  ForTemplateArg(0,0);
  ForTemplateArg(0,1);
  ForTemplateArg(1,0);
  ForTemplateArg(1,1);
  assert(false);
  return NULL;
}

template<bool reverse, bool ZipOffset>
InternalIterator*
TerarkZipTableMultiReader::
NewIteratorImp(const ReadOptions& ro, Arena* arena) {
  typedef IterZO<TerarkZipTableMultiIterator<reverse>, ZipOffset> IterType;
  if (arena) {
    return new(arena->AllocateAligned(sizeof(IterType)))
      IterType(table_reader_options_, subIndex_, ro, global_seqno_);
  }
  else {
    return new IterType(table_reader_options_, subIndex_, ro, global_seqno_);
  }
}

Status
TerarkZipTableMultiReader::Get(const ReadOptions& ro, const Slice& ikey, GetContext* get_context,
                               const SliceTransform* prefix_extractor, bool skip_filters) {
  int flag = skip_filters ? TerarkZipSubReader::FlagSkipFilter : TerarkZipSubReader::FlagNone;
  if (ikey.size() < 8 + subIndex_.GetPrefixLen()) {
    return Status::InvalidArgument("TerarkZipTableMultiReader::Get()",
      "param target.size() < 8 + PrefixLen");
  }
  auto subReader = subIndex_.LowerBoundSubReader(fstringOf(ikey).substr(0, ikey.size() - 8));
  if (subReader == nullptr || !ikey.starts_with(subReader->prefix_)) {
    return Status::OK();
  }
  return subReader->Get(global_seqno_, ro, ikey, get_context, flag);
}

void TerarkZipTableMultiReader::RangeScan(const Slice* begin, const SliceTransform* prefix_extractor, void* arg,
                                          bool(*callback_func)(void* arg, const Slice& ikey,
                                                               const Slice& value)) {
  // TODO
  TableReader::RangeScan(begin, prefix_extractor, arg, callback_func);
}

uint64_t TerarkZipTableMultiReader::ApproximateOffsetOf_old(const Slice& ikey) {
  auto iter = UniquePtrOf(static_cast<TerarkZipTableIndexIterator*>(
    NewIterator(ReadOptions(), nullptr, nullptr, true, false)));
  iter->Seek(ikey);
  auto indexIter = iter->GetIndexIterator();
  auto subReader = iter->GetSubReader();
  size_t numRecords;
  size_t rank;
  if (!iter->Valid()) {
    if (isReverseBytewiseOrder_) {
      subReader = subIndex_.GetSubReader(0);
      numRecords = subReader->index_->NumKeys();
      rank = 0;
    }
    else {
      subReader = subIndex_.GetSubReader(subIndex_.GetSubCount() - 1);
      numRecords = subReader->index_->NumKeys();
      rank = numRecords;
    }
  }
  else {
    numRecords = subReader->index_->NumKeys();
    if (indexIter->Valid())
      rank = indexIter->DictRank();
    else
      rank = numRecords;
  }
  auto offset = uint64_t(subReader->rawReaderOffset_ +
    1.0 * subReader->rawReaderSize_ * rank / numRecords);
  if (isReverseBytewiseOrder_) {
    subReader = subIndex_.GetSubReader(subIndex_.GetSubCount() - 1);
    return subReader->rawReaderOffset_ + subReader->rawReaderSize_ - offset;
  }
  return offset;
}

uint64_t TerarkZipTableMultiReader::ApproximateOffsetOf_new(const Slice& ikey) {
  fstring key = fstringOf(ExtractUserKey(ikey));
  const TerarkZipSubReader* subReader = subIndex_.LowerBoundSubReader(key);
  size_t numRecords;
  size_t rank;
  if (subReader == nullptr) {
    subReader = subIndex_.GetSubReader(subIndex_.GetSubCount() - 1);
    numRecords = subReader->index_->NumKeys();
    rank = numRecords;
  }
  else {
    numRecords = subReader->index_->NumKeys();
    rank = subReader->DictRank(key.substr(subIndex_.GetPrefixLen()));
  }
  auto offset = uint64_t(subReader->rawReaderOffset_ +
                         1.0 * subReader->rawReaderSize_ * rank / numRecords);
  if (isReverseBytewiseOrder_) {
    subReader = subIndex_.GetSubReader(subIndex_.GetSubCount() - 1);
    return subReader->rawReaderOffset_ + subReader->rawReaderSize_ - offset;
  }
  return offset;
}

uint64_t TerarkZipTableMultiReader::ApproximateOffsetOf(const Slice& ikey) {
  if (g_useOldOffsetOf)
    return ApproximateOffsetOf_old(ikey);
  else
    return ApproximateOffsetOf_new(ikey);
}

TerarkZipTableMultiReader::~TerarkZipTableMultiReader() {
}

TerarkZipTableMultiReader::TerarkZipTableMultiReader(
              const TerarkZipTableFactory* table_factory,
              const TableReaderOptions& tro,
              const TerarkZipTableOptions& tzto)
  : table_reader_options_(tro)
  , table_factory_(table_factory)
  , global_seqno_(kDisableGlobalSequenceNumber)
  , tzto_(tzto)
{
  isReverseBytewiseOrder_ = false;
}

Status
TerarkZipTableMultiReader::Open(RandomAccessFileReader* file, uint64_t file_size) {
  file_.reset(file); // take ownership
  const auto& ioptions = table_reader_options_.ioptions;
  TableProperties* props = nullptr;
  Status s = ReadTableProperties(file, file_size,
    kTerarkZipTableMagicNumber, ioptions, &props);
  if (!s.ok()) {
    return s;
  }
  assert(nullptr != props);
  table_properties_.reset(props);
  Slice file_data;
  if (table_reader_options_.env_options.use_mmap_reads) {
    s = file->Read(0, file_size, &file_data, nullptr);
    if (!s.ok())
      return s;
  }
  else {
    return Status::InvalidArgument("TerarkZipTableReader::Open()",
      "EnvOptions::use_mmap_reads must be true");
  }
  if (props->comparator_name != fstring(ioptions.user_comparator->Name()) && 0) {
    return Status::InvalidArgument("TerarkZipTableReader::Open()",
      "Invalid user_comparator , need " + props->comparator_name
      + ", but provid " + ioptions.user_comparator->Name());
  }
  file_data_ = file_data;
  global_seqno_ = GetGlobalSequenceNumber(*props, ioptions.info_log);
  isReverseBytewiseOrder_ =
    fstring(ioptions.user_comparator->Name()).startsWith("rev:");
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
  assert(fstring(ioptions.user_comparator->Name()) != "rocksdb.Uint64Comparator");
#endif
  BlockContents valueDictBlock, indexBlock, zValueTypeBlock, commonPrefixBlock;
  BlockContents offsetBlock;
#if defined(TerocksPrivateCode)
  BlockContents licenseBlock;
  s = ReadMetaBlockAdapte(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableExtendedBlock, &licenseBlock);
  if (s.ok()) {
    s = UpdateLicenseInfo(table_factory_, ioptions.info_log, licenseBlock);
    if (!s.ok()) {
      return s;
    }
  }
#endif // TerocksPrivateCode
  UpdateCollectInfo(table_factory_, &tzto_, props, file_size);
  s = ReadMetaBlockAdapte(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableOffsetBlock, &offsetBlock);
  if (!s.ok()) {
    return s;
  }
  s = ReadMetaBlockAdapte(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableValueDictBlock, &valueDictBlock);
  Slice dict;
  if (s.ok()) {
    s = DecompressDict(*props, fstringOf(valueDictBlock.data), &dict_);
    if (!s.ok()) {
      return s;
    }
    dict = dict_.empty() ? valueDictBlock.data : SliceOf(dict_);
  }
  props->user_collected_properties.emplace(kTerarkZipTableDictSize, lcast(dict.size()));
  s = ReadMetaBlockAdapte(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableIndexBlock, &indexBlock);
  if (!s.ok()) {
    return s;
  }
  s = LoadTombstone(file, file_size);
  if (global_seqno_ == kDisableGlobalSequenceNumber) {
    global_seqno_ = 0;
  }
  s = ReadMetaBlockAdapte(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableCommonPrefixBlock, &commonPrefixBlock);
  if (!s.ok()) {
    return s;
  }
  s = ReadMetaBlockAdapte(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableValueTypeBlock, &zValueTypeBlock);
  if (!s.ok()) {
    return s;
  }
  s = subIndex_.Init(fstringOf(offsetBlock.data)
    , fstringOf(indexBlock.data)
    , fstring(file_data.data(), props->data_size)
    , getVerifyDict(dict)
    , fstringOf(zValueTypeBlock.data)
    , fstringOf(commonPrefixBlock.data)
    , tzto_.minPreadLen
    , file_->file()
    , table_factory_->cache()
    , isReverseBytewiseOrder_
  );
  if (!s.ok()) {
    return s;
  }
  long long t0 = g_pf.now();

  if (tzto_.warmUpIndexOnOpen) {
    MmapWarmUp(fstringOf(indexBlock.data));
    if (!tzto_.warmUpValueOnOpen) {
      MmapWarmUp(fstringOf(valueDictBlock.data));
      for (size_t i = 0; i < subIndex_.GetSubCount(); ++i) {
        auto part = subIndex_.GetSubReader(i);
        for (fstring block : part->store_->get_meta_blocks()) {
          MmapWarmUp(block);
        }
      }
    }
  }
  if (tzto_.warmUpValueOnOpen) {
    for (size_t i = 0; i < subIndex_.GetSubCount(); ++i) {
      auto part = subIndex_.GetSubReader(i);
      for (fstring block : part->store_->get_data_blocks()) {
        MmapWarmUp(block);
      }
    }
  } else {
  //MmapColdize(fstring(file_data.data(), props->data_size));
    if (ioptions.advise_random_on_open) {
      for (size_t i = 0; i < subIndex_.GetSubCount(); ++i) {
        auto part = subIndex_.GetSubReader(i);
        for (fstring block : part->store_->get_data_blocks()) {
          MmapAdviseRandom(block);
        }
      }
    }
  }

  long long t1 = g_pf.now();
  size_t keyCount = 0;
  for (size_t i = 0; i < subIndex_.GetSubCount(); ++i) {
    auto part = subIndex_.GetSubReader(i);
    part->index_->BuildCache(tzto_.indexCacheRatio);
    keyCount += part->index_->NumKeys();
  }
  long long t2 = g_pf.now();
  INFO(ioptions.info_log
    , "TerarkZipTableReader::Open(): fsize = %zd, entries = %zd keys = %zd indexSize = %zd valueSize=%zd, warm up time = %6.3f'sec, build cache time = %6.3f'sec\n"
    , size_t(file_size), size_t(props->num_entries)
    , keyCount
    , size_t(props->index_size)
    , size_t(props->data_size)
    , g_pf.sf(t0, t1)
    , g_pf.sf(t1, t2)
  );
  return Status::OK();
}

}
