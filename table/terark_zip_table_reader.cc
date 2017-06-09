// project headers
#include "terark_zip_table_reader.h"
#include "terark_zip_common.h"
// boost headers
#include <boost/scope_exit.hpp>
// rocksdb headers
#include <table/internal_iterator.h>
#include <table/sst_file_writer_collectors.h>
#include <table/meta_blocks.h>
#include <table/get_context.h>
// terark headers
#include <terark/util/crc.hpp>


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
      madvise((void*)aligned_ptr, sz, MADV_DONTNEED);
    }
  }
#endif
  return new Block(
    BlockContents(std::move(tombstoneBuf), tombstoneBlock.data.size(), false, kNoCompression),
    global_seqno);
}

void SharedBlockCleanupFunction(void* arg1, void* arg2) {
  delete reinterpret_cast<shared_ptr<Block>*>(arg1);
}


static void MmapWarmUpBytes(const void* addr, size_t len) {
  auto base = (const byte_t*)(uintptr_t(addr) & uintptr_t(~4095));
  auto size = terark::align_up((size_t(addr) & 4095) + len, 4096);
#ifdef POSIX_MADV_WILLNEED
  posix_madvise((void*)addr, len, POSIX_MADV_WILLNEED);
#endif
  for (size_t i = 0; i < size; i += 4096) {
    volatile byte_t unused = ((const volatile byte_t*)base)[i];
    (void)unused;
  }
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


}


namespace rocksdb {

using terark::BadCrc32cException;
using terark::byte_swap;
using terark::BlobStore;

template<bool reverse>
class TerarkZipTableIterator : public InternalIterator, boost::noncopyable {
protected:
  const TableReaderOptions* table_reader_options_;
  const TerarkZipSubReader* subReader_;
  unique_ptr<TerarkIndex::Iterator> iter_;
  SequenceNumber          global_seqno_;
  ParsedInternalKey       pInterKey_;
  std::string             interKeyBuf_;
  valvec<byte_t>          interKeyBuf_xx_;
  valvec<byte_t>          valueBuf_;
  Slice                   userValue_;
  ZipValueType            zValtype_;
  size_t                  valnum_;
  size_t                  validx_;
  uint32_t                value_data_offset;
  uint32_t                value_data_length;
  Status                  status_;
  PinnedIteratorsManager* pinned_iters_mgr_;

public:
  TerarkZipTableIterator(const TableReaderOptions& tro
                       , const TerarkZipSubReader* subReader
                       , const ReadOptions& ro
                       , SequenceNumber global_seqno)
    : table_reader_options_(&tro)
    , subReader_(subReader)
    , global_seqno_(global_seqno)
  {
    if (subReader_ != nullptr) {
      iter_.reset(subReader_->index_->NewIterator());
      iter_->SetInvalid();
    }
    pinned_iters_mgr_ = NULL;
    TryPinBuffer(interKeyBuf_xx_);
    validx_ = 0;
    valnum_ = 0;
    pInterKey_.user_key = Slice();
    pInterKey_.sequence = uint64_t(-1);
    pInterKey_.type = kMaxValue;
    value_data_offset = ro.value_data_offset;
    value_data_length = ro.value_data_length;
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
  }

  void SeekToLast() override {
    if (UnzipIterRecord(IndexIterSeekToLast())) {
      validx_ = valnum_ - 1;
      DecodeCurrKeyValue();
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
  }

  void SeekForPrev(const Slice& target) override {
    SeekForPrevImpl(target, &table_reader_options_->internal_comparator);
  }

  void Next() override {
    assert(iter_->Valid());
    validx_++;
    if (validx_ < valnum_) {
      DecodeCurrKeyValue();
    }
    else {
      if (UnzipIterRecord(IndexIterNext())) {
        DecodeCurrKeyValue();
      }
    }
  }

  void Prev() override {
    assert(iter_->Valid());
    if (validx_ > 0) {
      validx_--;
      DecodeCurrKeyValue();
    }
    else {
      if (UnzipIterRecord(IndexIterPrev())) {
        validx_ = valnum_ - 1;
        DecodeCurrKeyValue();
      }
    }
  }

  Slice key() const override {
    assert(iter_->Valid());
    return SliceOf(interKeyBuf_xx_);
  }

  Slice value() const override {
    assert(iter_->Valid());
    return userValue_;
  }

  Status status() const override {
    return status_;
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
        validx_ = valnum_ - 1;
      DecodeCurrKeyValue();
    }
  }
  void SeekToAscendingLast() {
    if (UnzipIterRecord(iter_->SeekToLast())) {
      if (!reverse)
        validx_ = valnum_ - 1;
      DecodeCurrKeyValue();
    }
  }
  void SeekInternal(const ParsedInternalKey& pikey) {
    TryPinBuffer(interKeyBuf_xx_);
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
        if ((byte_t(pikey.user_key[cplen]) < subReader_->commonPrefix_[cplen]) ^ reverse) {
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
      ok = iter_->Seek(fstringOf(pikey.user_key).substr(cplen));
      if (reverse) {
        if (!ok) {
          // searchKey is reverse_bytewise less than all keys in database
          iter_->SeekToLast();
          assert(iter_->Valid()); // TerarkIndex should not empty
          ok = true;
          cmp = -1;
        }
        else {
          cmp = SliceOf(iter_->key()).compare(SubStr(pikey.user_key, cplen));
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
          cmp = SliceOf(iter_->key()).compare(SubStr(pikey.user_key, cplen));
      }
      if (UnzipIterRecord(ok)) {
        if (0 == cmp) {
          validx_ = size_t(-1);
          do {
            validx_++;
            DecodeCurrKeyValue();
            if (pInterKey_.sequence <= pikey.sequence) {
              return; // done
            }
          } while (validx_ + 1 < valnum_);
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
    TryPinBuffer(interKeyBuf_xx_);
    if (iter_)
      iter_->SetInvalid();
    validx_ = 0;
    valnum_ = 0;
    pInterKey_.user_key = Slice();
    pInterKey_.sequence = uint64_t(-1);
    pInterKey_.type = kMaxValue;
  }
  virtual bool IndexIterSeekToFirst() {
    TryPinBuffer(interKeyBuf_xx_);
    if (reverse)
      return iter_->SeekToLast();
    else
      return iter_->SeekToFirst();
  }
  virtual bool IndexIterSeekToLast() {
    TryPinBuffer(interKeyBuf_xx_);
    if (reverse)
      return iter_->SeekToFirst();
    else
      return iter_->SeekToLast();
  }
  virtual bool IndexIterPrev() {
    TryPinBuffer(interKeyBuf_xx_);
    if (reverse)
      return iter_->Next();
    else
      return iter_->Prev();
  }
  virtual bool IndexIterNext() {
    TryPinBuffer(interKeyBuf_xx_);
    if (reverse)
      return iter_->Prev();
    else
      return iter_->Next();
  }
  virtual void DecodeCurrKeyValue() {
    DecodeCurrKeyValueInternal();
    interKeyBuf_.assign(subReader_->commonPrefix_.data(), subReader_->commonPrefix_.size());
    AppendInternalKey(&interKeyBuf_, pInterKey_);
    interKeyBuf_xx_.assign((byte_t*)interKeyBuf_.data(), interKeyBuf_.size());
  }
  void TryPinBuffer(valvec<byte_t>& buf) {
    if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
      pinned_iters_mgr_->PinPtr(buf.data(), free);
      buf.risk_release_ownership();
    }
  }
  bool UnzipIterRecord(bool hasRecord) {
    if (hasRecord) {
      size_t recId = iter_->id();
      zValtype_ = subReader_->type_.size()
        ? ZipValueType(subReader_->type_[recId])
        : ZipValueType::kZeroSeq;
      try {
        TryPinBuffer(valueBuf_);
        if (ZipValueType::kMulti == zValtype_) {
          valueBuf_.resize_no_init(sizeof(uint32_t)); // for offsets[valnum_]
          subReader_->store_->get_record_append(recId, &valueBuf_);
        }
        else if (0 == value_data_offset && UINT32_MAX == value_data_length) {
          valueBuf_.erase_all();
          subReader_->store_->get_record_append(recId, &valueBuf_);
        }
        else {
          valueBuf_.erase_all();
          subReader_->store_->get_slice_append(recId,
              value_data_offset, value_data_length, &valueBuf_);
        }
      }
      catch (const BadCrc32cException& ex) { // crc checksum error
        SetIterInvalid();
        status_ = Status::Corruption(
          "TerarkZipTableIterator::UnzipIterRecord()", ex.what());
        return false;
      }
      if (ZipValueType::kMulti == zValtype_) {
        ZipValueMultiValue::decode(valueBuf_, &valnum_);
        size_t rvOffset = value_data_offset;
        size_t rvLength = value_data_length;
        if (rvOffset || rvLength < UINT32_MAX) {
            uint32_t* offsets = (uint32_t*)valueBuf_.data();
            size_t pos = 0;
            char* base = (char*)(offsets + valnum_ + 1);
            for(size_t i = 0; i < valnum_; ++i) {
                size_t q = offsets[i + 0];
                size_t r = offsets[i + 1];
                size_t l = r - q;
                offsets[i] = pos;
                if (l > rvOffset) {
                    size_t l2 = std::min(l - rvOffset, rvLength);
                    memmove(base + pos, base + q + rvOffset, l2);
                    pos += l2;
                }
            }
            offsets[valnum_] = pos;
        }
      }
      else {
        valnum_ = 1;
      }
      validx_ = 0;
      pInterKey_.user_key = SliceOf(iter_->key());
      return true;
    }
    else {
      SetIterInvalid();
      return false;
    }
  }
  void DecodeCurrKeyValueInternal() {
    assert(status_.ok());
    assert(iter_->id() < subReader_->index_->NumKeys());
    switch (zValtype_) {
    default:
      status_ = Status::Aborted("TerarkZipTableIterator::DecodeCurrKeyValue()",
        "Bad ZipValueType");
      abort(); // must not goes here, if it does, it should be a bug!!
      break;
    case ZipValueType::kZeroSeq:
      assert(0 == validx_);
      assert(1 == valnum_);
      pInterKey_.sequence = global_seqno_;
      pInterKey_.type = kTypeValue;
      userValue_ = SliceOf(valueBuf_);
      break;
    case ZipValueType::kValue: // should be a kTypeValue, the normal case
      assert(0 == validx_);
      assert(1 == valnum_);
      // little endian uint64_t
      pInterKey_.sequence = *(uint64_t*)valueBuf_.data() & kMaxSequenceNumber;
      pInterKey_.type = kTypeValue;
      userValue_ = SliceOf(fstring(valueBuf_).substr(7));
      break;
    case ZipValueType::kDelete:
      assert(0 == validx_);
      assert(1 == valnum_);
      // little endian uint64_t
      pInterKey_.sequence = *(uint64_t*)valueBuf_.data() & kMaxSequenceNumber;
      pInterKey_.type = kTypeDeletion;
      userValue_ = Slice();
      break;
    case ZipValueType::kMulti: { // more than one value
      auto zmValue = (const ZipValueMultiValue*)(valueBuf_.data());
      assert(0 != valnum_);
      assert(validx_ < valnum_);
      Slice d = zmValue->getValueData(validx_, valnum_);
      auto snt = unaligned_load<SequenceNumber>(d.data());
      UnPackSequenceAndType(snt, &pInterKey_.sequence, &pInterKey_.type);
      d.remove_prefix(sizeof(SequenceNumber));
      userValue_ = d;
      break; }
    }
  }
};


#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
class TerarkZipTableUint64Iterator : public TerarkZipTableIterator<false> {
public:
  TerarkZipTableUint64Iterator(const TableReaderOptions& tro
    , const TerarkZipSubReader *subReader
    , SequenceNumber global_seqno)
    : TerarkZipTableIterator<false>(tro, subReader, global_seqno) {
  }
protected:
  typedef TerarkZipTableIterator<false> base_t;
  using base_t::subReader_;
  using base_t::pInterKey_;
  using base_t::interKeyBuf_;
  using base_t::interKeyBuf_xx_;
  using base_t::status_;

  using base_t::SeekInternal;
  using base_t::DecodeCurrKeyValueInternal;

public:
  void Seek(const Slice& target) override {
    ParsedInternalKey pikey;
    if (!ParseInternalKey(target, &pikey)) {
      status_ = Status::InvalidArgument("TerarkZipTableIterator::Seek()",
        "param target.size() < 8");
      SetIterInvalid();
      return;
    }
    uint64_t u64_target;
    assert(pikey.user_key.size() == 8);
    u64_target = byte_swap(*reinterpret_cast<const uint64_t*>(pikey.user_key.data()));
    pikey.user_key = Slice(reinterpret_cast<const char*>(&u64_target), 8);
    SeekInternal(pikey);
  }
  void DecodeCurrKeyValue() override {
    DecodeCurrKeyValueInternal();
    interKeyBuf_.assign(subReader_->commonPrefix_.data(), subReader_->commonPrefix_.size());
    AppendInternalKey(&interKeyBuf_, pInterKey_);
    assert(interKeyBuf_.size() == 16);
    uint64_t *ukey = reinterpret_cast<uint64_t*>(&interKeyBuf_[0]);
    *ukey = byte_swap(*ukey);
    interKeyBuf_xx_.assign((byte_t*)interKeyBuf_.data(), interKeyBuf_.size());
  }
};
#endif

#if defined(TerocksPrivateCode)

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
  using base_t::pInterKey_;
  using base_t::interKeyBuf_;
  using base_t::interKeyBuf_xx_;
  using base_t::valnum_;
  using base_t::validx_;
  using base_t::status_;

  using base_t::SeekToAscendingFirst;
  using base_t::SeekToAscendingLast;
  using base_t::SetIterInvalid;
  using base_t::TryPinBuffer;
  using base_t::SeekInternal;
  using base_t::UnzipIterRecord;
  using base_t::DecodeCurrKeyValueInternal;

public:
  bool Valid() const override {
    return iter_ && iter_->Valid();
  }

  void Seek(const Slice& target) override {
    ParsedInternalKey pikey;
    if (!ParseInternalKey(target, &pikey)) {
      status_ = Status::InvalidArgument("TerarkZipTableIterator::Seek()",
        "param target.size() < 8");
      SetIterInvalid();
      return;
    }
    auto subReader = subIndex_->GetSubReader(fstringOf(pikey.user_key));
    if (subReader == nullptr) {
      SetIterInvalid();
      return;
    }
    if (subReader_ != subReader) {
      subReader_ = subReader;
      iter_.reset(subReader->index_->NewIterator());
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
            subReader_ = subIndex_->GetSubReader(subReader->subIndex_ - 1);
            iter_.reset(subReader_->index_->NewIterator());
            SeekToAscendingLast();
          }
        }
        else {
          if (subReader->subIndex_ != subIndex_->GetSubCount() - 1) {
            subReader_ = subIndex_->GetSubReader(subReader->subIndex_ + 1);
            iter_.reset(subReader_->index_->NewIterator());
            SeekToAscendingFirst();
          }
        }
      }
    }
  }

protected:
  bool IndexIterSeekToFirst() override {
    TryPinBuffer(interKeyBuf_xx_);
    if (reverse) {
      subReader_ = subIndex_->GetSubReader(subIndex_->GetSubCount() - 1);
      iter_.reset(subReader_->index_->NewIterator());
      return iter_->SeekToLast();
    }
    else {
      subReader_ = subIndex_->GetSubReader(0);
      iter_.reset(subReader_->index_->NewIterator());
      return iter_->SeekToFirst();
    }
  }
  bool IndexIterSeekToLast() override {
    TryPinBuffer(interKeyBuf_xx_);
    if (reverse) {
      subReader_ = subIndex_->GetSubReader(0);
      iter_.reset(subReader_->index_->NewIterator());
      return iter_->SeekToFirst();
    }
    else {
      subReader_ = subIndex_->GetSubReader(subIndex_->GetSubCount() - 1);
      iter_.reset(subReader_->index_->NewIterator());
      return iter_->SeekToLast();
    }
  }
  bool IndexIterPrev() override {
    TryPinBuffer(interKeyBuf_xx_);
    if (reverse) {
      if (iter_->Next())
        return true;
      if (subReader_->subIndex_ == subIndex_->GetSubCount() - 1)
        return false;
      subReader_ = subIndex_->GetSubReader(subReader_->subIndex_ + 1);
      iter_.reset(subReader_->index_->NewIterator());
      return iter_->SeekToFirst();
    }
    else {
      if (iter_->Prev())
        return true;
      if (subReader_->subIndex_ == 0)
        return false;
      subReader_ = subIndex_->GetSubReader(subReader_->subIndex_ - 1);
      iter_.reset(subReader_->index_->NewIterator());
      return iter_->SeekToLast();
    }
  }
  bool IndexIterNext() override {
    TryPinBuffer(interKeyBuf_xx_);
    if (reverse) {
      if (iter_->Prev())
        return true;
      if (subReader_->subIndex_ == 0)
        return false;
      subReader_ = subIndex_->GetSubReader(subReader_->subIndex_ - 1);
      iter_.reset(subReader_->index_->NewIterator());
      return iter_->SeekToLast();
    }
    else {
      if (iter_->Next())
        return true;
      if (subReader_->subIndex_ == subIndex_->GetSubCount() - 1)
        return false;
      subReader_ = subIndex_->GetSubReader(subReader_->subIndex_ + 1);
      iter_.reset(subReader_->index_->NewIterator());
      return iter_->SeekToFirst();
    }
  }
  void DecodeCurrKeyValue() override {
    DecodeCurrKeyValueInternal();
    interKeyBuf_.assign(subReader_->prefix_.data(), subReader_->prefix_.size());
    interKeyBuf_.append(subReader_->commonPrefix_.data(), subReader_->commonPrefix_.size());
    AppendInternalKey(&interKeyBuf_, pInterKey_);
    interKeyBuf_xx_.assign((byte_t*)interKeyBuf_.data(), interKeyBuf_.size());
  }
};

#endif // TerocksPrivateCode

Status rocksdb::TerarkZipTableTombstone::
LoadTombstone(RandomAccessFileReader * file, uint64_t file_size) {
  BlockContents tombstoneBlock;
  Status s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, 
    GetTableReaderOptions().ioptions,  kRangeDelBlock, &tombstoneBlock);
  if (s.ok()) {
    tombstone_.reset(DetachBlockContents(tombstoneBlock, GetSequenceNumber()));
  }
  return s;
}

InternalIterator* TerarkZipTableTombstone::
NewRangeTombstoneIterator(const ReadOptions & read_options) {
  if (tombstone_) {
    auto iter = tombstone_->NewIterator(
      &GetTableReaderOptions().internal_comparator,
      nullptr, true,
      GetTableReaderOptions().ioptions.statistics);
    iter->RegisterCleanup(SharedBlockCleanupFunction,
      new shared_ptr<Block>(tombstone_), nullptr);
    return iter;
  }
  return nullptr;
}

Status TerarkZipSubReader::Get(SequenceNumber global_seqno, const ReadOptions& ro, const Slice& ikey,
  GetContext* get_context, int flag) const {
  (void)flag;
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
  switch (zvType) {
  default:
    return Status::Aborted("TerarkZipTableReader::Get()", "Bad ZipValueType");
  case ZipValueType::kZeroSeq:
    g_tbuf.erase_all();
    try {
      if (0==ro.value_data_offset && UINT32_MAX==ro.value_data_length)
        store_->get_record_append(recId, &g_tbuf);
      else // get_slice_append does not support entropy zip
        store_->get_slice_append(recId,
          ro.value_data_offset, ro.value_data_length, &g_tbuf);
    }
    catch (const terark::BadChecksumException& ex) {
      return Status::Corruption("TerarkZipTableReader::Get()", ex.what());
    }
    get_context->SaveValue(ParsedInternalKey(pikey.user_key, global_seqno, kTypeValue),
      Slice((char*)g_tbuf.data(), g_tbuf.size()));
    break;
  case ZipValueType::kValue: { // should be a kTypeValue, the normal case
    g_tbuf.erase_all();
    try {
      if (0==ro.value_data_offset && UINT32_MAX==ro.value_data_length)
        store_->get_record_append(recId, &g_tbuf);
      else // get_slice_append does not support entropy zip
        store_->get_slice_append(recId,
          ro.value_data_offset, ro.value_data_length, &g_tbuf);
    }
    catch (const terark::BadChecksumException& ex) {
      return Status::Corruption("TerarkZipTableReader::Get()", ex.what());
    }
                               // little endian uint64_t
    uint64_t seq = *(uint64_t*)g_tbuf.data() & kMaxSequenceNumber;
    if (seq <= pikey.sequence) {
      get_context->SaveValue(ParsedInternalKey(pikey.user_key, seq, kTypeValue),
        SliceOf(fstring(g_tbuf).substr(7)));
    }
    break; }
  case ZipValueType::kDelete: {
    // little endian uint64_t
    uint64_t seq = *(uint64_t*)g_tbuf.data() & kMaxSequenceNumber;
    if (seq <= pikey.sequence) {
      get_context->SaveValue(ParsedInternalKey(pikey.user_key, seq, kTypeDeletion),
        Slice());
    }
    break; }
  case ZipValueType::kMulti: { // more than one value
    g_tbuf.resize_no_init(sizeof(uint32_t));
    try {
      store_->get_record_append(recId, &g_tbuf);
    }
    catch (const terark::BadChecksumException& ex) {
      return Status::Corruption("TerarkZipTableReader::Get()", ex.what());
    }
    size_t num = 0;
    auto mVal = ZipValueMultiValue::decode(g_tbuf, &num);
    const size_t rvOffset = ro.value_data_offset;
    const size_t rvLength = ro.value_data_length;
    const size_t lenLimit = rvLength < UINT32_MAX
                          ? rvOffset + rvLength : size_t(-1);
    for (size_t i = 0; i < num; ++i) {
      Slice val = mVal->getValueData(i, num);
      SequenceNumber sn;
      ValueType valtype;
      {
        auto snt = unaligned_load<SequenceNumber>(val.data());
        UnPackSequenceAndType(snt, &sn, &valtype);
      }
      if (sn <= pikey.sequence) {
        val.remove_prefix(sizeof(SequenceNumber));
        // only kTypeMerge will return true
        if (val.size_ > lenLimit) {
            val.size_ = rvLength;
            val.data_ += rvOffset;
        } else {
            val.remove_prefix(rvOffset);
        }
        bool hasMoreValue = get_context->SaveValue(
          ParsedInternalKey(pikey.user_key, sn, valtype), val);
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
  unique_ptr<TableProperties> uniqueProps(props);
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
  file_data_ = file_data;
  table_properties_.reset(uniqueProps.release());
  global_seqno_ = GetGlobalSequenceNumber(*table_properties_, ioptions.info_log);
#if defined(TerocksPrivateCode)
  auto table_factory = dynamic_cast<TerarkZipTableFactory*>(ioptions.table_factory);
  assert(table_factory);
  auto& license = table_factory->GetLicense();
  BlockContents licenseBlock;
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableExtendedBlock, &licenseBlock);
  if (s.ok()) {
    auto res = license.merge(licenseBlock.data.data(), licenseBlock.data.size());
    assert(res == LicenseInfo::Result::OK);
    (void)res; // shut up !
    if (!license.check()) {
      license.print_error(nullptr, false, ioptions.info_log);
      return Status::Corruption("License expired", "contact@terark.com");
    }
  }
#endif // TerocksPrivateCode
  s = LoadTombstone(file, file_size);
  if (global_seqno_ == kDisableGlobalSequenceNumber) {
    global_seqno_ = 0;
  }
  INFO(ioptions.info_log
    , "TerarkZipTableReader::Open(): fsize = %zd, entries = %zd keys = 0 indexSize = 0 valueSize = 0, warm up time =      0.000'sec, build cache time =      0.000'sec\n"
    , size_t(file_size), size_t(table_properties_->num_entries)
  );
  return Status::OK();
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
  unique_ptr<TableProperties> uniqueProps(props);
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
  file_data_ = file_data;
  table_properties_.reset(uniqueProps.release());
  global_seqno_ = GetGlobalSequenceNumber(*table_properties_, ioptions.info_log);
  isReverseBytewiseOrder_ =
    fstring(ioptions.user_comparator->Name()).startsWith("rev:");
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
  isUint64Comparator_ =
    fstring(ioptions.user_comparator->Name()) == "rocksdb.Uint64Comparator";
#endif
  BlockContents valueDictBlock, indexBlock, zValueTypeBlock, commonPrefixBlock;
#if defined(TerocksPrivateCode)
  BlockContents licenseBlock;
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableExtendedBlock, &licenseBlock);
  if (s.ok()) {
    auto table_factory = dynamic_cast<TerarkZipTableFactory*>(ioptions.table_factory);
    assert(table_factory);
    auto& license = table_factory->GetLicense();
    auto res = license.merge(licenseBlock.data.data(), licenseBlock.data.size());
    assert(res == LicenseInfo::Result::OK);
    (void)res; // shut up !
    if (!license.check()) {
      license.print_error(nullptr, false, ioptions.info_log);
      return Status::Corruption("License expired", "contact@terark.com");
    }
  }
#endif // TerocksPrivateCode
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableValueDictBlock, &valueDictBlock);
#if defined(TerocksPrivateCode)
  // PlainBlobStore & MixedLenBlobStore no dict
#endif // TerocksPrivateCode
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableIndexBlock, &indexBlock);
  if (!s.ok()) {
    return s;
  }
  s = LoadTombstone(file, file_size);
  if (global_seqno_ == kDisableGlobalSequenceNumber) {
    global_seqno_ = 0;
  }
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
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
    subReader_.store_.reset(terark::BlobStore::load_from_user_memory(
      fstring(file_data.data(), props->data_size),
      fstringOf(valueDictBlock.data)
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
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableValueTypeBlock, &zValueTypeBlock);
  if (s.ok()) {
    subReader_.type_.risk_set_data((byte_t*)zValueTypeBlock.data.data(), recNum);
  }
  long long t0 = g_pf.now();
  if (tzto_.warmUpIndexOnOpen) {
    MmapWarmUp(fstringOf(indexBlock.data));
    if (!tzto_.warmUpValueOnOpen) {
      for (fstring block : subReader_.store_->get_index_blocks()) {
        MmapWarmUp(block);
      }
    }
  }
  if (tzto_.warmUpValueOnOpen) {
    MmapWarmUp(subReader_.store_->get_mmap());
  }
  long long t1 = g_pf.now();
  subReader_.index_->BuildCache(tzto_.indexCacheRatio);
  long long t2 = g_pf.now();
  INFO(ioptions.info_log
    , "TerarkZipTableReader::Open(): fsize = %zd, entries = %zd keys = %zd indexSize = %zd valueSize=%zd, warm up time = %6.3f'sec, build cache time = %6.3f'sec\n"
    , size_t(file_size), size_t(table_properties_->num_entries)
    , subReader_.index_->NumKeys()
    , size_t(table_properties_->index_size)
    , size_t(table_properties_->data_size)
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
NewIterator(const ReadOptions& ro, Arena* arena, bool skip_filters) {
  (void)skip_filters; // unused
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
  if (isUint64Comparator_) {
    if (arena) {
      return new(arena->AllocateAligned(sizeof(TerarkZipTableUint64Iterator)))
        TerarkZipTableUint64Iterator(table_reader_options_, &subReader_, global_seqno_);
    }
    else {
      return new TerarkZipTableUint64Iterator(table_reader_options_, &subReader_, global_seqno_);
    }
  }
#endif
  if (isReverseBytewiseOrder_) {
    if (arena) {
      return new(arena->AllocateAligned(sizeof(TerarkZipTableIterator<true>)))
        TerarkZipTableIterator<true>(table_reader_options_, &subReader_, ro, global_seqno_);
    }
    else {
      return new TerarkZipTableIterator<true>(table_reader_options_, &subReader_, ro, global_seqno_);
    }
  }
  else {
    if (arena) {
      return new(arena->AllocateAligned(sizeof(TerarkZipTableIterator<false>)))
        TerarkZipTableIterator<false>(table_reader_options_, &subReader_, ro, global_seqno_);
    }
    else {
      return new TerarkZipTableIterator<false>(table_reader_options_, &subReader_, ro, global_seqno_);
    }
  }
}


Status
TerarkZipTableReader::Get(const ReadOptions& ro, const Slice& ikey,
                          GetContext* get_context, bool skip_filters) {
  int flag = skip_filters ? TerarkZipSubReader::FlagSkipFilter : TerarkZipSubReader::FlagNone;
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
  if (isUint64Comparator_) {
    flag |= TerarkZipSubReader::FlagUint64Comparator;
  }
#endif
  return subReader_.Get(global_seqno_, ro, ikey, get_context, flag);
}

TerarkZipTableReader::~TerarkZipTableReader() {
}

TerarkZipTableReader::TerarkZipTableReader(const TableReaderOptions& tro,
  const TerarkZipTableOptions& tzto)
  : table_reader_options_(tro)
  , global_seqno_(kDisableGlobalSequenceNumber)
  , tzto_(tzto)
{
  isReverseBytewiseOrder_ = false;
}

#if defined(TerocksPrivateCode)

fstring TerarkZipTableMultiReader::SubIndex::PartIndexOperator::operator[](size_t i) const {
  return fstring(p->prefixSet_.data() + i * p->alignedPrefixLen_, p->prefixLen_);
};

const TerarkZipSubReader*
TerarkZipTableMultiReader::SubIndex::GetSubReaderU64Sequential(fstring key) const {
  byte_t targetBuffer[8] = {};
  memcpy(targetBuffer + (8 - prefixLen_), key.data(), std::min<size_t>(prefixLen_, key.size()));
  uint64_t targetValue = ReadUint64Aligned(targetBuffer, targetBuffer + 8);
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
TerarkZipTableMultiReader::SubIndex::GetSubReaderU64Binary(fstring key) const {
  byte_t targetBuffer[8] = {};
  memcpy(targetBuffer + (8 - prefixLen_), key.data(), std::min<size_t>(prefixLen_, key.size()));
  uint64_t targetValue = ReadUint64Aligned(targetBuffer, targetBuffer + 8);
  auto ptr = (const uint64_t*)prefixSet_.data();
  auto index = terark::lower_bound_n(ptr, 0, partCount_, targetValue);
  if (index == partCount_) {
    return nullptr;
  }
  return &subReader_[index];
}

const TerarkZipSubReader*
TerarkZipTableMultiReader::SubIndex::GetSubReaderU64BinaryReverse(fstring key) const {
  byte_t targetBuffer[8] = {};
  memcpy(targetBuffer + (8 - prefixLen_), key.data(), std::min<size_t>(prefixLen_, key.size()));
  uint64_t targetValue = ReadUint64Aligned(targetBuffer, targetBuffer + 8);
  auto ptr = (const uint64_t*)prefixSet_.data();
  auto index = terark::upper_bound_n(ptr, 0, partCount_, targetValue);
  if (index == 0) {
    return nullptr;
  }
  return &subReader_[index - 1];
}

const TerarkZipSubReader*
TerarkZipTableMultiReader::SubIndex::GetSubReaderBytewise(fstring key) const {
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
TerarkZipTableMultiReader::SubIndex::GetSubReaderBytewiseReverse(fstring key) const {
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

Status TerarkZipTableMultiReader::SubIndex::Init(
                      fstring offsetMemory,
                      fstring indexMempry,
                      fstring storeMemory,
                      fstring dictMemory,
                      fstring typeMemory,
                      fstring commonPrefixMemory,
                      bool reverse)
{
  TerarkZipMultiOffsetInfo offset;
  if (!offset.risk_set_memory(offsetMemory.data(), offsetMemory.size())) {
    return Status::Corruption("bad offset block");
  }
  BOOST_SCOPE_EXIT(&offset) {
    offset.risk_release_ownership();
  }BOOST_SCOPE_EXIT_END;
  subReader_.reserve(offset.partCount_);

  partCount_ = offset.partCount_;
  prefixLen_ = offset.prefixLen_;
  alignedPrefixLen_ = terark::align_up(prefixLen_, 8);
  prefixSet_.resize(alignedPrefixLen_ * partCount_);

  if (prefixLen_ <= 8) {
    for (size_t i = 0; i < partCount_; ++i) {
      auto u64p = (uint64_t*)(prefixSet_.data() + i * alignedPrefixLen_);
      auto src = (const byte_t *)offset.prefixSet_.data() + i * prefixLen_;
      *u64p = ReadUint64(src, src + prefixLen_);
    }
    if (reverse) {
        GetSubReaderPtr = &SubIndex::GetSubReaderU64BinaryReverse;
    }
    else {
        GetSubReaderPtr = partCount_ < 32 ? &SubIndex::GetSubReaderU64Sequential
                                          : &SubIndex::GetSubReaderU64Binary;
    }
  }
  else {
    for (size_t i = 0; i < partCount_; ++i) {
      memcpy(prefixSet_.data() + i * alignedPrefixLen_,
        offset.prefixSet_.data() + i * prefixLen_, prefixLen_);
    }
    GetSubReaderPtr = reverse ? &SubIndex::GetSubReaderBytewiseReverse
                              : &SubIndex::GetSubReaderBytewise;
  }

  TerarkZipMultiOffsetInfo::KeyValueOffset last = {0, 0, 0, 0};
  try {
    for (size_t i = 0; i < partCount_; ++i) {
      subReader_.push_back();
      auto& part = subReader_.back();
      auto& curr = offset.offset_[i];
      part.subIndex_ = i;
      part.prefix_.assign(offset.prefixSet_.data() + i * prefixLen_, prefixLen_);
      part.index_ = TerarkIndex::LoadMemory({indexMempry.data() + last.key,
                                             ptrdiff_t(curr.key - last.key)});
      part.store_.reset(BlobStore::load_from_user_memory(
        { storeMemory.data() + last.value,
          ptrdiff_t(curr.value - last.value) },
        dictMemory));
      assert(bitfield_array<2>::compute_mem_size(part.index_->NumKeys()) == curr.type - last.type);
      part.type_.risk_set_data((byte_t*)(typeMemory.data() + last.type), part.index_->NumKeys());
      part.commonPrefix_.assign(commonPrefixMemory.data() + last.commonPrefix,
                                curr.commonPrefix - last.commonPrefix);
      last = curr;
    }
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

const TerarkZipSubReader* TerarkZipTableMultiReader::SubIndex::GetSubReader(size_t i) const {
  return &subReader_[i];
}

const TerarkZipSubReader* TerarkZipTableMultiReader::SubIndex::GetSubReader(fstring key) const {
  return (this->*GetSubReaderPtr)(key);
}

InternalIterator* TerarkZipTableMultiReader::NewIterator(const ReadOptions& ro,
  Arena *arena, bool skip_filters) {
  (void)skip_filters; // unused
  if (isReverseBytewiseOrder_) {
    if (arena) {
      return new(arena->AllocateAligned(sizeof(TerarkZipTableMultiIterator<true>)))
        TerarkZipTableMultiIterator<true>(table_reader_options_, subIndex_, ro, global_seqno_);
    }
    else {
      return new TerarkZipTableMultiIterator<true>(table_reader_options_, subIndex_, ro, global_seqno_);
    }
  }
  else {
    if (arena) {
      return new(arena->AllocateAligned(sizeof(TerarkZipTableMultiIterator<false>)))
        TerarkZipTableMultiIterator<false>(table_reader_options_, subIndex_, ro, global_seqno_);
    }
    else {
      return new TerarkZipTableMultiIterator<false>(table_reader_options_, subIndex_, ro, global_seqno_);
    }
  }
}

Status
TerarkZipTableMultiReader::Get(const ReadOptions& ro, const Slice& ikey,
  GetContext* get_context, bool skip_filters) {
  int flag = skip_filters ? TerarkZipSubReader::FlagSkipFilter : TerarkZipSubReader::FlagNone;
  if (ikey.size() <= 8 + subIndex_.GetPrefixLen()) {
    return Status::InvalidArgument("TerarkZipTableMultiReader::Get()",
      "param target.size() < 8 + PrefixLen");
  }
  auto subReader = subIndex_.GetSubReader(fstringOf(ikey).substr(0, ikey.size() - 8));
  if (subReader == nullptr || !ikey.starts_with(subReader->prefix_)) {
    return Status::OK();
  }
  return subReader->Get(global_seqno_, ro, ikey, get_context, flag);
}

TerarkZipTableMultiReader::~TerarkZipTableMultiReader() {
}

TerarkZipTableMultiReader::TerarkZipTableMultiReader(const TableReaderOptions& tro
  , const TerarkZipTableOptions& tzto)
  : table_reader_options_(tro)
  , global_seqno_(kDisableGlobalSequenceNumber)
  , tzto_(tzto)
{
  isReverseBytewiseOrder_ = false;
}

Status
rocksdb::TerarkZipTableMultiReader::Open(RandomAccessFileReader* file, uint64_t file_size) {
  file_.reset(file); // take ownership
  const auto& ioptions = table_reader_options_.ioptions;
  TableProperties* props = nullptr;
  Status s = ReadTableProperties(file, file_size,
    kTerarkZipTableMagicNumber, ioptions, &props);
  if (!s.ok()) {
    return s;
  }
  assert(nullptr != props);
  unique_ptr<TableProperties> uniqueProps(props);
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
  file_data_ = file_data;
  table_properties_.reset(uniqueProps.release());
  global_seqno_ = GetGlobalSequenceNumber(*table_properties_, ioptions.info_log);
  isReverseBytewiseOrder_ =
    fstring(ioptions.user_comparator->Name()).startsWith("rev:");
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
  assert(fstring(ioptions.user_comparator->Name()) != "rocksdb.Uint64Comparator");
#endif
  BlockContents valueDictBlock, indexBlock, zValueTypeBlock, commonPrefixBlock;
  BlockContents offsetBlock;
  BlockContents licenseBlock;
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableExtendedBlock, &licenseBlock);
  if (s.ok()) {
    auto table_factory = dynamic_cast<TerarkZipTableFactory*>(ioptions.table_factory);
    assert(table_factory);
    auto& license = table_factory->GetLicense();
    auto res = license.merge(licenseBlock.data.data(), licenseBlock.data.size());
    assert(res == LicenseInfo::Result::OK);
    (void)res; // shut up !
    if (!license.check()) {
      license.print_error(nullptr, false, ioptions.info_log);
      return Status::Corruption("License expired", "contact@terark.com");
    }
  }
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableOffsetBlock, &offsetBlock);
  if (!s.ok()) {
    return s;
  }
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableValueDictBlock, &valueDictBlock);
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableIndexBlock, &indexBlock);
  if (!s.ok()) {
    return s;
  }
  s = LoadTombstone(file, file_size);
  if (global_seqno_ == kDisableGlobalSequenceNumber) {
    global_seqno_ = 0;
  }
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableCommonPrefixBlock, &commonPrefixBlock);
  if (!s.ok()) {
    return s;
  }
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kTerarkZipTableValueTypeBlock, &zValueTypeBlock);
  if (!s.ok()) {
    return s;
  }
  s = subIndex_.Init(fstringOf(offsetBlock.data)
    , fstringOf(indexBlock.data)
    , fstring(file_data.data(), props->data_size)
    , fstringOf(valueDictBlock.data)
    , fstringOf(zValueTypeBlock.data)
    , fstringOf(commonPrefixBlock.data)
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
        for (fstring block : part->store_->get_index_blocks()) {
          MmapWarmUp(block);
        }
      }
    }
  }
  if (tzto_.warmUpValueOnOpen) {
    MmapWarmUp(fstring(file_data.data(), props->data_size));
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
    , size_t(file_size), size_t(table_properties_->num_entries)
    , keyCount
    , size_t(table_properties_->index_size)
    , size_t(table_properties_->data_size)
    , g_pf.sf(t0, t1)
    , g_pf.sf(t1, t2)
  );
  return Status::OK();
}

#endif // TerocksPrivateCode

}
