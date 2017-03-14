/*
 * terark_zip_table.cc
 *
 *  Created on: 2016-08-09
 *      Author: leipeng
 */

#include "terark_zip_table.h"
#include "terark_zip_index.h"
#include "terark_zip_common.h"
#include <rocksdb/comparator.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <table/get_context.h>
#include <table/internal_iterator.h>
#include <table/table_builder.h>
#include <table/table_reader.h>
#include <table/block.h>
#include <table/meta_blocks.h>
#include <terark/bitmap.hpp>
#include <terark/gold_hash_map.hpp>
#include <terark/util/crc.hpp>
#include <terark/util/throw.hpp>
#include <terark/util/profiling.hpp>
#include <terark/util/fstrvec.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/zbs/fast_zip_blob_store.hpp>
#include <terark/bitfield_array.hpp>
#include <boost/scope_exit.hpp>
#include <future>
#include <random>
#include <stdlib.h>
#include <stdint.h>
#include <float.h>
#include <util/arena.h> // for #include <sys/mman.h>
#ifdef _MSC_VER
# include <io.h>
#endif

#if defined(TerocksPrivateCode)
  #include <terark/zbs/plain_blob_store.hpp>
  #include <terark/zbs/mixed_len_blob_store.hpp>
#endif // TerocksPrivateCode

namespace rocksdb {

using terark::DictZipBlobStore;
using terark::SortableStrVec;
using terark::bitfield_array;
using terark::BadCrc32cException;

static terark::profiling g_pf;

static const uint64_t kTerarkZipTableMagicNumber = 0x1122334455667788;

static const std::string kTerarkZipTableIndexBlock = "TerarkZipTableIndexBlock";
static const std::string kTerarkZipTableValueTypeBlock = "TerarkZipTableValueTypeBlock";
static const std::string kTerarkZipTableValueDictBlock = "TerarkZipTableValueDictBlock";
static const std::string kTerarkZipTableCommonPrefixBlock = "TerarkZipTableCommonPrefixBlock";
static const std::string kEmptyTableKey = "ThisIsAnEmptyTable";

class TerarkZipTableIterator;

#ifdef TERARK_ZIP_TRIAL_VERSION
const char g_trail_rand_delete[] = "TERARK_ZIP_TRIAL_VERSION random deleted this row";
#endif

template<class ByteArray>
inline Slice SliceOf(const ByteArray& ba) {
  BOOST_STATIC_ASSERT(sizeof(ba[0] == 1));
  return Slice((const char*)ba.data(), ba.size());
}

inline static fstring fstringOf(const Slice& x) {
  return fstring(x.data(), x.size());
}

template<class ByteArrayView>
inline ByteArrayView SubStr(const ByteArrayView& x, size_t pos) {
  assert(pos <= x.size());
  return ByteArrayView(x.data() + pos, x.size() - pos);
}

///////////////////////////////////////////////////////////////////////////////

enum class ZipValueType : unsigned char {
	kZeroSeq = 0,
	kDelete = 1,
	kValue = 2,
	kMulti = 3,
};
//const size_t kZipValueTypeBits = 2;

struct ZipValueMultiValue {
  // TODO: use offset[0] as num, and do not store offsets[num]
  // when unzip, reserve num+1 cells, set offsets[0] to 0,
  // and set offsets[num] to length of value pack
//	uint32_t num;
	uint32_t offsets[1];

	///@size size include the extra uint32, == encoded_size + 4
	static
	const ZipValueMultiValue* decode(void* data, size_t size, size_t* pNum) {
	  // data + 4 is the encoded data
	  auto me = (ZipValueMultiValue*)(data);
	  size_t num = me->offsets[1];
	  assert(num > 0);
	  memmove(me->offsets+1, me->offsets+2, sizeof(uint32_t)*(num-1));
	  me->offsets[0] = 0;
	  me->offsets[num] = size - sizeof(uint32_t)*(num + 1);
	  *pNum = num;
	  return me;
	}
	static
	const ZipValueMultiValue* decode(valvec<byte_t>& buf, size_t* pNum) {
	  return decode(buf.data(), buf.size(), pNum);
	}
	Slice getValueData(size_t nth, size_t num) const {
		assert(nth < num);
		size_t offset0 = offsets[nth+0];
		size_t offset1 = offsets[nth+1];
		size_t dlength = offset1 - offset0;
		const char* base = (const char*)(offsets + num + 1);
		return Slice(base + offset0, dlength);
	}
	static size_t calcHeaderSize(size_t n) {
		return sizeof(uint32_t) * (n);
	}
};

class TerarkEmptyTableReader : public TableReader, boost::noncopyable {
  class Iter : public InternalIterator, boost::noncopyable {
  public:
    Iter() {}
    ~Iter() {}
    void SetPinnedItersMgr(PinnedIteratorsManager*) {}
    bool Valid() const override { return false; }
    void SeekToFirst() override {}
    void SeekToLast() override {}
    void SeekForPrev(const Slice&) override {}
    void Seek(const Slice&) override {}
    void Next() override {}
    void Prev() override {}
    Slice key() const override { THROW_STD(invalid_argument, "Invalid call"); }
    Slice value() const override { THROW_STD(invalid_argument, "Invalid call"); }
    Status status() const override { return Status::OK(); }
    bool IsKeyPinned() const override { return false; }
    bool IsValuePinned() const override { return false; }
  };
  std::shared_ptr<const TableProperties> table_properties_;
public:
  InternalIterator*
  NewIterator(const ReadOptions&, Arena* a, bool) override {
    return a ? new(a->AllocateAligned(sizeof(Iter)))Iter() : new Iter();
  }
  void Prepare(const Slice&) override {}
  Status Get(const ReadOptions&, const Slice&, GetContext*, bool) override {
    return Status::OK();
  }
  size_t ApproximateMemoryUsage() const override { return 100; }
  uint64_t ApproximateOffsetOf(const Slice&) override { return 0; }
  void SetupForCompaction() override {}
  std::shared_ptr<const TableProperties>
  GetTableProperties() const override { return table_properties_; }
  ~TerarkEmptyTableReader() {}
  TerarkEmptyTableReader() : table_properties_(new TableProperties()){}
};

/**
 * one user key map to a record id: the index NO. of a key in NestLoudsTrie,
 * the record id is used to direct index a type enum(small integer) array,
 * the record id is also used to access the value store
 */
class TerarkZipTableReader: public TableReader, boost::noncopyable {
public:
  InternalIterator*
  NewIterator(const ReadOptions&, Arena*, bool skip_filters) override;

  InternalIterator*
  NewRangeTombstoneIterator(const ReadOptions& read_options) override;

  void Prepare(const Slice& target) override {}

  Status Get(const ReadOptions&, const Slice& key, GetContext*,
             bool skip_filters) override;

  uint64_t ApproximateOffsetOf(const Slice& key) override { return 0; }
  void SetupForCompaction() override {}

  std::shared_ptr<const TableProperties>
  GetTableProperties() const override { return table_properties_; }

  size_t ApproximateMemoryUsage() const override { return file_data_.size(); }

  ~TerarkZipTableReader();
  TerarkZipTableReader(const TableReaderOptions&, const TerarkZipTableOptions&);
  Status Open(RandomAccessFileReader* file, uint64_t file_size);

private:
  unique_ptr<terark::BlobStore> valstore_;
  unique_ptr<TerocksIndex> keyIndex_;
  unique_ptr<Block> tombstone_;
  Slice commonPrefix_;
  bitfield_array<2> typeArray_;
  static const size_t kNumInternalBytes = 8;
  Slice  file_data_;
  unique_ptr<RandomAccessFileReader> file_;
  const TableReaderOptions table_reader_options_;
  std::shared_ptr<const TableProperties> table_properties_;
  const TerarkZipTableOptions& tzto_;
  bool isReverseBytewiseOrder_;
  friend class TerarkZipTableIterator;
  Status LoadIndex(Slice mem);
};

class Uint32Histogram {
  terark::valvec<uint32_t> m_small_cnt;
  terark::valvec<std::pair<uint32_t, uint32_t> > m_large_cnt_compact;
  terark::gold_hash_map<uint32_t, uint32_t> m_large_cnt;
  static const size_t MAX_SMALL_VALUE = 4096;

public:
  size_t m_distinct_key_cnt;
  size_t m_cnt_sum;
  size_t m_min_cnt_key, m_cnt_of_min_cnt_key;
  size_t m_max_cnt_key, m_cnt_of_max_cnt_key;

  Uint32Histogram() {
    m_cnt_of_min_cnt_key = 0;
    m_cnt_of_max_cnt_key = 0;
    m_min_cnt_key = size_t(-1);
    m_max_cnt_key = 0;
    m_distinct_key_cnt = 0;
    m_cnt_sum = 0;
    m_small_cnt.resize(MAX_SMALL_VALUE, 0);
  }
  uint32_t& operator[](size_t val) {
    if (val < MAX_SMALL_VALUE)
      return m_small_cnt[val];
    else
      return m_large_cnt[val];
  }
  void finish() {
    const uint32_t* pCnt = m_small_cnt.data();
    for (size_t maxKey = MAX_SMALL_VALUE; maxKey > 0; --maxKey) {
      if (pCnt[maxKey-1] > 0) {
        m_small_cnt.risk_set_size(maxKey);
        break;
      }
    }
    size_t sum = 0;
    size_t distinct_cnt = 0;
    uint32_t cnt_of_max_cnt_key = 0;
    uint32_t cnt_of_min_cnt_key = 0;
    for (size_t key = 0, maxKey = m_small_cnt.size(); key < maxKey; ++key) {
      uint32_t cnt = pCnt[key];
      if (cnt) {
        distinct_cnt++;
        sum += cnt;
        if (cnt_of_max_cnt_key < cnt) {
          cnt_of_max_cnt_key = cnt;
          m_max_cnt_key = key;
        }
        if (cnt_of_min_cnt_key > cnt) {
          cnt_of_min_cnt_key = cnt;
          m_min_cnt_key = key;
        }
      }
    }
    for (size_t idx = m_large_cnt.beg_i(); idx < m_large_cnt.end_i(); ++idx) {
      sum += m_large_cnt.val(idx);
    }
    distinct_cnt += m_large_cnt.size();
    m_distinct_key_cnt = distinct_cnt;
    m_cnt_sum = sum;

    m_large_cnt_compact.resize_no_init(m_large_cnt.size());
    auto large_beg = m_large_cnt_compact.begin();
    auto large_num = m_large_cnt.end_i();
    for(size_t idx = 0; idx < large_num; ++idx) {
      uint32_t key = m_large_cnt.key(idx);
      uint32_t val = m_large_cnt.val(idx);
      large_beg[idx] = std::make_pair(key, val);
    }
    std::sort(large_beg, large_beg + large_num);
    m_large_cnt_compact.risk_set_size(large_num);
  }
  template<class OP>
  void for_each(OP op) const {
    const uint32_t* pCnt = m_small_cnt.data();
    for (size_t key = 0, maxKey = m_small_cnt.size(); key < maxKey; ++key) {
      if (pCnt[key]) {
        op(uint32_t(key), pCnt[key]);
      }
    }
    for (auto kv : m_large_cnt_compact) {
      op(kv.first, kv.second);
    }
  }
};

class TerarkZipTableBuilder: public TableBuilder, boost::noncopyable {
public:
  TerarkZipTableBuilder(
		  const TerarkZipTableOptions&,
		  const TableBuilderOptions& tbo,
		  uint32_t column_family_id,
		  WritableFileWriter* file);

  ~TerarkZipTableBuilder();

  void Add(const Slice& key, const Slice& value) override;
  Status status() const override { return status_; }
  Status Finish() override;
  void Abandon() override;
  uint64_t NumEntries() const override { return properties_.num_entries; }
  uint64_t FileSize() const override;
  TableProperties GetTableProperties() const override { return properties_; }
  void SetSecondPassIterator(InternalIterator* reader) override {
    second_pass_iter_ = reader;
  }

private:
  void AddPrevUserKey();
  void OfflineZipValueData();
  void UpdateValueLenHistogram();
  Status EmptyTableFinish();
  Status OfflineFinish();
#if defined(TerocksPrivateCode)
  Status PlainValueToFinish(fstring tmpIndexFile, std::function<void()> waitIndex);
#endif // TerocksPrivateCode
  Status ZipValueToFinish(fstring tmpIndexFile, std::function<void()> waitIndex);
  void BuilderWriteValues(std::function<void(fstring val)> write);
  Status WriteSSTFile(long long t3, long long t4
      , fstring tmpIndexFile, terark::BlobStore* zstore
      , fstring dictMem
      , const DictZipBlobStore::ZipStat& dzstat);
  Status WriteMetaData(std::initializer_list<std::pair<const std::string*, BlockHandle> > blocks);
  DictZipBlobStore::ZipBuilder* createZipBuilder() const;

  Arena arena_;
  const TerarkZipTableOptions& table_options_;
// fuck out TableBuilderOptions
  const ImmutableCFOptions& ioptions_;
  std::vector<std::unique_ptr<IntTblPropCollector>> collectors_;
// end fuck out TableBuilderOptions
  InternalIterator* second_pass_iter_ = nullptr;
  Uint32Histogram keyLenHistogram_;
  Uint32Histogram valueLenHistogram_;
  valvec<byte_t> prevUserKey_;
  terark::febitvec valueBits_;
  TempFileDeleteOnClose tmpKeyFile_;
  TempFileDeleteOnClose tmpValueFile_;
  TempFileDeleteOnClose tmpSampleFile_;
  AutoDeleteFile tmpZipDictFile_;
  AutoDeleteFile tmpZipValueFile_;
  std::mt19937_64 randomGenerator_;
  uint64_t sampleUpperBound_;
  TerocksIndex::KeyStat keyStat_;
  size_t sampleLenSum_ = 0;
  WritableFileWriter* file_;
  uint64_t offset_ = 0;
  Status status_;
  TableProperties properties_;
  std::unique_ptr<DictZipBlobStore::ZipBuilder> zbuilder_;
  BlockBuilder range_del_block_;
  bitfield_array<2> bzvType_;
  terark::fstrvec valueBuf_; // collect multiple values for one key
  bool closed_ = false;  // Either Finish() or Abandon() has been called.

  long long t0 = 0;
};

///////////////////////////////////////////////////////////////////////////////

class TerarkZipTableIterator : public InternalIterator, boost::noncopyable {
public:
  explicit TerarkZipTableIterator(const TerarkZipTableReader* table)
  : table_(table)
  , valstore_(table->valstore_.get())
  , iter_(table->keyIndex_->NewIterator())
  , commonPrefix_(fstringOf(table->commonPrefix_))
  , reverse_(table->isReverseBytewiseOrder_)
  {
    // isReverseBytewiseOrder_ just reverse key order
    // do not reverse multi values of a single key
    // so it can not use a reverse wrapper iterator
    zValtype_ = ZipValueType::kZeroSeq;
    pinned_iters_mgr_ = NULL;
    SetIterInvalid();
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) {
    if (pinned_iters_mgr_ && pinned_iters_mgr_ != pinned_iters_mgr) {
      pinned_buffer_.clear();
    }
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

  void SeekForPrev(const Slice& target) override {
    SeekForPrevImpl(target, &table_->table_reader_options_.internal_comparator);
  }

  void Seek(const Slice& target) override {
	  ParsedInternalKey pikey;
	  if (!ParseInternalKey(target, &pikey)) {
		  status_ = Status::InvalidArgument("TerarkZipTableIterator::Seek()",
				  "param target.size() < 8");
		  SetIterInvalid();
		  return;
	  }
	  TryPinBuffer(interKeyBuf_xx_);
	  // Damn MySQL-rocksdb may use "rev:" comparator
	  size_t cplen = commonPrefix_.commonPrefixLen(fstringOf(pikey.user_key));
    if (commonPrefix_.size() != cplen) {
      if (pikey.user_key.size() == cplen) {
        assert(pikey.user_key.size() < commonPrefix_.size());
        if (reverse_) {
          SeekToLast();
          this->Next(); // move  to EOF
          assert(!this->Valid());
        }
        else {
          SeekToFirst();
        }
      }
      else {
        assert(pikey.user_key.size() > cplen);
        assert(pikey.user_key[cplen] != commonPrefix_[cplen]);
        if ((byte_t(pikey.user_key[cplen]) < commonPrefix_[cplen]) ^ reverse_) {
          SeekToFirst();
        } else {
          SeekToLast();
          this->Next(); // move  to EOF
          assert(!this->Valid());
        }
      }
    }
    else {
      bool ok = iter_->Seek(fstringOf(pikey.user_key).substr(cplen));
      int cmp; // compare(iterKey, searchKey)
      if (!ok) { // searchKey is bytewise greater than all keys in database
        if (reverse_) {
          // searchKey is reverse_bytewise less than all keys in database
          iter_->SeekToLast();
          ok = iter_->Valid();
        }
        cmp = -1;
      }
      else { // now iter is at bytewise lower bound position
        cmp = SliceOf(iter_->key()).compare(SubStr(pikey.user_key, cplen));
        assert(cmp >= 0); // iterKey >= searchKey
        if (cmp > 0 && reverse_) {
          iter_->Prev();
          ok = iter_->Valid();
        }
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

private:
  void SetIterInvalid() {
    TryPinBuffer(interKeyBuf_xx_);
    iter_->SetInvalid();
	  validx_ = 0;
	  valnum_ = 0;
	  pInterKey_.user_key = Slice();
	  pInterKey_.sequence = uint64_t(-1);
	  pInterKey_.type = kMaxValue;
  }
  bool IndexIterSeekToFirst() {
    TryPinBuffer(interKeyBuf_xx_);
    if (reverse_)
      return iter_->SeekToLast();
    else
      return iter_->SeekToFirst();
  }
  bool IndexIterSeekToLast() {
    TryPinBuffer(interKeyBuf_xx_);
    if (reverse_)
      return iter_->SeekToFirst();
    else
      return iter_->SeekToLast();
  }
  bool IndexIterPrev() {
    TryPinBuffer(interKeyBuf_xx_);
    if (reverse_)
      return iter_->Next();
    else
      return iter_->Prev();
  }
  bool IndexIterNext() {
    TryPinBuffer(interKeyBuf_xx_);
    if (reverse_)
      return iter_->Prev();
    else
      return iter_->Next();
  }
  void TryPinBuffer(valvec<byte_t>& buf) {
    if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
      pinned_buffer_.push_back();
      pinned_buffer_.back().swap(buf);
    }
  }
  bool UnzipIterRecord(bool hasRecord) {
	  if (hasRecord) {
		  size_t recId = iter_->id();
		  zValtype_ = ZipValueType(table_->typeArray_[recId]);
		  try {
		    TryPinBuffer(valueBuf_);
		    if (ZipValueType::kMulti == zValtype_) {
		      valueBuf_.resize_no_init(sizeof(uint32_t)); // for offsets[valnum_]
		    } else {
		      valueBuf_.erase_all();
		    }
			  valstore_->get_record_append(recId, &valueBuf_);
		  }
		  catch (const BadCrc32cException& ex) { // crc checksum error
			  SetIterInvalid();
			  status_ = Status::Corruption(
				"TerarkZipTableIterator::UnzipIterRecord()", ex.what());
			  return false;
		  }
		  if (ZipValueType::kMulti == zValtype_) {
			  ZipValueMultiValue::decode(valueBuf_, &valnum_);
		  } else {
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
  void DecodeCurrKeyValue() {
    assert(status_.ok());
    assert(iter_->id() < table_->keyIndex_->NumKeys());
    switch (zValtype_) {
    default:
      status_ = Status::Aborted("TerarkZipTableIterator::DecodeCurrKeyValue()",
          "Bad ZipValueType");
      abort(); // must not goes here, if it does, it should be a bug!!
      break;
    case ZipValueType::kZeroSeq:
      assert(0 == validx_);
      assert(1 == valnum_);
      pInterKey_.sequence = 0;
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
    interKeyBuf_.assign(commonPrefix_.data(), commonPrefix_.size());
    AppendInternalKey(&interKeyBuf_, pInterKey_);
    interKeyBuf_xx_.assign((byte_t*)interKeyBuf_.data(), interKeyBuf_.size());
  }

  const TerarkZipTableReader* const table_;
  const terark::BlobStore*    const valstore_;
  const unique_ptr<TerocksIndex::Iterator> iter_;
  const fstring commonPrefix_;
  const bool reverse_;
  ParsedInternalKey pInterKey_;
  std::string interKeyBuf_;
  valvec<byte_t> interKeyBuf_xx_;
  valvec<byte_t> valueBuf_;
  Slice  userValue_;
  ZipValueType zValtype_;
  size_t valnum_;
  size_t validx_;
  Status status_;
  PinnedIteratorsManager*  pinned_iters_mgr_;
  valvec<valvec<byte_t> >  pinned_buffer_;
};

TerarkZipTableReader::~TerarkZipTableReader() {
	typeArray_.risk_release_ownership();
}

TerarkZipTableReader::TerarkZipTableReader(const TableReaderOptions& tro,
      const TerarkZipTableOptions& tzto)
 : table_reader_options_(tro)
 , tzto_(tzto)
{
  isReverseBytewiseOrder_ = false;
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
  } else {
    return Status::InvalidArgument("TerarkZipTableReader::Open()",
			"EnvOptions::use_mmap_reads must be true");
  }
  file_data_ = file_data;
  table_properties_.reset(uniqueProps.release());
  isReverseBytewiseOrder_ =
      fstring(ioptions.user_comparator->Name()).startsWith("rev:");
  BlockContents valueDictBlock, indexBlock, zValueTypeBlock, tombstoneBlock;
  BlockContents commonPrefixBlock;
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
		  kTerarkZipTableValueDictBlock, &valueDictBlock);
  if (!s.ok()) {
	  return s;
  }
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
		  kTerarkZipTableIndexBlock, &indexBlock);
  if (!s.ok()) {
	  return s;
  }
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
		  kTerarkZipTableValueTypeBlock, &zValueTypeBlock);
  if (!s.ok()) {
	  return s;
  }
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
    kRangeDelBlock, &tombstoneBlock);
  if (s.ok()) {
    tombstone_.reset(new Block(std::move(tombstoneBlock), 0));
  }
  s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
      kTerarkZipTableCommonPrefixBlock, &commonPrefixBlock);
  if (s.ok()) {
    commonPrefix_ = commonPrefixBlock.data;
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
	  valstore_.reset(terark::BlobStore::load_from_user_memory(
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
  size_t recNum = keyIndex_->NumKeys();
  typeArray_.risk_set_data((byte_t*)zValueTypeBlock.data.data(), recNum);
  long long t0 = g_pf.now();
  if (tzto_.warmUpIndexOnOpen) {
    MmapWarmUp(fstringOf(indexBlock.data));
    if (!tzto_.warmUpValueOnOpen) {
      MmapWarmUp(valstore_->get_dict());
      for (fstring block : valstore_->get_index_blocks()) {
        MmapWarmUp(block);
      }
    }
  }
  if (tzto_.warmUpValueOnOpen) {
    MmapWarmUp(valstore_->get_mmap());
  }
  long long t1 = g_pf.now();
  keyIndex_->BuildCache(tzto_.indexCacheRatio);
  long long t2 = g_pf.now();
	INFO(ioptions.info_log
    , "TerarkZipTableReader::Open(): fsize = %zd, entries = %zd keys = %zd indexSize = %zd valueSize=%zd, warm up time = %6.3f'sec, build cache time = %6.3f'sec\n"
		, size_t(file_size), size_t(table_properties_->num_entries)
		, keyIndex_->NumKeys()
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
	  keyIndex_ = TerocksIndex::LoadMemory(fstringOf(mem));
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
	if (arena) {
		return new(arena->AllocateAligned(sizeof(TerarkZipTableIterator)))
				TerarkZipTableIterator(this);
	}
	else {
		return new TerarkZipTableIterator(this);
	}
}

InternalIterator*
TerarkZipTableReader::
NewRangeTombstoneIterator(const ReadOptions& read_options) {
  if (tombstone_) {
    auto iter = tombstone_->NewIterator(&table_reader_options_.internal_comparator);
    return iter;
  }
  return nullptr;
}

Status
TerarkZipTableReader::Get(const ReadOptions& ro, const Slice& ikey,
						  GetContext* get_context, bool skip_filters) {
  MY_THREAD_LOCAL(valvec<byte_t>, g_tbuf);
	ParsedInternalKey pikey;
	if (!ParseInternalKey(ikey, &pikey)) {
	  return Status::InvalidArgument("TerarkZipTableReader::Get()",
	      "bad internal key causing ParseInternalKey() failed");
	}
  size_t cplen = pikey.user_key.difference_offset(commonPrefix_);
  if (commonPrefix_.size() != cplen) {
    return Status::OK();
  }
	size_t recId = keyIndex_->Find(fstringOf(pikey.user_key).substr(cplen));
	if (size_t(-1) == recId) {
		return Status::OK();
	}
	auto zvType = ZipValueType(typeArray_[recId]);
	if (ZipValueType::kMulti == zvType) {
	  g_tbuf.resize_no_init(sizeof(uint32_t));
	} else {
	  g_tbuf.erase_all();
	}
	try {
		valstore_->get_record_append(recId, &g_tbuf);
	}
	catch (const terark::BadChecksumException& ex) {
		return Status::Corruption("TerarkZipTableReader::Get()", ex.what());
	}
	switch (zvType) {
	default:
		return Status::Aborted("TerarkZipTableReader::Get()", "Bad ZipValueType");
	case ZipValueType::kZeroSeq:
		get_context->SaveValue(Slice((char*)g_tbuf.data(), g_tbuf.size()), 0);
		break;
	case ZipValueType::kValue: { // should be a kTypeValue, the normal case
		// little endian uint64_t
		uint64_t seq = *(uint64_t*)g_tbuf.data() & kMaxSequenceNumber;
		if (seq <= pikey.sequence) {
			get_context->SaveValue(SliceOf(fstring(g_tbuf).substr(7)), seq);
		}
		break; }
	case ZipValueType::kDelete: {
		// little endian uint64_t
		uint64_t seq = *(uint64_t*)g_tbuf.data() & kMaxSequenceNumber;
		if (seq <= pikey.sequence) {
			get_context->SaveValue(
				ParsedInternalKey(pikey.user_key, seq, kTypeDeletion),
				Slice());
		}
		break; }
	case ZipValueType::kMulti: { // more than one value
	  size_t num = 0;
		auto mVal = ZipValueMultiValue::decode(g_tbuf, &num);
		for(size_t i = 0; i < num; ++i) {
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
				bool hasMoreValue = get_context->SaveValue(
					ParsedInternalKey(pikey.user_key, sn, valtype), val);
				if (!hasMoreValue) {
					break;
				}
			}
		}
		break; }
	}
	if (g_tbuf.capacity() > 512*1024) {
	  g_tbuf.clear(); // free large thread local memory
	}
	return Status::OK();
}

///////////////////////////////////////////////////////////////////////////////

TerarkZipTableBuilder::TerarkZipTableBuilder(
		const TerarkZipTableOptions& tzto,
		const TableBuilderOptions& tbo,
		uint32_t column_family_id,
		WritableFileWriter* file)
  : table_options_(tzto)
  , ioptions_(tbo.ioptions)
  , range_del_block_(1)
{
  if (tbo.int_tbl_prop_collector_factories) {
    const auto& factories = *tbo.int_tbl_prop_collector_factories;
    collectors_.resize(factories.size());
    auto cfId = properties_.column_family_id;
    for (size_t i = 0; i < collectors_.size(); ++i) {
      collectors_[i].reset(factories[i]->CreateIntTblPropCollector(cfId));
    }
  }

  file_ = file;
  sampleUpperBound_ = randomGenerator_.max() * table_options_.sampleRatio;
  tmpValueFile_.path = tzto.localTempDir + "/Terocks-XXXXXX";
#if _MSC_VER
  if (int err = _mktemp_s(&tmpValueFile_.path[0], tmpValueFile_.path.size() + 1)) {
    fprintf(stderr
      , "ERROR: _mktemp_s(%s) failed with: %s, so we may use large memory\n"
      , tmpValueFile_.path.c_str(), strerror(err));
  }
  tmpValueFile_.open();
#else
  int fd = mkstemp(&tmpValueFile_.path[0]);
  if (fd < 0) {
    int err = errno;
    THROW_STD(invalid_argument
        , "ERROR: TerarkZipTableBuilder::TerarkZipTableBuilder(): mkstemp(%s) = %s\n"
        , tmpValueFile_.path.c_str(), strerror(err));
  }
  tmpValueFile_.dopen(fd);
#endif
  tmpKeyFile_.path = tmpValueFile_.path + ".keydata";
  tmpKeyFile_.open();
  tmpSampleFile_.path = tmpValueFile_.path + ".sample";
  tmpSampleFile_.open();

  properties_.fixed_key_len = 0;
  properties_.num_data_blocks = 1;
  properties_.column_family_id = column_family_id;
  properties_.column_family_name = tbo.column_family_name;

  if (tzto.isOfflineBuild) {
    if (tbo.compression_dict && tbo.compression_dict->size()) {
      auto data = (byte_t*)tbo.compression_dict->data();
      auto size = tbo.compression_dict->size();
      tmpZipValueFile_.fpath = tmpValueFile_.path + ".zbs";
      tmpZipDictFile_.fpath  = tmpValueFile_.path + ".zbs-dict";
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
}

DictZipBlobStore::ZipBuilder*
TerarkZipTableBuilder::createZipBuilder() const {
  DictZipBlobStore::Options dzopt;
  dzopt.entropyAlgo = DictZipBlobStore::Options::EntropyAlgo(table_options_.entropyAlgo);
  dzopt.checksumLevel = table_options_.checksumLevel;
  dzopt.useSuffixArrayLocalMatch = table_options_.useSuffixArrayLocalMatch;
  return DictZipBlobStore::createZipBuilder(dzopt);
}

TerarkZipTableBuilder::~TerarkZipTableBuilder() {
}

uint64_t TerarkZipTableBuilder::FileSize() const {
  if (0 == offset_) {
    // for compaction caller to split file by increasing size
    auto kvLen = properties_.raw_key_size +  properties_.raw_value_size;
    auto fsize = uint64_t(kvLen * table_options_.estimateCompressionRatio);
    if (terark_unlikely(size_t(-1) == keyStat_.sumKeyLen)) {
      return fsize;
    }
    size_t dictZipMemSize = std::min<size_t>(sampleLenSum_, INT32_MAX) * 6;
    size_t nltTrieMemSize = keyStat_.sumKeyLen +
        sizeof(SortableStrVec::SEntry) * keyStat_.numKeys;
    size_t peakMemSize = std::max(dictZipMemSize, nltTrieMemSize);
    if (peakMemSize < table_options_.softZipWorkingMemLimit) {
      return fsize;
    } else {
      return fsize * 5; // notify rocksdb to `Finish()` this table asap.
    }
  }
  else {
    return offset_;
  }
}

static std::mutex g_sumMutex;
static size_t g_sumKeyLen = 0;
static size_t g_sumValueLen = 0;
static size_t g_sumUserKeyLen = 0;
static size_t g_sumUserKeyNum = 0;
static size_t g_sumEntryNum = 0;
static long long g_lastTime = g_pf.now();

void TerarkZipTableBuilder::Add(const Slice& key, const Slice& value) {

  ValueType value_type = ExtractValueType(key);
  uint64_t offset = uint64_t((properties_.raw_key_size + properties_.raw_value_size) * table_options_.estimateCompressionRatio);
  if (IsValueType(value_type)) {
    assert(key.size() >= 8);
    fstring userKey(key.data(), key.size() - 8);
    if (terark_likely(size_t(-1) != keyStat_.numKeys)) {
      if (prevUserKey_ != userKey) {
        assert(prevUserKey_ < userKey);
        keyStat_.commonPrefixLen = fstring(prevUserKey_.data(), keyStat_.commonPrefixLen)
          .commonPrefixLen(userKey);
        keyStat_.minKeyLen = std::min(userKey.size(), keyStat_.minKeyLen);
        keyStat_.maxKeyLen = std::max(userKey.size(), keyStat_.maxKeyLen);
        AddPrevUserKey();
        prevUserKey_.assign(userKey);
      }
    }
    else {
      keyStat_.commonPrefixLen = userKey.size();
      keyStat_.minKeyLen = userKey.size();
      keyStat_.maxKeyLen = userKey.size();
      prevUserKey_.assign(userKey);
      keyStat_.sumKeyLen = 0;
      keyStat_.numKeys = 0;
      t0 = g_pf.now();
    }
    valueBits_.push_back(true);
    valueBuf_.emplace_back(userKey.end(), 8);
    valueBuf_.back_append(value.data(), value.size());
    if (!zbuilder_) {
      if (!value.empty() && randomGenerator_() < sampleUpperBound_) {
        tmpSampleFile_.writer << fstringOf(value);
        sampleLenSum_ += value.size();
      }
      if (!second_pass_iter_) {
        tmpValueFile_.writer.ensureWrite(userKey.end(), 8);
        tmpValueFile_.writer << fstringOf(value);
      }
    }
    properties_.num_entries++;
    properties_.raw_key_size += key.size();
    properties_.raw_value_size += value.size();
    NotifyCollectTableCollectorsOnAdd(key, value, offset,
      collectors_, ioptions_.info_log);
  }
  else if (value_type == kTypeRangeDeletion) {
    range_del_block_.Add(key, value);
    properties_.num_entries++;
    properties_.raw_key_size += key.size();
    properties_.raw_value_size += value.size();
    NotifyCollectTableCollectorsOnAdd(key, value, offset,
      collectors_, ioptions_.info_log);
  }
  else {
    assert(false);
  }
}

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

Status TerarkZipTableBuilder::EmptyTableFinish() {
  INFO(tbo_.ioptions.info_log
      , "TerarkZipTableBuilder::EmptyFinish():this=%p\n", this);
  offset_ = 0;
  BlockHandle emptyTableBH;
  Status s = WriteBlock(Slice("Empty"), file_, &offset_, &emptyTableBH);
  if (!s.ok()) {
    return s;
  }
  return WriteMetaData({{&kEmptyTableKey, emptyTableBH}});
}

namespace {
  struct PendingTask {
    const TerarkZipTableBuilder* tztb;
    long long startTime;
  };
}

Status TerarkZipTableBuilder::Finish() {
	assert(!closed_);
	closed_ = true;

	if (size_t(-1) == keyStat_.numKeys) {
	  keyStat_.maxKeyLen = 0;
	  keyStat_.sumKeyLen = 0;
	  keyStat_.numKeys = 0;
	  return EmptyTableFinish();
	}

  AddPrevUserKey();
  keyLenHistogram_.finish();
  valueLenHistogram_.finish();
  tmpKeyFile_.complete_write();
  if (1 == keyStat_.numKeys) {
    assert(keyStat_.commonPrefixLen == keyStat_.sumKeyLen);
    keyStat_.commonPrefixLen = 0; // to avoid empty nlt trie
  }
	if (zbuilder_) {
	  return OfflineFinish();
	}

  if (!second_pass_iter_) {
    tmpValueFile_.complete_write();
  }
  tmpSampleFile_.complete_write();
	{
	  long long rawBytes = properties_.raw_key_size + properties_.raw_value_size;
	  long long tt = g_pf.now();
	  INFO(tbo_.ioptions.info_log
	      , "TerarkZipTableBuilder::Finish():this=%p:  first pass time =%7.2f's, %8.3f'MB/sec\n"
	      , this, g_pf.sf(t0,tt), rawBytes*1.0/g_pf.uf(t0,tt)
	      );
	}
  static std::mutex zipMutex;
  static std::condition_variable zipCond;
  static valvec<PendingTask> waitQueue;
  static size_t sumWaitingMem = 0;
  static size_t sumWorkingMem = 0;
  const  size_t softMemLimit = table_options_.softZipWorkingMemLimit;
  const  size_t hardMemLimit = std::max(table_options_.hardZipWorkingMemLimit, softMemLimit);
  const  size_t smallmem = table_options_.smallTaskMemory;
  const  long long myStartTime = g_pf.now();
  {
    std::unique_lock<std::mutex> zipLock(zipMutex);
    waitQueue.push_back({this, myStartTime});
  }
auto waitForMemory = [&](size_t myWorkMem, const char* who) {
  const std::chrono::seconds waitForTime(10);
  long long now = myStartTime;
  auto shouldWait = [&]() {
    bool w;
    if (myWorkMem < softMemLimit) {
      w = (sumWorkingMem + myWorkMem >= hardMemLimit) ||
          (sumWorkingMem + myWorkMem >= softMemLimit && myWorkMem >= smallmem);
    }
    else {
      w = sumWorkingMem > softMemLimit / 4;
    }
    if (!w) {
      assert(!waitQueue.empty());
      now = g_pf.now();
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
    }
    return true; // wait
  };
  std::unique_lock<std::mutex> zipLock(zipMutex);
  if (myWorkMem > smallmem / 2) { // never wait for very smallmem(SST flush)
    sumWaitingMem += myWorkMem;
    while (shouldWait()) {
      INFO(tbo_.ioptions.info_log
          , "TerarkZipTableBuilder::Finish():this=%p: sumWaitingMem = %f GB, sumWorkingMem = %f GB, %s WorkingMem = %f GB, wait...\n"
          , this, sumWaitingMem/1e9, sumWorkingMem/1e9, who, myWorkMem/1e9
          );
      zipCond.wait_for(zipLock, waitForTime);
    }
    INFO(tbo_.ioptions.info_log
        , "TerarkZipTableBuilder::Finish():this=%p: sumWaitingMem = %f GB, sumWorkingMem = %f GB, %s WorkingMem = %f GB, waited %8.3f sec, Key+Value bytes = %f GB\n"
        , this, sumWaitingMem/1e9, sumWorkingMem/1e9, who, myWorkMem/1e9
        , g_pf.sf(myStartTime, now)
        , (properties_.raw_key_size + properties_.raw_value_size) / 1e9
        );
    sumWaitingMem -= myWorkMem;
  }
  sumWorkingMem += myWorkMem;
};
// indexing is also slow, run it in parallel
AutoDeleteFile tmpIndexFile{tmpValueFile_.path + ".index"};
std::future<void> asyncIndexResult = std::async(std::launch::async, [&]()
{
  auto factory = TerocksIndex::GetFactory(table_options_.indexType);
  if (!factory) {
    THROW_STD(invalid_argument,
        "invalid indexType: %s", table_options_.indexType.c_str());
  }
  const size_t myWorkMem = factory->MemSizeForBuild(keyStat_);
  waitForMemory(myWorkMem, "nltTrie");
  BOOST_SCOPE_EXIT(myWorkMem){
    std::unique_lock<std::mutex> zipLock(zipMutex);
    assert(sumWorkingMem >= myWorkMem);
    sumWorkingMem -= myWorkMem;
    zipCond.notify_all();
  }BOOST_SCOPE_EXIT_END;

  long long t1 = g_pf.now();
  factory->Build(tmpKeyFile_, table_options_, tmpIndexFile, keyStat_);
  long long tt = g_pf.now();
  INFO(tbo_.ioptions.info_log
      , "TerarkZipTableBuilder::Finish():this=%p:  index pass time =%7.2f's, %8.3f'MB/sec\n"
      , this, g_pf.sf(t1,tt), properties_.raw_key_size*1.0/g_pf.uf(t1,tt)
      );
});
  size_t myDictMem = std::min<size_t>(sampleLenSum_, INT32_MAX) * 6;
  waitForMemory(myDictMem, "dictZip");

  BOOST_SCOPE_EXIT(&myDictMem){
    std::unique_lock<std::mutex> zipLock(zipMutex);
    assert(sumWorkingMem >= myDictMem);
    // if success, myDictMem is 0, else sumWorkingMem should be restored
    sumWorkingMem -= myDictMem;
    zipCond.notify_all();
  }BOOST_SCOPE_EXIT_END;

  auto waitIndex = [&]() {
    {
      std::unique_lock<std::mutex> zipLock(zipMutex);
      sumWorkingMem -= myDictMem;
    }
    myDictMem = 0; // success, set to 0
    asyncIndexResult.get();
    std::unique_lock<std::mutex> zipLock(zipMutex);
    waitQueue.trim(std::remove_if(waitQueue.begin(), waitQueue.end(),
        [this](PendingTask x){return this==x.tztb;}));
  };
#if defined(TerocksPrivateCode)
  auto avgValueLen = properties_.raw_value_size / properties_.num_entries;
  if (avgValueLen < table_options_.minDictZipValueSize)
    return PlainValueToFinish(tmpIndexFile, waitIndex);
  else
#endif // TerocksPrivateCode
  return ZipValueToFinish(tmpIndexFile, waitIndex);
}

#if defined(TerocksPrivateCode)
Status
TerarkZipTableBuilder::
PlainValueToFinish(fstring tmpIndexFile, std::function<void()> waitIndex) {
//  double maxFrequentKeyRatio =
//  double(valueLenHistogram_.m_cnt_of_max_cnt_key) / keyStat_.numKeys;
//  size_t totalLen = valueLenHistogram_.m_cnt_sum;
  size_t fixedLen = valueLenHistogram_.m_max_cnt_key;
  size_t fixedNum = valueLenHistogram_.m_cnt_of_max_cnt_key;
  size_t variaNum = keyStat_.numKeys - fixedNum;
  std::unique_ptr<terark::BlobStore> store;
  long long t3 = g_pf.now();
  if (4 * variaNum + keyStat_.numKeys*5/4 < 4 * keyStat_.numKeys) {
    // use MixedLenBlobStore
    auto builder = UniquePtrOf(new terark::MixedLenBlobStore::Builder(fixedLen));
    BuilderWriteValues([&](fstring value){builder->add_record(value);});
    store.reset(builder->finish());
  }
  else {
    // use PlainBlobStore
    auto plain = new terark::PlainBlobStore();
    store.reset(plain);
    plain->reset_with_content_size(keyStat_.numKeys);
    BuilderWriteValues([&](fstring value){plain->add_record(value);});
  }
  long long t4 = g_pf.now();

  // wait for indexing complete, if indexing is slower than value compressing
  waitIndex();
  fstring dictMem("");
  DictZipBlobStore::ZipStat dzstat; // fake dzstat
  dzstat.dictBuildTime = 0.000001;
  dzstat.dictFileTime  = 0.000001;
  dzstat.dictZipTime   = g_pf.sf(t3, t4);
  dzstat.sampleTime    = 0.000001;
  return WriteSSTFile(t3, t4, tmpIndexFile, store.get(), dictMem, dzstat);
}
#endif // TerocksPrivateCode

Status
TerarkZipTableBuilder::
ZipValueToFinish(fstring tmpIndexFile, std::function<void()> waitIndex) {
  AutoDeleteFile tmpStoreFile{tmpValueFile_.path + ".zbs"};
  AutoDeleteFile tmpStoreDict{tmpValueFile_.path + ".zbs-dict"};
  long long t3 = g_pf.now();
  auto zbuilder = UniquePtrOf(this->createZipBuilder());
{
#if defined(TERARK_ZIP_TRIAL_VERSION)
  zbuilder->addSample(g_trail_rand_delete);
#endif
  valvec<byte_t> sample;
  NativeDataInput<InputBuffer> input(&tmpSampleFile_.fp);
  size_t realsampleLenSum = 0;
  if (sampleLenSum_ < INT32_MAX) {
    for (size_t len = 0; len < sampleLenSum_; ) {
      input >> sample;
      zbuilder->addSample(sample);
      len += sample.size();
    }
    realsampleLenSum = sampleLenSum_;
  }
  else {
    uint64_t upperBound2 = uint64_t(
        randomGenerator_.max() * double(INT32_MAX) / sampleLenSum_);
    for (size_t len = 0; len < sampleLenSum_; ) {
      input >> sample;
      if (randomGenerator_() < upperBound2) {
        zbuilder->addSample(sample);
        realsampleLenSum += sample.size();
      }
      len += sample.size();
    }
  }
  tmpSampleFile_.close();
  if (0 == realsampleLenSum) { // prevent from empty
    zbuilder->addSample("Hello World!");
  }
  zbuilder->finishSample();
  zbuilder->prepare(keyStat_.numKeys, tmpStoreFile);
}

  BuilderWriteValues([&](fstring value){zbuilder->addRecord(value);});

  unique_ptr<DictZipBlobStore> zstore(zbuilder->finish());
  DictZipBlobStore::ZipStat dzstat = zbuilder->getZipStat();
  zbuilder.reset();

  long long t4 = g_pf.now();

  // wait for indexing complete, if indexing is slower than value compressing
  waitIndex();

  fstring dictMem = zstore->get_dict();
  return WriteSSTFile(t3, t4, tmpIndexFile, zstore.get(), dictMem, dzstat);
}

void
TerarkZipTableBuilder::BuilderWriteValues(std::function<void(fstring)> write) {
  bzvType_.resize(keyStat_.numKeys);
  if (nullptr == second_pass_iter_)
{
  NativeDataInput<InputBuffer> input(&tmpValueFile_.fp);
  valvec<byte_t> value;
#if defined(TERARK_ZIP_TRIAL_VERSION)
  valvec<byte_t> tmpValueBuf;
#endif
  size_t entryId = 0;
  size_t bitPos = 0;
  for (size_t recId = 0; recId < keyStat_.numKeys; recId++) {
    uint64_t seqType = input.load_as<uint64_t>();
    uint64_t seqNum;
    ValueType vType;
    UnPackSequenceAndType(seqType, &seqNum, &vType);
    size_t oneSeqLen = valueBits_.one_seq_len(bitPos);
    assert(oneSeqLen >= 1);
    if (1==oneSeqLen && (kTypeDeletion==vType || kTypeValue==vType)) {
      if (0 == seqNum && kTypeValue==vType) {
        bzvType_.set0(recId, size_t(ZipValueType::kZeroSeq));
#if defined(TERARK_ZIP_TRIAL_VERSION)
        if (randomGenerator_() < randomGenerator_.max()/1000) {
          input >> tmpValueBuf;
          value.assign(fstring(g_trail_rand_delete));
        } else
#endif
          input >> value;
      } else {
        if (kTypeValue==vType) {
          bzvType_.set0(recId, size_t(ZipValueType::kValue));
        } else {
          bzvType_.set0(recId, size_t(ZipValueType::kDelete));
        }
        value.erase_all();
        value.append((byte_t*)&seqNum, 7);
        input.load_add(value);
      }
    }
    else {
      bzvType_.set0(recId, size_t(ZipValueType::kMulti));
      size_t headerSize = ZipValueMultiValue::calcHeaderSize(oneSeqLen);
      value.resize(headerSize);
      ((ZipValueMultiValue*)value.data())->offsets[0] = uint32_t(oneSeqLen);
      for (size_t j = 0; j < oneSeqLen; j++) {
        if (j > 0) {
          seqType = input.load_as<uint64_t>();
        }
        value.append((byte_t*)&seqType, 8);
        input.load_add(value);
        if (j + 1 < oneSeqLen) {
          ((ZipValueMultiValue*)value.data())->offsets[j+1] = value.size() - headerSize;
        }
      }
    }
    write(value);
    bitPos += oneSeqLen + 1;
    entryId += oneSeqLen;
  }
  // tmpValueFile_ ignore kTypeRangeDeletion keys
  // so entryId may less than properties_.num_entries
  assert(entryId <= properties_.num_entries);
}
  else
{
  valvec<byte_t> value;
  size_t entryId = 0;
  size_t bitPos = 0;
  for (size_t recId = 0; recId < keyStat_.numKeys; recId++) {
    value.erase_all();
    assert(second_pass_iter_->Valid());
    ParsedInternalKey pikey;
    Slice curKey = second_pass_iter_->key();
    ParseInternalKey(curKey, &pikey);
    while (kTypeRangeDeletion == pikey.type) {
      second_pass_iter_->Next();
      assert(second_pass_iter_->Valid());
      curKey = second_pass_iter_->key();
      ParseInternalKey(curKey, &pikey);
      entryId += 1;
    }
    Slice curVal = second_pass_iter_->value();
    size_t oneSeqLen = valueBits_.one_seq_len(bitPos);
    assert(oneSeqLen >= 1);
    if (1==oneSeqLen && (kTypeDeletion==pikey.type || kTypeValue==pikey.type)) {
      //assert(fstringOf(pikey.user_key) == backupKeys[recId]);
      if (0 == pikey.sequence && kTypeValue==pikey.type) {
        bzvType_.set0(recId, size_t(ZipValueType::kZeroSeq));
#if defined(TERARK_ZIP_TRIAL_VERSION)
        if (randomGenerator_() < randomGenerator_.max()/1000)
          write(fstring(g_trail_rand_delete));
        else
#endif
          write(fstringOf(curVal));
      }
      else {
        if (kTypeValue == pikey.type) {
          bzvType_.set0(recId, size_t(ZipValueType::kValue));
        }
        else {
          bzvType_.set0(recId, size_t(ZipValueType::kDelete));
        }
        value.append((byte_t*)&pikey.sequence, 7);
        value.append(fstringOf(curVal));
        write(value);
      }
      second_pass_iter_->Next();
    }
    else {
      bzvType_.set0(recId, size_t(ZipValueType::kMulti));
      size_t headerSize = ZipValueMultiValue::calcHeaderSize(oneSeqLen);
      value.resize(headerSize);
      ((ZipValueMultiValue*)value.data())->offsets[0] = uint32_t(oneSeqLen);
      for (size_t j = 0; j < oneSeqLen; j++) {
        if (j > 0) {
          assert(second_pass_iter_->Valid());
          curKey = second_pass_iter_->key();
          ParseInternalKey(curKey, &pikey);
          while (kTypeRangeDeletion == pikey.type) {
            second_pass_iter_->Next();
            assert(second_pass_iter_->Valid());
            curKey = second_pass_iter_->key();
            ParseInternalKey(curKey, &pikey);
            entryId += 1;
          }
          curVal = second_pass_iter_->value();
        }
        else {
          assert(kTypeRangeDeletion != pikey.type);
        }
        //assert(fstringOf(pikey.user_key) == backupKeys[recId]);
        uint64_t seqType = PackSequenceAndType(pikey.sequence, pikey.type);
        value.append((byte_t*)&seqType, 8);
        value.append(fstringOf(curVal));
        if (j+1 < oneSeqLen) {
          ((ZipValueMultiValue*)value.data())->offsets[j+1] = value.size() - headerSize;
        }
        second_pass_iter_->Next();
      }
      write(value);
    }
    bitPos += oneSeqLen + 1;
    entryId += oneSeqLen;
  }
  assert(entryId == properties_.num_entries);
}
  tmpValueFile_.close();
}

Status TerarkZipTableBuilder::WriteSSTFile(long long t3, long long t4
    , fstring tmpIndexFile, terark::BlobStore* zstore
    , fstring dictMem
    , const DictZipBlobStore::ZipStat& dzstat)
{
  const size_t realsampleLenSum = dictMem.size();
  long long rawBytes = properties_.raw_key_size + properties_.raw_value_size;
  long long t5 = g_pf.now();
	unique_ptr<TerocksIndex> index(TerocksIndex::LoadFile(tmpIndexFile));
	assert(index->NumKeys() == keyStat_.numKeys);
	Status s;
  BlockHandle dataBlock, dictBlock, indexBlock, zvTypeBlock, tombstoneBlock;
  BlockHandle commonPrefixBlock;
{
  size_t real_size = index->Memory().size() + zstore->mem_size() + bzvType_.mem_size();
  size_t block_size, last_allocated_block;
  file_->writable_file()->GetPreallocationStatus(&block_size, &last_allocated_block);
  INFO(tbo_.ioptions.info_log
    , "TerarkZipTableBuilder::Finish():this=%p: old prealloc_size = %zd, real_size = %zd\n"
    , this, block_size, real_size
    );
  file_->writable_file()->SetPreallocationBlockSize(1*1024*1024 + real_size);
}
  long long t6, t7;
  offset_ = 0;
  if (index->NeedsReorder()) {
    bitfield_array<2> zvType2(keyStat_.numKeys);
		terark::AutoFree<uint32_t> newToOld(keyStat_.numKeys, UINT32_MAX);
		index->GetOrderMap(newToOld.p);
		t6 = g_pf.now();
		if (fstring(ioptions_.user_comparator->Name()).startsWith("rev:")) {
		  // Damn reverse bytewise order
      for (size_t newId = 0; newId < keyStat_.numKeys; ++newId) {
        size_t dictOrderOldId = newToOld.p[newId];
        size_t reverseOrderId = keyStat_.numKeys - dictOrderOldId - 1;
        newToOld.p[newId] = reverseOrderId;
        zvType2.set0(newId, bzvType_[reverseOrderId]);
      }
		}
		else {
		  for (size_t newId = 0; newId < keyStat_.numKeys; ++newId) {
		    size_t dictOrderOldId = newToOld.p[newId];
		    zvType2.set0(newId, bzvType_[dictOrderOldId]);
		  }
		}
		t7 = g_pf.now();
		try {
		  dataBlock.set_offset(offset_);
		  auto writeAppend = [&](const void* data, size_t size) {
		    s = file_->Append(Slice((const char*)data, size));
		    if (!s.ok()) {
		      throw s;
		    }
		    offset_ += size;
		  };
		  zstore->reorder_zip_data(newToOld, std::ref(writeAppend));
		  dataBlock.set_size(offset_ - dataBlock.offset());
		} catch (const Status&) {
		  return s;
		}
		bzvType_.clear();
		bzvType_.swap(zvType2);
	}
  else {
    t7 = t6 = t5;
    WriteBlock(zstore->get_mmap(), file_, &offset_, &dataBlock);
  }
  fstring commonPrefix(prevUserKey_.data(), keyStat_.commonPrefixLen);
  WriteBlock(commonPrefix, file_, &offset_, &commonPrefixBlock);
	properties_.data_size = dataBlock.size();
	s = WriteBlock(dictMem, file_, &offset_, &dictBlock);
	if (!s.ok()) {
		return s;
	}
	s = WriteBlock(index->Memory(), file_, &offset_, &indexBlock);
	if (!s.ok()) {
		return s;
	}
	fstring zvTypeMem(bzvType_.data(), bzvType_.mem_size());
	s = WriteBlock(zvTypeMem, file_, &offset_, &zvTypeBlock);
	if (!s.ok()) {
		return s;
	}
	index.reset();
  if (!range_del_block_.empty()) {
    s = WriteBlock(range_del_block_.Finish(), file_, &offset_, &tombstoneBlock);
    if (!s.ok()) {
      return s;
    }
  }
  range_del_block_.Reset();
	properties_.index_size = indexBlock.size();
	WriteMetaData({
    {dictMem.size() ? &kTerarkZipTableValueDictBlock : NULL, dictBlock},
    {&kTerarkZipTableIndexBlock       , indexBlock},
    {&kTerarkZipTableValueTypeBlock   , zvTypeBlock},
    {&kTerarkZipTableCommonPrefixBlock, commonPrefixBlock},
    {!range_del_block_.empty() ? &kRangeDelBlock : NULL, tombstoneBlock},
	});
  long long t8 = g_pf.now();
  {
    std::unique_lock<std::mutex> lock(g_sumMutex);
    g_sumKeyLen += properties_.raw_key_size;
    g_sumValueLen += properties_.raw_value_size;
    g_sumUserKeyLen += keyStat_.sumKeyLen;
    g_sumUserKeyNum += keyStat_.numKeys;
    g_sumEntryNum += properties_.num_entries;
  }
  INFO(tbo_.ioptions.info_log
    ,
R"EOS(TerarkZipTableBuilder::Finish():this=%p: second pass time =%7.2f's, %8.3f'MB/sec, value only(%4.1f%% of KV)
   wait indexing time = %7.2f's,
  remap KeyValue time = %7.2f's, %8.3f'MB/sec (all stages of remap)
    Get OrderMap time = %7.2f's, %8.3f'MB/sec (index lex order gen)
  rebuild zvType time = %7.2f's, %8.3f'MB/sec
  write SST data time = %7.2f's, %8.3f'MB/sec
    z-dict build time = %7.2f's, sample length = %7.3f'MB, throughput = %6.3f'MB/sec
    zip my value time = %7.2f's, unzip  length = %7.3f'GB
    zip my value throughput = %7.3f'MB/sec
    zip pipeline throughput = %7.3f'MB/sec
    entries = %zd  keys = %zd  avg-key = %.2f  avg-zkey = %.2f  avg-val = %.2f  avg-zval = %.2f
    UnZipSize{ index =%9.4f GB  value =%9.4f GB   all =%9.4f GB }
    __ZipSize{ index =%9.4f GB  value =%9.4f GB   all =%9.4f GB }
    UnZip/Zip{ index =%9.4f     value =%9.4f      all =%9.4f    }
    Zip/UnZip{ index =%9.4f     value =%9.4f      all =%9.4f    }
----------------------------
    total value len =%12.6f GB     avg =%8.3f KB (by entry num)
    total  key  len =%12.6f GB     avg =%8.3f KB
    total ukey  len =%12.6f GB     avg =%8.3f KB
    total ukey  num =%15.9f Billion
    total entry num =%15.9f Billion
    write speed all =%15.9f MB/sec (with    version num)
    write speed all =%15.9f MB/sec (without version num)
)EOS"
    , this, g_pf.sf(t3,t4)
    , properties_.raw_value_size*1.0/g_pf.uf(t3,t4)
    , properties_.raw_value_size*100.0/rawBytes

    , g_pf.sf(t4,t5) // wait indexing time
    , g_pf.sf(t5,t8), double(offset_) / g_pf.uf(t5,t8)

    , g_pf.sf(t5,t6), properties_.index_size/g_pf.uf(t5,t6) // index lex walk

    , g_pf.sf(t6,t7), keyStat_.numKeys*2/8/(g_pf.uf(t6,t7)+1.0) // rebuild zvType

    , g_pf.sf(t7,t8), double(offset_) / g_pf.uf(t7,t8) // write SST data

    , dzstat.dictBuildTime, realsampleLenSum / 1e6
    , realsampleLenSum / dzstat.dictBuildTime / 1e6

    , dzstat.dictZipTime, properties_.raw_value_size / 1e9
    , properties_.raw_value_size  / dzstat.dictZipTime / 1e6
    , dzstat.pipelineThroughBytes / dzstat.dictZipTime / 1e6

    , size_t(properties_.num_entries), keyStat_.numKeys
    , double(keyStat_.sumKeyLen) / keyStat_.numKeys
    , double(properties_.index_size) / keyStat_.numKeys
    , double(properties_.raw_value_size) / keyStat_.numKeys
    , double(properties_.data_size) / keyStat_.numKeys

    , keyStat_.sumKeyLen/1e9, properties_.raw_value_size/1e9, rawBytes/1e9

    , properties_.index_size/1e9, properties_.data_size/1e9, offset_/1e9

    , double(keyStat_.sumKeyLen) / properties_.index_size
    , double(properties_.raw_value_size) / properties_.data_size
    , double(rawBytes) / offset_

    , properties_.index_size / double(keyStat_.sumKeyLen)
    , properties_.data_size  / double(properties_.raw_value_size)
    , offset_ / double(rawBytes)

    , g_sumValueLen/1e9, g_sumValueLen/1e3/g_sumEntryNum
    , g_sumKeyLen  /1e9, g_sumKeyLen  /1e3/g_sumEntryNum
    , g_sumUserKeyLen/1e9, g_sumUserKeyLen/1e3/g_sumUserKeyNum
    , g_sumUserKeyNum/1e9
    , g_sumEntryNum/1e9
    , (g_sumKeyLen + g_sumValueLen) / g_pf.uf(g_lastTime, t8)
    , (g_sumKeyLen + g_sumValueLen - g_sumEntryNum*8) / g_pf.uf(g_lastTime, t8)
  );
	return s;
}

Status TerarkZipTableBuilder::WriteMetaData(std::initializer_list<std::pair<const std::string*, BlockHandle> > blocks) {
  MetaIndexBuilder metaindexBuiler;
  for (const auto& block : blocks) {
    if (block.first) {
      metaindexBuiler.Add(*block.first, block.second);
    }
  }
  PropertyBlockBuilder propBlockBuilder;
  propBlockBuilder.AddTableProperty(properties_);
  propBlockBuilder.Add(properties_.user_collected_properties);
  NotifyCollectTableCollectorsOnFinish(collectors_,
                                       ioptions_.info_log,
                                       &propBlockBuilder);
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
  std::unique_ptr<DictZipBlobStore> zstore(zbuilder_->finish());
  auto dzstat = zbuilder_->getZipStat();
  zbuilder_.reset();
  valvec<byte_t> commonPrefix(prevUserKey_.data(), keyStat_.commonPrefixLen);
  AutoDeleteFile tmpIndexFile{tmpValueFile_.path + ".index"};
  long long t1 = g_pf.now();
  {
    auto factory = TerocksIndex::GetFactory(table_options_.indexType);
    if (!factory) {
      THROW_STD(invalid_argument,
          "invalid indexType: %s", table_options_.indexType.c_str());
    }
    factory->Build(tmpKeyFile_, table_options_, tmpIndexFile, keyStat_);
  }
  long long tt = g_pf.now();
  INFO(tbo_.ioptions.info_log
      , "TerarkZipTableBuilder::Finish():this=%p:  index pass time =%7.2f's, %8.3f'MB/sec\n"
      , this, g_pf.sf(t1,tt), properties_.raw_key_size*1.0/g_pf.uf(t1,tt)
      );
  fstring dictMem = zstore->get_dict();
  return WriteSSTFile(t1, tt, tmpIndexFile, zstore.get(), dictMem, dzstat);
}

void TerarkZipTableBuilder::Abandon() {
  closed_ = true;
  tmpKeyFile_.complete_write();
  tmpValueFile_.complete_write();
  tmpSampleFile_.complete_write();
  zbuilder_.reset();
  tmpZipDictFile_.Delete();
  tmpZipValueFile_.Delete();
}

void TerarkZipTableBuilder::AddPrevUserKey() {
  UpdateValueLenHistogram(); // will use valueBuf_
  if (zbuilder_) {
    OfflineZipValueData(); // will change valueBuf_
  }
  valueBuf_.erase_all();
  keyLenHistogram_[prevUserKey_.size()]++;
  tmpKeyFile_.writer << prevUserKey_;
	valueBits_.push_back(false);
	keyStat_.sumKeyLen += prevUserKey_.size();
	keyStat_.numKeys++;
}

void TerarkZipTableBuilder::OfflineZipValueData() {
  uint64_t seq, seqType = *(uint64_t*)valueBuf_.strpool.data();
  ValueType type;
  UnPackSequenceAndType(seqType, &seq, &type);
  const size_t vNum = valueBuf_.size();
  if (vNum == 1 && (kTypeDeletion==type || kTypeValue==type)) {
    if (0 == seq && kTypeValue==type) {
      bzvType_.push_back(byte_t(ZipValueType::kZeroSeq));
      zbuilder_->addRecord(fstring(valueBuf_.strpool).substr(8));
    } else {
      if (kTypeValue==type) {
        bzvType_.push_back(byte_t(ZipValueType::kValue));
      } else {
        bzvType_.push_back(byte_t(ZipValueType::kDelete));
      }
      // use Little Endian upper 7 bytes
      *(uint64_t*)valueBuf_.strpool.data() <<= 8;
      zbuilder_->addRecord(fstring(valueBuf_.strpool).substr(1));
    }
  }
  else {
    bzvType_.push_back(byte_t(ZipValueType::kMulti));
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
  if (vNum == 1 && (kTypeDeletion==type || kTypeValue==type)) {
    if (0 == seq && kTypeValue==type)
      valueLen = valueBuf_.strpool.size() - 8;
    else
      valueLen = valueBuf_.strpool.size() - 1;
  }
  else {
    valueLen = valueBuf_.strpool.size() + sizeof(uint32_t)*vNum;
  }
  valueLenHistogram_[valueLen]++;
}

/////////////////////////////////////////////////////////////////////////////

class TerarkZipTableFactory : public TableFactory, boost::noncopyable {
 public:
  explicit
  TerarkZipTableFactory(const TerarkZipTableOptions& tzto, TableFactory* fallback)
  : table_options_(tzto), fallback_factory_(fallback) {
    adaptive_factory_ = NewAdaptiveTableFactory();
  }

  const char* Name() const override { return "TerarkZipTable"; }

  Status
  NewTableReader(const TableReaderOptions& table_reader_options,
                 unique_ptr<RandomAccessFileReader>&& file,
                 uint64_t file_size,
				 unique_ptr<TableReader>* table,
				 bool prefetch_index_and_filter_in_cache) const override;

  TableBuilder*
  NewTableBuilder(const TableBuilderOptions& table_builder_options,
				  uint32_t column_family_id,
				  WritableFileWriter* file) const override;

  std::string GetPrintableTableOptions() const override;

  // Sanitizes the specified DB Options.
  Status SanitizeOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;

  void* GetOptions() override { return &table_options_; }

 private:
  TerarkZipTableOptions table_options_;
  TableFactory* fallback_factory_;
  TableFactory* adaptive_factory_; // just for open table
  mutable size_t nth_new_terark_table_ = 0;
  mutable size_t nth_new_fallback_table_ = 0;
};

class TableFactory*
NewTerarkZipTableFactory(const TerarkZipTableOptions& tzto,
						 class TableFactory* fallback) {
  STD_INFO("NewTerarkZipTableFactory()\n");
	return new TerarkZipTableFactory(tzto, fallback);
}

inline static
bool IsBytewiseComparator(const Comparator* cmp) {
#if 1
  const fstring name = cmp->Name();
  if (name.startsWith("RocksDB_SE_")) {
    return true;
  }
  if (name.startsWith("rev:RocksDB_SE_")) {
    // reverse bytewise compare, needs reverse in iterator
    return true;
  }
	return name == "leveldb.BytewiseComparator";
#else
	return BytewiseComparator() == cmp;
#endif
}

Status
TerarkZipTableFactory::NewTableReader(
		const TableReaderOptions& table_reader_options,
		unique_ptr<RandomAccessFileReader>&& file,
		uint64_t file_size, unique_ptr<TableReader>* table,
		bool prefetch_index_and_filter_in_cache)
const {
  auto userCmp = table_reader_options.internal_comparator.user_comparator();
	if (!IsBytewiseComparator(userCmp)) {
		return Status::InvalidArgument("TerarkZipTableFactory::NewTableReader()",
				"user comparator must be 'leveldb.BytewiseComparator'");
	}
#if 0
  if (fstring(userCmp->Name()).startsWith("rev:")) {
    return Status::InvalidArgument("TerarkZipTableFactory::NewTableReader",
        "Damn, fuck out the reverse bytewise comparator");
  }
#endif
	Footer footer;
	Status s = ReadFooterFromFile(file.get(), file_size, &footer);
	if (!s.ok()) {
		return s;
	}
	if (footer.table_magic_number() != kTerarkZipTableMagicNumber) {
	  if (adaptive_factory_) {
	    // just for open table
	    return adaptive_factory_->NewTableReader(table_reader_options,
	              std::move(file), file_size, table,
	              prefetch_index_and_filter_in_cache);
	  }
		if (fallback_factory_) {
			return fallback_factory_->NewTableReader(table_reader_options,
					std::move(file), file_size, table,
					prefetch_index_and_filter_in_cache);
		}
		return Status::InvalidArgument(
			"TerarkZipTableFactory::NewTableReader()",
			"fallback_factory is null and magic_number is not kTerarkZipTable"
			);
	}
#if 0
	if (!prefetch_index_and_filter_in_cache) {
		WARN(table_reader_options.ioptions.info_log
				, "TerarkZipTableFactory::NewTableReader(): "
				  "prefetch_index_and_filter_in_cache = false is ignored, "
				  "all index and data will be loaded in memory\n");
	}
#endif
  BlockContents emptyTableBC;
  s = ReadMetaBlock(file.get(), file_size, kTerarkZipTableMagicNumber
      , table_reader_options.ioptions, kEmptyTableKey, &emptyTableBC);
  if (s.ok()) {
    table->reset(new TerarkEmptyTableReader());
    return s;
  }
	std::unique_ptr<TerarkZipTableReader>
	t(new TerarkZipTableReader(table_reader_options, table_options_));
	s = t->Open(file.release(), file_size);
	if (s.ok()) {
	  *table = std::move(t);
	}
	return s;
}

TableBuilder*
TerarkZipTableFactory::NewTableBuilder(
		const TableBuilderOptions& table_builder_options,
		uint32_t column_family_id,
		WritableFileWriter* file)
const {
  auto userCmp = table_builder_options.internal_comparator.user_comparator();
	if (!IsBytewiseComparator(userCmp)) {
		THROW_STD(invalid_argument,
				"TerarkZipTableFactory::NewTableBuilder(): "
				"user comparator must be 'leveldb.BytewiseComparator'");
	}
#if 0
  if (fstring(userCmp->Name()).startsWith("rev:")) {
    THROW_STD(invalid_argument,
        "TerarkZipTableFactory::NewTableBuilder(): "
        "user comparator must be 'leveldb.BytewiseComparator'");
  }
#endif
  int curlevel = table_builder_options.level;
  int numlevel = table_builder_options.ioptions.num_levels;
  int minlevel = table_options_.terarkZipMinLevel;
  if (minlevel < 0) {
    minlevel = numlevel-1;
  }
#if 1
  INFO(table_builder_options.ioptions.info_log
      , "nth_newtable{ terark = %3zd fallback = %3zd } curlevel = %d minlevel = %d numlevel = %d fallback = %p\n"
      , nth_new_terark_table_, nth_new_fallback_table_, curlevel, minlevel, numlevel, fallback_factory_
      );
#endif
  if (0 == nth_new_terark_table_) {
    g_lastTime = g_pf.now();
  }
	if (fallback_factory_) {
    if (curlevel >= 0 && curlevel < minlevel) {
      nth_new_fallback_table_++;
      TableBuilder* tb = fallback_factory_->NewTableBuilder(table_builder_options,
          column_family_id, file);
      INFO(table_builder_options.ioptions.info_log
          , "TerarkZipTableFactory::NewTableBuilder() returns class: %s\n"
          , ClassName(*tb).c_str());
      return tb;
    }
	}
	nth_new_terark_table_++;
	return new TerarkZipTableBuilder(
        table_options_,
		    table_builder_options,
        column_family_id,
		    file);
}

std::string TerarkZipTableFactory::GetPrintableTableOptions() const {
  std::string ret;
  ret.reserve(2000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  const auto& to =  table_options_;
#undef AppendF
#define AppendF(...) ret.append(buffer, snprintf(buffer, kBufferSize, ##__VA_ARGS__))
  AppendF("indexNestLevel: %d\n", to.indexNestLevel);
  AppendF("checksumLevel : %d\n", to.checksumLevel);
  AppendF("entropyAlgo   : %d\n", to.entropyAlgo);
  AppendF("terarkZipMinLevel: %d\n", to.terarkZipMinLevel);
  AppendF("useSuffixArrayLocalMatch: %d\n", to.useSuffixArrayLocalMatch);
  AppendF("estimateCompressionRatio: %f\n", to.estimateCompressionRatio);
  AppendF("sampleRatio  : %f\n", to.sampleRatio);
  AppendF("softZipWorkingMemLimit: %8.3f GB\n", to.softZipWorkingMemLimit/1e9);
  AppendF("hardZipWorkingMemLimit: %8.3f GB\n", to.hardZipWorkingMemLimit/1e9);
  ret += "localTempDir : ";
  ret += to.localTempDir;
  ret += "\n";
  return ret;
}

Status
TerarkZipTableFactory::SanitizeOptions(const DBOptions& db_opts,
                       	   	   	   	   const ColumnFamilyOptions& cf_opts)
const {
	if (!IsBytewiseComparator(cf_opts.comparator)) {
		return Status::InvalidArgument("TerarkZipTableFactory::SanitizeOptions()",
				"user comparator must be 'leveldb.BytewiseComparator'");
	}
#if 0
	if (fstring(cf_opts.comparator->Name()).startsWith("rev:")) {
	  return Status::InvalidArgument("TerarkZipTableFactory::SanitizeOptions",
	      "Damn, fuck out the reverse bytewise comparator");
	}
#endif
  auto indexFactory = TerocksIndex::GetFactory(table_options_.indexType);
  if (!indexFactory) {
    std::string msg = "invalid indexType: " + table_options_.indexType;
    return Status::InvalidArgument(msg);
  }
	return Status::OK();
}

} /* namespace rocksdb */
