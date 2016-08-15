/*
 * terark_zip_table.cc
 *
 *  Created on: 2016Äê8ÔÂ9ÈÕ
 *      Author: leipeng
 */

#include "terark_zip_table.h"
#include <table/table_reader.h>
#include <table/meta_blocks.h>
#include <terark/stdtypes.hpp>
#include <terark/util/blob_store.hpp>
#include <terark/fast_zip_blob_store.hpp>
#include <terark/fsa/nest_louds_trie.hpp>
#include <terark/fsa/nest_trie_dawg.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/io/DataIO.hpp>
#include <random>

namespace terark { namespace fsa {

}} // namespace terark::fsa

namespace rocksdb {

using std::unique_ptr;
using std::unordered_map;
using std::vector;

using terark::NestLoudsTrie_SE_512;
using terark::NestLoudsTrieDAWG_SE_512;
using terark::DictZipBlobStore;
using terark::byte_t;
using terark::valvec;
using terark::valvec_no_init;
using terark::valvec_reserve;
using terark::fstring;
using terark::initial_state;
using terark::MemIO;
using terark::AutoGrownMemIO;
using terark::FileStream;
using terark::InputBuffer;
using terark::OutputBuffer;
using terark::LittleEndianDataInput;
using terark::LittleEndianDataOutput;
using terark::SortableStrVec;
using terark::var_uint32_t;

static const uint64_t kTerarkZipTableMagicNumber = 0x1122334455667788;

static const std::string kTerarkZipTableIndexBlock = "TerarkZipTableIndexBlock";
static const std::string kTerarkZipTableValueTypeBlock = "TerarkZipTableValueTypeBlock";
static const std::string kTerarkZipTableValueDictBlock = "TerarkZipTableValueDictBlock";

class TerarkZipTableIterator;

#if defined(IOS_CROSS_COMPILE)
  #define MY_THREAD_LOCAL(Type, Var)  Type Var
#elif defined(_WIN32)
  #define MY_THREAD_LOCAL(Type, Var)  __declspec(thread) Type Var
#else
  #define MY_THREAD_LOCAL(Type, Var)  __thread Type Var
#endif

MY_THREAD_LOCAL(terark::MatchContext, g_mctx);
MY_THREAD_LOCAL(valvec<byte_t>, g_tbuf);

enum class ZipValueType : unsigned char {
	kZeroSeq = 0,
	kDelete = 1,
	kValue = 2,
	kMulti = 3,
};
const size_t kZipValueTypeBits = 2;

struct ZipValueMultiValue {
	uint32_t num;
	uint32_t offsets[1];

	Slice getValueData(size_t nth) const {
		assert(nth < num);
		size_t offset0 = offsets[nth+0];
		size_t offset1 = offsets[nth+1];
		size_t dlength = offset1 - offset0;
		const char* base = (const char*)(offsets + num + 1);
		return Slice(base + offset0, dlength);
	}
	static size_t calcHeaderSize(size_t n) {
		return sizeof(uint32_t) * (n + 2);
	}
};

template<class ByteArray>
Slice SliceOf(const ByteArray& ba) {
	BOOST_STATIC_ASSERT(sizeof(ba[0] == 1));
	return Slice((const char*)ba.data(), ba.size());
}

struct TerarkZipTableReaderFileInfo {
  bool   is_mmap_mode;
  Slice  file_data;
  size_t data_end_offset;
  unique_ptr<RandomAccessFileReader> file;

  TerarkZipTableReaderFileInfo(unique_ptr<RandomAccessFileReader>&& _file,
                         	   const EnvOptions& storage_options,
							   uint32_t _data_size_offset)
    : is_mmap_mode(storage_options.use_mmap_reads)
    , data_end_offset(_data_size_offset)
  	, file(std::move(_file))
  {}
};

/**
 * one user key map to a record id: the index NO. of a key in NestLoudsTrie,
 * the record id is used to direct index a type enum(small integer) array,
 * the record id is also used to access the value store
 */

class TerarkZipTableReader: public TableReader {
public:
  static Status Open(const ImmutableCFOptions& ioptions,
                     const EnvOptions& env_options,
                     RandomAccessFileReader* file,
                     uint64_t file_size,
					 unique_ptr<TableReader>* table);

  InternalIterator*
  NewIterator(const ReadOptions&, Arena*, bool skip_filters) override;

  void Prepare(const Slice& target) override;

  Status Get(const ReadOptions&, const Slice& key, GetContext*,
             bool skip_filters) override;

  uint64_t ApproximateOffsetOf(const Slice& key) override;

  void SetupForCompaction() override;

  std::shared_ptr<const TableProperties>
  GetTableProperties() const override;

  size_t ApproximateMemoryUsage() const override;

  ~TerarkZipTableReader();
  TerarkZipTableReader(size_t user_key_len, const ImmutableCFOptions& ioptions);

  Status GetRecId(const Slice& userKey, size_t* pRecId) const;
  void GetValue(size_t recId, valvec<byte_t>* value) const;

private:
  unique_ptr<DictZipBlobStore> valstore_;
  unique_ptr<NestLoudsTrieDAWG_SE_512> keyIndex_;
  terark::UintVecMin0 typeArray_;
  const size_t fixed_key_len_;
  static const size_t kNumInternalBytes = 8;
  Slice  file_data_;
  unique_ptr<RandomAccessFileReader> file_;

  const ImmutableCFOptions& ioptions_;
  uint64_t file_size_ = 0;
  std::shared_ptr<const TableProperties> table_properties_;

  friend class TerarkZipTableIterator;

  Status LoadIndex(Slice mem);

  // No copying allowed
  explicit TerarkZipTableReader(const TableReader&) = delete;
  void operator=(const TableReader&) = delete;
};

class TerarkZipTableBuilder: public TableBuilder {
public:
  TerarkZipTableBuilder(
		  const TerarkZipTableOptions&,
		  const ImmutableCFOptions& ioptions,
		  const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*,
		  uint32_t column_family_id,
		  WritableFileWriter* file,
		  const std::string& column_family_name);

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~TerarkZipTableBuilder();

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override;

  // Return non-ok iff some error has been detected.
  Status status() const override;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  Status Finish() override;

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  void Abandon() override;

  // Number of calls to Add() so far.
  uint64_t NumEntries() const override;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t FileSize() const override;

  TableProperties GetTableProperties() const override { return properties_; }

private:
  Arena arena_;
  const TerarkZipTableOptions& table_options_;
  const ImmutableCFOptions& ioptions_;
  std::vector<unique_ptr<IntTblPropCollector>> table_properties_collectors_;

  unique_ptr<terark::DictZipBlobStore::ZipBuilder> zbuilder_;
  unique_ptr<terark::DictZipBlobStore> zstore_;
  valvec<byte_t> prevUserKey_;
  terark::febitvec valueBits_;
  std::string tmpValueFilePath_;
  FileStream  tmpValueFile_;
  NativeDataOutput<OutputBuffer> tmpValueWriter_;
  SortableStrVec tmpKeyVec_;
  std::mt19937_64 randomGenerator_;
  uint64_t sampleUpperBound_;

  WritableFileWriter* file_;
  uint64_t offset_ = 0;
  size_t huge_page_tlb_size_;
  Status status_;
  TableProperties properties_;

  std::vector<uint32_t> keys_or_prefixes_hashes_;
  bool closed_ = false;  // Either Finish() or Abandon() has been called.

  // No copying allowed
  TerarkZipTableBuilder(const TerarkZipTableBuilder&) = delete;
  void operator=(const TerarkZipTableBuilder&) = delete;
};

///////////////////////////////////////////////////////////////////////////////

class TerarkZipTableIterator : public InternalIterator {
public:
  explicit TerarkZipTableIterator(TerarkZipTableReader* table) {
	  table_ = table;
	  status_ = Status::InvalidArgument("TerarkZipTableIterator",
			  "Not point to a position");
	  auto dfa = table->keyIndex_.get();
	  iter_.reset(dfa->adfa_make_iter());
	  valnum_ = 0;
	  validx_ = 0;
  }
  ~TerarkZipTableIterator() {
  }

  bool Valid() const override {
	  return status_.ok();
  }

  void SeekToFirst() override {
	  if (UnzipIterRecord(iter_->seek_begin())) {
		  DecodeCurrKeyValue();
		  validx_ = 1;
	  }
  }

  void SeekToLast() override {
	  if (UnzipIterRecord(iter_->seek_end())) {
		  validx_ = valnum_ - 1;
		  DecodeCurrKeyValue();
	  }
  }

  void Seek(const Slice& target) override {
	  ParsedInternalKey pikey;
	  ParseInternalKey(target, &pikey);
	  interKey_.DecodeFrom(target);
	  fstring userKey(pikey.user_key);
	  if (UnzipIterRecord(iter_->seek_lower_bound(userKey))) {
		  DecodeCurrKeyValue();
		  validx_ = 1;
	  }
  }

  void Next() override {
	  if (validx_ < valnum_) {
		  DecodeCurrKeyValue();
		  validx_++;
	  }
	  else {
		  if (UnzipIterRecord(iter_->incr())) {
			  DecodeCurrKeyValue();
			  validx_ = 1;
		  }
	  }
  }

  void Prev() override {
	  if (validx_ > 0) {
		  validx_--;
		  DecodeCurrKeyValue();
	  }
	  else {
		  if (UnzipIterRecord(iter_->decr())) {
			  validx_ = valnum_ - 1;
			  DecodeCurrKeyValue();
		  }
	  }
  }

  Slice key() const override {
	  assert(status_.ok());
	  return interKey_.user_key();
  }

  Slice value() const override {
	  assert(status_.ok());
	  return userValue_;
  }

  Status status() const override {
	  return status_;
  }

private:
  size_t GetIterRecId() const {
	  auto dfa = table_->keyIndex_.get();
	  return dfa->state_to_word_id(iter_->word_state());
  }
  bool UnzipIterRecord(bool hasRecord) {
	  validx_ = 0;
	  if (hasRecord) {
		  size_t recId = GetIterRecId();
		  table_->GetValue(recId, &valueBuf_);
		  status_ = Status::OK();
		  auto& typeArray = table_->typeArray_;
		  if (ZipValueType(typeArray[recId]) == ZipValueType::kMulti) {
			  auto zmValue = (ZipValueMultiValue*)(valueBuf_.data());
			  valnum_ = zmValue->num;
		  } else {
			  valnum_ = 1;
		  }
		  return true;
	  }
	  else {
		  valnum_ = 0;
		  status_ = Status::NotFound();
		  return false;
	  }
  }
  bool DecodeCurrKeyValue() {
	assert(status_.ok());
	size_t recId = GetIterRecId();
	auto valstore = table_->valstore_.get();
	auto& typeArray = table_->typeArray_;
	valstore->get_record(recId, &valueBuf_);
	Slice userKey = SliceOf(iter_->word());
	switch (ZipValueType(typeArray[recId])) {
	default:
		status_ = Status::Aborted("TerarkZipTableReader::Get()",
				"Bad ZipValueType");
		return false;
	case ZipValueType::kZeroSeq:
		interKey_.Set(userKey, 0, kTypeValue);
		userValue_ = Slice(valueBuf_.data(), valueBuf_.size());
		return true;
	case ZipValueType::kValue: { // should be a kTypeValue, the normal case
		// little endian uint64_t
		uint64_t seq = *(uint64_t*)valueBuf_.data() & kMaxSequenceNumber;
		interKey_.Set(userKey, seq, kTypeValue);
		userValue_ = Slice(valueBuf_.data()+7, valueBuf_.size()-7);
		return true; }
	case ZipValueType::kDelete: {
		// little endian uint64_t
		uint64_t seq = *(uint64_t*)valueBuf_.data() & kMaxSequenceNumber;
		interKey_.Set(userKey, seq, kTypeDeletion);
		userValue_ = Slice((char*)valueBuf_.data()+7, valueBuf_.size()-7);
		return true; }
	case ZipValueType::kMulti: { // more than one value
		auto zmValue = (const ZipValueMultiValue*)(valueBuf_.data());
		assert(0 != valnum_);
		assert(validx_ < valnum_);
		assert(valnum_ == zmValue->num);
		Slice d = zmValue->getValueData(validx_);
		SequenceNumber sn;
		ValueType valtype;
		{
			auto snt = unaligned_load<SequenceNumber>(d.data());
			UnPackSequenceAndType(snt, &sn, &valtype);
		}
		interKey_.Set(userKey, sn, valtype);
		d.remove_prefix(sizeof(SequenceNumber));
		userValue_ = d;
		return true; }
	}
  }

  TerarkZipTableReader* table_;
  unique_ptr<terark::ADFA_LexIterator> iter_;
  InternalKey    interKey_;
  valvec<byte_t> valueBuf_;
  Slice  userValue_;
  size_t valnum_;
  size_t validx_;
  Status status_;
  // No copying allowed
  TerarkZipTableIterator(const PlainTableIterator&) = delete;
  void operator=(const Iterator&) = delete;
};

TerarkZipTableReader::~TerarkZipTableReader() {
	typeArray_.risk_release_ownership();
}

TerarkZipTableReader::TerarkZipTableReader(size_t user_key_len,
							const ImmutableCFOptions& ioptions)
 : fixed_key_len_(user_key_len), ioptions_(ioptions) {}

Status
TerarkZipTableReader::Open(const ImmutableCFOptions& ioptions,
						   const EnvOptions& env_options,
						   RandomAccessFileReader* file,
						   uint64_t file_size,
						   unique_ptr<TableReader>* table) {
  TableProperties* props = nullptr;
  auto s = ReadTableProperties(file, file_size,
		  	  kTerarkZipTableMagicNumber, ioptions, &props);
  if (!s.ok()) {
	return s;
  }
  Slice file_data;
  if (env_options.use_mmap_reads) {
	Status s = file->Read(0, file_size, &file_data, nullptr);
	if (!s.ok())
		return s;
  } else {
	return Status::InvalidArgument("TerarkZipTableReader::Open()",
			"EnvOptions::use_mmap_reads must be true");
  }
  unique_ptr<TerarkZipTableReader>
  r(new TerarkZipTableReader(size_t(props->fixed_key_len), ioptions));
  r->file_ = file;
  r->file_data_ = file_data;
  r->file_size_ = file_size;
  BlockContents indexBlock, zValueTypeBlock, valueDictBlock;
  Status s;
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
  r->valstore_.reset(new DictZipBlobStore());
  r->valstore_->load_user_memory(
		  fstring(valueDictBlock.data),
		  fstring(file_data.data(), props->data_size));
  s = r->LoadIndex(indexBlock.data);
  if (!s.ok()) {
	  return s;
  }
  r->typeArray_.risk_set_data((byte_t*)zValueTypeBlock.data.data(),
		  zValueTypeBlock.data.size(), kZipValueTypeBits);
  *table = r.release();
  return Status::OK();
}

Status TerarkZipTableReader::LoadIndex(Slice mem) {
  try {
	  auto trie = terark::MatchingDFA::load_mmap_range(mem.data(), mem.size());
	  keyIndex_.reset(dynamic_cast<NestLoudsTrieDAWG_SE_512*>(trie));
	  if (!keyIndex_) {
		  return Status::InvalidArgument("TerarkZipTableReader::Open()",
				  "Index class is not NestLoudsTrieDAWG_SE_512");
	  }
  }
  catch (const std::exception& ex) {
	  return Status::InvalidArgument("TerarkZipTableReader::Open()", ex.what());
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

void TerarkZipTableReader::Prepare(const Slice& target) {
	// do nothing
}

Status
TerarkZipTableReader::Get(const ReadOptions& ro, const Slice& ikey,
						  GetContext* get_context, bool skip_filters) {
	ParsedInternalKey pikey;
	ParseInternalKey(ikey, &pikey);
	size_t recId;
	{
		Status s = GetRecId(pikey.user_key, &recId);
		if (!s.ok()) {
			return s;
		}
	}
	valstore_->get_record(recId, &g_tbuf);
	switch (ZipValueType(typeArray_[recId])) {
	default:
		return Status::Aborted("TerarkZipTableReader::Get()",
				"Bad ZipValueType");
	case ZipValueType::kZeroSeq:
		get_context->SaveValue(Slice((char*)g_tbuf.data(), g_tbuf.size()), 0);
		return Status::OK();
	case ZipValueType::kValue: { // should be a kTypeValue, the normal case
		// little endian uint64_t
		uint64_t seq = *(uint64_t*)g_tbuf.data() & kMaxSequenceNumber;
		get_context->SaveValue(Slice((char*)g_tbuf.data()+7, g_tbuf.size()-7), seq);
		return Status::OK(); }
	case ZipValueType::kDelete: {
		// little endian uint64_t
		uint64_t seq = *(uint64_t*)g_tbuf.data() & kMaxSequenceNumber;
		get_context->SaveValue(
				ParsedInternalKey(pikey.user_key, seq, kTypeDeletion),
				Slice());
		return Status::OK(); }
	case ZipValueType::kMulti: { // more than one value
		auto mVal = (const ZipValueMultiValue*)g_tbuf.data();
		const size_t num = mVal->num;
		for(size_t i = 0; i < num; ++i) {
			Slice val = mVal->getValueData(i);
			SequenceNumber sn;
			ValueType valtype;
			{
				auto snt = unaligned_load<SequenceNumber>(val.data());
				UnPackSequenceAndType(snt, &sn, &valtype);
			}
			val.remove_prefix(sizeof(SequenceNumber));
			bool hasMoreValue = get_context->SaveValue(
				ParsedInternalKey(pikey.user_key, sn, valtype), val);
			if (!hasMoreValue) {
				assert(i+1 == num);
				break;
			}
		}
		return Status::OK(); }
	}
}

uint64_t TerarkZipTableReader::ApproximateOffsetOf(const Slice& key) {
	return 0;
}

void TerarkZipTableReader::SetupForCompaction() {
}

std::shared_ptr<const TableProperties>
TerarkZipTableReader::GetTableProperties() const {
  return table_properties_;
}

size_t TerarkZipTableReader::ApproximateMemoryUsage() const {
  return file_size_;
}

Status
TerarkZipTableReader::GetRecId(const Slice& userKey, size_t* pRecId) const {
	auto dfa = keyIndex_.get();
	const size_t  kn = userKey.size();
	const byte_t* kp = (const byte_t*)userKey.data();
	size_t state = initial_state;
	for (size_t pos = 0; pos < kn; ++pos) {
		if (dfa->is_pzip(state)) {
			fstring zs = dfa->get_zpath_data(state, &g_mctx);
			if (kn - pos < zs.size()) {
				return Status::NotFound("TerarkZipTableReader::Get()",
						"zpath is longer than remaining key");
			}
			for (size_t j = 0; j < zs.size(); ++j, ++pos) {
				if (zs[j] != kp[pos]) {
					return Status::NotFound("TerarkZipTableReader::Get()",
							"zpath match fail");
				}
			}
		}
		byte_t c = kp[pos];
		size_t next = dfa->state_move(state, c);
		if (dfa->nil_state == next) {
			return Status::NotFound("TerarkZipTableReader::Get()",
					"reached nil_state");
		}
		assert(next < dfa->total_states());
		state = next;
	}
	if (!dfa->is_term(state)) {
		return Status::NotFound("TerarkZipTableReader::Get()",
				"input key is a prefix but is not a dfa key");
	}
	*pRecId = dfa->state_to_word_id(state);
	return Status::OK();
}

void
TerarkZipTableReader::GetValue(size_t recId, valvec<byte_t>* value) const {
	assert(recId < keyIndex_->num_words());
	valstore_->get_record(recId, value);
}

///////////////////////////////////////////////////////////////////////////////

TerarkZipTableBuilder::TerarkZipTableBuilder(
		const TerarkZipTableOptions& table_options,
		const ImmutableCFOptions& ioptions,
		const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*,
		uint32_t column_family_id,
		WritableFileWriter* file,
		const std::string& column_family_name)
  : table_options_(table_options)
  , ioptions_(ioptions)
{
  huge_page_tlb_size_ = 0;
  file_ = nullptr;
  status_ = Status::OK();
  zstore_.reset(new DictZipBlobStore());
  zbuilder_ = zstore_->createZipBuilder();
  sampleUpperBound_ = randomGenerator_.max() * table_options_.sampleRatio;
}

TerarkZipTableBuilder::~TerarkZipTableBuilder() {
}

void TerarkZipTableBuilder::Add(const Slice& key, const Slice& value) {
	assert(key.size() >= 8);
	fstring userKey(key.data(), key.size()-8);
	valueBits_.push_back(true);
	if (prevUserKey_ != userKey) {
		assert(prevUserKey_ < userKey);
		if (table_options_.fixed_key_len) {
			tmpKeyVec_.m_strpool.append(userKey);
		} else {
			tmpKeyVec_.push_back(userKey);
		}
		prevUserKey_.assign(userKey);
		valueBits_.push_back(false);
	}
	if (randomGenerator_() < sampleUpperBound_) {
		zbuilder_->addSample(fstring(value));
	}
	tmpValueWriter_.ensureWrite(userKey.end(), 8);
	tmpValueWriter_ << fstring(value);
	properties_.num_entries++;
}

Status TerarkZipTableBuilder::status() const {
	return status_;
}

template<class ByteArray>
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

Status TerarkZipTableBuilder::Finish() {
	assert(0 == table_options_.fixed_key_len);
	assert(!closed_);
	closed_ = true;

	// the guard, if last same key seq is longer than 1, this is required
	valueBits_.push_back(false);
	tmpValueWriter_.flush();
	tmpValueFile_.rewind();
	unique_ptr<NestLoudsTrieDAWG_SE_512> dawg(new NestLoudsTrieDAWG_SE_512());
	terark::NestLoudsTrieConfig conf;
	conf.nestLevel = table_options_.indexNestLevel;
	dawg->build_from(tmpKeyVec_, conf);
	tmpKeyVec_.clear();
	zbuilder_->prepare(properties_.num_entries, tmpValueFilePath_ + ".zbs");
	NativeDataInput<InputBuffer> input(&tmpValueWriter_);
	valvec<byte_t> typeArray(properties_.num_entries, valvec_reserve());
	valvec<byte_t> value;
	valvec<byte_t> mValue;
	size_t bitPos = 0;
	size_t numEntries = size_t(properties_.num_entries);
	for (; numEntries > 0; numEntries--) {
		uint64_t seqType = input.load_as<uint64_t>();
		uint64_t seqNum;
		ValueType vType;
		UnPackSequenceAndType(seqType, &seqNum, &vType);
		input >> value;
		size_t oneSeqLen = valueBits_.one_seq_len(bitPos);
		assert(oneSeqLen >= 1);
		if (1==oneSeqLen && (kTypeDeletion==vType || kTypeValue==vType)) {
			if (0 == seqNum && kTypeValue==vType) {
				typeArray.push_back(byte_t(ZipValueType::kZeroSeq));
			} else {
				if (kTypeValue==vType) {
					typeArray.push_back(byte_t(ZipValueType::kValue));
				} else {
					typeArray.push_back(byte_t(ZipValueType::kDelete));
				}
				value.insert(0, (byte_t*)seqNum, 7);
			}
			zbuilder_->addRecord(value);
		}
		else {
			size_t headerSize = ZipValueMultiValue::calcHeaderSize(oneSeqLen);
			mValue.erase_all();
			mValue.resize(headerSize);
			((ZipValueMultiValue*)mValue.data())->num = oneSeqLen;
			((ZipValueMultiValue*)mValue.data())->offsets[0] = 0;
			for (size_t j = 0; j < oneSeqLen; j++) {
				if (j > 0) {
					seqType = input.load_as<uint64_t>();
					input >> value;
				}
				mValue.append((byte_t*)&seqType, 8);
				mValue.append(value);
				((ZipValueMultiValue*)mValue.data())->offsets[j+1] = mValue.size() - headerSize;
			}
			zbuilder_->addRecord(mValue);
		}
		bitPos += oneSeqLen + 1;
	}
	zstore_->completeBuild(*zbuilder_);
	zbuilder_.reset();
	terark::UintVecMin0 zvType;
	zvType.build_from(typeArray);

	BlockHandle dataBlock, dictBlock, indexBlock, zvTypeBlock;
	offset_ = 0;
	Status s = WriteBlock(zstore_->get_data(), file_, &offset_, &dataBlock);
	if (!s.ok()) {
		return s;
	}
	s = WriteBlock(zstore_->get_dict(), file_, &offset_, &dictBlock);
	if (!s.ok()) {
		return s;
	}
	s = WriteBlock(dawg->get_mmap(), file_, &offset_, &indexBlock);
	if (!s.ok()) {
		return s;
	}
	fstring zvTypeMem(zvType.data(), zvType.mem_size());
	s = WriteBlock(zvTypeMem, file_, &offset_, &zvTypeBlock);
	if (!s.ok()) {
		return s;
	}
	MetaIndexBuilder metaindexBuiler;
	metaindexBuiler.Add(kTerarkZipTableValueDictBlock, dictBlock);
	metaindexBuiler.Add(kTerarkZipTableIndexBlock, indexBlock);
	metaindexBuiler.Add(kTerarkZipTableValueTypeBlock, zvTypeBlock);
	PropertyBlockBuilder propBlockBuilder;
	propBlockBuilder.AddTableProperty(properties_);
	propBlockBuilder.Add(properties_.user_collected_properties);
	NotifyCollectTableCollectorsOnFinish(table_properties_collectors_,
	                                     ioptions_.info_log,
	                                     &propBlockBuilder);
	BlockHandle propBlock, metaindexBlock;
	s = WriteBlock(propBlockBuilder.Finish(), file_, &offset_, &propBlock);
	if (!s.ok()) {
		return s;
	}
	metaindexBuiler.Add(kPropertiesBlock, propBlock);
	s = WriteBlock(metaindexBuiler.Finish(), file_, &offset_, &metaindexBlock);
	if (!s.ok()) {
		return s;
	}
	Footer footer(kLegacyPlainTableMagicNumber, 0);
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

void TerarkZipTableBuilder::Abandon() {
	closed_ = true;
}

uint64_t TerarkZipTableBuilder::NumEntries() const {
	return properties_.num_entries;
}

uint64_t TerarkZipTableBuilder::FileSize() const {
	return offset_;
}

/////////////////////////////////////////////////////////////////////////////

TerarkZipTableFactory::~TerarkZipTableFactory() {
}

TerarkZipTableFactory::TerarkZipTableFactory(const TerarkZipTableOptions& tzto)
  : table_options_(tzto)
{
}

const char*
TerarkZipTableFactory::Name() const { return "TerarkZipTable"; }

Status
TerarkZipTableFactory::NewTableReader(
		const TableReaderOptions& table_reader_options,
		unique_ptr<RandomAccessFileReader>&& file,
		uint64_t file_size, unique_ptr<TableReader>* table)
const {
	auto& icmp = table_reader_options.internal_comparator;
	auto cmpName = icmp.user_comparator()->Name();
	if (strcmp(cmpName, "leveldb.BytewiseComparator") == 0) {
		return Status::InvalidArgument("TerarkZipTableFactory::NewTableReader()",
				"user comparator must be 'leveldb.BytewiseComparator'");
	}
	return TerarkZipTableReader::Open(
			table_reader_options.ioptions,
			table_reader_options.env_options,
			file.release(),
			file_size,
			table);
}

TableBuilder*
TerarkZipTableFactory::NewTableBuilder(
		const TableBuilderOptions& table_builder_options,
		uint32_t column_family_id,
		WritableFileWriter* file)
const {
	return new TerarkZipTableBuilder(
			table_options_,
		    table_builder_options.ioptions,
		    table_builder_options.int_tbl_prop_collector_factories,
			column_family_id,
		    file,
		    table_builder_options.column_family_name);
}

std::string
TerarkZipTableFactory::GetPrintableTableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  snprintf(buffer, kBufferSize, "  fixed_key_len: %u\n",
		   table_options_.fixed_key_len);
  ret.append(buffer);
  return ret;
}

const TerarkZipTableOptions&
TerarkZipTableFactory::table_options() const {
	return table_options_;
}

// Sanitizes the specified DB Options.
Status
TerarkZipTableFactory::SanitizeOptions(const DBOptions& db_opts,
                       	   	   	   	   const ColumnFamilyOptions& cf_opts)
const {
	return Status::OK();
}

void*
TerarkZipTableFactory::GetOptions() { return &table_options_; }


} /* namespace rocksdb */
