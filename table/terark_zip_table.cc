/*
 * terark_zip_table.cc
 *
 *  Created on: 2016-08-09
 *      Author: leipeng
 */

#include "terark_zip_table.h"
#include <rocksdb/comparator.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <table/get_context.h>
#include <table/internal_iterator.h>
#include <table/table_builder.h>
#include <table/table_reader.h>
#include <table/meta_blocks.h>
#include <db/compaction_iterator.h>
#include <terark/stdtypes.hpp>
#include <terark/util/crc.hpp>
#include <terark/util/throw.hpp>
#include <terark/fast_zip_blob_store.hpp>
#include <terark/fsa/nest_trie_dawg.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/io/DataIO.hpp>
#include <boost/scope_exit.hpp>
#include <memory>
#include <random>
#include <stdlib.h>
#include <stdint.h>

namespace rocksdb {

using std::unique_ptr;
using std::unordered_map;
using std::vector;

using terark::BaseDFA;
using terark::NestLoudsTrieDAWG_SE_512;
using terark::DictZipBlobStore;
using terark::byte_t;
using terark::valvec;
using terark::valvec_no_init;
using terark::valvec_reserve;
using terark::fstring;
using terark::initial_state;
using terark::FileStream;
using terark::InputBuffer;
using terark::OutputBuffer;
using terark::LittleEndianDataInput;
using terark::LittleEndianDataOutput;
using terark::SortableStrVec;
using terark::UintVecMin0;
using terark::BadCrc32cException;

static const uint64_t kTerarkZipTableMagicNumber = 0x1122334455667788;

static const std::string kTerarkZipTableIndexBlock = "TerarkZipTableIndexBlock";
static const std::string kTerarkZipTableValueTypeBlock = "TerarkZipTableValueTypeBlock";
static const std::string kTerarkZipTableValueDictBlock = "TerarkZipTableValueDictBlock";

class TerarkZipTableIterator;

#if defined(IOS_CROSS_COMPILE)
  #define MY_THREAD_LOCAL(Type, Var)  Type Var
#elif defined(_WIN32)
  #define MY_THREAD_LOCAL(Type, Var)  static __declspec(thread) Type Var
#else
  #define MY_THREAD_LOCAL(Type, Var)  static thread_local Type Var
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

inline static fstring fstringOf(const Slice& x) {
	return fstring(x.data(), x.size());
}

/**
 * one user key map to a record id: the index NO. of a key in NestLoudsTrie,
 * the record id is used to direct index a type enum(small integer) array,
 * the record id is also used to access the value store
 */
class TerarkZipTableReader: public TableReader, boost::noncopyable {
public:
  static Status Open(const ImmutableCFOptions& ioptions,
                     const EnvOptions& env_options,
                     RandomAccessFileReader* file,
                     uint64_t file_size,
					 unique_ptr<TableReader>* table);

  InternalIterator*
  NewIterator(const ReadOptions&, Arena*, bool skip_filters) override;

  void Prepare(const Slice& target) override {}

  Status Get(const ReadOptions&, const Slice& key, GetContext*,
             bool skip_filters) override;

  uint64_t ApproximateOffsetOf(const Slice& key) override { return 0; }
  void SetupForCompaction() override {}

  std::shared_ptr<const TableProperties>
  GetTableProperties() const override { return table_properties_; }

  size_t ApproximateMemoryUsage() const override { return file_size_; }

  ~TerarkZipTableReader();
  TerarkZipTableReader(size_t user_key_len, const ImmutableCFOptions& ioptions);

  size_t GetRecId(const Slice& userKey) const;

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
};

class TerarkZipTableBuilder: public TableBuilder, boost::noncopyable {
public:
  TerarkZipTableBuilder(
		  const TerarkZipTableOptions&,
		  const ImmutableCFOptions& ioptions,
		  const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*,
		  uint32_t column_family_id,
		  WritableFileWriter* file,
		  const std::string& column_family_name);

  ~TerarkZipTableBuilder();

  void Add(const Slice& key, const Slice& value) override;
  Status status() const override { return status_; }
  Status Finish() override;
  void Abandon() override { closed_ = true; }
  uint64_t NumEntries() const override { return properties_.num_entries; }
  uint64_t FileSize() const override;
  TableProperties GetTableProperties() const override { return properties_; }
  void SetCompactionIterator(CompactionIterator* c_iter) override {
    c_iter_ = c_iter;
  }

private:
  void AddPrevUserKey();

  Arena arena_;
  const TerarkZipTableOptions& table_options_;
  const ImmutableCFOptions& ioptions_;
  std::vector<unique_ptr<IntTblPropCollector>> table_properties_collectors_;

  unique_ptr<DictZipBlobStore::ZipBuilder> zbuilder_;
  CompactionIterator* c_iter_ = nullptr;
  valvec<byte_t> prevUserKey_;
  terark::febitvec valueBits_;
  std::string tmpValueFilePath_;
  FileStream  tmpValueFile_;
  NativeDataOutput<OutputBuffer> tmpValueWriter_;
  SortableStrVec tmpKeyVec_;
  std::mt19937_64 randomGenerator_;
  uint64_t sampleUpperBound_;
  size_t numUserKeys_ = size_t(-1);
  size_t sampleLenSum_ = 0;
  WritableFileWriter* file_;
  uint64_t offset_ = 0;
  Status status_;
  TableProperties properties_;
  bool closed_ = false;  // Either Finish() or Abandon() has been called.
};

///////////////////////////////////////////////////////////////////////////////

class TerarkZipTableIterator : public InternalIterator, boost::noncopyable {
public:
  explicit TerarkZipTableIterator(TerarkZipTableReader* table) {
	  table_ = table;
	  auto dfa = table->keyIndex_.get();
	  iter_.reset(dfa->adfa_make_iter());
	  zValtype_ = ZipValueType::kZeroSeq;
	  SetIterInvalid();
  }

  bool Valid() const override {
	  return size_t(-1) != recId_;
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
	  if (!ParseInternalKey(target, &pikey)) {
		  status_ = Status::InvalidArgument("TerarkZipTableIterator::Seek()",
				  "param target.size() < 8");
		  SetIterInvalid();
		  return;
	  }
	  if (UnzipIterRecord(iter_->seek_lower_bound(fstringOf(pikey.user_key)))) {
		  do {
			  DecodeCurrKeyValue();
			  validx_++;
			  if (pInterKey_.sequence <= pikey.sequence) {
				  return; // done
			  }
		  } while (validx_ < valnum_);
		  // no visible version/sequence for target, use Next();
		  // if using Next(), version check is not needed
		  Next();
	  }
  }

  void Next() override {
	  assert(size_t(-1) != recId_);
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
	  assert(size_t(-1) != recId_);
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
	  assert(size_t(-1) != recId_);
	  return interKeyBuf_;
  }

  Slice value() const override {
	  assert(size_t(-1) != recId_);
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
  void SetIterInvalid() {
	  recId_ = size_t(-1);
	  validx_ = 0;
	  valnum_ = 0;
	  pInterKey_.user_key = Slice();
	  pInterKey_.sequence = uint64_t(-1);
	  pInterKey_.type = kMaxValue;
  }
  bool UnzipIterRecord(bool hasRecord) {
	  if (hasRecord) {
		  size_t recId = GetIterRecId();
		  try {
			  table_->valstore_->get_record(recId, &valueBuf_);
		  }
		  catch (const BadCrc32cException& ex) { // crc checksum error
			  SetIterInvalid();
			  status_ = Status::Corruption(
				"TerarkZipTableIterator::UnzipIterRecord()", ex.what());
			  return false;
		  }
		  zValtype_ = ZipValueType(table_->typeArray_[recId]);
		  if (ZipValueType::kMulti == zValtype_) {
			  auto zmValue = (ZipValueMultiValue*)(valueBuf_.data());
			  assert(zmValue->num > 0);
			  valnum_ = zmValue->num;
		  } else {
			  valnum_ = 1;
		  }
		  validx_ = 0;
		  recId_ = recId;
		  pInterKey_.user_key = SliceOf(iter_->word());
		  return true;
	  }
	  else {
		  SetIterInvalid();
		  return false;
	  }
  }
  void DecodeCurrKeyValue() {
	assert(status_.ok());
	assert(recId_ < table_->keyIndex_->num_words());
	switch (zValtype_) {
	default:
		status_ = Status::Aborted("TerarkZipTableReader::Get()",
				"Bad ZipValueType");
		abort(); // must not goes here, if it does, it should be a bug!!
		break;
	case ZipValueType::kZeroSeq:
		pInterKey_.sequence = 0;
		pInterKey_.type = kTypeValue;
		userValue_ = SliceOf(valueBuf_);
		break;
	case ZipValueType::kValue: // should be a kTypeValue, the normal case
		// little endian uint64_t
		pInterKey_.sequence = *(uint64_t*)valueBuf_.data() & kMaxSequenceNumber;
		pInterKey_.type = kTypeValue;
		userValue_ = SliceOf(fstring(valueBuf_).substr(7));
		break;
	case ZipValueType::kDelete:
		// little endian uint64_t
		pInterKey_.sequence = *(uint64_t*)valueBuf_.data() & kMaxSequenceNumber;
		pInterKey_.type = kTypeDeletion;
		userValue_ = Slice();
		break;
	case ZipValueType::kMulti: { // more than one value
		auto zmValue = (const ZipValueMultiValue*)(valueBuf_.data());
		assert(0 != valnum_);
		assert(validx_ < valnum_);
		assert(valnum_ == zmValue->num);
		Slice d = zmValue->getValueData(validx_);
		auto snt = unaligned_load<SequenceNumber>(d.data());
		UnPackSequenceAndType(snt, &pInterKey_.sequence, &pInterKey_.type);
		d.remove_prefix(sizeof(SequenceNumber));
		userValue_ = d;
		break; }
	}
	interKeyBuf_.resize(0);
	AppendInternalKey(&interKeyBuf_, pInterKey_);
  }

  TerarkZipTableReader* table_;
  unique_ptr<terark::ADFA_LexIterator> iter_;
  ParsedInternalKey pInterKey_;
  std::string interKeyBuf_;
  valvec<byte_t> valueBuf_;
  Slice  userValue_;
  ZipValueType zValtype_;
  size_t recId_; // save as member to reduce a rank1(state)
  size_t valnum_;
  size_t validx_;
  Status status_;
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
  Status s = ReadTableProperties(file, file_size,
		  	  kTerarkZipTableMagicNumber, ioptions, &props);
  if (!s.ok()) {
	return s;
  }
  assert(nullptr != props);
  unique_ptr<TableProperties> uniqueProps(props);
  Slice file_data;
  if (env_options.use_mmap_reads) {
	s = file->Read(0, file_size, &file_data, nullptr);
	if (!s.ok())
		return s;
  } else {
	return Status::InvalidArgument("TerarkZipTableReader::Open()",
			"EnvOptions::use_mmap_reads must be true");
  }
  unique_ptr<TerarkZipTableReader>
  r(new TerarkZipTableReader(size_t(props->fixed_key_len), ioptions));
  r->file_.reset(file);
  r->file_data_ = file_data;
  r->file_size_ = file_size;
  r->table_properties_.reset(uniqueProps.release());
  BlockContents valueDictBlock, indexBlock, zValueTypeBlock;
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
  try {
	  r->valstore_.reset(new DictZipBlobStore());
	  r->valstore_->load_user_memory(
			  fstringOf(valueDictBlock.data),
			  fstring(file_data.data(), props->data_size));
  }
  catch (const BadCrc32cException& ex) {
	  return Status::Corruption("TerarkZipTableReader::Open()", ex.what());
  }
  s = r->LoadIndex(indexBlock.data);
  if (!s.ok()) {
	  return s;
  }
  size_t recNum = r->keyIndex_->num_words();
  r->typeArray_.risk_set_data((byte_t*)zValueTypeBlock.data.data(),
		  recNum, kZipValueTypeBits);
  *table = std::move(r);
  return Status::OK();
}

Status TerarkZipTableReader::LoadIndex(Slice mem) {
  auto func = "TerarkZipTableReader::LoadIndex()";
  try {
	  auto trie = BaseDFA::load_mmap_user_mem(mem.data(), mem.size());
	  keyIndex_.reset(dynamic_cast<NestLoudsTrieDAWG_SE_512*>(trie));
	  if (!keyIndex_) {
		  return Status::InvalidArgument("TerarkZipTableReader::LoadIndex()",
				  "Index class is not NestLoudsTrieDAWG_SE_512");
	  }
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

Status
TerarkZipTableReader::Get(const ReadOptions& ro, const Slice& ikey,
						  GetContext* get_context, bool skip_filters) {
	ParsedInternalKey pikey;
	ParseInternalKey(ikey, &pikey);
	size_t recId = GetRecId(pikey.user_key);
	if (size_t(-1) == recId) {
		return Status::OK();
	}
	try {
		valstore_->get_record(recId, &g_tbuf);
	}
	catch (const BadCrc32cException& ex) { // crc checksum error
		return Status::Corruption("TerarkZipTableReader::Get()", ex.what());
	}
	switch (ZipValueType(typeArray_[recId])) {
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
		auto mVal = (const ZipValueMultiValue*)g_tbuf.data();
		for(size_t i = 0; i < size_t(mVal->num); ++i) {
			Slice val = mVal->getValueData(i);
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
	return Status::OK();
}

size_t TerarkZipTableReader::GetRecId(const Slice& userKey) const {
	auto dfa = keyIndex_.get();
	const size_t  kn = userKey.size();
	const byte_t* kp = (const byte_t*)userKey.data();
	size_t state = initial_state;
	g_mctx.zbuf_state = size_t(-1);
	for (size_t pos = 0; pos < kn; ++pos) {
		if (dfa->is_pzip(state)) {
			fstring zs = dfa->get_zpath_data(state, &g_mctx);
			if (kn - pos < zs.size()) {
				return size_t(-1);
			}
			for (size_t j = 0; j < zs.size(); ++j, ++pos) {
				if (zs[j] != kp[pos]) {
					return size_t(-1);
				}
			}
			if (pos == kn)
				break;
		}
		byte_t c = kp[pos];
		size_t next = dfa->state_move(state, c);
		if (dfa->nil_state == next) {
			return size_t(-1);
		}
		assert(next < dfa->total_states());
		state = next;
	}
	if (dfa->is_term(state)) {
		return dfa->state_to_word_id(state);
	}
	return size_t(-1);
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
  file_ = file;
  zbuilder_.reset(DictZipBlobStore::createZipBuilder(table_options.checksumLevel));
  sampleUpperBound_ = randomGenerator_.max() * table_options_.sampleRatio;
  tmpValueFilePath_ = table_options.localTempDir;
  tmpValueFilePath_.append("/TerarkRocks-XXXXXX");
  int fd = mkstemp(&tmpValueFilePath_[0]);
  if (fd < 0) {
	int err = errno;
	THROW_STD(invalid_argument
	  , "ERROR: TerarkZipTableBuilder::TerarkZipTableBuilder(): mkstemp(%s) = %s\n"
	  , tmpValueFilePath_.c_str(), strerror(err));
  }
  tmpValueFile_.dopen(fd, "rb+");
  tmpValueWriter_.attach(&tmpValueFile_);

  properties_.fixed_key_len = table_options.fixed_key_len;
  properties_.num_data_blocks = 1;
  properties_.column_family_id = column_family_id;
  properties_.column_family_name = column_family_name;
}

TerarkZipTableBuilder::~TerarkZipTableBuilder() {
}

uint64_t TerarkZipTableBuilder::FileSize() const {
	if (0 == offset_) {
		// for compaction caller to split file by increasing size
		auto kvLen = properties_.raw_key_size +  properties_.raw_value_size;
		return uint64_t(kvLen * table_options_.estimateCompressionRatio);
	} else {
		return offset_;
	}
}

void TerarkZipTableBuilder::Add(const Slice& key, const Slice& value) {
	assert(key.size() >= 8);
	fstring userKey(key.data(), key.size()-8);
	if (terark_likely(size_t(-1) != numUserKeys_)) {
		if (prevUserKey_ != userKey) {
			assert(prevUserKey_ < userKey);
			AddPrevUserKey();
			prevUserKey_.assign(userKey);
		}
	}
	else {
		prevUserKey_.assign(userKey);
		numUserKeys_ = 0;
	}
	valueBits_.push_back(true);
	if (!value.empty() && randomGenerator_() < sampleUpperBound_) {
		zbuilder_->addSample(fstringOf(value));
		sampleLenSum_ += value.size();
	}
	if (!c_iter_) {
	  tmpValueWriter_.ensureWrite(userKey.end(), 8);
	  tmpValueWriter_ << fstringOf(value);
	}
	properties_.num_entries++;
	properties_.raw_key_size += key.size();
	properties_.raw_value_size += value.size();
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

Status TerarkZipTableBuilder::Finish() {
	assert(0 == table_options_.fixed_key_len);
	assert(!closed_);
	closed_ = true;

#if !defined(NDEBUG)
	for (size_t i = 1; i < tmpKeyVec_.size(); ++i) {
		fstring prev = tmpKeyVec_[i-1];
		fstring curr = tmpKeyVec_[i];
		assert(prev < curr);
	}
	SortableStrVec backupKeys = tmpKeyVec_;
#endif

	if (0 == sampleLenSum_) { // prevent from empty
		zbuilder_->addSample("Hello World!");
	}
	AddPrevUserKey();

	if (!c_iter_) {
	  tmpValueWriter_.flush();
	  tmpValueFile_.rewind();
	}
	std::string tmpIndexFile = tmpValueFilePath_ + ".index";
  std::string tmpStoreFile = tmpValueFilePath_ + ".zbs";
  std::string tmpStoreDict = tmpValueFilePath_ + ".zbs-dict";
	terark::NestLoudsTrieConfig conf;
	conf.nestLevel = table_options_.indexNestLevel;
	unique_ptr<NestLoudsTrieDAWG_SE_512> dawg(new NestLoudsTrieDAWG_SE_512());
	dawg->build_from(tmpKeyVec_, conf);
	assert(dawg->num_words() == numUserKeys_);
	tmpKeyVec_.clear();
	dawg->save_mmap(tmpIndexFile);
	dawg.reset(); // free memory
#if 0
	BOOST_SCOPE_EXIT(&tmpIndexFile, &tmpStoreFile, &tmpValueFilePath_){
    ::remove(tmpIndexFile.c_str());
    ::remove(tmpStoreFile.c_str());
    ::remove(tmpStoreDict.c_str());
    ::remove(tmpValueFilePath_.c_str());
	}BOOST_SCOPE_EXIT_END;
#endif
	unique_ptr<DictZipBlobStore> zstore;
	UintVecMin0 zvType(properties_.num_entries, kZipValueTypeBits);
{
  static std::mutex zipMutex;
  std::unique_lock<std::mutex> zipLock(zipMutex);
  zbuilder_->prepare(properties_.num_entries, tmpStoreFile);

	if (nullptr == c_iter_)
{
	NativeDataInput<InputBuffer> input(&tmpValueFile_);
	valvec<byte_t> value;
	size_t entryId = 0;
	size_t bitPos = 0;
	for (size_t recId = 0; recId < numUserKeys_; recId++) {
		uint64_t seqType = input.load_as<uint64_t>();
		uint64_t seqNum;
		ValueType vType;
		UnPackSequenceAndType(seqType, &seqNum, &vType);
		size_t oneSeqLen = valueBits_.one_seq_len(bitPos);
		assert(oneSeqLen >= 1);
		if (1==oneSeqLen && (kTypeDeletion==vType || kTypeValue==vType)) {
			if (0 == seqNum && kTypeValue==vType) {
				zvType.set_wire(recId, size_t(ZipValueType::kZeroSeq));
				input >> value;
			} else {
				if (kTypeValue==vType) {
					zvType.set_wire(recId, size_t(ZipValueType::kValue));
				} else {
					zvType.set_wire(recId, size_t(ZipValueType::kDelete));
				}
				value.erase_all();
				value.append((byte_t*)&seqNum, 7);
				input.load_add(value);
			}
		}
		else {
			zvType.set_wire(recId, size_t(ZipValueType::kMulti));
			size_t headerSize = ZipValueMultiValue::calcHeaderSize(oneSeqLen);
			value.resize(headerSize);
			((ZipValueMultiValue*)value.data())->num = oneSeqLen;
			((ZipValueMultiValue*)value.data())->offsets[0] = 0;
			for (size_t j = 0; j < oneSeqLen; j++) {
				if (j > 0) {
					seqType = input.load_as<uint64_t>();
				}
				value.append((byte_t*)&seqType, 8);
				input.load_add(value);
				((ZipValueMultiValue*)value.data())->offsets[j+1] = value.size() - headerSize;
			}
		}
		zbuilder_->addRecord(value);
		bitPos += oneSeqLen + 1;
		entryId += oneSeqLen;
	}
  assert(entryId == properties_.num_entries);
}
	else
{
  c_iter_->Rewind();
  valvec<byte_t> value;
  size_t entryId = 0;
  size_t bitPos = 0;
  ParsedInternalKey pikey;
  for (size_t recId = 0; recId < numUserKeys_; recId++) {
    value.erase_all();
    assert(c_iter_->Valid());
    ParseInternalKey(c_iter_->key(), &pikey);
    size_t oneSeqLen = valueBits_.one_seq_len(bitPos);
    assert(oneSeqLen >= 1);
    assert(fstringOf(pikey.user_key) == backupKeys[recId]);
    if (1==oneSeqLen && (kTypeDeletion==pikey.type || kTypeValue==pikey.type)) {
      if (0 == pikey.sequence && kTypeValue==pikey.type) {
        zvType.set_wire(recId, size_t(ZipValueType::kZeroSeq));
        zbuilder_->addRecord(fstringOf(c_iter_->value()));
      } else {
        if (kTypeValue==pikey.type) {
          zvType.set_wire(recId, size_t(ZipValueType::kValue));
        } else {
          zvType.set_wire(recId, size_t(ZipValueType::kDelete));
        }
        value.append((byte_t*)&pikey.sequence, 7);
        value.append(fstringOf(c_iter_->value()));
        zbuilder_->addRecord(value);
      }
      c_iter_->Next();
    }
    else {
      zvType.set_wire(recId, size_t(ZipValueType::kMulti));
      size_t headerSize = ZipValueMultiValue::calcHeaderSize(oneSeqLen);
      value.resize(headerSize);
      ((ZipValueMultiValue*)value.data())->num = oneSeqLen;
      ((ZipValueMultiValue*)value.data())->offsets[0] = 0;
      for (size_t j = 0; j < oneSeqLen; j++) {
        assert(c_iter_->Valid());
        uint64_t seqType = PackSequenceAndType(pikey.sequence, pikey.type);
        value.append((byte_t*)&seqType, 8);
        value.append(fstringOf(c_iter_->value()));
        ((ZipValueMultiValue*)value.data())->offsets[j+1] = value.size() - headerSize;
        c_iter_->Next();
      }
      zbuilder_->addRecord(value);
    }
    bitPos += oneSeqLen + 1;
    entryId += oneSeqLen;
  }
  assert(entryId == properties_.num_entries);
}

  tmpValueFile_.close();
  ::remove(this->tmpValueFilePath_.c_str());
  zstore.reset(zbuilder_->finish());
  zbuilder_.reset();
}

	try{auto trie = BaseDFA::load_mmap(tmpIndexFile);
		dawg.reset(dynamic_cast<NestLoudsTrieDAWG_SE_512*>(trie));
	} catch (const std::exception&) {}
	if (!dawg) {
		return Status::InvalidArgument("TerarkZipTableBuilder::Finish()",
				"index temp file is broken");
	}
	Status s;
  BlockHandle dataBlock, dictBlock, indexBlock, zvTypeBlock;
  offset_ = 0;
	{
		UintVecMin0 zvType2(numUserKeys_, kZipValueTypeBits);
		terark::AutoFree<uint32_t> newToOld(dawg->num_words(), UINT32_MAX);
		terark::NonRecursiveDictionaryOrderToStateMapGenerator gen;
		gen(*dawg, [&](size_t dictOrderOldId, size_t state) {
			size_t newId = dawg->state_to_word_id(state);
			newToOld[newId] = uint32_t(dictOrderOldId);
			zvType2.set_wire(newId, zvType[dictOrderOldId]);
		});
		try {
		  dataBlock.set_offset(offset_);
		  zstore->reorder_zip_data(newToOld, [&](const void* data, size_t size) {
		    s = file_->Append(Slice((const char*)data, size));
		    if (!s.ok()) {
		      throw s;
		    }
		    offset_ += size;
		  });
		  dataBlock.set_size(offset_ - dataBlock.offset());
		} catch (const Status&) {
		  return s;
		}
		zvType.clear();
		zvType.swap(zvType2);
	}
	properties_.data_size = dataBlock.size();
	s = WriteBlock(zstore->get_dict(), file_, &offset_, &dictBlock);
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
	dawg.reset();
	zstore.reset();
  ::remove(tmpStoreFile.c_str());
  ::remove(tmpStoreDict.c_str());
  ::remove(tmpIndexFile.c_str());
	properties_.index_size = indexBlock.size();
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

void TerarkZipTableBuilder::AddPrevUserKey() {
	if (table_options_.fixed_key_len) {
		tmpKeyVec_.m_strpool.append(prevUserKey_);
	} else {
		tmpKeyVec_.push_back(prevUserKey_);
	}
	valueBits_.push_back(false);
	numUserKeys_++;
}

/////////////////////////////////////////////////////////////////////////////

class TerarkZipTableFactory : public TableFactory, boost::noncopyable {
 public:
  explicit
  TerarkZipTableFactory(const TerarkZipTableOptions& tzto, TableFactory* fallback)
  : table_options_(tzto), fallback_factory_(fallback) {}

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
};

class TableFactory*
NewTerarkZipTableFactory(const TerarkZipTableOptions& tzto,
						 class TableFactory* fallback) {
	return new TerarkZipTableFactory(tzto, fallback);
}

inline static
bool IsBytewiseComparator(const Comparator* cmp) {
#if 1
	return fstring(cmp->Name()) == "leveldb.BytewiseComparator";
#else
	return BytewiseComparator() == cmp;
#endif
}
inline static
bool IsBytewiseComparator(const InternalKeyComparator& icmp) {
	return IsBytewiseComparator(icmp.user_comparator());
}

Status
TerarkZipTableFactory::NewTableReader(
		const TableReaderOptions& table_reader_options,
		unique_ptr<RandomAccessFileReader>&& file,
		uint64_t file_size, unique_ptr<TableReader>* table,
		bool prefetch_index_and_filter_in_cache)
const {
	if (!IsBytewiseComparator(table_reader_options.internal_comparator)) {
		return Status::InvalidArgument("TerarkZipTableFactory::NewTableReader()",
				"user comparator must be 'leveldb.BytewiseComparator'");
	}
	Footer footer;
	Status s = ReadFooterFromFile(file.get(), file_size, &footer);
	if (!s.ok()) {
		return s;
	}
	if (footer.table_magic_number() != kTerarkZipTableMagicNumber) {
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
	if (!prefetch_index_and_filter_in_cache) {
		fprintf(stderr
				, "WARN: TerarkZipTableFactory::NewTableReader(): "
				  "prefetch_index_and_filter_in_cache = false is ignored, "
				  "all index and data will be loaded in memory");
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
	if (!IsBytewiseComparator(table_builder_options.internal_comparator)) {
		THROW_STD(invalid_argument,
				"TerarkZipTableFactory::NewTableBuilder(): "
				"user comparator must be 'leveldb.BytewiseComparator'");
	}
	if (fallback_factory_) {
		int curlevel = table_builder_options.level;
		int numlevel = table_builder_options.ioptions.num_levels;
		if (curlevel >= 0 && curlevel < numlevel-1) {
			return fallback_factory_->NewTableBuilder(table_builder_options,
					column_family_id, file);
		}
	}
	return new TerarkZipTableBuilder(
			table_options_,
		    table_builder_options.ioptions,
		    table_builder_options.int_tbl_prop_collector_factories,
			column_family_id,
		    file,
		    table_builder_options.column_family_name);
}

std::string TerarkZipTableFactory::GetPrintableTableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  snprintf(buffer, kBufferSize, "  fixed_key_len: %u\n",
		   table_options_.fixed_key_len);
  ret.append(buffer);
  return ret;
}

Status
TerarkZipTableFactory::SanitizeOptions(const DBOptions& db_opts,
                       	   	   	   	   const ColumnFamilyOptions& cf_opts)
const {
	if (!IsBytewiseComparator(cf_opts.comparator)) {
		return Status::InvalidArgument("TerarkZipTableFactory::NewTableReader()",
				"user comparator must be 'leveldb.BytewiseComparator'");
	}
	return Status::OK();
}

} /* namespace rocksdb */
