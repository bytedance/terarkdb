#pragma warning(disable:4996)

#include <utility>
#include <iterator>
#include <memory>
#include <cstdlib>
#include <vector>
#include <thread>
#include <random>
#include <iostream>
#include <thread>
#include <cctype>

#include <util/threaded_rbtree.h>

#include <rocksdb/compaction_filter.h>
#include <rocksdb/db.h>
#include <rocksdb/memtablerep.h>
#include <rocksdb/table.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/experimental.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/convenience.h>
#include <table/iterator_wrapper.h>
#include <util/coding.h>
#include <db/memtable.h>
#include <table/terark_zip_table.h>

#include <table/terark_zip_index.h>
#include <table/terark_zip_common.h>
#include <terark/io/FileStream.hpp>
#include <terark/io/DataInput.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/histogram.hpp>

#include <terark/util/mmap.hpp>
#include <terark/util/sorted_uint_vec.hpp>
#include <terark/zbs/blob_store.hpp>
#include <terark/zbs/blob_store_file_header.hpp>
#include <terark/entropy/rans_encoding.hpp>
#include <terark/entropy/huffman_encoding.hpp>
#include <boost/filesystem.hpp>

#undef assert
#define assert(exp) do { if(!(exp)) DebugBreak(); } while(false)

struct Slice_compare :public std::binary_function<rocksdb::Slice, rocksdb::Slice, bool> {
	bool operator()(const rocksdb::Slice &a, const rocksdb::Slice &b) const {
		const size_t a_size = a.size();
		const size_t b_size = b.size();
		const size_t len = (a_size < b_size) ? a_size : b_size;
		int res;

		if ((res = memcmp(a.data(), b.data(), len)))
			return (res < 0);

		if (a_size != b_size) {
			return a_size < b_size ? true : false;
		}
		return false;
	}
};

class TargetSetFilter : public rocksdb::CompactionFilter {
public:
	bool Filter(int lv, const rocksdb::Slice& key, const rocksdb::Slice& val, std::string* nval, bool* chg) {
		return TargetSet.find(key) == TargetSet.end();
	}

	void Target(const rocksdb::Slice& key) {
		TargetSet.insert(key);
	}

	const char* Name() const override {
		return "TargetSetFilter";
	}

private:

	std::set<rocksdb::Slice, Slice_compare> TargetSet;

};

class TargetMapFilter : public rocksdb::CompactionFilter{
public:
	bool Filter(int lv, const rocksdb::Slice& key, const rocksdb::Slice& val, std::string* nval, bool* chg) {
		return TargetMap.find(key) == TargetMap.end();
	}

	void Target(const rocksdb::Slice& key, const rocksdb::Slice& val) {
		TargetMap.insert_or_assign(key, val);
	}

	const char* Name() const override {
		return "TargetMapFilter";
	}

private:

	std::map<rocksdb::Slice, rocksdb::Slice, Slice_compare> TargetMap;

};

class RandomDeleteFilter : public rocksdb::CompactionFilter {
public:
	bool Filter(int /*level*/, const rocksdb::Slice& /*key*/,
		const rocksdb::Slice& /*existing_value*/,
		std::string* /*new_value*/,
		bool* value_changed) const override {
		return rand() % 4 == 0;
	}

	const char* Name() const override {
		return "RandomDeleteFilter";
	}
};

class Rdb_pk_comparator : public rocksdb::Comparator {
public:
	Rdb_pk_comparator(const Rdb_pk_comparator &) = delete;
	Rdb_pk_comparator &operator=(const Rdb_pk_comparator &) = delete;
	Rdb_pk_comparator() = default;

	static int bytewise_compare(const rocksdb::Slice &a,
		const rocksdb::Slice &b) {
		const size_t a_size = a.size();
		const size_t b_size = b.size();
		const size_t len = (a_size < b_size) ? a_size : b_size;
		int res;

		if ((res = memcmp(a.data(), b.data(), len)))
			return res;

		/* Ok, res== 0 */
		if (a_size != b_size) {
			return a_size < b_size ? -1 : 1;
		}
		return 0;
	}

	/* Override virtual methods of interest */

	int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const override {
		return bytewise_compare(a, b);
	}

	const char *Name() const override { return "RocksDB_SE_v3.10"; }

	// TODO: advanced funcs:
	// - FindShortestSeparator
	// - FindShortSuccessor

	// for now, do-nothing implementations:
	void FindShortestSeparator(std::string *start,
		const rocksdb::Slice &limit) const override {}
	void FindShortSuccessor(std::string *key) const override {}
};

class Rdb_rev_comparator : public rocksdb::Comparator {
public:
	Rdb_rev_comparator(const Rdb_rev_comparator &) = delete;
	Rdb_rev_comparator &operator=(const Rdb_rev_comparator &) = delete;
	Rdb_rev_comparator() = default;

	static int bytewise_compare(const rocksdb::Slice &a,
		const rocksdb::Slice &b) {
		return -Rdb_pk_comparator::bytewise_compare(a, b);
	}

	int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const override {
		return -Rdb_pk_comparator::bytewise_compare(a, b);
	}
	const char *Name() const override { return "rev:RocksDB_SE_v3.10"; }
	void FindShortestSeparator(std::string *start,
		const rocksdb::Slice &limit) const override {}
	void FindShortSuccessor(std::string *key) const override {}
};

class FlushEventListener : public rocksdb::EventListener {
public:
	void OnFlushCompleted(rocksdb::DB* db, const rocksdb::FlushJobInfo& flush_job_info) override {
		printf("");
	}
};

std::string get_key(size_t i)
{
	char buffer[32];
	snprintf(buffer, sizeof buffer, "%012zd", i);
	return buffer;
}

std::string get_rnd_key(size_t r)
{
	std::mt19937_64 mt(r);
	char buffer[32];
	snprintf(buffer, sizeof buffer, "%llX", mt());
	return std::string(buffer, buffer + std::uniform_int_distribution<size_t>(4, 16)(mt));
}
std::string get_value(size_t i)
{
	static std::string str = ([] {
		std::string s;
		for (int i = 0; i < 4; ++i) {
			s.append("0123456789QWERTYUIOPASDFGHJKLZXCVBNM");
		}
		return s;
	})();
	size_t pos = i % str.size();
	std::string value = get_key(i);
	value.append(str.data() + pos, str.size() - pos);
	return value;
}
int main(int argc, const char* argv[])
{
	if (argc < 2) {
		return -1;
	}
	std::mt19937_64 mt;

	Rdb_pk_comparator c;
	Rdb_rev_comparator rc;
	rocksdb::DB *db;
	rocksdb::Options options;

	rocksdb::BlockBasedTableOptions bto;
	bto.block_size = 4 * 1024;
	bto.block_cache = std::shared_ptr<rocksdb::Cache>(rocksdb::NewLRUCache(8ULL * 1024 * 1024));
	rocksdb::TerarkZipTableOptions tzto0, tzto1;
	tzto0.localTempDir = "D:/code/osc/test-terark-zip-rocksdb/tempdir";
	tzto0.extendedConfigFile = R"(D:/code/osc/test-terark-zip-rocksdb/license.txt)";
	//tzto.disableSecondPassIter = true;
	tzto0.debugLevel = 0;
	tzto0.minDictZipValueSize = 0;
	//tzto0.keyPrefixLen = 4;
	tzto0.keyPrefixLen = 0;
	tzto0.checksumLevel = 3;
	tzto0.offsetArrayBlockUnits = 128;
	tzto0.entropyAlgo = rocksdb::TerarkZipTableOptions::kFSE;
	tzto0.softZipWorkingMemLimit = 16ull << 30;
	tzto0.hardZipWorkingMemLimit = 32ull << 30;
	tzto0.smallTaskMemory = 1200 << 20; // 1.2G
	tzto1 = tzto0;
	tzto1.offsetArrayBlockUnits = 0;
	tzto1.entropyAlgo = rocksdb::TerarkZipTableOptions::kHuffman;
	auto fallback = std::shared_ptr<rocksdb::TableFactory>(rocksdb::NewBlockBasedTableFactory(bto));
	auto table_factory0 = std::shared_ptr<rocksdb::TableFactory>(rocksdb::NewTerarkZipTableFactory(tzto0, fallback));
	auto table_factory1 = std::shared_ptr<rocksdb::TableFactory>(rocksdb::NewTerarkZipTableFactory(tzto1, fallback));
	fallback.reset();
	options.allow_mmap_reads = true;
	//options.compaction_style = rocksdb::kCompactionStyleUniversal;
	options.compaction_pri = rocksdb::kMinOverlappingRatio;
	//options.compaction_style = rocksdb::kCompactionStyleNone;
	options.comparator = rocksdb::BytewiseComparator();
	//options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(1));
	//options.memtable_factory.reset(new rocksdb::VectorRepFactory(32ull << 10));
	options.allow_concurrent_memtable_write = false;
	options.max_open_files = 32768;
	RandomDeleteFilter rdf;
	options.compaction_filter = &rdf;

	//options.compression = rocksdb::kSnappyCompression;
	options.target_file_size_multiplier = 1;
	options.max_bytes_for_level_multiplier = 8;
	options.disable_auto_compactions = true;
	options.num_levels = 20;
	//options.max_compaction_bytes = options.target_file_size_base * 2;
	options.max_subcompactions = 4;
	options.level_compaction_dynamic_level_bytes = true;
	//options.max_write_buffer_number = 2;
	options.compaction_options_universal.min_merge_width = 2;
	options.compaction_options_universal.max_merge_width = 4;
	options.compaction_options_universal.allow_trivial_move = true;
	options.compaction_options_universal.stop_style = rocksdb::kCompactionStopStyleSimilarSize;

	options.level0_file_num_compaction_trigger = 4;
	options.level0_slowdown_writes_trigger = 20;
	options.level0_stop_writes_trigger = 36;

	options.base_background_compactions = 4;
	options.max_background_compactions = 8;
	//options.level0_slowdown_writes_trigger = INT_MAX;
	//options.level0_stop_writes_trigger = INT_MAX;
	//options.listeners.emplace_back(new FlushEventListener());

	//options.rate_limiter.reset(rocksdb::NewGenericRateLimiter(32ull << 10, 1000));

	std::vector<rocksdb::ColumnFamilyDescriptor> cfDescriptors;
	{
		options.comparator = &c;
		options.table_factory = table_factory0;
		options.enable_partial_remove = true;
		options.compaction_style = rocksdb::kCompactionStyleLevel;
		options.write_buffer_size = options.target_file_size_base = 10ull << 20;
		//options.arena_block_size = 512 << 10;
		options.max_bytes_for_level_base = options.target_file_size_base * 4;
		//options.memtable_factory.reset(rocksdb::NewThreadedRBTreeRepFactory());
		options.memtable_factory.reset(new rocksdb::SkipListFactory());
		cfDescriptors.emplace_back(rocksdb::kDefaultColumnFamilyName, options);

		options.comparator = rocksdb::BytewiseComparator();
		options.table_factory = table_factory1;
		options.enable_partial_remove = true;
		options.compaction_style = rocksdb::kCompactionStyleUniversal;
		options.write_buffer_size = options.target_file_size_base = 10ull << 20;
		//options.arena_block_size = 512 << 10;
		options.max_bytes_for_level_base = options.target_file_size_base * 4;
		options.memtable_factory.reset(rocksdb::NewPatriciaTrieRepFactory());
		cfDescriptors.emplace_back("test", options);
	}

	//options.table_properties_collector_factories.emplace_back(new MongoRocksOplogPropertiesCollectorFactory);

	options.env->SetBackgroundThreads(options.max_background_compactions, rocksdb::Env::LOW);
	options.env->SetBackgroundThreads(options.max_background_flushes, rocksdb::Env::HIGH);

	options.create_if_missing = true;

	rocksdb::DBOptions dbo = options;
	dbo.create_missing_column_families = true;
	//dbo.max_log_file_size = 2ull << 20;
	rocksdb::Status s, s0, s1;
	std::vector<rocksdb::ColumnFamilyHandle*> hs;

	//rocksdb::DestroyDB("C:/osc/rocksdb_test/testdb", options);
	s = rocksdb::DB::Open(dbo, argv[1], cfDescriptors, &hs, &db);
	rocksdb::ColumnFamilyHandle *h0 = hs[0], *h1 = hs[1];

	//std::this_thread::sleep_for(std::chrono::hours(1));
	TargetSetFilter tf;
	rocksdb::ReadOptions rdopt;
	rocksdb::WriteOptions wo;
	rocksdb::FlushOptions fo;
	rocksdb::CompactRangeOptions cropt;
	//std::string key, value;
	std::string value_out0;
	std::string value_out1;

#define ITER_TEST 0
#define GET_TEST 0
#define HEAVY_READ 0
#define RANGE_DEL 0

#if ITER_TEST
	std::unique_ptr<rocksdb::Iterator> raw_iter0(db->NewIterator(ro, h0));
	std::unique_ptr<rocksdb::Iterator> raw_iter1(db->NewIterator(ro, h1));
#endif
	std::vector<const rocksdb::Snapshot*> snapshot;

	rocksdb::CompactRangeOptions co;
	//co.exclusive_manual_compaction = false; //default value is true
	//std::this_thread::sleep_for(std::chrono::hours(1));
	//db->CompactRange(co, h0, nullptr, nullptr);
	//db->CompactRange(co, h0, nullptr, nullptr);
	//std::uniform_int_distribution<uint64_t> uid(0, stop);

	int sieve0 = 17, sieve1 = 23;
	std::string key, val, cmpBgn, cmpEnd;
	int keyNumBgn = 1, keyNumEnd = 10000;

	rocksdb::WriteBatchWithIndex wtbch(rocksdb::BytewiseComparator(), 0, true, 0);
	for (int i = keyNumBgn; i <= keyNumEnd; ++i)
	{
		key = get_rnd_key(rand() % 89 + 10);
		val = get_value(rand() % 89 + 10);
		wtbch.Put(h0, key, val);
		//if (i == keyNumBgn) cmpBgn = key;
		//if (i == keyNumEnd) cmpEnd = key;
		if (i % 10 == 0) tf.Target(rocksdb::Slice(key));
		if (i % 1000000 == 0) {
			s = db->CompactRange(cropt, &rocksdb::Slice("key" + std::to_string(i - 99999)), &rocksdb::Slice(key));
		}
		/*
		std::uniform_int_distribution<uint64_t> uid(10000, count);
		#if ITER_TEST
		if (count % 11111111 == 0) {
		raw_iter0.reset();
		raw_iter1.reset();
		for (auto s : snapshot) {
		db->ReleaseSnapshot(s);
		}
		snapshot.clear();
		db->DestroyColumnFamilyHandle(h0);
		db->DestroyColumnFamilyHandle(h1);
		delete db;
		s = rocksdb::DB::Open(dbo, argv[1], cfDescriptors, &hs, &db);
		h0 = hs[0];
		h1 = hs[1];
		ro.snapshot = nullptr;
		//DeleteFilesInRange(db, h0, nullptr, nullptr);
		//rocksdb::RangePtr r[1];
		//r->start = r->limit = nullptr;
		//rocksdb::DeleteFilesInRanges(db, h0, r, 1, true);
		//rocksdb::DeleteFilesInRanges(db, h1, r, 1, true);
		raw_iter0.reset(db->NewIterator(ro, h0));
		raw_iter1.reset(db->NewIterator(ro, h1));
		}
		#endif
		if (count < stop) {
		value = get_value(count);
		size_t r = uid(mt);
		key = get_rnd_key(r);
		b.Put(h0, key, value);
		b.Put(h1, key, value);
		key = get_key(count);
		value = get_value(r);
		b.Put(h0, key, value);
		b.Put(h1, key, value);
		//if (count % 2) {
		//  r = uid(mt);
		//  key = get_rnd_key(r);
		//  b.Merge(h0, key, value);
		//  b.Merge(h1, key, value);
		//}
		//if (count % 29 == 0) {
		//  r = uid(mt);
		//  key = get_key(r);
		//  b.Merge(h0, key, value);
		//  b.Merge(h1, key, value);
		//}
		//if (count % 5) {
		//  key = get_rnd_key(uid(mt));
		//  b.Delete(h0, key);
		//  b.Delete(h1, key);
		//}
		//if (count % 7) {
		//  key = get_key(uid(mt));
		//  b.Delete(h0, key);
		//  b.Delete(h1, key);
		//}
		#if RANGE_DEL
		if (count % 31) {
		size_t r0 = uid(mt);
		size_t r1 = uid(mt);
		auto key0 = get_rnd_key(r0);
		auto key1 = get_rnd_key(r1);
		if (key0 > key1) std::swap(key0, key1);
		b.DeleteRange(h0, key0, key1);
		b.DeleteRange(h1, key0, key1);
		}
		if (count % 37) {
		size_t r0 = uid(mt);
		size_t r1 = uid(mt);
		auto key0 = get_key(r0);
		auto key1 = get_key(r1);
		if (key0 > key1) std::swap(key0, key1);
		b.DeleteRange(h0, key0, key1);
		b.DeleteRange(h1, key0, key1);
		}
		#endif
		}
		else {
		count = stop;
		}
		if (std::uniform_int_distribution<uint64_t>(0, 1 << 3)(mt) == 0) {
		s = db->Write(wo, b.GetWriteBatch());
		b.Clear();
		}
		#if ITER_TEST
		if (count % 103 == 0) {
		ro.snapshot = nullptr;
		raw_iter0.reset(db->NewIterator(ro, h0));
		raw_iter1.reset(db->NewIterator(ro, h1));
		}
		#endif
		if (count % 89 == 0) {
		if (snapshot.size() < 2) {
		snapshot.emplace_back(db->GetSnapshot());
		}
		else if (snapshot.size() > 50 || (mt() & 1) == 0) {
		auto i = mt() % snapshot.size();
		db->ReleaseSnapshot(snapshot[i]);
		snapshot.erase(snapshot.begin() + i);
		}
		else {
		snapshot.emplace_back(db->GetSnapshot());
		}
		}
		auto for_each_check = [&] {
		#if ITER_TEST
		raw_iter0.reset(db->NewIterator(ro, h0));
		raw_iter1.reset(db->NewIterator(ro, h1));
		raw_iter0->SeekToFirst();
		for (raw_iter1->SeekToFirst(); raw_iter0->Valid(); raw_iter0->Next()) {
		assert(raw_iter0->key() == raw_iter1->key());
		assert(raw_iter0->value() == raw_iter1->value());
		raw_iter1->Next();
		}
		assert(!raw_iter1->Valid());

		raw_iter0->SeekToLast();
		for (raw_iter1->SeekToLast(); raw_iter0->Valid(); raw_iter0->Prev()) {
		assert(raw_iter0->key() == raw_iter1->key());
		assert(raw_iter0->value() == raw_iter1->value());
		raw_iter1->Prev();
		}
		assert(!raw_iter1->Valid());
		#endif
		return true;
		};
		#if GET_TEST
		auto check_assert = [&](bool assert_value) {
		assert(assert_value);
		assert(assert_value || for_each_check());
		};
		#if HEAVY_READ
		for (int i = 0; i < 32; ++i) {
		#else
		for (int i = 0; i < 4; ++i) {
		#endif
		size_t si = mt() % (snapshot.size() + 1);
		ro.snapshot = si == snapshot.size() ? nullptr : snapshot[si];
		key = i % 2 ? get_rnd_key(uid(mt)) : get_key(uid(mt));
		s0 = b.GetFromBatchAndDB(db, ro, h0, key, &value_out0);
		s1 = b.GetFromBatchAndDB(db, ro, h1, key, &value_out1);
		if (s0.IsNotFound()) {
		check_assert(s1.IsNotFound());
		}
		else {
		check_assert(!s1.IsNotFound());
		check_assert(value_out0 == value_out1);
		}
		}
		#endif
		#if ITER_TEST
		#if HEAVY_READ
		for (int i = 0; i < 4; ++i) {
		#else
		if (count % 7 == 0) {
		#endif
		key = count % 2 ? get_rnd_key(uid(mt)) : get_key(uid(mt));
		raw_iter0->Seek(key);
		raw_iter1->Seek(key);
		if (raw_iter0->Valid()) {
		check_assert(raw_iter0->key() == raw_iter1->key());
		check_assert(raw_iter0->value() == raw_iter1->value());
		raw_iter0->Next();
		raw_iter1->Next();
		if (raw_iter0->Valid()) {
		check_assert(raw_iter0->key() == raw_iter1->key());
		check_assert(raw_iter0->value() == raw_iter1->value());
		raw_iter0->Prev();
		raw_iter1->Prev();
		if (raw_iter0->Valid()) {
		check_assert(raw_iter0->key() == raw_iter1->key());
		check_assert(raw_iter0->value() == raw_iter1->value());
		}
		else {
		check_assert(!raw_iter1->Valid());
		}
		}
		else {
		check_assert(!raw_iter1->Valid());
		}
		}
		else {
		check_assert(!raw_iter1->Valid());
		}
		raw_iter0->SeekForPrev(key);
		raw_iter1->SeekForPrev(key);
		if (raw_iter0->Valid()) {
		check_assert(raw_iter0->key() == raw_iter1->key());
		check_assert(raw_iter0->value() == raw_iter1->value());
		raw_iter0->Prev();
		raw_iter1->Prev();
		if (raw_iter0->Valid()) {
		check_assert(raw_iter0->key() == raw_iter1->key());
		check_assert(raw_iter0->value() == raw_iter1->value());
		raw_iter0->Next();
		raw_iter1->Next();
		if (raw_iter0->Valid()) {
		check_assert(raw_iter0->key() == raw_iter1->key());
		check_assert(raw_iter0->value() == raw_iter1->value());
		}
		else {
		check_assert(!raw_iter1->Valid());
		}
		}
		else {
		check_assert(!raw_iter1->Valid());
		}
		}
		else {
		check_assert(!raw_iter1->Valid());
		}
		}
		#endif

		}
		//db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);

		for (auto s : snapshot) {
		db->ReleaseSnapshot(s);
		*/
	}
	
	//s = db->CompactRange(cropt, &sBgn, &sEnd);

	for (int i = keyNumBgn; i <= keyNumEnd; i++) {
		key = "key" + std::to_string(i);
		s = wtbch.GetFromBatchAndDB(db, rdopt, h0, key, &val);

		if (s.IsNotFound())
			assert(i % 10 == 0);
		//else
		//  assert(val == "val" + std::to_string(i) + "1234567890");
	}
#if ITER_TEST
	raw_iter0.reset();
	raw_iter1.reset();
#endif
	db->DestroyColumnFamilyHandle(h0);
	db->DestroyColumnFamilyHandle(h1);

	delete db;

	return 0;
}
