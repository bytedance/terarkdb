#include "test.hpp"

/*--------------------------------{ Controller }------------------------------*/
#define MAJOR_TEST true
#define ENABLE_PRINT_LSM false
#define ITER_DEBUG false
#define WRITE_FOREVER false
#define MIN_REP false

/*------------------------------{ Global Variable }---------------------------*/
volatile bool start_write = false;
static const size_t n_write = 20;
std::atomic<size_t> n_remain_write{0};

volatile bool start_verify = false;
static const size_t n_verify = 5;
std::atomic<size_t> n_remain_verify{0};

static const size_t file_size_base = 1 << 20;

static const size_t n_put = 100;
static const size_t n_merge = 33;
static const size_t n_delete = 3;
static const size_t n_once_range_del = 333;
static const size_t n_once_compact_range = 77777777;

size_t f_compact = 100;
size_t f_delete = 100;
int n_graphcnt = 0;
std::string outfname;

/*-----------------------------------{ Main }---------------------------------*/
int main(int argc, char** argv) {

/*--------------------------------{ White Hole }------------------------------*/
    std::mt19937_64 gen(42);

    rocksdb::Status s;
/*-------------------------------{ Metrics Name }-----------------------------*/
    //rocksdb::OperationTimerReporter::InitNamespace("terarkdb.iterdebug");

/*----------------------------{ Thread Container }----------------------------*/
    std::vector<std::thread> v_write;
    std::vector<std::thread> v_verify;

    std::vector<rocksdb::ColumnFamilyDescriptor> v_cfd;
    std::vector<rocksdb::ColumnFamilyHandle*> v_cfh;

/*------------------------{ Generic Options Details }-------------------------*/
    rocksdb::Options options;

    options.compaction_style = rocksdb::kCompactionStyleLevel;
    options.compaction_pri = rocksdb::kMinOverlappingRatio;
    options.comparator = rocksdb::BytewiseComparator();
    options.allow_concurrent_memtable_write = false;
    options.max_open_files = -1;
    options.allow_mmap_reads = true;
    options.target_file_size_multiplier = 1;
    options.disable_auto_compactions = false;
    options.max_bytes_for_level_multiplier = 8;
    options.num_levels = 5;
    options.level_compaction_dynamic_level_bytes = true;
    options.max_subcompactions = 8;
    options.max_background_jobs = 12;
    options.level0_file_num_compaction_trigger = 2;
    options.level0_slowdown_writes_trigger = 8; // INT_MAX;
    options.level0_stop_writes_trigger = 20; // INT_MAX;
    options.max_manifest_file_size = 1ull << 30;
    options.max_manifest_edit_count = 4096;
    options.force_consistency_checks = true;
    options.max_file_opening_threads = 64;
    options.memtable_factory.reset(new rocksdb::SkipListFactory());
    options.enable_lazy_compaction = false;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory());
    options.create_if_missing = true;
    options.blob_size = size_t(-1);
    options.create_missing_column_families = true;
    options.write_buffer_size = file_size_base;
    options.target_file_size_base = file_size_base;
    //options.blob_gc_ratio = 0.1;
    options.db_write_buffer_size = 1UL << 30;
    options.max_total_wal_size = 1UL << 30;

/*---------------------{ Terark Zip Table Options Details }-------------------*/
    rocksdb::TerarkZipTableOptions tzto;

    tzto.localTempDir = "./temp";
    tzto.indexNestLevel = 3;
    tzto.checksumLevel = 3;
    tzto.entropyAlgo = rocksdb::TerarkZipTableOptions::EntropyAlgo::kNoEntropy;
    tzto.terarkZipMinLevel = 0;
    tzto.debugLevel = 0;
    tzto.indexNestScale = 8;
    tzto.enableCompressionProbe = true;
    tzto.useSuffixArrayLocalMatch = false;
    tzto.warmUpIndexOnOpen = false;
    tzto.warmUpValueOnOpen = false;
    tzto.disableSecondPassIter = false;
    tzto.indexTempLevel = 0;
    tzto.offsetArrayBlockUnits = 128;
    tzto.sampleRatio = 0.03;
    //tzto.indexType = "Mixed_XL_256_32_FL";
    tzto.softZipWorkingMemLimit = 32ull << 30;
    tzto.hardZipWorkingMemLimit = 64ull << 30;
    tzto.smallTaskMemory = 1200 << 20; // 1.2G
    tzto.minDictZipValueSize = 15;
    tzto.indexCacheRatio = 0.001;
    tzto.singleIndexMinSize = 8ULL << 20;
    tzto.singleIndexMaxSize = 0x1E0000000; // 7.5G
    tzto.minPreadLen = -1;
    tzto.cacheShards = 17; // to reduce lock competition
    tzto.cacheCapacityBytes = 0;  // non-zero implies direct io read
    tzto.disableCompressDict = false;
    tzto.optimizeCpuL3Cache = true;
    tzto.forceMetaInMemory = false;
    //tzto.compressLevel = -1;

/*---------------------{ Default Rocksdb Column Family }----------------------*/
    options.compaction_style = rocksdb::kCompactionStyleLevel;
    options.write_buffer_size = size_t(file_size_base * 1.0);
    options.target_file_size_base = file_size_base * 1.0;
    v_cfd.emplace_back(rocksdb::kDefaultColumnFamilyName, options); 

/*--------------------{ Enable Terark Zip Table Factory }---------------------*/
    options.table_factory.reset(
        rocksdb::NewTerarkZipTableFactory(tzto, options.table_factory));
    options.enable_lazy_compaction = true;

/*----------------------{ Lazy Leveled Column Family }------------------------*/
    options.compaction_style = rocksdb::kCompactionStyleLevel;
    options.write_buffer_size = size_t(file_size_base * 1.1);
    options.target_file_size_base = file_size_base * 1.1;
    options.max_bytes_for_level_base = options.target_file_size_base * 4;

    v_cfd.emplace_back("LazyLeveled", options);
    
/*---------------------{ Lazy Universal Column Family }-----------------------*/
    options.compaction_style = rocksdb::kCompactionStyleUniversal;
    options.write_buffer_size = size_t(file_size_base * 1.2);
    options.target_file_size_base = file_size_base * 1.2;
    options.max_bytes_for_level_base = options.target_file_size_base * 4;

    v_cfd.emplace_back("LazyUniversal", options); // Lazy universal
    
/*--------------------{ Enable Huffman and KV Separate }----------------------*/
    tzto.entropyAlgo = rocksdb::TerarkZipTableOptions::EntropyAlgo::kHuffman;
    options.blob_size = 5; 
    options.table_factory.reset(
        rocksdb::NewTerarkZipTableFactory(tzto, options.table_factory));
    options.memtable_factory.reset(rocksdb::NewPatriciaTrieRepFactory({}, &s));

/*-----------------{ Lazy Leveled Huffman Column Family }---------------------*/
    options.compaction_style = rocksdb::kCompactionStyleLevel;
    options.write_buffer_size = size_t(file_size_base * 1.3);
    options.target_file_size_base = file_size_base * 1.3;
    options.max_bytes_for_level_base = options.target_file_size_base * 4;

    v_cfd.emplace_back("LazyLeveledHuffman", options);

/*----------------{ Lazy Universal Huffman Column Family }--------------------*/
    options.compaction_style = rocksdb::kCompactionStyleUniversal;
    options.write_buffer_size = size_t(file_size_base * 1.4);
    options.target_file_size_base = file_size_base * 1.4;
    options.max_bytes_for_level_base = options.target_file_size_base * 4;
    
    v_cfd.emplace_back("LazyUniversalHuffman", options); 

/*-------------------------{ Compact Range Options }--------------------------*/
    rocksdb::CompactRangeOptions cro;
    cro.exclusive_manual_compaction = false;

/*--------------------------------{ Open DB }---------------------------------*/
    rocksdb::DB* db;
    rocksdb::DBOptions dbo = options;
    s = rocksdb::DB::Open(dbo, "./db", v_cfd, &v_cfh, &db);

    if (!s.ok()) {
        fprintf(stderr, "%s\n", s.getState());
        return -2;
    }

/*------------------------{ Thread Write Function }---------------------------*/
    auto fn_write = [&](size_t seed, std::string name){
        std::mt19937_64 gen(seed);
        size_t cnt = 0;
        rocksdb::Status s;
        rocksdb::WriteBatch b;
        rocksdb::WriteOptions wo;
        t_str key, val;
        fprintf(stdout, "Thread %s for writing data...\n", name.c_str());
        while (start_write) {
            b.Clear();
            cnt++;
            for (int i = 0; i < n_put; ++i) {
                key = get_key(gen());
                val = get_val(gen());
                for (auto& h : v_cfh) b.Put(h, key, val);
                if (i & 8 == 0) {
                    val = get_val(gen());
                    for (auto& h : v_cfh) b.Merge(h, key, val);
                }
            }
            for (int i = 0; i < n_merge; ++i) {
                key = get_key(gen());
                val = get_val(gen());
                for (auto& h : v_cfh) b.Put(h, key, val);
            }
            for (int i = 0; i < n_delete; ++i) {
                key = get_key(gen());
                for (auto& h : v_cfh) b.Delete(h, key);
            }
            if (cnt % n_once_range_del == 0) {
                std::vector<t_str> vec(f_delete);
                for (auto& e : vec)
		    e = get_key(gen());
                std::sort(vec.begin(), vec.end());
                int select = gen() % (f_delete - 1);
                for (auto& h : v_cfh) 
		    b.DeleteRange(h, vec[select], vec[select+1]);
                fprintf(stdout, "RD : < Bgn = %s, End = %s >\n",
                        vec[select].c_str(), vec[select+1].c_str());
            }
            s = db->Write(wo, &b);
	    if (!s.ok()) {
	    	fprintf(stdout, "write status : %s\n", s.ToString().c_str());
	    }
            if (cnt % n_once_compact_range == 0) {
                std::vector<t_str> vec(f_compact);
                for (auto& e : vec)
		    e = get_key(gen());
                std::sort(vec.begin(), vec.end());
                int select = gen() % (f_compact - 1);
                rocksdb::Slice lhs(vec[select]);
                rocksdb::Slice rhs(vec[select+1]);
                for (auto& cfh : v_cfh) 
		    db->CompactRange(cro, cfh, &lhs, &rhs);
		fprintf(stdout, "CR : < Bgn = %s, End = %s >\n",
			vec[select].c_str(), vec[select+1].c_str());
            }
        }
        n_remain_write--;
        fprintf(stdout, "Thread %s : %zd batches had been writen.\n",
                name.c_str(), cnt);
    };

/*------------------------{ Thread Verify Function }--------------------------*/
    auto fn_verify = [&](){
        rocksdb::ReadOptions ro;
        ro.snapshot = db->GetSnapshot();
        uint64_t sp = ((const uint64_t *)ro.snapshot)[1];
        fprintf(stdout, "Create snapshot %d...\n", sp);
        while (!start_verify){
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        fprintf(stdout, "Verify data with snapshot %d...\n", sp);
        size_t n = 0, e = 0;
        std::vector<rocksdb::Iterator*> v_iter;
        db->NewIterators(ro, v_cfh, &v_iter);
        for (auto& iter : v_iter) iter->SeekToFirst();
        
        auto fn_legal = [&](){
            for (auto& iter : v_iter) if (!iter->Valid()) return false;
            for (int i = 0; i < v_iter.size() - 1; ++i) {
                if(v_iter[i]->key() != v_iter[i+1]->key()) return false;
                if(v_iter[i]->value() != v_iter[i+1]->value()) return false;
                return true;
            }
            return true;
        };
        
        auto fn_all_invalid = [&]{
            for (auto& iter : v_iter) if (iter->Valid()) return false;
            return true;
        };
        
        while (!fn_all_invalid()) {
            if (!fn_legal()) {
                fprintf(stderr, "---------------------------------\n");
                for (auto& iter : v_iter) {
                    if (iter->Valid()) {
                        fprintf(stderr, "Valid.\nKey : %s\nValue : %s\n",
                                iter->key().ToString().c_str(),
                                iter->value().ToString().c_str());
                    } else {
                        fprintf(stderr, "Not Valid.\n");
                    }
                }
                e++;
            } else {
                n++;
            }
            for (auto& iter : v_iter) iter->Next();
        }
        fprintf(stdout,
                "Snapshot[%zd] : %zd entries verified. %zd error.\n",
                sp, n+e, e);
        db->ReleaseSnapshot(ro.snapshot);
        n_remain_verify--;
    };
    
/*------------------------------{ Major Test }--------------------------------*/
    while (MAJOR_TEST) {
        start_write = true;
        while (n_remain_write.load() < n_write) {
	    std::this_thread::sleep_for(std::chrono::seconds(1));
            v_write.emplace_back(
                fn_write, gen(), std::to_string(n_remain_write));
            v_write.back().detach();
            n_remain_write++;
        }
        fprintf(stdout, "========== Start Writing ==========\n");
        while (n_remain_verify.load() < n_verify) {
            std::this_thread::sleep_for(std::chrono::seconds(60));
            v_verify.emplace_back(fn_verify);
            v_verify.back().detach();
            n_remain_verify++;
        }
        start_write = false;
        while (n_remain_write.load() > 0){
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        start_verify = true;
        fprintf(stdout, "========== Start Verify ==========\n");
        while (n_remain_verify.load() > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        start_verify = false;
        v_write.clear();
        v_verify.clear();

#if ENABLE_PRINT_LSM
        FILE* fp = nullptr;
        outfname = "graph_cf_" + std::to_string(n_graphcnt) + ".json";
        if ((fp = fopen(outfname.c_str(), "w+")) == nullptr) {
            fprintf(stderr, "Open graph file failed!\n");
        } else {
            fprintf(fp, "%s", db->GetSketch(v_cfh[3]).c_str());
            fclose(fp);
            n_graphcnt++;
        }
#endif
    }

/*--------------------------------{ Iter Debug }------------------------------*/
    while (ITER_DEBUG) {
        timeval st, ed;
        start_write = true;
        for (int i = 0; i < 1; ++i) {
            v_write.emplace_back(fn_write, gen(), std::to_string(n_remain_write));
            v_write.back().detach();
            n_remain_write++;
        }
        // start_verify = true;
        // while (true) {
        //     while (n_remain_verify.load() < 1000) {
        //         std::this_thread::sleep_for(std::chrono::seconds(10));
        //         v_verify.emplace_back(fn_verify);
        //         v_verify.back().detach();
        //         n_remain_verify++;
        //     }
        //     std::this_thread::sleep_for(std::chrono::seconds(60));
        // }
        gettimeofday(&st, NULL);
        std::vector<double> costs(2);
        rocksdb::ReadOptions ro;
        ro.snapshot = db->GetSnapshot();
        int n_iter = 1000;
        
        auto new_iter = [&]{
            std::vector<rocksdb::Iterator*> v_iter;
            for (int i = 0; i < n_iter; ++i) {
                if (v_iter.size() > 10) {
                    for (auto& iter : v_iter) iter->~Iterator();
                    v_iter.clear();
                }
                v_iter.emplace_back(db->NewIterator(ro, v_cfh[1]));
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
            for (auto& iter : v_iter) iter->~Iterator();
            v_iter.clear();
        };

        std::vector<std::thread> v_th;
        
        for (int i = 0; i < 100; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            v_th.emplace_back(new_iter);
        }
        for (auto& th : v_th) th.join();
        
        start_write = false;
        gettimeofday(&ed, NULL);
        double duration = 
            (ed.tv_sec - st.tv_sec) + (ed.tv_usec - st.tv_usec) * 1e-6;
        fprintf(stdout, "Test time cost : %.2f sec.\n", duration);
        while (n_remain_write.load() > 0 && n_remain_verify.load() > 0){
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        //  break;
    }
/*------------------------------{ Input Forever }-----------------------------*/
    while (WRITE_FOREVER) {
        start_write = true;
        while (n_remain_write.load() < n_write) {
            v_write.emplace_back(
                fn_write, gen(), std::to_string(n_remain_write));
            v_write.back().detach();
            n_remain_write++;
        }
        while (true) std::this_thread::sleep_for(std::chrono::seconds(1));
    }
/*----------------------------{ Minimal Reproduce }------------------------------*/
    while (MIN_REP) {
	rocksdb::Status s;
	rocksdb::WriteBatch b;
        rocksdb::WriteOptions wo;
	rocksdb::ReadOptions ro;
    	for (int i = 1000; i < 10000; ++i) {
	    b.Put(v_cfh[1], std::to_string(i), "MarkValue");
	}
	std::string st{"8000"};
	std::string ed{"9000"};
        s = db->Write(wo, &b);
        db->DeleteRange(wo, v_cfh[1], st, ed);
	if (!s.ok())
	    fprintf(stdout, "write status : %s\n", s.ToString().c_str());
	auto* iter = db->NewIterator(ro, v_cfh[1]);
	for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
	    int k = std::stoi(iter->key().ToString());
	    if (8000 <= k && k < 9000) {
	    	fprintf(stdout, "leak key : %d\n", k);
	    }  
	}
	break;	
    }
/*--------------------------------{ Cleaning }--------------------------------*/
    fprintf(stdout, "Test Done.\n");

    for(auto& cfh : v_cfh) db->DestroyColumnFamilyHandle(cfh);

    delete db;

    return 0;
}
