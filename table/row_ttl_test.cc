// Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License.

#include "db/db_test_util.h"
#include "db/table_properties_collector.h"
#include "rocksdb/terark_namespace.h"
#include "table/block_based_table_builder.h"
#ifdef WITH_TERARK_ZIP
#include "table/terark_zip_table_builder.h"
#endif
#include "util/coding.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace TERARKDB_NAMESPACE {

struct MyParams {
  MyParams(double ratio = 1.000, size_t scan = 0, size_t sst = 0,
           bool terark = false) {
    ttl_ratio = ratio;
    ttl_scan = scan;
    ttl_sst = sst;
    use_terark = terark;
  }
  double ttl_ratio;
  size_t ttl_scan;
  size_t ttl_sst;
  bool use_terark;
};

class RowTtl_Test : public DBTestBase,
                    public ::testing::WithParamInterface<MyParams> {
 public:
  RowTtl_Test() : DBTestBase("./row_ttl_test") {}
  ~RowTtl_Test() override { Close(); }
  void SetUp() override { params = GetParam(); }
  void init() {
    dbname = test::PerThreadDBPath("new_ttl_test");
    DestroyDB(dbname, options);
    options.create_if_missing = true;
    options.ttl_gc_ratio = params.ttl_ratio;
    options.ttl_max_scan_gap = params.ttl_scan;
    options.sst_ttl_seconds = params.ttl_sst;
    options.ttl_extractor_factory.reset(new test::TestTtlExtractorFactory());
    options.table_factory.reset(
        new BlockBasedTableFactory(BlockBasedTableOptions()));
#ifdef WITH_TERARK_ZIP
    if (params.use_terark) {
      use_terark = true;
      tablemagicnumber = kTerarkZipTableMagicNumber;
      TerarkZipTableOptions tzto;
      options.table_factory.reset(TERARKDB_NAMESPACE::NewTerarkZipTableFactory(
          tzto, options.table_factory));
    } else {
#endif
      use_terark = false;
      tablemagicnumber = kBlockBasedTableMagicNumber;
#ifdef WITH_TERARK_ZIP
    }
#endif
  }
  void run() {
    Reopen(options);
    std::unique_ptr<WritableFileWriter> file_writer(test::GetWritableFileWriter(
        new test::StringSink(), "" /* don't care */));
    uint64_t nowseconds = options.env->NowMicros() / 1000000ul;
    const ImmutableCFOptions ioptions(options);
    const MutableCFOptions moptions(options);
    InternalKeyComparator ikc(options.comparator);
    std::vector<std::unique_ptr<IntTblPropCollectorFactory>>
        int_tbl_prop_collector_factories;
    int_tbl_prop_collector_factories.emplace_back(
        NewTtlIntTblPropCollectorFactory(options.ttl_extractor_factory.get(),
                                         options.env, moptions.ttl_gc_ratio,
                                         moptions.ttl_max_scan_gap));
    std::string column_family_name;
    int unknown_level = -1;
    std::unique_ptr<TableBuilder> builder(
        options.table_factory->NewTableBuilder(
            TableBuilderOptions(
                ioptions, moptions, ikc, &int_tbl_prop_collector_factories,
                kNoCompression, CompressionOptions(),
                nullptr /* compression_dict */, false /* skip_filters */,
                column_family_name, unknown_level, 0 /* compaction_load */,
                nowseconds),
            TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
            file_writer.get()));

    std::vector<int> key_ttl(26, 0);
    for (int i = 0; i < 26; i++) {
      key_ttl[i] = i + 1;
    }
    int min_ttl = *std::min_element(key_ttl.begin(), key_ttl.end());
    int max_ttl = *std::max_element(key_ttl.begin(), key_ttl.end());

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(key_ttl.begin(), key_ttl.end(), g);
    for (char c = 'a'; c <= 'z'; ++c) {
      std::string key(8, c);
      key.append("\1       ");
      std::string value(28, c + 42);
      char ts_string[sizeof(uint64_t)];
      uint64_t ttl = static_cast<uint64_t>(key_ttl[c - 'a']);
      EncodeFixed64(ts_string, (uint64_t)ttl);
      value.append(ts_string, sizeof(uint64_t));
      ASSERT_OK(builder->Add(key, LazyBuffer(value)));
    }
    ASSERT_OK(builder->Finish(nullptr, nullptr));
    file_writer->Flush();

    test::StringSink* ss =
        static_cast<test::StringSink*>(file_writer->writable_file());
    std::unique_ptr<RandomAccessFileReader> file_reader(
        test::GetRandomAccessFileReader(
            new test::StringSource(ss->contents(), 72242, true)));
    TableProperties* props = nullptr;
    Status s = ReadTableProperties(
        file_reader.get(), ss->contents().size(), tablemagicnumber, ioptions,
        &props, true /* compression_type_missing */, nullptr);
    std::unique_ptr<TableProperties> props_guard(props);
    ASSERT_OK(s);
    ASSERT_EQ(0ul, props->filter_size);
    ASSERT_EQ(16ul * 26, props->raw_key_size);
    ASSERT_EQ(36ul * 26, props->raw_value_size);
    ASSERT_EQ(26ul, props->num_entries);
    if (use_terark) {
      ASSERT_EQ(26ul, props->num_data_blocks);
    } else {
      ASSERT_EQ(1ul, props->num_data_blocks);
    }
    ASSERT_EQ(props->creation_time, nowseconds);

    auto answer1 = props->user_collected_properties.find(
        TablePropertiesNames::kEarliestTimeBeginCompact);
    auto answer2 = props->user_collected_properties.find(
        TablePropertiesNames::kLatestTimeEndCompact);
    auto it_end = props->user_collected_properties.end();
    uint64_t creation_time = props->creation_time;
    // auto answer3 = props->user_collected_properties.end();
    auto get_varint64 = [](const std::string& v) {
      Slice s(v);
      uint64_t r;
      auto assert_true = [](bool b) { ASSERT_TRUE(b); };
      assert_true(GetVarint64(&s, &r));
      return r;
    };
    uint64_t act_answer1 =
        answer1 != it_end ? get_varint64(answer1->second) : port::kMaxUint64;
    if (moptions.sst_ttl_seconds > 0) {
      act_answer1 =
          std::min(act_answer1, creation_time + moptions.sst_ttl_seconds);
    }
    uint64_t act_answer2 =
        answer2 != it_end ? get_varint64(answer2->second) : port::kMaxUint64;

    if (options.ttl_max_scan_gap == 0 || options.ttl_max_scan_gap > 26) {
      ASSERT_EQ(act_answer2, std::numeric_limits<uint64_t>::max());
    } else {
      std::cout << "[==========]   scan_ttl:";
      std::cout << act_answer2 - nowseconds << "s" << std::endl
                << "[==========]  ttl_queue:";
      for_each(key_ttl.begin(), key_ttl.end(),
               [](const int& val) -> void { std::cout << val << "-"; });
      std::cout << std::endl;
    }

    if (options.ttl_gc_ratio > 1.000) {
      if (options.sst_ttl_seconds > 0) {
        ASSERT_EQ(act_answer1, nowseconds + options.sst_ttl_seconds);
      } else {
        ASSERT_EQ(act_answer1, std::numeric_limits<uint64_t>::max());
      }
    } else {
      if (options.ttl_gc_ratio <= 0.0) {
        ASSERT_EQ(act_answer1, nowseconds + min_ttl);
      } else if (options.ttl_gc_ratio == 1.0) {
        if (options.sst_ttl_seconds > 0 && options.sst_ttl_seconds < 26) {
          ASSERT_EQ(act_answer1, nowseconds + options.sst_ttl_seconds);
        } else {
          ASSERT_EQ(act_answer1, nowseconds + max_ttl);
        }
      } else {
        std::cout << "[==========]  ratio_ttl:";
        std::cout << act_answer1 - nowseconds << "s" << std::endl;
      }
    }
  }

 protected:
  // std::unique_ptr<TERARKDB_NAMESPACE::MockTimeEnv> mock_env_;
  Options options;
  std::string dbname;
  bool use_terark;
  uint64_t tablemagicnumber;
  MyParams params;
};

MyParams ttl_param[] = {
    {},
    {0.50, 2, 0, false},
    {1.280, 5, 10, false},
    {0.50, 0, 0, true},
    {1.280, 0, 10, true},
    {-1.000, 1000, 100, true},
    {0.000, 1, 100, false},
    {1.0, 0, 20, false},
    {1.0, 0, 40, false},
    {1.0, 1, 20, true},
    {1.0, 2, 40, true},
};

TEST_P(RowTtl_Test, BoundaryTest) {
  init();
  run();
}
INSTANTIATE_TEST_CASE_P(RowTtl_Test, RowTtl_Test,
                        ::testing::ValuesIn(ttl_param));
}  // namespace TERARKDB_NAMESPACE
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}