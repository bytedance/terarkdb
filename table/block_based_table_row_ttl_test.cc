// Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License.

#include "db/table_properties_collector.h"
#include "rocksdb/terark_namespace.h"
#include "table/block_based_table_builder.h"
#include "util/coding.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace TERARKDB_NAMESPACE {

struct params {
  params(double ratio = 1.000, size_t scan = 0, size_t sst = 0) {
    ttl_ratio = ratio;
    ttl_scan = scan;
    ttl_sst = sst;
  }
  double ttl_ratio;
  size_t ttl_scan;
  size_t ttl_sst;
};
class TestEnv : public EnvWrapper {
 public:
  explicit TestEnv() : EnvWrapper(Env::Default()), close_count(0) {}

  class TestLogger : public Logger {
   public:
    using Logger::Logv;
    TestLogger(TestEnv* env_ptr) : Logger() { env = env_ptr; }
    ~TestLogger() {
      if (!closed_) {
        CloseHelper();
      }
    }
    virtual void Logv(const char* /*format*/, va_list /*ap*/) override{};

   protected:
    virtual Status CloseImpl() override { return CloseHelper(); }

   private:
    Status CloseHelper() {
      env->CloseCountInc();
      ;
      return Status::IOError();
    }
    TestEnv* env;
  };

  void CloseCountInc() { close_count++; }

  int GetCloseCount() { return close_count; }

  virtual Status NewLogger(const std::string& /*fname*/,
                           std::shared_ptr<Logger>* result) {
    result->reset(new TestLogger(this));
    return Status::OK();
  }

 private:
  int close_count;
};

class BlockBasedTableBuilderTest : public ::testing::TestWithParam<params> {};

params ttl_param[] = {{0.50, 2},     {1.280, 5},  {0.800, 0}, {1.280, 0},
                      {1.000, 1000}, {0.000, 1},  {-10, 0},   {2, 0, 10},
                      {1.0, 0, 20},  {1.0, 0, 40}};
// INSTANTIATE_TEST_CASE_P(TrueReturn, BlockBasedTableBuilderTest,
//                         testing::Values(ttl_param[0], ttl_param[1],
//                                         ttl_param[2], ttl_param[3],
//                                         ttl_param[4], ttl_param[5]));
INSTANTIATE_TEST_CASE_P(CorrectnessTest, BlockBasedTableBuilderTest,
                        testing::ValuesIn(ttl_param));

TEST_F(BlockBasedTableBuilderTest, FunctionTest) {
  BlockBasedTableOptions blockbasedtableoptions;
  BlockBasedTableFactory factory(blockbasedtableoptions);
  test::StringSink sink;
  std::unique_ptr<WritableFileWriter> file_writer(
      test::GetWritableFileWriter(new test::StringSink(), "" /* don't care */));
  Options options;
  std::string dbname =
      test::PerThreadDBPath("block_based_table_builder_ttl_test_1");
  ASSERT_OK(DestroyDB(dbname, options));
  DB* db = nullptr;
  TestEnv* env = new TestEnv();
  options.info_log.reset(new TestEnv::TestLogger(env));
  options.create_if_missing = true;
  options.env = env;
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);
  s = db->Close();
  delete db;

  const ImmutableCFOptions ioptions(options);
  const MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>>
      int_tbl_prop_collector_factories;
  std::string column_family_name;
  int unknown_level = -1;
  std::unique_ptr<TableBuilder> builder(factory.NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, ikc,
                          &int_tbl_prop_collector_factories, kNoCompression,
                          CompressionOptions(), nullptr /* compression_dict */,
                          false /* skip_filters */, column_family_name,
                          unknown_level, 0 /* compaction_load */),
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
      file_writer.get()));
  for (char c = 'a'; c <= 'z'; ++c) {
    std::string key(8, c);
    key.append("\1       ");
    std::string value(28, c + 42);
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
  s = ReadTableProperties(file_reader.get(), ss->contents().size(),
                          kBlockBasedTableMagicNumber, ioptions, &props,
                          true /* compression_type_missing */, nullptr);
  std::unique_ptr<TableProperties> props_guard(props);

  ASSERT_OK(s);
  ASSERT_EQ(0ul, props->filter_size);
  ASSERT_EQ(16ul * 26, props->raw_key_size);
  ASSERT_EQ(28ul * 26, props->raw_value_size);
  ASSERT_EQ(26ul, props->num_entries);
  ASSERT_EQ(1ul, props->num_data_blocks);
  ASSERT_TRUE(props->user_collected_properties.find(
                  TablePropertiesNames::kEarliestTimeBeginCompact) ==
              props->user_collected_properties.end());
  ASSERT_TRUE(props->user_collected_properties.find(
                  TablePropertiesNames::kLatestTimeEndCompact) ==
              props->user_collected_properties.end());
  // ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
  // props->scan_gap_expire_time);
  delete options.env;
}

TEST_P(BlockBasedTableBuilderTest, BoundaryTest) {
  BlockBasedTableOptions blockbasedtableoptions;
  BlockBasedTableFactory factory(blockbasedtableoptions);
  test::StringSink sink;
  std::unique_ptr<WritableFileWriter> file_writer(
      test::GetWritableFileWriter(new test::StringSink(), "" /* don't care */));
  Options options;
  std::string dbname =
      test::PerThreadDBPath("block_based_table_builder_ttl_test_2");
  ASSERT_OK(DestroyDB(dbname, options));
  DB* db = nullptr;
  TestEnv* env = new TestEnv();
  options.info_log.reset(new TestEnv::TestLogger(env));
  options.create_if_missing = true;
  options.env = env;
  auto n = GetParam();
  options.ttl_gc_ratio = n.ttl_ratio;
  options.ttl_max_scan_gap = n.ttl_scan;
  options.sst_ttl_seconds = n.ttl_sst;
  options.ttl_extractor_factory.reset(new test::TestTtlExtractorFactory());
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);
  s = db->Close();
  delete db;

  uint64_t nowseconds = env->NowMicros() / 1000000ul;
  const ImmutableCFOptions ioptions(options);
  const MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>>
      int_tbl_prop_collector_factories;

  int_tbl_prop_collector_factories.emplace_back(
      NewTtlIntTblPropCollectorFactory(options.ttl_extractor_factory.get(), env,
                                       moptions.ttl_gc_ratio,
                                       moptions.ttl_max_scan_gap));
  std::string column_family_name;
  int unknown_level = -1;
  std::unique_ptr<TableBuilder> builder(factory.NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, ikc,
                          &int_tbl_prop_collector_factories, kNoCompression,
                          CompressionOptions(), nullptr /* compression_dict */,
                          false /* skip_filters */, column_family_name,
                          unknown_level, 0 /* compaction_load */, nowseconds),
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
    // if (c == 'a') {
    //   ttl = 0;
    // }
    EncodeFixed64(ts_string, (uint64_t)ttl);
    // AppendNumberTo(&value, ttl);
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
  s = ReadTableProperties(file_reader.get(), ss->contents().size(),
                          kBlockBasedTableMagicNumber, ioptions, &props,
                          true /* compression_type_missing */, nullptr,
                          moptions.sst_ttl_seconds);
  std::unique_ptr<TableProperties> props_guard(props);

  ASSERT_OK(s);
  ASSERT_EQ(0ul, props->filter_size);
  ASSERT_EQ(16ul * 26, props->raw_key_size);
  ASSERT_EQ(36ul * 26, props->raw_value_size);
  ASSERT_EQ(26ul, props->num_entries);
  ASSERT_EQ(1ul, props->num_data_blocks);
  ASSERT_EQ(props->creation_time, nowseconds);
  auto answer1 = props->user_collected_properties.find(
      TablePropertiesNames::kEarliestTimeBeginCompact);
  auto answer2 = props->user_collected_properties.find(
      TablePropertiesNames::kLatestTimeEndCompact);
  auto it_end = props->user_collected_properties.end();
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
        std::min(act_answer1, props->creation_time + moptions.sst_ttl_seconds);
  }
  uint64_t act_answer2 =
      answer2 != it_end ? get_varint64(answer2->second) : port::kMaxUint64;
  if (n.ttl_ratio > 1.000) {
    // ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
    // props->ratio_expire_time);
    if (n.ttl_sst > 0) {
      ASSERT_EQ(act_answer1, nowseconds + n.ttl_sst);
    } else {
      ASSERT_EQ(act_answer1, std::numeric_limits<uint64_t>::max());
    }
  } else {
    // ASSERT_NE(answer1, answer3);
    if (n.ttl_ratio <= 0.0) {
      // EXPECT_EQ(nowseconds + min_ttl, props->ratio_expire_time);
      ASSERT_EQ(act_answer1, nowseconds + min_ttl);
    } else if (n.ttl_ratio == 1.0) {
      if (n.ttl_sst > 0 && n.ttl_sst < 26) {
        ASSERT_EQ(act_answer1, nowseconds + n.ttl_sst);
      } else {
        ASSERT_EQ(act_answer1, nowseconds + max_ttl);
      }
    } else {
      std::cout << "[==========]  ratio_ttl:";
      // std::cout << props->ratio_expire_time - nowseconds << "s" << std::endl;
      std::cout << act_answer1 - nowseconds << "s" << std::endl;
    }
  }
  if (n.ttl_scan == 0 || n.ttl_scan > 26) {
    ASSERT_EQ(act_answer2, std::numeric_limits<uint64_t>::max());
    // ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
    //           props->scan_gap_expire_time);
  } else {
    // ASSERT_NE(answer2, answer3);
    // uint64_t act_answer = DecodeFixed64(answer2->second.c_str());
    std::cout << "[==========]   scan_ttl:";
    std::cout << act_answer2 - nowseconds << "s" << std::endl
              << "[==========]  ttl_queue:";
    for_each(key_ttl.begin(), key_ttl.end(),
             [](const int& val) -> void { std::cout << val << "-"; });
    std::cout << std::endl;
  }

  delete options.env;
}
}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
