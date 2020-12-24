// Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License.

// #include "include/rocksdb/ttl_extractor.h"
// #include "gtest/gtest.h"
#include "table/block_based_table_builder.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

struct params {
  params(double ratio=128.0, int scan=std::numeric_limits<int>::max()) {
    ttl_ratio = ratio;
    ttl_scan = scan;
  }
  double ttl_ratio;
  int ttl_scan;
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

params ttl_param[] = {{50.0, 2},
                      {128.0, 2},
                      {50.0, std::numeric_limits<int>::max()},
                      {128.0, std::numeric_limits<int>::max()},
                      {100.0, 1000},
                      {-10, -3}};
// INSTANTIATE_TEST_CASE_P(TrueReturn, BlockBasedTableBuilderTest,
//                         testing::Values(ttl_param[0], ttl_param[1],
//                                         ttl_param[2], ttl_param[3],
//                                         ttl_param[4], ttl_param[5]));
INSTANTIATE_TEST_CASE_P(TrueReturn, BlockBasedTableBuilderTest,
                        testing::ValuesIn(ttl_param));

TEST_F(BlockBasedTableBuilderTest, SimpleTest1) {
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
                          true /* compression_type_missing */);
  std::unique_ptr<TableProperties> props_guard(props);

  ASSERT_OK(s);
  ASSERT_EQ(0ul, props->filter_size);
  ASSERT_EQ(16ul * 26, props->raw_key_size);
  ASSERT_EQ(28ul * 26, props->raw_value_size);
  ASSERT_EQ(26ul, props->num_entries);
  ASSERT_EQ(1ul, props->num_data_blocks);
  ASSERT_EQ(std::numeric_limits<uint64_t>::max(), props->ratio_expire_time);
  ASSERT_EQ(std::numeric_limits<uint64_t>::max(), props->scan_gap_expire_time);
  delete options.env;
}

TEST_P(BlockBasedTableBuilderTest, SimpleTest2) {
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
  options.ttl_garbage_collection_percentage = n.ttl_ratio;
  options.ttl_scan_gap = n.ttl_scan;
  options.ttl_extractor_factory.reset(new test::TestTtlExtractorFactory());
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
    char ts_string[sizeof(uint64_t)];
    uint64_t ttl = 100;
    if (c == 'a') {
      ttl = 0;
    }
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
                          true /* compression_type_missing */);
  std::unique_ptr<TableProperties> props_guard(props);

  ASSERT_OK(s);
  ASSERT_EQ(0ul, props->filter_size);
  ASSERT_EQ(16ul * 26, props->raw_key_size);
  ASSERT_EQ(36ul * 26, props->raw_value_size);
  ASSERT_EQ(26ul, props->num_entries);
  ASSERT_EQ(1ul, props->num_data_blocks);

  uint64_t nowseconds = env->NowMicros() / 1000000ul;
  if (n.ttl_ratio > 100.0) {
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), props->ratio_expire_time);
  } else if (n.ttl_ratio <= 0.0) {
    EXPECT_EQ(nowseconds, props->ratio_expire_time);
  } else {
    std::cout << "[==========]  ratio_ttl:";
    std::cout << props->ratio_expire_time - nowseconds << "s" << std::endl;
  }
  if (n.ttl_scan == std::numeric_limits<int>::max()) {
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
              props->scan_gap_expire_time);
  } else if (n.ttl_scan <= 0) {
    EXPECT_EQ(nowseconds, props->scan_gap_expire_time);
  } else {
    std::cout << "[==========]  scan_ttl:";
    std::cout << props->scan_gap_expire_time - nowseconds << "s" << std::endl;
  }
  delete options.env;
}
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}