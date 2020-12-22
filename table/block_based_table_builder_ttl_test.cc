// Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License.

// #include "include/rocksdb/ttl_extractor.h"
#include "table/block_based_table_builder.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {
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

class BlockBasedTableBuilderTest : public testing::Test {
  //
};

TEST_F(BlockBasedTableBuilderTest, SimpleTest1) {
  BlockBasedTableOptions blockbasedtableoptions;
  BlockBasedTableFactory factory(blockbasedtableoptions);

  test::StringSink sink;
  std::unique_ptr<WritableFileWriter> file_writer(
      test::GetWritableFileWriter(new test::StringSink(), "" /* don't care */));
  Options options;

  std::string dbname = test::PerThreadDBPath("block_based_table_builder_test");
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

  // Env* env = Env::Default();
  // options.db_log_dir = std::string("log");
  // options.info_log = nullptr;
  // env->CreateDirIfMissing(options.db_log_dir);
  // if (options.info_log == nullptr) {
  //   Status s = CreateLoggerFromOptions(".", options, &options.info_log);
  //   if (!s.ok()) {
  //     // No place suitable for logging
  //     options.info_log = nullptr;
  //   }
  // }
  const ImmutableCFOptions ioptions(options);
  const MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>>
      int_tbl_prop_collector_factories;
  //   int_tbl_prop_collector_factories.emplace_back(
  //       make_unique<BlockBasedTableBuilder::BlockBasedTablePropertiesCollector>(
  //           BlockBasedTableOptions::IndexType::kBinarySearch, false, false));
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
    key.append("\1       ");  // PlainTable expects internal key structure
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
  // delete env;
  // env = nullptr;
  delete options.env;
}

TEST_F(BlockBasedTableBuilderTest, SimpleTest2) {
  BlockBasedTableOptions blockbasedtableoptions;
  BlockBasedTableFactory factory(blockbasedtableoptions);

  test::StringSink sink;
  std::unique_ptr<WritableFileWriter> file_writer(
      test::GetWritableFileWriter(new test::StringSink(), "" /* don't care */));
  Options options;

  std::string dbname = test::PerThreadDBPath("block_based_table_builder_test");
  ASSERT_OK(DestroyDB(dbname, options));

  DB* db = nullptr;
  TestEnv* env = new TestEnv();
  options.info_log.reset(new TestEnv::TestLogger(env));
  options.create_if_missing = true;
  options.env = env;
  options.ttl_garbage_collection_percentage = 50.0;
  options.ttl_scan_gap = 10;
  options.ttl_extractor_factory.reset(new test::TestTtlExtractorFactory());
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  s = db->Close();
  delete db;

  // Env* env = Env::Default();
  // options.db_log_dir = std::string("log");
  // options.info_log = nullptr;
  // env->CreateDirIfMissing(options.db_log_dir);
  // if (options.info_log == nullptr) {
  //   Status s = CreateLoggerFromOptions(".", options, &options.info_log);
  //   if (!s.ok()) {
  //     // No place suitable for logging
  //     options.info_log = nullptr;
  //   }
  // }
  const ImmutableCFOptions ioptions(options);
  const MutableCFOptions moptions(options);

  InternalKeyComparator ikc(options.comparator);
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>>
      int_tbl_prop_collector_factories;
  //   int_tbl_prop_collector_factories.emplace_back(
  //       make_unique<BlockBasedTableBuilder::BlockBasedTablePropertiesCollector>(
  //           BlockBasedTableOptions::IndexType::kBinarySearch, false, false));
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
    key.append("\1       ");  // PlainTable expects internal key structure
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
  env->SleepForMicroseconds(2000000);
  std::cout << props->ratio_expire_time << std::endl;
  std::cout << props->scan_gap_expire_time << std::endl;
  std::cout << env->NowMicros() / 1000000ul << std::endl;
  // delete env;
  // env = nullptr;
  delete options.env;
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}