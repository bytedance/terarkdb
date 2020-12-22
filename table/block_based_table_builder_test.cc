// Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License.

#include "table/block_based_table_builder.h"

#include "include/rocksdb/ttl_extractor.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

int kTtlLength = sizeof(uint64_t);

class TestTtlExtractor : public TtlExtractor {
  //
  Status Extract(EntryType entry_type, const Slice& user_key,
                 const Slice& value_or_meta, bool* has_ttl,
                 std::chrono::seconds* ttl) const {
    if (entry_type == EntryType::kEntryPut ||
        entry_type == EntryType::kEntryMerge) {
      *has_ttl = true;
      assert(value_or_meta.size() > kTtlLength);
      *ttl = static_cast<std::chrono::seconds>(DecodeFixed64(
          value_or_meta.data() + value_or_meta.size() - kTtlLength));
    }
    *has_ttl = false;
    return Status::OK();
  }
};

class TestTtlExtractorFactory : public TtlExtractorFactory {
  using TtlContext = TtlExtractorContext;

  virtual std::unique_ptr<TtlExtractor> CreateTtlExtractor(
      const TtlContext& context) const {
    return std::make_unique<TestTtlExtractor>();
  }

  virtual const char* Name() const { return "TestTtlExtractorFactor"; }

  virtual Status Serialize(std::string* /*bytes*/) const {
    return Status::NotSupported();
  }
};

class BlockBasedTableBuilderTest : public testing::Test {
  //
};

TEST_F(BlockBasedTableBuilderTest, SimpleTest) {
  BlockBasedTableOptions blockbasedtableoptions;
  BlockBasedTableFactory factory(blockbasedtableoptions);

  test::StringSink sink;
  std::unique_ptr<WritableFileWriter> file_writer(
      test::GetWritableFileWriter(new test::StringSink(), "" /* don't care */));
  Options options;
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

  // test::StringSink* ss =
  //     static_cast<test::StringSink*>(file_writer->writable_file());
  // std::unique_ptr<RandomAccessFileReader> file_reader(
  //     test::GetRandomAccessFileReader(
  //         new test::StringSource(ss->contents(), 72242, true)));

  // TableProperties* props = nullptr;
  // auto s = ReadTableProperties(file_reader.get(), ss->contents().size(),
  //                              kBlockBasedTableMagicNumber, ioptions, &props,
  //                              true /* compression_type_missing */);
  // std::unique_ptr<TableProperties> props_guard(props);
  // ASSERT_OK(s);

  // ASSERT_EQ(0ul, props->index_size);
  // ASSERT_EQ(0ul, props->filter_size);
  // ASSERT_EQ(16ul * 26, props->raw_key_size);
  // ASSERT_EQ(28ul * 26, props->raw_value_size);
  // ASSERT_EQ(26ul, props->num_entries);
  // ASSERT_EQ(1ul, props->num_data_blocks);
}
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}