//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#ifndef ROCKSDB_LITE

#include "db/map_builder.h"

#include <atomic>
#include <memory>

#include "db/column_family.h"
#include "db/db_impl.h"
#include "db/db_test_util.h"
#include "db/log_writer.h"
#include "db/version_set.h"
#include "db/wal_manager.h"
#include "env/mock_env.h"
#include "table/mock_table.h"
#include "util/file_reader_writer.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

class MapBuilderTest : public testing::Test {
 public:
  Env* env_;
  std::string dbname_;
  EnvOptions env_options_;
  ImmutableDBOptions db_options_;
  ColumnFamilyOptions cf_options_;
  MutableCFOptions mutable_cf_options_;
  ImmutableCFOptions ioptions_;
  std::shared_ptr<Cache> table_cache_;
  WriteController write_controller_;
  WriteBufferManager write_buffer_manager_;
  std::unique_ptr<VersionSet> versions_;
  InstrumentedMutex mutex_;
  std::shared_ptr<mock::MockTableFactory> mock_table_factory_;
  mock::MockTableFileSystem mock_table_system_;
  ColumnFamilyData* cfd_;
  VersionStorageInfo* vstorage_;
  Statistics* stats_;
  ErrorHandler error_handler_;
  SequenceNumber sequence_number;
  std::vector<FileMetaData*> files_;
  // input files to compaction process.
  std::vector<CompactionInputFiles> input_files_;
  std::vector<Range> push_range_;
  VersionEdit edit;
  MapBuilderTest()
      : env_(Env::Default()),
        dbname_(test::PerThreadDBPath("map_builder_test")),
        db_options_(),
        mutable_cf_options_(cf_options_),
        ioptions_(db_options_, cf_options_),
        table_cache_(NewLRUCache(50000, 16)),
        write_buffer_manager_(db_options_.db_write_buffer_size),
        versions_(new VersionSet(dbname_, &db_options_, env_options_,
                                 /* seq_per_batch */ false, table_cache_.get(),
                                 &write_buffer_manager_, &write_controller_)),
        mock_table_factory_(new mock::MockTableFactory()),
        stats_(nullptr),
        error_handler_(nullptr, db_options_, &mutex_),
        sequence_number(0) {
    db_options_.db_paths.emplace_back(dbname_,
                                      std::numeric_limits<uint64_t>::max());
    env_->CreateDirIfMissing(dbname_);
  }

  ~MapBuilderTest(){};

  void UpdateVersionStorageInfo() {
    vstorage_->CalculateBaseBytes(ioptions_, mutable_cf_options_);
    vstorage_->UpdateFilesByCompactionPri(ioptions_.compaction_pri);
    vstorage_->UpdateNumNonEmptyLevels();
    vstorage_->GenerateFileIndexer();
    vstorage_->GenerateLevelFilesBrief();
    vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
    // vstorage_->GenerateLevel0NonOverlapping();
    vstorage_->ComputeFilesMarkedForCompaction();
    vstorage_->SetFinalized();
  }

  void SetLastSequence(const SequenceNumber sequence_number) {
    versions_->SetLastAllocatedSequence(sequence_number + 1);
    versions_->SetLastPublishedSequence(sequence_number + 1);
    versions_->SetLastSequence(sequence_number + 1);
  }

  std::string GenerateFileName(uint64_t file_number) {
    FileMetaData meta;
    std::vector<DbPath> db_paths;
    db_paths.emplace_back(dbname_, std::numeric_limits<uint64_t>::max());
    meta.fd = FileDescriptor(file_number, 0, 0);
    return TableFileName(db_paths, meta.fd.GetNumber(), meta.fd.GetPathId());
  }

  void Init() {
    sequence_number = 0;
    vstorage_ = nullptr;
    files_.clear();
    input_files_.clear();
    edit.SetColumnFamily(0);
    edit.AddColumnFamily("aaaaaa");
    cf_options_.compaction_style = CompactionStyle::kCompactionStyleLevel;
    cf_options_.enable_lazy_compaction = true;
    cfd_ = versions_->TEST_CreateColumnFamily(cf_options_, &edit);
    vstorage_ = cfd_->current()->storage_info();
  }

  void AddMockFile(const stl_wrappers::KVMap& kv_contents, int level_,
                   bool has_range_del,
                   const stl_wrappers::KVMap& del_contents) {
    // add kv_content filemetadata
    FileMetaData* f = new FileMetaData;
    uint64_t file_number = versions_->NewFileNumber();
    f->fd = FileDescriptor(file_number, 0, 10);
    f->prop.flags |= has_range_del ? 0 : TablePropertyCache::kNoRangeDeletions;
    f->refs = 0;
    for (auto& kv : kv_contents) {
      ParsedInternalKey key;
      auto& skey = kv.first;
      ParseInternalKey(skey, &key);
      f->UpdateBoundaries(skey, key.sequence);
    }
    if (has_range_del) {
      for (auto& range : del_contents) {
        ParsedInternalKey key;
        auto& skey = range.first;
        auto& ekey = range.second;
        ParseInternalKey(skey, &key);
        RangeTombstone range_del(key, ekey);
        f->UpdateBoundariesForRange(range_del.SerializeKey(),
                                    range_del.SerializeEndKey(), key.sequence,
                                    cfd_->internal_comparator());
      }
    }

    EXPECT_OK(mock_table_factory_->CreateMockTable(
        env_, GenerateFileName(file_number), std::move(kv_contents)));

    // add reader
    mock::MockTableReader* reader;
    mock::MockTableFileSystem::FileData file_data;
    file_data.table = kv_contents;
    file_data.tombstone = del_contents;
    assert(file_number <= std::numeric_limits<uint32_t>::max());
    auto ib = mock_table_system_.files.emplace(uint32_t(file_number),
                                               std::move(file_data));
    reader = new mock::MockTableReader(ib.first->second);
    cfd_->table_cache()->TEST_AddMockTableReader(reader, f->fd);

    // add files to version
    input_files_[level_].level = level_;
    input_files_[level_].files.emplace_back(f);
    vstorage_->AddFile(level_, f);
    edit.AddFile(level_, *f);
    files_.emplace_back(f);
  }

  stl_wrappers::KVMap CreateFile(int start, int end, bool is_range_delete) {
    auto contents = mock::MakeMockFile();
    if (is_range_delete) {
      auto start_key = ToString(start);
      auto end_key = ToString(end);
      RangeTombstone range_del(start_key, end_key, ++sequence_number);
      auto key_and_value = range_del.Serialize();
      contents.insert({key_and_value.first.Encode().ToString(),
                       key_and_value.second.ToString()});
    } else {
      for (int k = start; k <= end; ++k) {
        auto key = ToString(k);
        auto value = ToString(100 * k);
        InternalKey internal_key(key, ++sequence_number, kTypeValue);
        contents.insert({internal_key.Encode().ToString(), value});
      }
    }
    SetLastSequence(sequence_number);
    return contents;
  }

  Slice FindInternalKey(const Slice& user_key_,
                        const stl_wrappers::KVMap& content_) {
    auto ucmp = cfd_->user_comparator();
    for (auto& kv : content_) {
      ParsedInternalKey key;
      auto& skey = kv.first;
      ParseInternalKey(skey, &key);
      if (ucmp->Equal(user_key_, key.user_key)) {
        return skey;
      }
    }
    return "";
  }

  bool CheckFile(FileMetaData* f) {
    Status s;
    DependenceMap empty_dependence_map;
    std::unique_ptr<InternalIterator> iter(cfd_->table_cache()->NewIterator(
        ReadOptions(), env_options_, *f, empty_dependence_map,
        nullptr /* range_del_agg */, mutable_cf_options_.prefix_extractor.get(),
        nullptr, nullptr, false /* for_compaction */, nullptr /* arena */,
        false /* skip_filter */, 1));
    s = iter->status();
    if (!s.ok()) {
      return false;
    }
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      auto value = iter->value();
      if (!value.fetch().ok()) {
        return false;
      }
      printf("%s   %s\n", iter->key().data(), value.data());
    }
    // MapSstElement element;
    // if (!element.Decode(iter->key(), value.slice())) {
    //   return false;
    // }
    //   std::string buffer = "";
    //   auto& icmp = cfd_->internal_comparator();
    //   printf("%lu   %lu\n", GetInternalKeySeqno(element.Key()),
    //          GetInternalKeySeqno(element.Value(&buffer)));
    //   printf("%lu   %lu\n", GetInternalKeySeqno(output[i].first),
    //          GetInternalKeySeqno(output[i].second));
    //   if (icmp.Compare(element.smallest_key, output[i].second) != 0 ||
    //       icmp.Compare(element.largest_key, output[i].first) != 0) {
    //     return false;
    //   }
    // }
    // if (i != output.size() || iter->Valid() || !iter->status().ok()) {
    //   return false;
    // }
    return true;
  }
};

TEST_F(MapBuilderTest, KV) {
  Init();
  MapBuilder map_builder(0, db_options_, env_options_, versions_.get(), stats_,
                         dbname_);
  input_files_.resize(4);  // compaction input files size
  stl_wrappers::KVMap kv_contents1, kv_contents2;
  kv_contents1 = CreateFile(0, 9, false /*is_range_delete*/);
  kv_contents2 = CreateFile(5, 6, false);
  stl_wrappers::KVMap del_contents1;
  AddMockFile(kv_contents1, 0, false, del_contents1);
  AddMockFile(kv_contents2, 1, false, del_contents1);
  AddMockFile(kv_contents1, 2, false, del_contents1);
  AddMockFile(kv_contents2, 3, false, del_contents1);
  UpdateVersionStorageInfo();
  std::vector<Range> deleted_range;
  std::vector<FileMetaData*> added_files;
  std::unique_ptr<FileMetaData> output_file(new FileMetaData);
  Status s = map_builder.Build(input_files_, deleted_range, added_files, 2, 0,
                               cfd_, cfd_->current(), &edit, output_file.get(),
                               nullptr, nullptr);
  CheckFile(output_file.get());
  ASSERT_OK(s);
}

TEST_F(MapBuilderTest, RangeDel) {
  Init();
  MapBuilder map_builder(0, db_options_, env_options_, versions_.get(), stats_,
                         dbname_);
  input_files_.resize(4);
  stl_wrappers::KVMap kv_contents1, kv_contents2;
  kv_contents1 = CreateFile(0, 9, false /*is_range_delete*/);
  kv_contents2 = CreateFile(5, 6, false);
  stl_wrappers::KVMap del_contents1, del_contents2;
  del_contents1 = CreateFile(6, 8, true);
  del_contents2 = CreateFile(7, 9, true);
  AddMockFile(kv_contents1, 0 /*level*/, false, del_contents1);
  AddMockFile(kv_contents2, 1, true, del_contents1);
  AddMockFile(kv_contents1, 2, true, del_contents2);
  AddMockFile(kv_contents2, 3, false, del_contents2);
  UpdateVersionStorageInfo();
  std::vector<Range> deleted_range;
  std::vector<FileMetaData*> added_files;
  std::unique_ptr<FileMetaData> output_file(new FileMetaData);
  Status s = map_builder.Build(input_files_, deleted_range, added_files, 1, 0,
                               cfd_, cfd_->current(), &edit, output_file.get(),
                               nullptr, nullptr);
  CheckFile(output_file.get());
  ASSERT_OK(s);
}

TEST_F(MapBuilderTest, DeletedRange) {
  Init();
  MapBuilder map_builder(0, db_options_, env_options_, versions_.get(), stats_,
                         dbname_);
  input_files_.resize(4);
  stl_wrappers::KVMap kv_contents1, kv_contents2;
  kv_contents1 = CreateFile(0, 9, false /*is_range_delete*/);
  kv_contents2 = CreateFile(5, 6, false);
  stl_wrappers::KVMap del_contents1, del_contents2;
  del_contents1 = CreateFile(6, 8, true);
  del_contents2 = CreateFile(7, 9, true);
  AddMockFile(kv_contents1, 0 /*level*/, false, del_contents1);
  AddMockFile(kv_contents2, 1, true, del_contents1);
  AddMockFile(kv_contents1, 2, true, del_contents2);
  AddMockFile(kv_contents2, 3, false, del_contents2);
  UpdateVersionStorageInfo();
  std::vector<Range> deleted_range({Range(Slice("3"), Slice("4"))});
  std::vector<FileMetaData*> added_files;
  std::unique_ptr<FileMetaData> output_file(new FileMetaData);
  Status s = map_builder.Build(input_files_, deleted_range, added_files, 1, 0,
                               cfd_, cfd_->current(), &edit, output_file.get(),
                               nullptr, nullptr);
  CheckFile(output_file.get());
  ASSERT_OK(s);
}

TEST_F(MapBuilderTest, AddedFiles) {
  Init();
  MapBuilder map_builder(0, db_options_, env_options_, versions_.get(), stats_,
                         dbname_);
  input_files_.resize(4);
  stl_wrappers::KVMap kv_contents1, kv_contents2;
  kv_contents1 = CreateFile(0, 9, false /*is_range_delete*/);
  kv_contents2 = CreateFile(5, 6, false);
  stl_wrappers::KVMap del_contents1, del_contents2;
  del_contents1 = CreateFile(6, 8, true);
  del_contents2 = CreateFile(7, 9, true);
  AddMockFile(kv_contents1, 0 /*level*/, false, del_contents1);
  AddMockFile(kv_contents2, 1, true, del_contents1);
  AddMockFile(kv_contents1, 2, true, del_contents2);
  AddMockFile(kv_contents2, 3, false, del_contents2);
  UpdateVersionStorageInfo();
  std::vector<Range> deleted_range({Range(Slice("3"), Slice("4"))});
  std::vector<FileMetaData*> added_files;
  added_files.emplace_back(files_[1]);  // files_[1] has range deletion
  std::unique_ptr<FileMetaData> output_file(new FileMetaData);
  Status s = map_builder.Build(input_files_, deleted_range, added_files, 1, 0,
                               cfd_, cfd_->current(), &edit, output_file.get(),
                               nullptr, nullptr);
  CheckFile(output_file.get());
  ASSERT_OK(s);
}

TEST_F(MapBuilderTest, MapSstInput) {
  Init();
  MapBuilder map_builder(0, db_options_, env_options_, versions_.get(), stats_,
                         dbname_);
  input_files_.resize(4);
  stl_wrappers::KVMap kv_contents1, kv_contents2;
  kv_contents1 = CreateFile(0, 9, false /*is_range_delete*/);
  kv_contents2 = CreateFile(5, 6, false);
  stl_wrappers::KVMap del_contents1, del_contents2;
  del_contents1 = CreateFile(6, 8, true);
  del_contents2 = CreateFile(7, 9, true);
  AddMockFile(kv_contents1, 0 /*level*/, false, del_contents1);
  AddMockFile(kv_contents2, 1, true, del_contents1);
  AddMockFile(kv_contents1, 2, true, del_contents2);
  AddMockFile(kv_contents2, 3, false, del_contents2);
  UpdateVersionStorageInfo();
  std::vector<Range> deleted_range({Range(Slice("3"), Slice("4"))});
  std::vector<FileMetaData*> added_files;
  added_files.emplace_back(files_[1]);  // files_[1] has range deletion
  std::unique_ptr<FileMetaData> output_file(new FileMetaData);
  Status s = map_builder.Build(input_files_, deleted_range, added_files, 1, 0,
                               cfd_, cfd_->current(), &edit, output_file.get(),
                               nullptr, nullptr);
  // kNoRangeDeletions=0 kHasSnapshots=0 kMapHandleRangeDeletions=0
  output_file.get()->prop.flags = 0;
  input_files_[0].files.emplace_back(output_file.get());
  s = map_builder.Build(input_files_, deleted_range, added_files, 1, 0, cfd_,
                        cfd_->current(), &edit, output_file.get(), nullptr,
                        nullptr);
  CheckFile(output_file.get());
  ASSERT_OK(s);
}

// only one dependence file
TEST_F(MapBuilderTest, NoNeedBuildMapSst) {
  Init();
  MapBuilder map_builder(0, db_options_, env_options_, versions_.get(), stats_,
                         dbname_);
  input_files_.resize(1);  // compaction input files size
  stl_wrappers::KVMap kv_contents;
  kv_contents = CreateFile(0, 9, false /*is_range_delete*/);
  stl_wrappers::KVMap del_contents;
  AddMockFile(kv_contents, 0, false, del_contents);
  UpdateVersionStorageInfo();
  std::vector<Range> deleted_range;
  std::vector<FileMetaData*> added_files;
  std::unique_ptr<FileMetaData> output_file(new FileMetaData);
  Status s = map_builder.Build(input_files_, deleted_range, added_files, 1, 0,
                               cfd_, cfd_->current(), &edit, output_file.get(),
                               nullptr, nullptr);
  ASSERT_LE(output_file->fd.GetNumber(), 0);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#endif  //  ROCKSDB_LITE
