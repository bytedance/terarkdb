// Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License.

#ifndef ROCKSDB_LITE

#include <algorithm>
#include <string>
#include <vector>

#include "db/db_impl.h"
#include "db/db_test_util.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/transaction_log.h"
#include "util/file_util.h"
#include "util/string_util.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE
class RepairTest : public DBTestBase {
   public:
    RepairTest() : DBTestBase("/repair_test") {}

    std::string GetFirstSstPath() {
        uint64_t manifest_size;
        std::vector<std::string> files;
        db_->GetLiveFiles(files, &manifest_size);
        auto sst_iter = std::find_if(
            files.begin(), files.end(), [](const std::string& file) {
                uint64_t number;
                FileType type;
                bool ok = ParseFileName(file, &number, &type);
                return ok && type == kTableFile;
            });
        return sst_iter == files.end() ? "" : dbname_ + *sst_iter;
    }
};

TEST_F(RepairTest, RepairColumnFamilyOptions0) {
    // Verify repair logic uses correct ColumnFamilyOptions when repairing a
    // database with different options for column families.
    const int kNumCfs = 4;
    const int kEntriesPerCf = 2;

    Options opts(CurrentOptions()), rev_opts(CurrentOptions());
    // opts.comparator = BytewiseComparator();
    // rev_opts.comparator = ReverseBytewiseComparator();
    // rev_opts.comparator = BytewiseComparator();
    // opts.comparator = ReverseBytewiseComparator();

    DestroyAndReopen(opts);
    CreateColumnFamilies({"normal"}, opts);
    CreateColumnFamilies({"reverse_default"}, rev_opts);
    CreateColumnFamilies({"reverse"}, rev_opts);
    ReopenWithColumnFamilies(
        {"default", "normal", "reverse_default", "reverse"},
        std::vector<Options>{opts, opts, rev_opts, rev_opts});
    for (int i = 0; i < kNumCfs; ++i) {
        for (int j = 0; j < kEntriesPerCf; ++j) {
            Put(i, "key" + ToString(i) + ToString(j), "val" + ToString(j));
            if (j == kEntriesPerCf - 1 && (i % kNumCfs == 0)) {
                // Leave one unflushed on default
                continue;
            }
            Flush(i);
        }
    }
    Close();

    // RepairDB() records the comparator in the manifest, and DB::Open would
    // fail if a different comparator were used.
    ASSERT_OK(RepairDB(dbname_, opts,
                       {{"default", opts},
                        {"normal", opts},
                        {"reverse_default", rev_opts},
                        {"reverse", rev_opts}},
                       opts /* unknown_cf_opts */));

    ASSERT_OK(TryReopenWithColumnFamilies(
        {"default", "normal", "reverse_default", "reverse"},
        std::vector<Options>{opts, opts, rev_opts, rev_opts}));
    for (int i = 0; i < kNumCfs; ++i) {
        for (int j = 0; j < kEntriesPerCf; ++j) {
            ASSERT_EQ(Get(i, "key" + ToString(i) + ToString(j)),
                      "val" + ToString(j));
        }
    }

    // Examine table properties to verify RepairDB() used the right options when
    // converting WAL->SST
    TablePropertiesCollection fname_to_props;
    db_->GetPropertiesOfAllTables(handles_[1], &fname_to_props);
    // ASSERT_EQ(fname_to_props.size(), 2U);//take care to change it
    for (const auto& fname_and_props : fname_to_props) {
        std::string comparator_name(
            InternalKeyComparator(rev_opts.comparator).Name());
        comparator_name = comparator_name.substr(comparator_name.find(':') + 1);
        ASSERT_EQ(comparator_name, fname_and_props.second->comparator_name);
    }
    Close();

    // Also check comparator when it's provided via "unknown" CF options
    ASSERT_OK(RepairDB(dbname_, opts, {{"default", opts}},
                       rev_opts /* unknown_cf_opts */));
    ASSERT_OK(TryReopenWithColumnFamilies(
        {"default", "normal", "reverse_default", "reverse"},
        std::vector<Options>{opts, opts, rev_opts, rev_opts}));
    for (int i = 0; i < kNumCfs; ++i) {
        for (int j = 0; j < kEntriesPerCf; ++j) {
            ASSERT_EQ(Get(i, "key" + ToString(i) + ToString(j)),
                      "val" + ToString(j));
        }
    }
}

TEST_F(RepairTest, RepairColumnFamilyOptions1) {
    // Verify repair logic uses correct ColumnFamilyOptions when repairing a
    // database with different options for column families.
    const int kNumCfs = 1;
    const int kEntriesPerCf = 2;

    Options opts(CurrentOptions()), rev_opts(CurrentOptions());
    // opts.comparator = BytewiseComparator();
    // rev_opts.comparator = ReverseBytewiseComparator();
    // rev_opts.comparator = BytewiseComparator();
    opts.comparator = ReverseBytewiseComparator();

    DestroyAndReopen(opts);
    ReopenWithColumnFamilies({"default"}, std::vector<Options>{opts});
    for (int i = 0; i < kNumCfs; ++i) {
        for (int j = 0; j < kEntriesPerCf; ++j) {
            Put(i, "key" + ToString(i) + ToString(j), "val" + ToString(j));
            if (j == kEntriesPerCf - 1 && (i % kNumCfs == 0)) {
                // Leave one unflushed on default
                continue;
            }
            Flush(i);
        }
    }
    Close();

    // RepairDB() records the comparator in the manifest, and DB::Open would
    // fail if a different comparator were used.
    ASSERT_OK(RepairDB(dbname_, opts, {{"default", opts}},
                       opts /* unknown_cf_opts */));

    ASSERT_OK(
        TryReopenWithColumnFamilies({"default"}, std::vector<Options>{opts}));
    for (int i = 0; i < kNumCfs; ++i) {
        for (int j = 0; j < kEntriesPerCf; ++j) {
            ASSERT_EQ(Get(i, "key" + ToString(i) + ToString(j)),
                      "val" + ToString(j));
        }
    }

    // Examine table properties to verify RepairDB() used the right options when
    // converting WAL->SST
    // TablePropertiesCollection fname_to_props;
    // db_->GetPropertiesOfAllTables(handles_[1], &fname_to_props);
    // // ASSERT_EQ(fname_to_props.size(), 2U);//take care to change it
    // for (const auto& fname_and_props : fname_to_props) {
    //     std::string comparator_name(
    //         InternalKeyComparator(rev_opts.comparator).Name());
    //     comparator_name = comparator_name.substr(comparator_name.find(':') +
    //     1); ASSERT_EQ(comparator_name,
    //     fname_and_props.second->comparator_name);
    // }
    Close();

    // Also check comparator when it's provided via "unknown" CF options
    ASSERT_OK(RepairDB(dbname_, opts, {{"default", opts}},
                       rev_opts /* unknown_cf_opts */));
    ASSERT_OK(
        TryReopenWithColumnFamilies({"default"}, std::vector<Options>{opts}));
    for (int i = 0; i < kNumCfs; ++i) {
        for (int j = 0; j < kEntriesPerCf; ++j) {
            ASSERT_EQ(Get(i, "key" + ToString(i) + ToString(j)),
                      "val" + ToString(j));
        }
    }
}

TEST_F(RepairTest, RepairColumnFamilyOptions2) {
    // Verify repair logic uses correct ColumnFamilyOptions when repairing a
    // database with different options for column families.
    const int kNumCfs = 3;
    const int kEntriesPerCf = 2;

    Options opts(CurrentOptions()), rev_opts(CurrentOptions());
    // opts.comparator = BytewiseComparator();
    rev_opts.comparator = ReverseBytewiseComparator();
    // rev_opts.comparator = BytewiseComparator();
    // opts.comparator = ReverseBytewiseComparator();

    DestroyAndReopen(opts);
    CreateColumnFamilies({"normal"}, opts);
    CreateColumnFamilies({"reverse_default"}, rev_opts);
    // CreateColumnFamilies({"reverse"}, rev_opts);
    ReopenWithColumnFamilies(
        {"default", "normal", "reverse_default"},     //, "reverse"},
        std::vector<Options>{opts, opts, rev_opts});  //, rev_opts});
    for (int i = 0; i < kNumCfs; ++i) {
        for (int j = 0; j < kEntriesPerCf; ++j) {
            Put(i, "key" + ToString(i) + ToString(j), "val" + ToString(j));
            // if (j == kEntriesPerCf - 1 && (i % 2 == 1)) {
            //     // Leave one unflushed on default
            //     continue;
            // }
            Flush(i);
        }
    }
    Close();

    // RepairDB() records the comparator in the manifest, and DB::Open would
    // fail if a different comparator were used.
    ASSERT_OK(RepairDB(dbname_, opts,
                           {{"default", opts},
                            {"normal", opts},
                            {"reverse_default", rev_opts}/*,
                            {"reverse", rev_opts}*/},
                           opts /* unknown_cf_opts */));

    ASSERT_OK(TryReopenWithColumnFamilies(
        {"default", "normal", "reverse_default" /*, "reverse"*/},
        std::vector<Options>{opts, opts, rev_opts /* rev_opts*/}));
    for (int i = 0; i < kNumCfs; ++i) {
        for (int j = 0; j < kEntriesPerCf; ++j) {
            ASSERT_EQ(Get(i, "key" + ToString(i) + ToString(j)),
                      "val" + ToString(j));
        }
    }

    // Examine table properties to verify RepairDB() used the right options
    // when converting WAL->SST
    TablePropertiesCollection fname_to_props;
    db_->GetPropertiesOfAllTables(handles_[1], &fname_to_props);
    // ASSERT_EQ(fname_to_props.size(), 2U);//take care to change it
    for (const auto& fname_and_props : fname_to_props) {
        std::string comparator_name(
            InternalKeyComparator(rev_opts.comparator).Name());
        comparator_name = comparator_name.substr(comparator_name.find(':') + 1);
        ASSERT_EQ(comparator_name, fname_and_props.second->comparator_name);
    }
    Close();

    // Also check comparator when it's provided via "unknown" CF options
    ASSERT_OK(RepairDB(dbname_, opts, {{"default", opts}},
                       rev_opts /* unknown_cf_opts */));
    ASSERT_OK(TryReopenWithColumnFamilies(
        {"default", "normal", "reverse_default" /*, "reverse"*/},
        std::vector<Options>{opts, opts, rev_opts /*, rev_opts*/}));
    for (int i = 0; i < kNumCfs; ++i) {
        for (int j = 0; j < kEntriesPerCf; ++j) {
            ASSERT_EQ(Get(i, "key" + ToString(i) + ToString(j)),
                      "val" + ToString(j));
        }
    }
}

#endif
}  // namespace rocksdb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
    fprintf(stderr, "SKIPPED as RepairDB is not supported in ROCKSDB_LITE\n");
    return 0;
}

#endif  // ROCKSDB_LITE
