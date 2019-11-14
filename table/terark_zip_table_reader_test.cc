//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <functional>

#include "db/db_test_util.h"
#include "table/terark_zip_table.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"

namespace rocksdb {

namespace {

std::string get_key(size_t i) {
  char buffer[32];
  snprintf(buffer, sizeof buffer, "%04zd", i);
  return buffer;
}

std::string get_value(size_t i) {
  char const* str = "0123456789QWERTYUIOPASDFGHJKLZXCVBNM";
  return get_key(i) + (str + (i % (strlen(str) - 1)));
}

std::string get_value(size_t i, size_t len) {
  char const* str = "0123456789QWERTYUIOPASDFGHJKLZXCVBNM";
  std::string value = get_key(i);
  size_t size = (strlen(str) - 1);
  while (len-- > 0) {
    value += str[i++ % size];
  }
  return value;
}

std::string get_clean_value(size_t i, size_t len) {
  char const* str = "0123456789QWERTYUIOPASDFGHJKLZXCVBNM";
  std::string value;
  size_t size = (strlen(str) - 1);
  while (len-- > 0) {
    value += str[i++ % size];
  }
  return value;
}
}

class TerarkZipReaderTest : public DBTestBase {
public:
  TerarkZipReaderTest() : DBTestBase("/terark_zip_reader_test") {}

  void CheckApproximateOffset(bool rev, Iterator* it) {
    size_t offset = 0;
    size_t i = 0;
    std::string MAX_KEY = "\255\255\255\255\255\255\255\255\255\255";
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      Range r;
      if (rev) {
        r.start = Slice(MAX_KEY);
        r.limit = it->key();
      }
      else {
        r.start = Slice();
        r.limit = it->key();
      }
      uint64_t size;
      db_->GetApproximateSizes(&r, 1, &size);
      ASSERT_GE(size, offset);
      offset = size;
      ++i;
    }
  }

  void BasicTest(bool rev, size_t count, uint32_t prefix, size_t blockUnits,
                 uint32_t minValue, size_t valueLen = 0) {
    Options options = CurrentOptions();
    TerarkZipTableOptions tzto;
    tzto.disableSecondPassIter = false;
    tzto.keyPrefixLen = prefix;
    tzto.offsetArrayBlockUnits = (uint16_t)blockUnits;
    tzto.minDictZipValueSize = minValue;
    tzto.entropyAlgo = TerarkZipTableOptions::kFSE;
    tzto.localTempDir = dbname_;
    options.allow_mmap_reads = true;
    if (rev) {
      options.comparator = ReverseBytewiseComparator();
    }
    else {
      options.comparator = BytewiseComparator();
    }
    options.table_factory.reset(NewTerarkZipTableFactory(tzto, nullptr));
    DestroyAndReopen(options);
    rocksdb::ReadOptions ro;
    rocksdb::WriteOptions wo;
    rocksdb::FlushOptions fo;
    std::string value;

    auto db = db_;

    auto gen_value = [valueLen](size_t i) {
        return valueLen == 0 ? get_value(i) : get_clean_value(i, valueLen);
    };

    for (size_t i = 0; i < count; ++i) {
      ASSERT_OK(db->Put(wo, get_key(i), gen_value(i)));
    }
    ASSERT_OK(db->Flush(fo));
    for (size_t i = count / 2; i < count; ++i) {
      ASSERT_OK(db->Delete(wo, get_key(i)));
    }
    ASSERT_OK(db->Flush(fo));

    for (size_t i = count / 2; i < count; ++i) {
      ASSERT_TRUE(db->Get(ro, get_key(i), &value).IsNotFound());
    }
    for (size_t i = 0; i < count / 2; ++i) {
      ASSERT_OK(db->Get(ro, get_key(i), &value));
      ASSERT_EQ(value, gen_value(i));
    }
    auto it = db->NewIterator(ro);
    auto forward = [&](size_t i, int d, size_t e) {
      for (it->SeekToFirst(); it->Valid(); it->Next()) {
        ASSERT_EQ(it->key().ToString(), get_key(i));
        ASSERT_EQ(it->value().ToString(), gen_value(i));
        i += d;
      }
      ASSERT_EQ(i, e);
    };
    auto backward = [&](size_t i, int d, size_t e) {
      for (it->SeekToLast(); it->Valid(); it->Prev()) {
        ASSERT_EQ(it->key().ToString(), get_key(i));
        ASSERT_EQ(it->value().ToString(), gen_value(i));
        i += d;
      }
      ASSERT_EQ(i, e);
    };
    if (rev) {
      forward(count / 2 - 1, -1, size_t(-1));
      backward(0, 1, count / 2);
    } else {
      forward(0, 1, count / 2);
      backward(count / 2 - 1, -1, size_t(-1));
    }
    CheckApproximateOffset(rev, it);
    delete it;
  }
  void HardZipTest(bool rev, size_t count, uint32_t prefix) {
    Options options = CurrentOptions();
    TerarkZipTableOptions tzto;
    tzto.disableSecondPassIter = false;
    tzto.keyPrefixLen = prefix;
    tzto.localTempDir = dbname_;
    tzto.minDictZipValueSize = 0;
    tzto.singleIndexMinSize = 512;
    tzto.singleIndexMaxSize = 512;
    options.allow_mmap_reads = true;
    if (rev) {
      options.comparator = ReverseBytewiseComparator();
    } else {
      options.comparator = BytewiseComparator();
    }
    options.table_factory.reset(NewTerarkZipTableFactory(tzto, nullptr));
    DestroyAndReopen(options);
    rocksdb::ReadOptions ro;
    rocksdb::WriteOptions wo;
    rocksdb::FlushOptions fo;
    std::string value, value_get;

    auto db = db_;
    auto seed = *(uint64_t*)"__Terark";
    std::mt19937_64 mt;
    mt.seed(seed);
    std::uniform_int_distribution<size_t> uid(16, 64);
    count = (count + 7) / 8;
    for (size_t sst = 0; sst < 8; ++sst) {
      for (size_t i = sst, e = i + 8 * count; i < e; i += 8) {
        value.resize(uid(mt) * 8);
        for (size_t l = 0; l < value.size(); l += 8) {
          *(uint64_t*)(&value.front() + l) = mt();
        }
        ASSERT_OK(db->Put(wo, get_key(i), value));
      }
      ASSERT_OK(db->Flush(fo));
    }
    ASSERT_OK(
        db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr));
    mt.seed(seed);
    for (size_t sst = 0; sst < 8; ++sst) {
      for (size_t i = sst, e = i + 8 * count; i < e; i += 8) {
        value.resize(uid(mt) * 8);
        for (size_t l = 0; l < value.size(); l += 8) {
          *(uint64_t*)(&value.front() + l) = mt();
        }
        ASSERT_OK(db->Get(ro, get_key(i), &value_get));
        ASSERT_EQ(value, value_get);
      }
    }
    auto it = db->NewIterator(ro);
    CheckApproximateOffset(rev, it);
    delete it;
  }
  enum BlobStoreType {
    PlainBlobStore,
    MixedLenBlobStore,
    ZipOffsetBlobStore,
  };
  void NonCompressedBlobStoreTest(bool rev, BlobStoreType type, size_t count,
                                  uint32_t prefix) {
    Options options = CurrentOptions();
    TerarkZipTableOptions tzto;
    tzto.disableSecondPassIter = false;
    tzto.keyPrefixLen = prefix;
    tzto.entropyAlgo = TerarkZipTableOptions::kFSE;
    tzto.localTempDir = dbname_;
    tzto.minDictZipValueSize = 4096;
    options.allow_mmap_reads = true;
    switch (type) {
    case PlainBlobStore:
    case MixedLenBlobStore:
      tzto.offsetArrayBlockUnits = 0;
      break;
    case ZipOffsetBlobStore:
      tzto.offsetArrayBlockUnits = 64;
      break;
    default:
      assert(0); // WTF ?
      return;
    }
    if (rev) {
      options.comparator = ReverseBytewiseComparator();
    }
    else {
      options.comparator = BytewiseComparator();
    }
    options.table_factory.reset(NewTerarkZipTableFactory(tzto, nullptr));
    options.disable_auto_compactions = true;
    DestroyAndReopen(options);
    rocksdb::ReadOptions ro;
    rocksdb::WriteOptions wo;
    rocksdb::FlushOptions fo;
    std::string value;
    auto get_value_by_type = [type](size_t i)->std::string {
      switch (type) {
      case PlainBlobStore:
      case ZipOffsetBlobStore:
        return get_value(i);
      case MixedLenBlobStore:
        if (i % 64 != 0) {
          return get_value(i, 10);
        }
        else {
          return get_value(i);
        }
      default:
        assert(0); // WTF ?
        return "";
      }
    };

    auto db = db_;

    count = (count + 3) / 4;
    for (size_t sst = 0; sst < 2; ++sst)
    {
      for (size_t i = sst * count, e = i + count; i < e; ++i)
      {
        ASSERT_OK(db->Put(wo, get_key(i), get_value_by_type(i)));
      }
      ASSERT_OK(db->Flush(fo));
    }
    ASSERT_OK(db->Put(wo, get_key(count * 2), get_value_by_type(count * 2)));
    ASSERT_OK(db->Flush(fo));
    rocksdb::ColumnFamilyMetaData meta;
    db->GetColumnFamilyMetaData(&meta);
    ASSERT_EQ(meta.levels[0].files.size(), 3);
    db->CompactFiles(CompactionOptions(), {
        meta.levels[0].files[0].name,
        meta.levels[0].files[1].name,
        meta.levels[0].files[2].name,
    }, 1);
    ASSERT_OK(db->Delete(wo, get_key(count * 2)));
    ASSERT_OK(db->Flush(fo));
    db->GetColumnFamilyMetaData(&meta);
    ASSERT_EQ(meta.levels[0].files.size(), 1);
    ASSERT_EQ(meta.levels[1].files.size(), 1);
    db->CompactFiles(CompactionOptions(), {
        meta.levels[0].files[0].name,
        meta.levels[1].files[0].name,
    }, 2);

    count *= 2;
    for (size_t i = 0; i < count; ++i)
    {
      ASSERT_OK(db->Get(ro, get_key(i), &value));
      ASSERT_EQ(value, get_value_by_type(i));
    }
    auto it = db->NewIterator(ro);
    auto forward = [&](size_t i, int d, size_t e) {
      for (it->SeekToFirst(); it->Valid(); it->Next())
      {
        ASSERT_EQ(it->key().ToString(), get_key(i));
        ASSERT_EQ(it->value().ToString(), get_value_by_type(i));
        i += d;
      }
      ASSERT_EQ(i, e);
    };
    auto backward = [&](size_t i, int d, size_t e) {
      for (it->SeekToLast(); it->Valid(); it->Prev())
      {
        ASSERT_EQ(it->key().ToString(), get_key(i));
        ASSERT_EQ(it->value().ToString(), get_value_by_type(i));
        i += d;
      }
      ASSERT_EQ(i, e);
    };
    if (rev) {
      forward(count - 1, -1, size_t(-1));
      backward(0, 1, count);
    }
    else {
      forward(0, 1, count);
      backward(count - 1, -1, size_t(-1));
    }
    CheckApproximateOffset(rev, it);
    delete it;
  }
  void ZeroLengthBlobStoreTest(bool rev, size_t count, uint32_t prefix) {
      Options options = CurrentOptions();
      TerarkZipTableOptions tzto;
      tzto.disableSecondPassIter = false;
      tzto.keyPrefixLen = prefix;
      tzto.entropyAlgo = TerarkZipTableOptions::kFSE;
      tzto.localTempDir = dbname_;
      options.allow_mmap_reads = true;
      if (rev) {
          options.comparator = ReverseBytewiseComparator();
      }
      else {
          options.comparator = BytewiseComparator();
      }
      options.table_factory.reset(NewTerarkZipTableFactory(tzto, nullptr));
      options.disable_auto_compactions = true;
      DestroyAndReopen(options);
      rocksdb::ReadOptions ro;
      rocksdb::WriteOptions wo;
      rocksdb::FlushOptions fo;
      std::string value;

      auto db = db_;

      count = (count + 3) / 4;
      for (size_t sst = 0; sst < 2; ++sst)
      {
          for (size_t i = sst * count, e = i + count; i < e; ++i)
          {
              ASSERT_OK(db->Put(wo, get_key(i), ""));
          }
          ASSERT_OK(db->Flush(fo));
      }
      ASSERT_OK(db->Put(wo, get_key(count * 2), ""));
      ASSERT_OK(db->Flush(fo));
      rocksdb::ColumnFamilyMetaData meta;
      db->GetColumnFamilyMetaData(&meta);
      ASSERT_EQ(meta.levels[0].files.size(), 3);
      db->CompactFiles(CompactionOptions(), {
          meta.levels[0].files[0].name,
          meta.levels[0].files[1].name,
          meta.levels[0].files[2].name,
      }, 1);
      ASSERT_OK(db->Delete(wo, get_key(count * 2)));
      ASSERT_OK(db->Flush(fo));
      db->GetColumnFamilyMetaData(&meta);
      ASSERT_EQ(meta.levels[0].files.size(), 1);
      ASSERT_EQ(meta.levels[1].files.size(), 1);
      db->CompactFiles(CompactionOptions(), {
          meta.levels[0].files[0].name,
          meta.levels[1].files[0].name,
      }, 2);

      count *= 2;
      for (size_t i = 0; i < count; ++i)
      {
          ASSERT_OK(db->Get(ro, get_key(i), &value));
          ASSERT_EQ(value, "");
      }
      auto it = db->NewIterator(ro);
      auto forward = [&](size_t i, int d, size_t e) {
          for (it->SeekToFirst(); it->Valid(); it->Next())
          {
              ASSERT_EQ(it->key().ToString(), get_key(i));
              ASSERT_EQ(it->value().ToString(), "");
              i += d;
          }
          ASSERT_EQ(i, e);
      };
      auto backward = [&](size_t i, int d, size_t e) {
          for (it->SeekToLast(); it->Valid(); it->Prev())
          {
              ASSERT_EQ(it->key().ToString(), get_key(i));
              ASSERT_EQ(it->value().ToString(), "");
              i += d;
          }
          ASSERT_EQ(i, e);
      };
      if (rev) {
          forward(count - 1, -1, size_t(-1));
          backward(0, 1, count);
      }
      else {
          forward(0, 1, count);
          backward(count - 1, -1, size_t(-1));
      }
      CheckApproximateOffset(rev, it);
      delete it;
  }
  void IterTest(std::initializer_list<const char*> data_list,
                std::initializer_list<const char*> test_list,
                bool rev, uint32_t prefix, size_t blockUnits, uint32_t minValue,
                size_t indexMem = 2ULL << 30) {
    Options options = CurrentOptions();
    TerarkZipTableOptions tzto;
    tzto.disableSecondPassIter = false;
    tzto.keyPrefixLen = prefix;
    tzto.offsetArrayBlockUnits = (uint16_t)blockUnits;
    tzto.minDictZipValueSize = minValue;
    tzto.singleIndexMinSize = indexMem;
    tzto.singleIndexMaxSize = indexMem;
    tzto.entropyAlgo = TerarkZipTableOptions::kFSE;
    tzto.localTempDir = dbname_;
    options.allow_mmap_reads = true;
    if (rev) {
      options.comparator = ReverseBytewiseComparator();
    }
    else {
      options.comparator = BytewiseComparator();
    }
    options.table_factory.reset(NewTerarkZipTableFactory(tzto, nullptr));
    DestroyAndReopen(options);
    rocksdb::ReadOptions ro1, ro2, ro3, ro4;
    rocksdb::WriteOptions wo;
    rocksdb::FlushOptions fo;

    struct comp_t
    {
      bool greater = 0;
      bool operator()(const std::string& l, const std::string& r) const
      {
        if (greater)
        {
          return l > r;
        }
        else
        {
          return l < r;
        }
      }
    } comp;
    comp.greater = rev;
    std::set<std::string, comp_t> key_set(comp);
    auto db = db_;
    auto put = [&](std::string&& k, const std::string& s)
    {
      ASSERT_OK(db->Put(wo, k, k + s));
    };
    for (auto d : data_list)
    {
      key_set.emplace(d);
      put(d, "-1");
    }
    auto s1 = db->GetSnapshot();
    ro1.snapshot = s1;
    for (auto d : data_list)
    {
      ASSERT_OK(db->Delete(wo, d));
    }
    auto s2 = db->GetSnapshot();
    ro2.snapshot = s2;
    for (auto d : data_list)
    {
      put(d, "-3");
    }
    auto s3 = db->GetSnapshot();
    for (auto d : data_list)
    {
      ASSERT_OK(db->Delete(wo, d));
    }
    ro3.snapshot = s3;

    ASSERT_OK(db->Flush(fo));

    auto i1 = db->NewIterator(ro1);
    auto i2 = db->NewIterator(ro2);
    auto i3 = db->NewIterator(ro3);
    auto i4 = db->NewIterator(ro4);

    i2->SeekToFirst();
    ASSERT_FALSE(i2->Valid());
    i2->SeekToLast();
    ASSERT_FALSE(i2->Valid());
    i4->SeekToFirst();
    ASSERT_FALSE(i4->Valid());
    i4->SeekToLast();
    ASSERT_FALSE(i4->Valid());

    std::string value;
    for (auto it = key_set.begin(); it != key_set.end(); ++it) {
      ASSERT_OK(db->Get(ro1, *it, &value));
      ASSERT_EQ(value, *it + "-1");
      ASSERT_TRUE(db->Get(ro2, *it, &value).IsNotFound());
      ASSERT_OK(db->Get(ro3, *it, &value));
      ASSERT_EQ(value, *it + "-3");
      ASSERT_TRUE(db->Get(ro4, *it, &value).IsNotFound());
    }

    auto basic_test = [&](Iterator *i, std::string s)
    {
      i->SeekToFirst();
      for (auto it = key_set.begin(); it != key_set.end(); ++it, i->Next()) {
        ASSERT_EQ(i->value().ToString(), *it + s);
      }
      ASSERT_FALSE(i->Valid());
      i->SeekToLast();
      for (auto it = key_set.rbegin(); it != key_set.rend(); ++it, i->Prev()) {
        ASSERT_EQ(i->value().ToString(), *it + s);
      }
      ASSERT_FALSE(i->Valid());
    };
    basic_test(i1, "-1");
    basic_test(i3, "-3");

    auto test = [&](const std::string& s)
    {
      auto lb = key_set.lower_bound(s);
      i1->Seek(s);
      i2->Seek(s);
      i3->Seek(s);
      i4->Seek(s);
      if (lb == key_set.end())
      {
        ASSERT_FALSE(i1->Valid());
        ASSERT_FALSE(i3->Valid());
      }
      else
      {
        ASSERT_EQ(i1->value().ToString(), *lb + "-1");
        ASSERT_EQ(i3->value().ToString(), *lb + "-3");
      }
      ASSERT_FALSE(i2->Valid());
      ASSERT_FALSE(i4->Valid());
      auto up = key_set.upper_bound(s);
      i1->SeekForPrev(s);
      i2->SeekForPrev(s);
      i3->SeekForPrev(s);
      i4->SeekForPrev(s);
      if (up == key_set.begin())
      {
        ASSERT_FALSE(i1->Valid());
        ASSERT_FALSE(i3->Valid());
      }
      else
      {
        --up;
        ASSERT_EQ(i1->value().ToString(), *up + "-1");
        ASSERT_EQ(i3->value().ToString(), *up + "-3");
      }
      ASSERT_FALSE(i2->Valid());
      ASSERT_FALSE(i4->Valid());
    };
    for (auto t : test_list)
    {
      test(t);
    }

    CheckApproximateOffset(rev, i1);
    CheckApproximateOffset(rev, i2);
    CheckApproximateOffset(rev, i3);
    CheckApproximateOffset(rev, i4);
    delete i1;
    delete i2;
    delete i3;
    delete i4;
    db->ReleaseSnapshot(s1);
    db->ReleaseSnapshot(s2);
    db->ReleaseSnapshot(s3);
  }
};

TEST_F(TerarkZipReaderTest, BasicTest                        )
{ BasicTest(false, 1000, 0,   0,    0); }
TEST_F(TerarkZipReaderTest, BasicTestRev                     )
{ BasicTest(true , 1000, 0,   0,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMulti                   )
{ BasicTest(false, 1000, 1,   0,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiRev                )
{ BasicTest(true , 1000, 1,   0,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiUint               )
{ BasicTest(false, 1000, 2,   0,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiUintRev            )
{ BasicTest(true , 1000, 2,   0,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiMany               )
{ BasicTest(false,  333, 3,   0,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiManyRev            )
{ BasicTest(true ,  333, 3,   0,    0); }
TEST_F(TerarkZipReaderTest, BasicTestDictZipZO64             )
{ BasicTest(false,  100, 0,  64,    0); }
TEST_F(TerarkZipReaderTest, BasicTestDictZipZO64Rev          )
{ BasicTest(true ,  100, 0,  64,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiDictZipZO64        )
{ BasicTest(false,  100, 3,  64,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiDictZipZO64Rev     )
{ BasicTest(true ,  100, 3,  64,    0); }
TEST_F(TerarkZipReaderTest, BasicTestDictZipZO128            )
{ BasicTest(false,  100, 0, 128,    0); }
TEST_F(TerarkZipReaderTest, BasicTestDictZipZO128Rev         )
{ BasicTest(true ,  100, 0, 128,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiDictZipZO128       )
{ BasicTest(false,  100, 3, 128,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiDictZipZO128Rev    )
{ BasicTest(true ,  100, 3, 128,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiManyDictZipZO128   )
{ BasicTest(false,   33, 4, 128,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiManyDictZipZO128Rev)
{ BasicTest(true ,   33, 4, 128,    0); }
TEST_F(TerarkZipReaderTest, BasicTestZipOffset64             )
{ BasicTest(false,  100, 0,  64, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestZipOffset64Rev          )
{ BasicTest(true ,  100, 0,  64, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestMultiZipOffset64        )
{ BasicTest(false,  100, 3,  64, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestMultiZipOffset64Rev     )
{ BasicTest(true ,  100, 3,  64, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestZipOffset128            )
{ BasicTest(false,  100, 0, 128, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestZipOffset128Rev         )
{ BasicTest(true ,  100, 0, 128, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestMultiZipOffset128       )
{ BasicTest(false,  100, 3, 128, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestMultiZipOffset128Rev    )
{ BasicTest(true ,  100, 3, 128, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestMultiManyZipOffset128   )
{ BasicTest(false,   33, 4, 128, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestMultiManyZipOffset128Rev)
{ BasicTest(true ,   33, 4, 128, 1024); }

TEST_F(TerarkZipReaderTest, BasicTestShortDictZipZO128   )
{ BasicTest(false, 2000, 1, 128,    0,    4); }
TEST_F(TerarkZipReaderTest, BasicTestShortDictZipZO128Rev)
{ BasicTest(true , 2000, 1, 128,    0,    4); }
TEST_F(TerarkZipReaderTest, BasicTestShortZipOffset128   )
{ BasicTest(false, 2000, 1, 128, 1024,    4); }
TEST_F(TerarkZipReaderTest, BasicTestShortZipOffset128Rev)
{ BasicTest(true , 2000, 1, 128, 1024,    4); }
TEST_F(TerarkZipReaderTest, BasicTestLongDictZipZO128    )
{ BasicTest(false, 2000, 1, 128,    0, 4096); }
TEST_F(TerarkZipReaderTest, BasicTestLongDictZipZO128Rev )
{ BasicTest(true , 2000, 1, 128,    0, 4096); }
TEST_F(TerarkZipReaderTest, BasicTestLongZipOffset128    )
{ BasicTest(false, 2000, 1, 128, 8192, 4096); }
TEST_F(TerarkZipReaderTest, BasicTestLongZipOffset128Rev )
{ BasicTest(true , 2000, 1, 128, 8192, 4096); }

TEST_F(TerarkZipReaderTest, HardZipTest            )
{ HardZipTest(false, 1000, 0); }
TEST_F(TerarkZipReaderTest, HardZipTestRev         )
{ HardZipTest(true , 1000, 0); }
TEST_F(TerarkZipReaderTest, HardZipTestMulti       )
{ HardZipTest(false, 1000, 1); }
TEST_F(TerarkZipReaderTest, HardZipTestMultiRev    )
{ HardZipTest(true , 1000, 1); }
TEST_F(TerarkZipReaderTest, HardZipTestMultiMany   )
{ HardZipTest(false,  333, 2); }
TEST_F(TerarkZipReaderTest, HardZipTestMultiManyRev)
{ HardZipTest(true ,  333, 2); }

TEST_F(TerarkZipReaderTest, ZeroLengthBlobStoreTest            )
{ ZeroLengthBlobStoreTest(false, 1000, 0); }
TEST_F(TerarkZipReaderTest, ZeroLengthBlobStoreTestRev         )
{ ZeroLengthBlobStoreTest(true , 1000, 0); }
TEST_F(TerarkZipReaderTest, ZeroLengthBlobStoreTestMulti       )
{ ZeroLengthBlobStoreTest(false, 1000, 1); }
TEST_F(TerarkZipReaderTest, ZeroLengthBlobStoreTestMultiRev    )
{ ZeroLengthBlobStoreTest(true , 1000, 1); }
TEST_F(TerarkZipReaderTest, ZeroLengthBlobStoreTestMultiMany   )
{ ZeroLengthBlobStoreTest(false,  333, 2); }
TEST_F(TerarkZipReaderTest, ZeroLengthBlobStoreTestMultiManyRev)
{ ZeroLengthBlobStoreTest(true ,  333, 2); }

TEST_F(TerarkZipReaderTest, PlainBlobStoreTest            )
{ NonCompressedBlobStoreTest(false, PlainBlobStore, 1000, 0); }
TEST_F(TerarkZipReaderTest, PlainBlobStoreTestRev         )
{ NonCompressedBlobStoreTest(true , PlainBlobStore, 1000, 0); }
TEST_F(TerarkZipReaderTest, PlainBlobStoreTestMulti       )
{ NonCompressedBlobStoreTest(false, PlainBlobStore, 1000, 1); }
TEST_F(TerarkZipReaderTest, PlainBlobStoreTestMultiRev    )
{ NonCompressedBlobStoreTest(true , PlainBlobStore, 1000, 1); }
TEST_F(TerarkZipReaderTest, PlainBlobStoreTestMultiMany   )
{ NonCompressedBlobStoreTest(false, PlainBlobStore,  333, 2); }
TEST_F(TerarkZipReaderTest, PlainBlobStoreTestMultiManyRev)
{ NonCompressedBlobStoreTest(true , PlainBlobStore,  333, 2); }

TEST_F(TerarkZipReaderTest, MixedBlobStoreTest            )
{ NonCompressedBlobStoreTest(false, MixedLenBlobStore, 1000, 0); }
TEST_F(TerarkZipReaderTest, MixedBlobStoreTestRev         )
{ NonCompressedBlobStoreTest(true , MixedLenBlobStore, 1000, 0); }
TEST_F(TerarkZipReaderTest, MixedBlobStoreTestMulti       )
{ NonCompressedBlobStoreTest(false, MixedLenBlobStore, 1000, 1); }
TEST_F(TerarkZipReaderTest, MixedBlobStoreTestMultiRev    )
{ NonCompressedBlobStoreTest(true , MixedLenBlobStore, 1000, 1); }
TEST_F(TerarkZipReaderTest, MixedBlobStoreTestMultiMany   )
{ NonCompressedBlobStoreTest(false, MixedLenBlobStore,  333, 2); }
TEST_F(TerarkZipReaderTest, MixedBlobStoreTestMultiManyRev)
{ NonCompressedBlobStoreTest(true , MixedLenBlobStore,  333, 2); }

TEST_F(TerarkZipReaderTest, ZipOffsetBlobStoreTest            )
{ NonCompressedBlobStoreTest(false, ZipOffsetBlobStore, 1000, 0); }
TEST_F(TerarkZipReaderTest, ZipOffsetBlobStoreTestRev         )
{ NonCompressedBlobStoreTest(true , ZipOffsetBlobStore, 1000, 0); }
TEST_F(TerarkZipReaderTest, ZipOffsetBlobStoreTestMulti       )
{ NonCompressedBlobStoreTest(false, ZipOffsetBlobStore, 1000, 1); }
TEST_F(TerarkZipReaderTest, ZipOffsetBlobStoreTestMultiRev    )
{ NonCompressedBlobStoreTest(true , ZipOffsetBlobStore, 1000, 1); }
TEST_F(TerarkZipReaderTest, ZipOffsetBlobStoreTestMultiMany   )
{ NonCompressedBlobStoreTest(false, ZipOffsetBlobStore,  333, 2); }
TEST_F(TerarkZipReaderTest, ZipOffsetBlobStoreTestMultiManyRev)
{ NonCompressedBlobStoreTest(true , ZipOffsetBlobStore,  333, 2); }

TEST_F(TerarkZipReaderTest, SingleRecordTest) {
  auto data_list = {
    "0000AAAAXXXX",
  };
  IterTest(data_list, {}, false, 0, 0, 1024);
  IterTest(data_list, {}, true , 0, 0, 1024);
  IterTest(data_list, {}, false, 4, 0, 1024);
  IterTest(data_list, {}, true , 4, 0, 1024);
}


TEST_F(TerarkZipReaderTest, UIntIteratorTest) {
  auto data_list = {
    "0000AAAAXXXX",
    "0000AAAAYYYY",
    "0000AAAAZZZZ",
  };
  auto test_list = {
    "",
    "#",
    "0",
    "0000",
    "00000",
    "0000A",
    "0A",
    "0AAA",
    "0AAAA",
    "0000B",
    "0000AB",
    "0000AAAA",
    "0000AAAAA",
    "0000ABBB",
    "0000ABBBB",
    "0000AAAAX",
    "0000AAAAXY",
    "0000AAAAXXXX",
    "0000AAAAXXXXX",
    "1",
    "1111",
    "11111",
  };
  IterTest(data_list, test_list, false, 0, 0, 1024);
  IterTest(data_list, test_list, true , 0, 0, 1024);
  IterTest(data_list, test_list, false, 1, 0, 1024);
  IterTest(data_list, test_list, true , 1, 0, 1024);
  IterTest(data_list, test_list, false, 2, 0, 1024);
  IterTest(data_list, test_list, true , 2, 0, 1024);
  IterTest(data_list, test_list, false, 3, 0, 1024);
  IterTest(data_list, test_list, true , 3, 0, 1024);
  IterTest(data_list, test_list, false, 4, 0, 1024);
  IterTest(data_list, test_list, true , 4, 0, 1024);
}

TEST_F(TerarkZipReaderTest, PrefixTest) {
  auto data_list = {
    "0000AAAAXXXX",
    "1111BBBBYYYY",
    "2222CCCCZZZZ",
  };
  auto test_list = {
    "",
    "#",
    "0",
    "0000",
    "00000",
    "0000A",
    "0A",
    "0AAA",
    "0AAAA",
    "0000B",
    "0000AB",
    "0000AAAA",
    "0000AAAAA",
    "0000ABBB",
    "0000ABBBB",
    "0000AAAAX",
    "0000AAAAXY",
    "0000AAAAXXXX",
    "0000AAAAXXXXX",
    "1",
    "1111",
    "11111",
  };
  IterTest(data_list, test_list, false, 0, 0, 1024);
  IterTest(data_list, test_list, true , 0, 0, 1024);
  IterTest(data_list, test_list, false, 1, 0, 1024);
  IterTest(data_list, test_list, true , 1, 0, 1024);
  IterTest(data_list, test_list, false, 2, 0, 1024);
  IterTest(data_list, test_list, true , 2, 0, 1024);
  IterTest(data_list, test_list, false, 3, 0, 1024);
  IterTest(data_list, test_list, true , 3, 0, 1024);
  IterTest(data_list, test_list, false, 4, 0, 1024);
  IterTest(data_list, test_list, true , 4, 0, 1024);
  IterTest(data_list, test_list, false, 7, 0, 1024);
  IterTest(data_list, test_list, true , 7, 0, 1024);
  IterTest(data_list, test_list, false, 8, 0, 1024);
  IterTest(data_list, test_list, true , 8, 0, 1024);
  IterTest(data_list, test_list, false, 9, 0, 1024);
  IterTest(data_list, test_list, true , 9, 0, 1024);
}

TEST_F(TerarkZipReaderTest, PrefixMoreTest) {
  auto data_list = {
    "####0000",
    "####0001",
    "####0002",
    "####0009",
    "0000AAAAXXXX",
    "0000AAAAYYYY",
    "0000AAAAZZZZ",
    "1111AAAA",
    "1111BBBB",
    "1111CCCC",
    "AAAA",
    "BBBB",
    "CCCC",
  };
  auto test_list = {
    "",
    "#",
    "####",
    "#####",
    "####0",
    "####000",
    "####0000",
    "####00000",
    "####0001",
    "####0003",
    "####1000",
    "####A",
    "0",
    "0000",
    "00000",
    "0000A",
    "0A",
    "0AAA",
    "0AAAA",
    "0000B",
    "0000AB",
    "0000AAAA",
    "0000AAAAA",
    "0000ABBB",
    "0000ABBBB",
    "0000AAAAX",
    "0000AAAAXY",
    "0000AAAAXXXX",
    "0000AAAAXXXXX",
    "1",
    "1111",
    "11111",
    "1111A",
    "1A",
    "1AAA",
    "1AAAA",
    "1111AAAA",
    "1111AAAAA",
    "1111AB",
    "1111ABBB",
    "1111ABBBB",
    "2",
    "2222",
    "22222",
    "2222A",
    "A",
    "AAAA",
    "AAAAA",
    "AB",
    "ABBB",
    "ABBBB",
    "D",
    "DDDD",
    "DDDDD",
  };
  IterTest(data_list, test_list, false, 0, 0, 1024);
  IterTest(data_list, test_list, true , 0, 0, 1024);
  IterTest(data_list, test_list, false, 1, 0, 1024);
  IterTest(data_list, test_list, true , 1, 0, 1024);
  IterTest(data_list, test_list, false, 2, 0, 1024);
  IterTest(data_list, test_list, true , 2, 0, 1024);
  IterTest(data_list, test_list, false, 3, 0, 1024);
  IterTest(data_list, test_list, true , 3, 0, 1024);
  IterTest(data_list, test_list, false, 4, 0, 1024);
  IterTest(data_list, test_list, true , 4, 0, 1024);
  IterTest(data_list, test_list, false, 0, 0, 1024, 1);
  IterTest(data_list, test_list, true , 0, 0, 1024, 1);
  IterTest(data_list, test_list, false, 1, 0, 1024, 1);
  IterTest(data_list, test_list, true , 1, 0, 1024, 1);
  IterTest(data_list, test_list, false, 2, 0, 1024, 1);
  IterTest(data_list, test_list, true , 2, 0, 1024, 1);
  IterTest(data_list, test_list, false, 3, 0, 1024, 1);
  IterTest(data_list, test_list, true , 3, 0, 1024, 1);
  IterTest(data_list, test_list, false, 4, 0, 1024, 1);
  IterTest(data_list, test_list, true , 4, 0, 1024, 1);
}

TEST_F(TerarkZipReaderTest, StoreBuildTest) {
  auto data_list = {
    "0000AAAAXXXX",
    "1111BBBBYYYY",
    "2222CCCCZZZZ",
  };
  auto test_list = {
    "",
    "0",
    "2222",
  };
  IterTest(data_list, test_list, false, 0,  0,    0);
  IterTest(data_list, test_list, true , 0,  0,    0);
  IterTest(data_list, test_list, false, 0, 64,    0);
  IterTest(data_list, test_list, true , 0, 64,    0);
  IterTest(data_list, test_list, false, 0,  0, 1024);
  IterTest(data_list, test_list, true , 0,  0, 1024);
  IterTest(data_list, test_list, false, 0, 64, 1024);
  IterTest(data_list, test_list, true , 0, 64, 1024);
  IterTest(data_list, test_list, false, 4,  0,    0);
  IterTest(data_list, test_list, true , 4,  0,    0);
  IterTest(data_list, test_list, false, 4, 64,    0);
  IterTest(data_list, test_list, true , 4, 64,    0);
  IterTest(data_list, test_list, false, 4,  0, 1024);
  IterTest(data_list, test_list, true , 4,  0, 1024);
  IterTest(data_list, test_list, false, 4, 64, 1024);
  IterTest(data_list, test_list, true , 4, 64, 1024);
}

TEST_F(TerarkZipReaderTest, IndexBuildTest) {
  auto data_list =
  {
    "0000AAAAWWWW",
    "0000AAAAXXX",
    "0000AAAAYY",
    "0000AAAAZ",
    "1111BBBB",
    "1111CCC",
    "1111DD",
    "1111E",
    "2222",
  };
  auto test_list =
  {
    "",
    "3",
    "0000AAAAZZZZ",
  };
  IterTest(data_list, test_list, false, 0,  0, 1024);
  IterTest(data_list, test_list, true , 0,  0, 1024);
  IterTest(data_list, test_list, false, 0, 64, 1024);
  IterTest(data_list, test_list, true , 0, 64, 1024);
  IterTest(data_list, test_list, false, 4,  0, 1024);
  IterTest(data_list, test_list, true , 4,  0, 1024);
  IterTest(data_list, test_list, false, 4, 64, 1024);
  IterTest(data_list, test_list, true , 4, 64, 1024);
  IterTest(data_list, test_list, false, 0,  0, 1024, 1);
  IterTest(data_list, test_list, true , 0,  0, 1024, 1);
  IterTest(data_list, test_list, false, 0, 64, 1024, 1);
  IterTest(data_list, test_list, true , 0, 64, 1024, 1);
  IterTest(data_list, test_list, false, 4,  0, 1024, 1);
  IterTest(data_list, test_list, true , 4,  0, 1024, 1);
  IterTest(data_list, test_list, false, 4, 64, 1024, 1);
  IterTest(data_list, test_list, true , 4, 64, 1024, 1);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  fprintf(stderr, "exit in 5 seconds\n");
  std::this_thread::sleep_for(std::chrono::seconds(5));
  return ret;
}