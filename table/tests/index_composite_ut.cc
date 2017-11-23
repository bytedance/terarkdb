
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <terark/io/FileStream.hpp>
#include <terark/rank_select.hpp>

#include "../terark_zip_common.h"
#include "../terark_zip_index.h"

using namespace std;

using std::string;
using std::unique_ptr;

using terark::byte_t;
using terark::fstring;
using terark::valvec;
using terark::valvec_no_init;
using terark::valvec_reserve;

using terark::FileStream;
using terark::InputBuffer;
using terark::OutputBuffer;
using terark::LittleEndianDataInput;
using terark::LittleEndianDataOutput;

using rocksdb::TerarkIndex;

namespace rocksdb {
  struct TerarkZipTableOptions {};

  class FileWriter {
  public:
    std::string path;
    FileStream  fp;
    NativeDataOutput<OutputBuffer> writer;
    ~FileWriter() {}
    void open() {
      fp.open(path.c_str(), "wb+");
      fp.disbuf();
      writer.attach(&fp);
    }
    void close() {
      writer.flush_buffer();
      fp.close();
    }
  };
}

/*
 * index1st: uint64, index2nd: uint64
 */
vector<string> keys;
string key_path;
string index_path;
TerarkIndex::KeyStat stat;
enum DataStored {
  standard_ascend = 0,
  standard_descend,
  standard_allzero,
};

void clear() {
  keys.clear();
  key_path.clear();
  index_path.clear();
  memset(&stat, 0, sizeof(stat));
}

TerarkIndex* save_reload(TerarkIndex* index, 
                         const TerarkIndex::Factory* factory) {
  FileStream writer(index_path, "wb");
  index->SaveMmap([&writer](const void* data, size_t size) {
      writer.ensureWrite(data, size);
    });
  writer.flush();
  writer.close();
  
  return TerarkIndex::LoadFile(index_path).release();
}

/*
 * il256 il256
 */
void init_data_il256_il256_ascend() {
  rocksdb::FileWriter fwriter;
  fwriter.path = key_path;
  fwriter.open();
  keys.resize(100);
  char carr[16] = { 0 };
  for (int i = 0; i < 100; i++) {
    keys[i] = string(carr, carr + 16);
  }
	for (int i = 0; i < 10; i += 2) {
    carr[7] = i;
    for (int j = 0; j < 10; j++) {
      carr[15] = j;
      // keep the last 10 elem for 'Find Fail Test'
      if (i < 9) {
        fwriter.writer << fstring(carr, 16);
      }
      if (i == 0 && j == 0) {
        stat.minKey.assign(carr, carr + 16);
      } else if (i == 8 && j == 9) {
        stat.maxKey.assign(carr, carr + 16);
      }
      keys[i * 10 + j] = string(carr, 16);
    }
	}
  fwriter.close();
  stat.numKeys = 50;
  stat.commonPrefixLen = 0;
  stat.minKeyLen = 16;
  stat.maxKeyLen = 16;
  stat.sumKeyLen = 16 * 50;
}

void init_data_il256_il256_descend() {
    rocksdb::FileWriter fwriter;
  fwriter.path = key_path;
  fwriter.open();
  keys.resize(100);
  char carr[16] = { 0 };
  for (int i = 0; i < 100; i++) {
    keys[i] = string(carr, carr + 16);
  }
	for (int i = 8; i >= 0; i -= 2) {
    carr[7] = i;
    for (int j = 9; j >= 0; j--) {
      carr[15] = j;
      fwriter.writer << fstring(carr, 16);
      if (i == 0 && j == 0) {
        stat.maxKey.assign(carr, carr + 16);
      } else if (i == 8 && j == 9) {
        stat.minKey.assign(carr, carr + 16);
      }
      keys[i * 10 + j] = string(carr, 16);
    }
	}
  fwriter.close();
  stat.numKeys = 50;
  stat.commonPrefixLen = 0;
  stat.minKeyLen = 16;
  stat.maxKeyLen = 16;
  stat.sumKeyLen = 16 * 50;
}

void test_il256_il256(DataStored dtype) {
  // init data
  clear();
  key_path = "./tmp_key.txt";
  if (dtype == standard_ascend) {
    printf("==== Ascend Test started\n");
    init_data_il256_il256_ascend();
  } else if (dtype == standard_descend) {
    printf("==== Descend Test started\n");
    init_data_il256_il256_descend();
  } else {
    assert(0);
  }
  // build index
  FileStream fp(key_path, "rb");
  NativeDataInput<InputBuffer> tempKeyFileReader(&fp);
  auto factory = rocksdb::TerarkIndex::GetFactory("CompositeUintIndex_IL_256_32_IL_256_32");
  rocksdb::TerarkZipTableOptions tableOpt;
  TerarkIndex* index = factory->Build(tempKeyFileReader, tableOpt, stat);
  assert(index->Name() == string("CompositeUintIndex_IL_256_32_IL_256_32"));
  printf("\tbuild done\n");
  // save & reload
  index_path = "./tmp_index.txt";
  index = save_reload(index, factory);
  printf("\tsave & reload done\n");
  // search test
  for (size_t i = 0; i < 10; i += 2) {
    for (size_t j = 0; j < 10; j++) {
      size_t idx = i * 10 + j;
      size_t expected = i / 2 * 10 + j;
      size_t result = index->Find(keys[idx]);
      assert(result == expected);
    }
  }
  {
    char arr[16] = { 0 };
    for (size_t i = 1; i < 10; i += 2) {
      arr[7] = i;
      for (size_t j = 0; j < 10; j++) {
        arr[15] = j;
        assert(index->Find(arr) == size_t(-1));
      }
    }
  }
  printf("\tFind done\n");
  // iterator
  auto iter = index->NewIterator();
  {
    // seek to 1st, next()
    char arr[16] = { 0 };
    assert(iter->SeekToFirst());
    assert(iter->DictRank() == 0);
    assert(fstring(arr, 16) == iter->key());
    for (int i = 0; i < 10; i += 2) {
      arr[7] = i;
      for (int j = 0; j < 10; j++) {
        arr[15] = j;
        if (i == 0 && j == 0)
          continue;
        assert(iter->Next());
        assert(iter->DictRank() == i / 2 * 10 + j);
        assert(fstring(arr, 16) == iter->key());
      }
    }
    assert(iter->Next() == false);
  }
  {
    // seek to last, prev()
    char arr[16] = { 0 };
    assert(iter->SeekToLast());
    assert(iter->DictRank() == 49);
    for (int i = 8; i >= 0; i -= 2) {
      arr[7] = i;
      for (int j = 9; j >= 0; j--) {
        arr[15] = j;
        if (i == 8 && j == 9) {
          assert(fstring(arr, 16) == iter->key());
          continue;
        }
        assert(iter->Prev());
        assert(iter->DictRank() == i / 2 * 10 + j);
        assert(fstring(arr, 16) == iter->key());
      }
    }
    assert(iter->Prev() == false);
  }
  {
    // seek matches
    char arr[16] = { 0 };
    for (int i = 0; i < 9; i += 2) {
      arr[7] = i;
      for (int j = 9; j >= 0; j--) {
        arr[15] = j;
        int idx = i * 10 + j;
        int expected = i / 2 * 10 + j;
        assert(iter->Seek(keys[idx]));
        assert(iter->DictRank() == expected);
        assert(fstring(arr, 16) == iter->key());
      }
    }
  }
  {
    // seek larger than larger @apple
    char arr[16] = { 0 };
    arr[7] = 20; arr[15] = 0;
    assert(iter->Seek(fstring(arr, 16)) == false);
    // smaller than smaller
    char sarr[4] = { 0 };
    assert(iter->Seek(fstring(sarr, 4)));
    assert(iter->DictRank() == 0);
    // 
    char marr[12] = { 0 };
    marr[7] = 4;
    assert(iter->Seek(fstring(marr, 12)));
    assert(iter->DictRank() == 2 * 10 + 0);
    arr[7] = 4; arr[15] = 0;
    assert(fstring(arr, 16) == iter->key());
  }
  {
    // lower_bound
    char arr[17] = { 0 };
    arr[16] = 1;
    for (int i = 0; i < 9; i += 2) {
      arr[7] = i;
      for (int j = 0; j < 9; j++) {
        if (i == 8 && j == 9)
          break;
        arr[15] = j;
        int expected = i / 2 * 10 + j + 1;
        assert(iter->Seek(fstring(arr, 17)));
        assert(iter->DictRank() == expected);
      }
    }
  }
  {
    // cross index1st boundary lower_bound
    char arr[16] = { 0 };
    arr[7] = 4; arr[15] = 14;
    assert(iter->Seek(fstring(arr, 16)));
    int expected = (4 + 2) / 2 * 10;
    assert(iter->DictRank() == expected);

    arr[7] = 8; arr[15] = 9;
    assert(iter->Seek(fstring(arr, 16)));
    arr[7] = 8; arr[15] = 10;
    assert(iter->Seek(fstring(arr, 16)) == false);
  }
  printf("\tIterator done\n");
  ::remove(key_path.c_str());
  ::remove(index_path.c_str());
}


/*
 * allone il256
 */
void init_data_allone_il256_ascend() {
  rocksdb::FileWriter fwriter;
  fwriter.path = key_path;
  fwriter.open();
  keys.resize(110);
  char carr[16] = { 0 };
	for (int i = 0; i < 11; i++) {
    carr[7] = i;
    for (int j = 0; j < 10; j++) {
      carr[15] = j;
      // keep the last 10 elem for 'Find Fail Test'
      if (i < 10) {
        fwriter.writer << fstring(carr, 16);
      }
      if (i == 0 && j == 0) {
        stat.minKey.assign(carr, carr + 16);
      } else if (i == 9 && j == 9) {
        stat.maxKey.assign(carr, carr + 16);
      }
      keys[i * 10 + j] = string(carr, 16);
    }
	}
  fwriter.close();
  stat.numKeys = 100;
  stat.commonPrefixLen = 0;
  stat.minKeyLen = 16;
  stat.maxKeyLen = 16;
  stat.sumKeyLen = 16 * 100;
}

void init_data_allone_il256_descend() {
  rocksdb::FileWriter fwriter;
  fwriter.path = key_path;
  fwriter.open();
  keys.resize(110);
  char carr[16] = { 0 };
	for (int i = 10; i >= 0; i--) {
    carr[7] = i;
    for (int j = 9; j >= 0; j--) {
      carr[15] = j;
      // keep the last 10 elem for 'Find Fail Test'
      if (i < 10) {
        fwriter.writer << fstring(carr, 16);
      }
      if (i == 0 && j == 0) {
        stat.maxKey.assign(carr, carr + 16);
      } else if (i == 9 && j == 9) {
        stat.minKey.assign(carr, carr + 16);
      }
      keys[i * 10 + j] = string(carr, 16);
    }
	}
  fwriter.close();
  stat.numKeys = 100;
  stat.commonPrefixLen = 0;
  stat.minKeyLen = 16;
  stat.maxKeyLen = 16;
  stat.sumKeyLen = 16 * 100;
}

void test_allone_il256(DataStored dtype) {
  // init data
  clear();
  key_path = "./tmp_key.txt";
  if (dtype == standard_ascend) {
    printf("==== Ascend Test started\n");
    init_data_allone_il256_ascend();
  } else if (dtype == standard_descend) {
    printf("==== Descend Test started\n");
    init_data_allone_il256_descend();
  } else {
    assert(0);
  }
  // build index
  FileStream fp(key_path, "rb");
  NativeDataInput<InputBuffer> tempKeyFileReader(&fp);
  auto factory = rocksdb::TerarkIndex::GetFactory("CompositeUintIndex_IL_256_32_IL_256_32");
  rocksdb::TerarkZipTableOptions tableOpt;
  TerarkIndex* index = factory->Build(tempKeyFileReader, tableOpt, stat);
  assert(index->Name() == string("CompositeUintIndex_AllOne_IL_256_32"));
  printf("\tbuild done\n");
  // save & reload
  index_path = "./tmp_index.txt";
  index = save_reload(index, factory);
  printf("\tsave & reload done\n");
  // search test
  for (size_t idx = 0; idx < stat.numKeys; idx++) {
    size_t result = index->Find(keys[idx]);
    assert(idx == result);
  }
  for (size_t idx = stat.numKeys; idx < 110; idx++) {
    size_t result = index->Find(keys[idx]);
    assert(result == size_t(-1));
  }
  printf("\tFind done\n");
  // iterator
  auto iter = index->NewIterator();
  {
    // seek to 1st, next()
    char arr[16] = { 0 };
    assert(iter->SeekToFirst());
    assert(iter->DictRank() == 0);
    assert(fstring(arr, 16) == iter->key());
    for (int i = 0; i < 10; i++) {
      arr[7] = i;
      for (int j = 0; j < 10; j++) {
        arr[15] = j;
        if (i == 0 && j == 0)
          continue;
        assert(iter->Next());
        assert(iter->DictRank() == i * 10 + j);
        assert(fstring(arr, 16) == iter->key());
      }
    }
    assert(iter->Next() == false);
  }
  {
    // seek to last, prev()
    char arr[16] = { 0 };
    assert(iter->SeekToLast());
    assert(iter->DictRank() == 99);
    for (int i = 9; i >= 0; i--) {
      arr[7] = i;
      for (int j = 9; j >= 0; j--) {
        arr[15] = j;
        if (i == 9 && j == 9) {
          assert(fstring(arr, 16) == iter->key());
          continue;
        }
        assert(iter->Prev());
        assert(iter->DictRank() == i * 10 + j);
        assert(fstring(arr, 16) == iter->key());
      }
    }
    assert(iter->Prev() == false);
  }
  {
    // seek matches
    char arr[16] = { 0 };
    for (int i = 0; i < 9; i++) {
      arr[7] = i;
      for (int j = 9; j >= 0; j--) {
        arr[15] = j;
        int idx = i * 10 + j;
        assert(iter->Seek(keys[idx]));
        assert(iter->DictRank() == idx);
        assert(fstring(arr, 16) == iter->key());
      }
    }
  }
  {
    // seek larger than larger @apple
    char arr[16] = { 0 };
    arr[7] = 20; arr[15] = 0;
    assert(iter->Seek(fstring(arr, 16)) == false);
    // smaller than smaller
    char sarr[4] = { 0 };
    assert(iter->Seek(fstring(sarr, 4)));
    assert(iter->DictRank() == 0);
    // 
    char marr[12] = { 0 };
    marr[7] = 4;
    assert(iter->Seek(fstring(marr, 12)));
    assert(iter->DictRank() == 4 * 10 + 0);
    arr[7] = 4; arr[15] = 0;
    assert(fstring(arr, 16) == iter->key());
  }
  {
    // lower_bound
    char arr[17] = { 0 };
    arr[16] = 1;
    for (int i = 0; i < 9; i++) {
      arr[7] = i;
      for (int j = 0; j < 9; j++) {
        if (i == 9 && j == 9)
          break;
        arr[15] = j;
        int idx = i * 10 + j + 1;
        assert(iter->Seek(fstring(arr, 17)));
        assert(iter->DictRank() == idx);
      }
    }
  }
  {
    // cross index1st boundary lower_bound
    char arr[16] = { 0 };
    arr[7] = 4; arr[15] = 14;
    assert(iter->Seek(fstring(arr, 16)));
    int idx = (4 + 1) * 10;
    assert(iter->DictRank() == idx);

    arr[7] = 9; arr[15] = 9;
    assert(iter->Seek(fstring(arr, 16)));
    arr[7] = 9; arr[15] = 10;
    assert(iter->Seek(fstring(arr, 16)) == false);
  }
  printf("\tIterator done\n");
  ::remove(key_path.c_str());
  ::remove(index_path.c_str());
}

/*
 * allone allzero
 */
void init_data_allone_allzero() {
  rocksdb::FileWriter fwriter;
  fwriter.path = key_path;
  fwriter.open();
  keys.resize(110);
  char carr[16] = { 0 };
	for (int i = 0; i < 110; i++) {
    carr[7] = i;
    carr[15] = i;
    // keep the last 10 elem for 'Find Fail Test'
    if (i < 100) {
      fwriter.writer << fstring(carr, 16);
    }
    if (i == 0) {
      stat.minKey.assign(carr, carr + 16);
    } else if (i == 99) {
      stat.maxKey.assign(carr, carr + 16);
    }
    keys[i] = string(carr, 16);
	}
  fwriter.close();
  stat.numKeys = 100;
  stat.commonPrefixLen = 0;
  stat.minKeyLen = 16;
  stat.maxKeyLen = 16;
  stat.sumKeyLen = 16 * 100;
}

void test_allone_allzero(DataStored dtype) {
  // init data
  clear();
  key_path = "./tmp_key.txt";
  if (dtype == standard_allzero) {
    printf("==== AllZero Test started\n");
    init_data_allone_allzero();
  } else {
    assert(0);
  }
  // build index
  FileStream fp(key_path, "rb");
  NativeDataInput<InputBuffer> tempKeyFileReader(&fp);
  auto factory = rocksdb::TerarkIndex::GetFactory("CompositeUintIndex_IL_256_32_IL_256_32");
  rocksdb::TerarkZipTableOptions tableOpt;
  TerarkIndex* index = factory->Build(tempKeyFileReader, tableOpt, stat);
  assert(index->Name() == string("CompositeUintIndex_AllOne_AllZero"));
  printf("\tbuild done\n");
  // save & reload
  index_path = "./tmp_index.txt";
  index = save_reload(index, factory);
  printf("\tsave & reload done\n");
  // search test
  for (size_t idx = 0; idx < stat.numKeys; idx++) {
    size_t result = index->Find(keys[idx]);
    assert(idx == result);
  }
  for (size_t idx = stat.numKeys; idx < 110; idx++) {
    size_t result = index->Find(keys[idx]);
    assert(result == size_t(-1));
  }
  printf("\tFind done\n");
  // iterator
  auto iter = index->NewIterator();
  {
    // seek to 1st, next()
    char arr[16] = { 0 };
    assert(iter->SeekToFirst());
    assert(iter->DictRank() == 0);
    assert(fstring(arr, 16) == iter->key());
    for (int i = 1; i < 100; i++) {
      arr[7] = i; arr[15] = i;
      assert(iter->Next());
      assert(iter->DictRank() == i);
      assert(fstring(arr, 16) == iter->key());
    }
    assert(iter->Next() == false);
  }
  {
    // seek to last, prev()
    char arr[16] = { 0 };
    assert(iter->SeekToLast());
    assert(iter->DictRank() == 99);
    for (int i = 98; i >= 0; i--) {
      arr[7] = i; arr[15] = i;
      assert(iter->Prev());
      assert(iter->DictRank() == i);
      assert(fstring(arr, 16) == iter->key());
    }
    assert(iter->Prev() == false);
  }
  {
    // seek matches
    char arr[16] = { 0 };
    for (int i = 0; i < 100; i++) {
      arr[7] = i; arr[15] = i;
      assert(iter->Seek(keys[i]));
      assert(iter->DictRank() == i);
      assert(fstring(arr, 16) == iter->key());
    }
  }
  {
    // seek larger than larger @apple
    char arr[16] = { 0 };
    arr[7] = 120; arr[15] = 0;
    assert(iter->Seek(fstring(arr, 16)) == false);
    // smaller than smaller
    char sarr[4] = { 0 };
    assert(iter->Seek(fstring(sarr, 4)));
    assert(iter->DictRank() == 0);
    //
    char marr[12] = { 0 };
    marr[7] = 4;
    assert(iter->Seek(fstring(marr, 12)));
    assert(iter->DictRank() == 4);
    arr[7] = 4; arr[15] = 4;
    assert(fstring(arr, 16) == iter->key());
  }
  {
    // lower_bound
    char arr[17] = { 0 };
    arr[16] = 1;
    for (int i = 0; i < 100; i++) {
      arr[7] = i; arr[15] = i;
      if (i == 99)
        break;
      int idx = i + 1;
      assert(iter->Seek(fstring(arr, 17)));
      assert(iter->DictRank() == idx);
    }
  }
  printf("\tIterator done\n");
  ::remove(key_path.c_str());
  ::remove(index_path.c_str());
}

/*
 *       pos6  pos7       pos15
 * key : 0    [0..249]    [0..249]
 *       1    [0..249]    [0..249]
 *       ....
 * then key1: pos0 ~ 7, key2: pos8 ~ 15
 * when Seek(2), expected result should be 500, not 2.
 * keep in mind, treat like string, compare from left to right      
 */
void init_data_seek_short_target() {
  rocksdb::FileWriter fwriter;
  fwriter.path = key_path;
  fwriter.open();
  keys.resize(400);
  byte_t carr[16] = { 0 };
	for (int i = 0; i < 4; i++) {
    carr[6] = i;
    for (int j = 0; j < 250; j++) {
      carr[7] = j; 
      carr[15] = j;
      fwriter.writer << fstring(carr, 16);
      if (i == 0 && j == 0) {
        stat.minKey.assign(carr, carr + 16);
      } else if (i == 3 && j == 249) {
        stat.maxKey.assign(carr, carr + 16);
      }
    }
	}
  fwriter.close();
  stat.numKeys = 1000;
  stat.commonPrefixLen = 0;
  stat.minKeyLen = 16;
  stat.maxKeyLen = 16;
  stat.sumKeyLen = 16 * stat.numKeys;
}

void test_data_seek_short_target() {
  // init data
  printf("==== Seek-short Test started\n");
  clear();
  key_path = "./tmp_key.txt";
  init_data_seek_short_target();
  {
    size_t celen;
    assert(rocksdb::TerarkIndex::SeekCostEffectiveIndexLen(stat, celen));
    assert(celen == 2);
  }
  // build index
  FileStream fp(key_path, "rb");
  NativeDataInput<InputBuffer> tempKeyFileReader(&fp);
  auto factory = rocksdb::TerarkIndex::GetFactory("CompositeUintIndex_IL_256_32_IL_256_32");
  rocksdb::TerarkZipTableOptions tableOpt;
  TerarkIndex* index = factory->Build(tempKeyFileReader, tableOpt, stat);
  assert(index->Name() == string("CompositeUintIndex_IL_256_32_AllZero"));
  printf("\tbuild done\n");
  // save & reload
  index_path = "./tmp_index.txt";
  index = save_reload(index, factory);
  printf("\tsave & reload done\n");
  // iterator
  auto iter = index->NewIterator();
  {
    // seek lower_bound
    char arr[7] = { 0 };
    arr[6] = 2;
    assert(iter->Seek(fstring(arr, arr + 7)));
    assert(iter->DictRank() == 250 * 2);
  }
  printf("\tSeek done\n");
}
/*
 * select
 */
void test_select() {
  // prepare
  const rocksdb::TerarkIndex::Factory* f_il85_il85 = 
    TerarkIndex::GetFactory("CompositeUintIndex_IL_256_32_IL_256_32");
  const rocksdb::TerarkIndex::Factory* f_allone_allzero = 
    TerarkIndex::GetFactory("CompositeUintIndex_AllOne_AllZero");
  {
    TerarkIndex::KeyStat stat;
    stat.numKeys = 100;
    stat.minKeyLen = stat.maxKeyLen = 16;
    stat.commonPrefixLen = 0;
    char arr[16] = { 0 };
    arr[7] = arr[15] = 4;
    stat.minKey.assign(arr, arr + 16);
    arr[7] = arr[15] = 14;
    stat.maxKey.assign(arr, arr + 16);
    auto factory = TerarkIndex::SelectFactory(stat, "");
    assert(factory != nullptr);
    assert(factory == f_il85_il85);
  }
  {
    // do NOT select composite: size == 8
    TerarkIndex::KeyStat stat;
    stat.commonPrefixLen = 0;
    stat.numKeys = 1;
    char arr[8] = { 0 };
    stat.minKey.assign(arr, arr + 8);
    stat.maxKey.assign(arr, arr + 8);
    stat.minKeyLen = stat.maxKeyLen = 8;
    auto factory = TerarkIndex::SelectFactory(stat, "");
    assert(factory != nullptr);
    assert(factory != f_il85_il85);
  }
  {
    auto assign32 = [](char* beg, char* end, int32_t value) {
        union {
          char bytes[4];
          int32_t value;
        } c;
#if BOOST_ENDIAN_LITTLE_BYTE
        c.value = terark::byte_swap(value);
#else
        c.value = value;
#endif
        size_t l = end - beg;
        memcpy(beg, c.bytes + (4 - l), l);
    };
    TerarkIndex::KeyStat stat;
    stat.commonPrefixLen = 0;
    stat.numKeys = 10000000;
    {
      char arr[8] = { 0 };
      int v1 = 1350476, v2 = 9531880;
      assign32(arr, arr + 4, v1);
      assign32(arr + 4, arr + 8, v2);
      stat.minKey.assign(arr, arr + 8);
    }
    {
      char arr[8] = { 0 };
      int v1 = 8778214, v2 = 8569467;
      assign32(arr, arr + 4, v1);
      assign32(arr + 4, arr + 8, v2);
      stat.maxKey.assign(arr, arr + 8);
    }
    stat.minKeyLen = stat.maxKeyLen = 8;
    size_t ceLen = 0;
    assert(TerarkIndex::SeekCostEffectiveIndexLen(stat, ceLen));
  }
  {
    // do NOT select composite index: gap ratio too high
    TerarkIndex::KeyStat stat;
    stat.numKeys = 2;
    stat.commonPrefixLen = 0;
    stat.minKeyLen = stat.maxKeyLen = 16;
    char arr[16] = { 0 };
    arr[5] = 1; arr[7] = 1;
    stat.minKey.assign(arr, arr + 16);
    arr[5] = 100; arr[7] = 1;
    stat.maxKey.assign(arr, arr + 16);
    size_t celen = size_t(-1);
    assert(TerarkIndex::SeekCostEffectiveIndexLen(stat, celen) == false);
  }
  {
    // select composite: key count > 4G
    TerarkIndex::KeyStat stat;
    stat.numKeys = 1ull << 32;
    stat.commonPrefixLen = 0;
    stat.minKeyLen = stat.maxKeyLen = 16;
    byte_t arr[16] = { 0 };
    arr[4] = 0; arr[8] = 1;
    stat.minKey.assign(arr, arr + 16);
    arr[4] = 1; arr[8] = 0;
    stat.maxKey.assign(arr, arr + 16);
    size_t celen = size_t(-1);
    auto factory = TerarkIndex::SelectFactory(stat, "");
    assert(factory != nullptr);
  }
  {
    // seek index1stLen: len = 2
    TerarkIndex::KeyStat stat;
    stat.numKeys = 2;
    stat.commonPrefixLen = 0;
    stat.minKeyLen = stat.maxKeyLen = 16;
    char arr[16] = { 0 };
    arr[7] = 1;
    stat.minKey.assign(arr, arr + 16);
    arr[5] = 1; arr[7] = 1;
    stat.maxKey.assign(arr, arr + 16);
    size_t celen = size_t(-1);
    assert(TerarkIndex::SeekCostEffectiveIndexLen(stat, celen));
    assert(celen == 1);
  }
  {
    // seek index1stLen: len = 2
    TerarkIndex::KeyStat stat;
    stat.numKeys = 256;
    stat.commonPrefixLen = 0;
    stat.minKeyLen = stat.maxKeyLen = 16;
    byte_t arr[16] = { 0 };
    arr[8] = 1;
    stat.minKey.assign(arr, arr + 16);
    arr[7] = 1; arr[8] = 0;
    stat.maxKey.assign(arr, arr + 16);
    size_t celen = size_t(-1);
    assert(TerarkIndex::SeekCostEffectiveIndexLen(stat, celen));
    assert(celen == 2);
  }
  {
    // seek index1stLen: len = 3
    TerarkIndex::KeyStat stat;
    stat.numKeys = 1 << 16;
    stat.commonPrefixLen = 0;
    stat.minKeyLen = stat.maxKeyLen = 16;
    byte_t arr[16] = { 0 };
    arr[8] = 1;
    stat.minKey.assign(arr, arr + 16);
    arr[6] = 1; arr[8] = 0;
    stat.maxKey.assign(arr, arr + 16);
    size_t celen = size_t(-1);
    assert(TerarkIndex::SeekCostEffectiveIndexLen(stat, celen));
    assert(celen == 3);
  }
  {
    // do select composite: il85_il85
    TerarkIndex::KeyStat stat;
    stat.numKeys = 100;
    stat.commonPrefixLen = 0;
    stat.minKeyLen = stat.maxKeyLen = 16;
    char arr[16] = { 0 };
    arr[7] = 1;
    stat.minKey.assign(arr, arr + 16);
    arr[7] = 14;
    stat.maxKey.assign(arr, arr + 16);
    auto factory = TerarkIndex::SelectFactory(stat, "");
    assert(factory == f_il85_il85);
  }
}


int main(int argc, char** argv) {
  printf("EXAGGERATE\n");

  test_il256_il256(standard_ascend);
  test_il256_il256(standard_descend);

  test_allone_il256(standard_ascend);
  test_allone_il256(standard_descend);

  test_allone_allzero(standard_allzero);

  test_data_seek_short_target();

  test_select();

  return 0;
}
