
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <terark/io/FileStream.hpp>
#include <terark/rank_select.hpp>

#include "../terark_zip_common.h"
#include "../terark_zip_index.h"

#include "index_composite_ut.h"

using namespace std;

static const size_t KEY_LEN = 16;

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

namespace {
  vector<string> keys;
  string key_path;
  string index_path;
  TerarkIndex::KeyStat stat;

  void clear() {
    keys.clear();
    key_path.clear();
    index_path.clear();
    memset(&stat, 0, sizeof(stat));
    ::remove(key_path.c_str());
    ::remove(index_path.c_str());
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
}

/*
 * il256 il256
 */
static void init_data_il256_il256_ascend() {
  rocksdb::FileWriter fwriter;
  fwriter.path = key_path;
  fwriter.open();
  keys.resize(100);
  char carr[KEY_LEN] = { 0 };
  for (int i = 0; i < 100; i++) {
    keys[i] = string(carr, carr + KEY_LEN);
  }
  size_t kcnt = 0;
	for (int i = 0; i < 10; i += 2) {
    carr[7] = i;
    for (int j = 2; j < 10; j++) { // key2 min > 0
      carr[15] = j;
      // keep the last 10 elem for 'Find Fail Test'
      if (i < 9) {
        kcnt ++;
        fwriter.writer << fstring(carr, KEY_LEN);
      }
      if (i == 0 && j == 2) {
        stat.minKey.assign(carr, carr + KEY_LEN);
      } else if (i == 8 && j == 9) {
        stat.maxKey.assign(carr, carr + KEY_LEN);
      }
      keys[i * 10 + j] = string(carr, KEY_LEN);
    }
	}
  fwriter.close();
  stat.numKeys = kcnt;
  stat.commonPrefixLen = 0;
  stat.minKeyLen = KEY_LEN;
  stat.maxKeyLen = KEY_LEN;
  stat.sumKeyLen = KEY_LEN * kcnt;
}

static void init_data_il256_il256_descend() {
  rocksdb::FileWriter fwriter;
  fwriter.path = key_path;
  fwriter.open();
  keys.resize(100);
  char carr[KEY_LEN] = { 0 };
  for (int i = 0; i < 100; i++) {
    keys[i] = string(carr, carr + KEY_LEN);
  }
  size_t kcnt = 0;
	for (int i = 8; i >= 0; i -= 2) {
    carr[7] = i;
    for (int j = 9; j >= 2; j--) {
      carr[15] = j;
      fwriter.writer << fstring(carr, KEY_LEN);
      kcnt ++;
      if (i == 0 && j == 2) {
        stat.maxKey.assign(carr, carr + KEY_LEN);
      } else if (i == 8 && j == 9) {
        stat.minKey.assign(carr, carr + KEY_LEN);
      }
      keys[i * 10 + j] = string(carr, KEY_LEN);
    }
	}
  fwriter.close();
  stat.numKeys = kcnt;
  stat.commonPrefixLen = 0;
  stat.minKeyLen = KEY_LEN;
  stat.maxKeyLen = KEY_LEN;
  stat.sumKeyLen = KEY_LEN * kcnt;
}

void test_il256_il256_sorteduint(DataStored dtype) {
  // init data
  clear();
  key_path = "./tmp_key.txt";
  if (dtype == standard_ascend) {
    printf("==== SortedUint Ascend Test started\n");
    init_data_il256_il256_ascend();
  } else if (dtype == standard_descend) {
    printf("==== SortedUint Descend Test started\n");
    init_data_il256_il256_descend();
  } else {
    assert(0);
  }
  const size_t key2min = 2;
  // build index
  FileStream fp(key_path, "rb");
  NativeDataInput<InputBuffer> tempKeyFileReader(&fp);
  auto factory = rocksdb::TerarkIndex::GetFactory("CompositeUintIndex_IL_256_32_IL_256_32_SortedUint");
  {
    size_t memsz = factory->MemSizeForBuild(stat);
    assert(memsz < stat.sumKeyLen * 0.8);
    printf("\tcompress check done\n");
  }
  rocksdb::TerarkZipTableOptions tableOpt;
  TerarkIndex* index = factory->Build(tempKeyFileReader, tableOpt, stat);
  assert(index->Name() == string("CompositeUintIndex_IL_256_32_IL_256_32_SortedUint"));

  printf("\tbuild done\n");
  // save & reload
  index_path = "./tmp_index.txt";
  index = save_reload(index, factory);
  printf("\tsave & reload done\n");
  // search test
  size_t expected = 0;
  for (size_t i = 0; i < 10; i += 2) {
    for (size_t j = key2min; j < 10; j++) {
      size_t idx = i * 10 + j;
      //size_t expected = i / 2 * 10 + j;
      size_t result = index->Find(keys[idx]);
      assert(result == expected);
      expected ++;
    }
  }
  {
    char arr[KEY_LEN] = { 0 };
    for (size_t i = 1; i < 10; i += 2) {
      arr[7] = i;
      for (size_t j = 0; j < 10; j++) {
        arr[15] = j;
        assert(index->Find(fstring(arr, arr + KEY_LEN)) == size_t(-1));
      }
    }
  }
  {
    char arr[KEY_LEN] = { 0 };
    arr[7] = 2; arr[15] = 14;
    assert(index->Find(fstring(arr, arr + KEY_LEN)) == size_t(-1));
    //
    char narr[KEY_LEN + 1] = { 0 };
    arr[7] = 2; arr[15] = 14;
    assert(index->Find(fstring(arr, arr + KEY_LEN)) == size_t(-1));
    //
    char marr[KEY_LEN] = { 0 };
    arr[6] = 2; arr[15] = 14;
    assert(index->Find(fstring(arr, arr + KEY_LEN)) == size_t(-1));
  }
  printf("\tFind done\n");
  // iterator
  auto iter = index->NewIterator();
  {
    // seek to 1st, next()
    size_t expected = 0;
    char arr[KEY_LEN] = { 0 };
    arr[15] = 2;
    assert(iter->SeekToFirst());
    assert(iter->DictRank() == expected);
    assert(fstring(arr, KEY_LEN) == iter->key());
    for (int i = 0; i < 10; i += 2) {
      arr[7] = i;
      for (int j = key2min; j < 10; j++) {
        arr[15] = j;
        if (i == 0 && j == key2min)
          continue;
        expected ++;
        assert(iter->Next());
        assert(iter->DictRank() == expected);
        assert(fstring(arr, KEY_LEN) == iter->key());
      }
    }
    assert(iter->Next() == false);
  }
  {
    // seek to last, prev()
    char arr[KEY_LEN] = { 0 };
    size_t expected = stat.numKeys - 1;
    assert(iter->SeekToLast());
    assert(iter->DictRank() == expected);
    for (int i = 8; i >= 0; i -= 2) {
      arr[7] = i;
      for (int j = 9; j >= key2min; j--) {
        arr[15] = j;
        if (i == 8 && j == 9) {
          assert(fstring(arr, KEY_LEN) == iter->key());
          continue;
        }
        expected --;
        assert(iter->Prev());
        assert(iter->DictRank() == expected);
        assert(fstring(arr, KEY_LEN) == iter->key());
      }
    }
    assert(iter->Prev() == false);
  }
  {
    // seek matches
    char arr[KEY_LEN] = { 0 };
    size_t expected = 0;
    for (int i = 0; i < 9; i += 2) {
      arr[7] = i;
      for (int j = key2min; j < 10; j++) {
        arr[15] = j;
        int idx = i * 10 + j;
        assert(iter->Seek(keys[idx]));
        assert(iter->DictRank() == expected);
        assert(fstring(arr, KEY_LEN) == iter->key());
        expected ++;
      }
    }
  }
  {
    // seek larger than larger @apple
    char arr[KEY_LEN] = { 0 };
    arr[7] = 20; arr[15] = 0;
    assert(iter->Seek(fstring(arr, KEY_LEN)) == false);
    // smaller than smaller
    char sarr[4] = { 0 };
    assert(iter->Seek(fstring(sarr, 4)));
    assert(iter->DictRank() == 0);
    // shorter key2, smaller than key2min
    char marr[12] = { 0 };
    marr[7] = 4;
    assert(iter->Seek(fstring(marr, 12)));
    assert(iter->DictRank() == 2 * 8);
    arr[7] = 4; arr[15] = key2min;
    assert(fstring(arr, KEY_LEN) == iter->key());
    // shorter key2, equal as key2min
    // for align to left, should larger than all narr[7] = 4
    // return narr[7] = 6
    char narr[12] = { 0 };
    narr[7] = 4; narr[11] = key2min;
    assert(iter->Seek(fstring(narr, 12)));
    assert(iter->DictRank() == 3 * 8);
    // none exist
    char larr[12] = { 0 };
    larr[3] = 5;
    assert(iter->Seek(fstring(larr, 12)) == false);
    
  }
  {
    // lower_bound
    char arr[17] = { 0 };
    arr[KEY_LEN] = 1;
    size_t expected = 1;
    for (int i = 0; i < 9; i += 2) {
      arr[7] = i;
      for (int j = key2min; j < 10; j++) {
        if (i == 8 && j == 9)
          break;
        arr[15] = j;
        assert(iter->Seek(fstring(arr, 17)));
        assert(iter->DictRank() == expected);
        expected ++;
      }
    }
  }
  {
    // cross index1st boundary lower_bound
    char arr[KEY_LEN] = { 0 };
    arr[7] = 4; arr[15] = 14;
    assert(iter->Seek(fstring(arr, KEY_LEN)));
    int expected = 3 * 8;
    assert(iter->DictRank() == expected);

    arr[7] = 8; arr[15] = 9;
    assert(iter->Seek(fstring(arr, KEY_LEN)));
    arr[7] = 8; arr[15] = 10;
    assert(iter->Seek(fstring(arr, KEY_LEN)) == false);
  }
  printf("\tIterator done\n");
  delete iter;
  delete index;
  clear();
}


/*
 * allone il256
 */
static void init_data_allone_il256_ascend() {
  rocksdb::FileWriter fwriter;
  fwriter.path = key_path;
  fwriter.open();
  keys.resize(120);
  char carr[KEY_LEN] = { 0 };
  for (int i = 0; i < 120; i++) {
    keys[i] = string(carr, carr + KEY_LEN);
  }
  const int key1min = 2;
  size_t kcnt = 0;
	for (int i = key1min; i < 11; i++) {
    carr[7] = i;
    for (int j = 0; j < 10; j++) {
      carr[15] = j;
      // keep the last 10 elem for 'Find Fail Test'
      if (i < 10) {
        kcnt ++;
        fwriter.writer << fstring(carr, KEY_LEN);
      }
      if (i == key1min && j == 0) {
        stat.minKey.assign(carr, carr + KEY_LEN);
      } else if (i == 9 && j == 9) {
        stat.maxKey.assign(carr, carr + KEY_LEN);
      }
      keys[i * 10 + j] = string(carr, KEY_LEN);
    }
	}
  fwriter.close();
  stat.numKeys = kcnt;
  stat.commonPrefixLen = 0;
  stat.minKeyLen = KEY_LEN;
  stat.maxKeyLen = KEY_LEN;
  stat.sumKeyLen = KEY_LEN * kcnt;
}

static void init_data_allone_il256_descend() {
  rocksdb::FileWriter fwriter;
  fwriter.path = key_path;
  fwriter.open();
  keys.resize(120);
  char carr[KEY_LEN] = { 0 };
  for (int i = 0; i < 120; i++) {
    keys[i] = string(carr, carr + KEY_LEN);
  }
  const int key1min = 2; // key1 min > 0
  size_t kcnt = 0;
	for (int i = 10; i >= key1min; i--) {
    carr[7] = i;
    for (int j = 9; j >= 0; j--) {
      carr[15] = j;
      // keep the last 10 elem for 'Find Fail Test'
      if (i < 10) {
        kcnt ++;
        fwriter.writer << fstring(carr, KEY_LEN);
      }
      if (i == key1min && j == 0) {
        stat.maxKey.assign(carr, carr + KEY_LEN);
      } else if (i == 9 && j == 9) {
        stat.minKey.assign(carr, carr + KEY_LEN);
      }
      keys[i * 10 + j] = string(carr, KEY_LEN);
    }
	}
  fwriter.close();
  stat.numKeys = kcnt;
  stat.commonPrefixLen = 0;
  stat.minKeyLen = KEY_LEN;
  stat.maxKeyLen = KEY_LEN;
  stat.sumKeyLen = KEY_LEN * kcnt;
}

void test_allone_il256_sorteduint(DataStored dtype) {
  // init data
  clear();
  key_path = "./tmp_key.txt";
  if (dtype == standard_ascend) {
    printf("==== SortedUint Ascend Test started\n");
    init_data_allone_il256_ascend();
  } else if (dtype == standard_descend) {
    printf("==== SortedUint Descend Test started\n");
    init_data_allone_il256_descend();
  } else {
    assert(0);
  }
  // build index
  const int key1min = 2;
  FileStream fp(key_path, "rb");
  NativeDataInput<InputBuffer> tempKeyFileReader(&fp);
  auto factory = rocksdb::TerarkIndex::GetFactory("CompositeUintIndex_IL_256_32_IL_256_32_SortedUint");
  {
    size_t memsz = factory->MemSizeForBuild(stat);
    assert(memsz < stat.sumKeyLen * 0.8);
    printf("\tcompress check done\n");
  }
  rocksdb::TerarkZipTableOptions tableOpt;
  TerarkIndex* index = factory->Build(tempKeyFileReader, tableOpt, stat);
  assert(index->Name() == string("CompositeUintIndex_AllOne_IL_256_32_SortedUint"));
  printf("\tbuild done\n");
  // save & reload
  index_path = "./tmp_index.txt";
  index = save_reload(index, factory);
  printf("\tsave & reload done\n");
  // search test
  size_t expected = 0;
  for (size_t i = key1min; i < 10; i++) {
    for (size_t j = 0; j < 10; j++) {
      size_t idx = i * 10 + j;
      size_t result = index->Find(keys[idx]);
      assert(expected == result);
      expected ++;
    }
  }
  for (size_t idx = 0; idx < 10; idx++) {
    size_t result = index->Find(keys[idx]);
    assert(result == size_t(-1));
  }
  printf("\tFind done\n");
  // iterator
  auto iter = index->NewIterator();
  {
    // seek to 1st, next()
    char arr[KEY_LEN] = { 0 };
    arr[7] = key1min;
    size_t expected = 0;
    assert(iter->SeekToFirst());
    assert(iter->DictRank() == expected);
    assert(fstring(arr, KEY_LEN) == iter->key());
    for (int i = key1min; i < 10; i++) {
      arr[7] = i;
      for (int j = 0; j < 10; j++) {
        arr[15] = j;
        if (i == key1min && j == 0)
          continue;
        expected ++;
        assert(iter->Next());
        assert(iter->DictRank() == expected);
        assert(fstring(arr, KEY_LEN) == iter->key());
      }
    }
    assert(iter->Next() == false);
  }
  {
    // seek to last, prev()
    char arr[KEY_LEN] = { 0 };
    size_t expected = stat.numKeys - 1;
    assert(iter->SeekToLast());
    assert(iter->DictRank() == expected);
    for (int i = 9; i >= key1min; i--) {
      arr[7] = i;
      for (int j = 9; j >= 0; j--) {
        arr[15] = j;
        if (i == 9 && j == 9) {
          assert(fstring(arr, KEY_LEN) == iter->key());
          continue;
        }
        expected --;
        assert(iter->Prev());
        assert(iter->DictRank() == expected);
        assert(fstring(arr, KEY_LEN) == iter->key());
      }
    }
    assert(iter->Prev() == false);
  }
  {
    // seek matches
    char arr[KEY_LEN] = { 0 };
    size_t expected = 0;
    for (int i = key1min; i < 9; i++) {
      arr[7] = i;
      for (int j = 0; j < 10; j++) {
        arr[15] = j;
        int idx = i * 10 + j;
        assert(iter->Seek(keys[idx]));
        assert(iter->DictRank() == expected);
        assert(fstring(arr, KEY_LEN) == iter->key());
        expected ++;
      }
    }
  }
  {
    // seek larger than larger @apple
    char arr[KEY_LEN] = { 0 };
    arr[7] = 20; arr[15] = 0;
    assert(iter->Seek(fstring(arr, KEY_LEN)) == false);
    // smaller than smaller
    char sarr[4] = { 0 };
    assert(iter->Seek(fstring(sarr, 4)));
    assert(iter->DictRank() == 0);
    // 
    char marr[12] = { 0 };
    marr[7] = 4;
    assert(iter->Seek(fstring(marr, 12)));
    assert(iter->DictRank() == 2 * 10);
    arr[7] = 4; arr[15] = 0;
    assert(fstring(arr, KEY_LEN) == iter->key());
  }
  {
    // lower_bound
    char arr[17] = { 0 };
    arr[KEY_LEN] = 1;
    size_t expected = 1;
    for (int i = key1min; i < 10; i++) {
      arr[7] = i;
      for (int j = 0; j < 10; j++) {
        if (i == 9 && j == 9)
          break;
        arr[15] = j;
        assert(iter->Seek(fstring(arr, 17)));
        assert(iter->DictRank() == expected);
        expected ++;
      }
    }
  }
  {
    // cross index1st boundary lower_bound
    char arr[KEY_LEN] = { 0 };
    arr[7] = 4; arr[15] = 14;
    assert(iter->Seek(fstring(arr, KEY_LEN)));
    int expected = 3 * 10;
    assert(iter->DictRank() == expected);

    arr[7] = 9; arr[15] = 9;
    assert(iter->Seek(fstring(arr, KEY_LEN)));
    arr[7] = 9; arr[15] = 10;
    assert(iter->Seek(fstring(arr, KEY_LEN)) == false);
  }
  printf("\tIterator done\n");
  delete iter;
  delete index;
  clear();
}

/*
 * allone allzero
 */
static void init_data_allone_allzero() {
  rocksdb::FileWriter fwriter;
  fwriter.path = key_path;
  fwriter.open();
  keys.resize(110);
  char carr[KEY_LEN] = { 0 };
	for (int i = 0; i < 110; i++) {
    carr[7] = i;
    carr[15] = i;
    // keep the last 10 elem for 'Find Fail Test'
    if (i < 100) {
      fwriter.writer << fstring(carr, KEY_LEN);
    }
    if (i == 0) {
      stat.minKey.assign(carr, carr + KEY_LEN);
    } else if (i == 99) {
      stat.maxKey.assign(carr, carr + KEY_LEN);
    }
    keys[i] = string(carr, KEY_LEN);
	}
  fwriter.close();
  stat.numKeys = 100;
  stat.commonPrefixLen = 0;
  stat.minKeyLen = KEY_LEN;
  stat.maxKeyLen = KEY_LEN;
  stat.sumKeyLen = KEY_LEN * 100;
}

void test_allone_allzero_sorteduint(DataStored dtype) {
  // init data
  clear();
  key_path = "./tmp_key.txt";
  if (dtype == standard_allzero) {
    printf("==== SortedUint AllZero Test started\n");
    init_data_allone_allzero();
  } else {
    assert(0);
  }
  // build index
  FileStream fp(key_path, "rb");
  NativeDataInput<InputBuffer> tempKeyFileReader(&fp);
  auto factory = rocksdb::TerarkIndex::GetFactory("CompositeUintIndex_IL_256_32_IL_256_32_SortedUint");
  {
    size_t memsz = factory->MemSizeForBuild(stat);
    assert(memsz < stat.sumKeyLen * 0.8);
    printf("\tcompress check done\n");
  }
  rocksdb::TerarkZipTableOptions tableOpt;
  TerarkIndex* index = factory->Build(tempKeyFileReader, tableOpt, stat);
  assert(index->Name() == string("CompositeUintIndex_AllOne_AllZero_SortedUint"));
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
    char arr[KEY_LEN] = { 0 };
    assert(iter->SeekToFirst());
    assert(iter->DictRank() == 0);
    assert(fstring(arr, KEY_LEN) == iter->key());
    for (int i = 1; i < 100; i++) {
      arr[7] = i; arr[15] = i;
      assert(iter->Next());
      assert(iter->DictRank() == i);
      assert(fstring(arr, KEY_LEN) == iter->key());
    }
    assert(iter->Next() == false);
  }
  {
    // seek to last, prev()
    char arr[KEY_LEN] = { 0 };
    assert(iter->SeekToLast());
    assert(iter->DictRank() == 99);
    for (int i = 98; i >= 0; i--) {
      arr[7] = i; arr[15] = i;
      assert(iter->Prev());
      assert(iter->DictRank() == i);
      assert(fstring(arr, KEY_LEN) == iter->key());
    }
    assert(iter->Prev() == false);
  }
  {
    // seek matches
    char arr[KEY_LEN] = { 0 };
    for (int i = 0; i < 100; i++) {
      arr[7] = i; arr[15] = i;
      assert(iter->Seek(keys[i]));
      assert(iter->DictRank() == i);
      assert(fstring(arr, KEY_LEN) == iter->key());
    }
  }
  {
    // seek larger than larger @apple
    char arr[KEY_LEN] = { 0 };
    arr[7] = 120; arr[15] = 0;
    assert(iter->Seek(fstring(arr, KEY_LEN)) == false);
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
    assert(fstring(arr, KEY_LEN) == iter->key());
  }
  {
    // lower_bound
    char arr[17] = { 0 };
    arr[KEY_LEN] = 1;
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
  delete iter;
  delete index;
  clear();
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
static void init_data_seek_short_target() {
  rocksdb::FileWriter fwriter;
  fwriter.path = key_path;
  fwriter.open();
  keys.resize(400);
  byte_t carr[KEY_LEN] = { 0 };
	for (int i = 0; i < 4; i++) {
    carr[6] = i;
    for (int j = 0; j < 250; j++) {
      carr[7] = j; 
      carr[15] = j;
      fwriter.writer << fstring(carr, KEY_LEN);
      if (i == 0 && j == 0) {
        stat.minKey.assign(carr, carr + KEY_LEN);
      } else if (i == 3 && j == 249) {
        stat.maxKey.assign(carr, carr + KEY_LEN);
      }
    }
	}
  fwriter.close();
  stat.numKeys = 1000;
  stat.commonPrefixLen = 0;
  stat.minKeyLen = KEY_LEN;
  stat.maxKeyLen = KEY_LEN;
  stat.sumKeyLen = KEY_LEN * stat.numKeys;
}

void test_data_seek_short_target_sorteduint() {
  // init data
  printf("==== SortedUint Seek-short Test started\n");
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
  auto factory = rocksdb::TerarkIndex::GetFactory("CompositeUintIndex_IL_256_32_IL_256_32_SortedUint");
  {
    size_t memsz = factory->MemSizeForBuild(stat);
    assert(memsz < stat.sumKeyLen * 0.8);
    printf("\tcompress check done\n");
  }
  rocksdb::TerarkZipTableOptions tableOpt;
  TerarkIndex* index = factory->Build(tempKeyFileReader, tableOpt, stat);
  assert(index->Name() == string("CompositeUintIndex_IL_256_32_AllZero_SortedUint"));
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
  delete iter;
  delete index;
  clear();
}
