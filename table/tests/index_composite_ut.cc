
#include <fstream>
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
//typedef map<string, int> IndexDict;
//IndexDict dict;
vector<string> keys;
string key_path;
string index_path;
TerarkIndex::KeyStat stat;
void clear() {
  keys.clear();
  key_path.clear();
  index_path.clear();
  memset(&stat, 0, sizeof(stat));
}

void init_data_ascend() {
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

void init_data_descend() {
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

TerarkIndex* save_reload(TerarkIndex* index, const TerarkIndex::Factory* factory) {
  FileStream writer(index_path, "wb");
  index->SaveMmap([&writer](const void* data, size_t size) {
      writer.ensureWrite(data, size);
    });
  writer.flush();
  writer.close();
  
  return factory->LoadFile(index_path).release();
}

void test_simple(bool ascend) {
  // init data
  clear();
  key_path = "./tmp_key.txt";
  if (ascend) {
    printf("==== Ascend Test started\n");
    init_data_ascend();
  } else {
    printf("==== Descend Test started\n");
    init_data_descend();
  }
  // build index
  FileStream fp(key_path, "rb");
  NativeDataInput<InputBuffer> tempKeyFileReader(&fp);
  auto factory = rocksdb::TerarkIndex::GetFactory("CompositeIndex_IL_256_32");
  rocksdb::TerarkZipTableOptions tableOpt;
  TerarkIndex* index = factory->Build(tempKeyFileReader, tableOpt, stat);
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
  printf("\tIterator done\n");
  ::remove(key_path.c_str());
  ::remove(index_path.c_str());
}

void test_select() {
  // cfactory -> CompositeIndexFactory
  const rocksdb::TerarkIndex::Factory* cfactory = 
    TerarkIndex::GetFactory("CompositeIndex_IL_256_32");
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
    assert(factory == cfactory);
  }
  {
    // do NOT select composite
    TerarkIndex::KeyStat stat;
    stat.commonPrefixLen = 0;
    stat.numKeys = 1;
    char arr[8] = { 0 };
    stat.minKey.assign(arr, arr + 8);
    stat.maxKey.assign(arr, arr + 8);
    stat.minKeyLen = stat.maxKeyLen = 8;
    auto factory = TerarkIndex::SelectFactory(stat, "");
    assert(factory != nullptr);
    assert(factory != cfactory);
  }
  {
    // do NOT select composite
    TerarkIndex::KeyStat stat;
    stat.numKeys = 1;
    stat.commonPrefixLen = 0;
    stat.minKeyLen = stat.maxKeyLen = 16;
    char arr[16] = { 0 };
    arr[7] = 1;
    stat.minKey.assign(arr, arr + 16);
    arr[5] = 1;
    stat.maxKey.assign(arr, arr + 16);
    auto factory = TerarkIndex::SelectFactory(stat, "");
    assert(factory == nullptr);
  }
  {
    // do select composite
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
    assert(factory == cfactory);
  }

}


int main(int argc, char** argv) {
  printf("EXAGGERATE\n");
  test_simple(true/*ascend*/);
  test_simple(false);
  test_select();
  return 0;
}
