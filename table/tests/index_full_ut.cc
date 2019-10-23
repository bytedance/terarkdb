
#include <fstream>
#include <map>
#include <string>
#include <vector>

#include <terark/io/FileStream.hpp>
#include <terark/rank_select.hpp>
#include <terark/idx/terark_zip_index.hpp>

#include "../terark_zip_common.h"

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
  char carr[8] = { 0 };
  for (int i = 0; i < 110; i++) {
    carr[7] = i;
    // keep the last 5 elem for 'Find Fail Test'
    if (i < 100) {
      fwriter.writer << fstring(carr, 8);
    }
    if (i == 0) {
      stat.minKey.assign(carr, carr + 8);
    } else if (i == 99) {
      stat.maxKey.assign(carr, carr + 8);
    }
    keys[i] = string(carr, 8);
  }
  fwriter.close();
  stat.numKeys = 100;
  stat.commonPrefixLen = 0;
  stat.minKeyLen = 8;
  stat.maxKeyLen = 8;
  stat.sumKeyLen = 8 * 100;
}

void init_data_descend() {
  rocksdb::FileWriter fwriter;
  fwriter.path = key_path;
  fwriter.open();
  keys.resize(110);
  char carr[8] = { 0 };
  for (int i = 109; i >= 0; i--) {
    carr[7] = i;
    // keep the last 10 elem for 'Find Fail Test'
    if (i < 100) {
      fwriter.writer << fstring(carr, 8);
    }
    if (i == 0) {
      stat.maxKey.assign(carr, carr + 8);
    } else if (i == 99) {
      stat.minKey.assign(carr, carr + 8);
    }
    keys[i] = string(carr, 8);
  }
  fwriter.close();
  stat.numKeys = 100;
  stat.commonPrefixLen = 0;
  stat.minKeyLen = 8;
  stat.maxKeyLen = 8;
  stat.sumKeyLen = 8 * 100;
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
  auto factory = rocksdb::TerarkIndex::GetFactory("UintIndex_AllOne");
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
  for (size_t idx = 100; idx < 110; idx++) {
    size_t result = index->Find(keys[idx]);
    assert(result == size_t(-1));
  }
  printf("\tFind done\n");
  // iterator
  auto iter = index->NewIterator();
  {
    // seek to 1st, next()
    char arr[8] = { 0 };
    assert(iter->SeekToFirst());
    assert(iter->DictRank() == 0);
    assert(fstring(arr, 8) == iter->key());
    for (int i = 0; i < 100; i++) {
      arr[7] = i;
      if (i == 0)
        continue;
      assert(iter->Next());
      assert(iter->DictRank() == i);
      //fstring expected = fstring(arr, 8);
      //fstring res = iter->key();
      //assert(expected == res);
      assert(fstring(arr, 8) == iter->key());
    }
    assert(iter->Next() == false);
  }
  {
    // seek to last, prev()
    char arr[8] = { 0 };
    assert(iter->SeekToLast());
    assert(iter->DictRank() == 99);
    for (int i = 99; i >= 0; i--) {
      arr[7] = i;
      if (i == 99) {
        assert(fstring(arr, 8) == iter->key());
        continue;
      }
      assert(iter->Prev());
      assert(iter->DictRank() == i);
      assert(fstring(arr, 8) == iter->key());
    }
    assert(iter->Prev() == false);
  }
  {
    // seek matches
    char arr[8] = { 0 };
    for (int i = 0; i < 100; i++) {
      arr[7] = i;
      int idx = i;
      assert(iter->Seek(keys[idx]));
      assert(iter->DictRank() == idx);
      assert(fstring(arr, 8) == iter->key());
    }
  }
  {
    // seek larger than larger @apple
    char arr[8] = { 0 };
    arr[7] = 101;
    assert(iter->Seek(fstring(arr, 8)) == false);
    // smaller than smaller
    char sarr[4] = { 0 };
    assert(iter->Seek(fstring(sarr, 4)));
    assert(iter->DictRank() == 0);
    // longer -> lower_bound
    char marr[12] = { 0 };
    marr[7] = 4; marr[11] = 1;
    assert(iter->Seek(fstring(marr, 12)));
    assert(iter->DictRank() == 5);
    arr[7] = 5;
    assert(fstring(arr, 8) == iter->key());
  }
  {
    // lower_bound
    char arr[8] = { 0 };
    char marr[12] = { 0 };
    for (int i = 0; i < 99; i++) {
      marr[7] = i; marr[11] = 1;
      int idx = i + 1;
      arr[7] = idx;
      assert(iter->Seek(fstring(marr, 12)));
      assert(iter->DictRank() == idx);
      assert(fstring(arr, 8) == iter->key());
    }
  }
  printf("\tIterator done\n");
  ::remove(key_path.c_str());
  ::remove(index_path.c_str());
}

void test_select() {
  // cfactory -> UintIndexFactory
  const rocksdb::TerarkIndex::Factory* cfactory = nullptr;
  {
    TerarkIndex::KeyStat stat;
    stat.numKeys = 100;
    stat.minKeyLen = stat.maxKeyLen = 8;
    stat.commonPrefixLen = 0;
    char arr[8] = { 0 };
    arr[7] = 0;
    stat.minKey.assign(arr, arr + 8);
    arr[7] = 101;
    stat.maxKey.assign(arr, arr + 8);
    cfactory = TerarkIndex::SelectFactory(stat, "");
    assert(cfactory != nullptr);
  }
  /*{
    // do NOT select composite
    TerarkIndex::KeyStat stat;
    stat.commonPrefixLen = 0;
    stat.minKeyLen = stat.maxKeyLen = 9;
    auto factory = TerarkIndex::SelectFactory(stat, "");
    assert(factory == nullptr);
    }*/
  /*{
    // do NOT select composite
    TerarkIndex::KeyStat stat;
    stat.numKeys = 1;
    stat.commonPrefixLen = 0;
    stat.minKeyLen = stat.maxKeyLen = 8;
    char arr[8] = { 0 };
    stat.minKey.assign(arr, arr + 8);
    arr[3] = 1;
    stat.maxKey.assign(arr, arr + 8);
    auto factory = TerarkIndex::SelectFactory(stat, "");
    assert(factory == nullptr);
    }*/
  {
    // do select composite
    TerarkIndex::KeyStat stat;
    stat.numKeys = 100;
    stat.commonPrefixLen = 0;
    stat.minKeyLen = stat.maxKeyLen = 8;
    char arr[8] = { 0 };
    arr[7] = 1;
    stat.minKey.assign(arr, arr + 8);
    arr[6] = 1;
    stat.maxKey.assign(arr, arr + 8);
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
