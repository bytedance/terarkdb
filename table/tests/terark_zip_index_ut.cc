
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
}

/*
 * 1. index1st: uint64, index2nd: uint64
 */
typedef map<string, int> IndexDict;

void init_stat(string& fpath, TerarkIndex::KeyStat& stat) {
  std::fstream fp(fpath.c_str(), std::ios::in);
  char prev[17] = { 0 };
  int line = 0;
  fp.read(prev, 17);
  stat.minKey.assign(prev, prev + 16);
  while (fp) {
    line ++;
    char buf[17] = { 0 };
    fp.read(buf, 17);
    if (!fp) break;
    else memcpy(prev, buf, 16);
  }
  stat.maxKey.assign(prev, prev + 16);
  stat.numKeys = line;
  stat.commonPrefixLen = 0;
  stat.minKeyLen = 16;
  stat.maxKeyLen = 16;
  stat.sumKeyLen = 16 * line;
  fp.close();
}

void read_input_simple() {
  string fpath = "./samples_simple.txt";
  rocksdb::TerarkIndex::KeyStat stat;
  init_stat(fpath, stat);

  FileStream fp(fpath, "r");
  NativeDataInput<InputBuffer> tempKeyFileReader(&fp);
  auto factory = rocksdb::TerarkIndex::GetFactory("CompositeIndex");
  //rocksdb::TerarkCompositeIndex_IL_256_32::MyFactory factory;
  rocksdb::TerarkZipTableOptions tableOpt;

  factory->Build(tempKeyFileReader, tableOpt, stat);
}

int main(int argc, char** argv) {
  printf("EXAGGERATE\n");
  read_input_simple();
  return 0;
}

//}
