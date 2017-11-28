
#include <memory>

#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <terark/io/FileStream.hpp>
#include <terark/rank_select.hpp>

#include "../terark_zip_common.h"
#include "../terark_zip_index.h"

#include "rank_select_fewzero.h"

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

/*
 * 1 0 1 1
 */
void test_simple() {
  printf("==== Test started\n");
  rank_select_fewzero rs(4);
  size_t pos = 0;
  rs.set1(pos++); rs.set0(pos++);
  rs.set1(pos++); rs.set1(pos++);
  rs.build_cache(true, true);
  {
    assert(rs.rank0(0) == 0);
    assert(rs.rank0(1) == 0);
    assert(rs.rank0(2) == 1);
    assert(rs.rank0(3) == 1);
  }
  {
    assert(rs.rank1(0) == 0);
    assert(rs.rank1(1) == 1);
    assert(rs.rank1(2) == 1);
    assert(rs.rank1(3) == 2);
  }
  printf("\tdone\n");
}

int main() {
  printf("EXAGGERATE\n");
  test_simple();

  return 0;
}
