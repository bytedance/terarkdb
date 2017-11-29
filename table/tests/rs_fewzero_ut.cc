
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
 * pos: 0 1 2 3 4 5
 *      1 0 1 1 0 1
 */
void test_simple() {
  printf("==== Test started\n");
  rank_select_fewzero rs(6);
  size_t pos = 0;
  rs.set1(pos++); rs.set0(pos++);
  rs.set1(pos++); rs.set1(pos++);
  rs.set0(pos++); rs.set1(pos++);
  rs.build_cache(true, true);
  {
    assert(rs.max_rank0() == 2);
    assert(rs.max_rank1() == 4);
  }
  {
    assert(rs.select0(0) == 0);
    assert(rs.select0(1) == 1);
    assert(rs.select0(2) == 4);
  }
  {
    assert(rs.select1(0) == 0);
    assert(rs.select1(1) == 0);
    assert(rs.select1(2) == 2);
    assert(rs.select1(3) == 3);
    assert(rs.select1(4) == 5);
  }
  {
    assert(rs.rank0(0) == 0);
    assert(rs.rank0(1) == 0);
    assert(rs.rank0(2) == 1);
    assert(rs.rank0(3) == 1);
    assert(rs.rank0(4) == 1);
    assert(rs.rank0(5) == 2);
  }
  {
    assert(rs.rank1(0) == 0);
    assert(rs.rank1(1) == 1);
    assert(rs.rank1(2) == 1);
    assert(rs.rank1(3) == 2);
    assert(rs.rank1(4) == 3);
    assert(rs.rank1(5) == 3);
  }
  printf("\tdone\n");
}

int main() {
  printf("EXAGGERATE\n");
  test_simple();

  return 0;
}
