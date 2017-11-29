
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
 * pos: 0 1 2 3 4 5 6
 *      1 0 1 1 0 0 1
 */
void test_simple() {
  printf("==== Test started\n");
  const size_t kCnt = 7;
  terark::rank_select_simple simple(kCnt);
  size_t pos = 0;
  simple.set1(pos++); simple.set0(pos++);
  simple.set1(pos++); simple.set1(pos++);
  simple.set0(pos++); simple.set0(pos++);
  simple.set1(pos++);
  simple.build_cache(true, true);
  
  rank_select_fewzero rs(kCnt);
  rs.build_from(simple);
  {
    assert(rs.zero_seq_revlen(0) == 0);
    assert(rs.zero_seq_revlen(1) == 0);
    assert(rs.zero_seq_revlen(2) == 1);
    assert(rs.zero_seq_revlen(3) == 0);
    assert(rs.zero_seq_revlen(4) == 0);
    assert(rs.zero_seq_revlen(5) == 1);
    assert(rs.zero_seq_revlen(6) == 2);
  }
  {
    assert(rs.one_seq_revlen(0) == 0);
    assert(rs.one_seq_revlen(1) == 1);
    assert(rs.one_seq_revlen(2) == 0);
    assert(rs.one_seq_revlen(3) == 1);
    assert(rs.one_seq_revlen(4) == 2);
    assert(rs.one_seq_revlen(5) == 0);
    assert(rs.one_seq_revlen(6) == 0);
  }
  {
    assert(rs.zero_seq_len(0) == 0);
    assert(rs.zero_seq_len(1) == 1);
    assert(rs.zero_seq_len(2) == 0);
    assert(rs.zero_seq_len(3) == 0);
    assert(rs.zero_seq_len(4) == 2);
    assert(rs.zero_seq_len(5) == 1);
    assert(rs.zero_seq_len(6) == 0);
  }
  {
    assert(rs.one_seq_len(0) == 1);
    assert(rs.one_seq_len(1) == 0);
    assert(rs.one_seq_len(2) == 2);
    assert(rs.one_seq_len(3) == 1);
    assert(rs.one_seq_len(4) == 0);
    assert(rs.one_seq_len(5) == 0);
    assert(rs.one_seq_len(6) == 1);
  }
  {
    assert(rs.is0(0) == false);
    assert(rs.is0(1) == true);
    assert(rs.is0(2) == false);
    assert(rs.is0(3) == false);
    assert(rs.is0(4) == true);
    assert(rs.is0(5) == true);
    assert(rs.is0(6) == false);
  }
  {
    assert(rs.is1(0) == true);
    assert(rs.is1(1) == false);
    assert(rs.is1(2) == true);
    assert(rs.is1(3) == true);
    assert(rs.is1(4) == false);
    assert(rs.is1(5) == false);
    assert(rs.is1(6) == true);
  }
  {
    assert(rs.rank0(0) == 0);
    assert(rs.rank0(1) == 0);
    assert(rs.rank0(2) == 1);
    assert(rs.rank0(3) == 1);
    assert(rs.rank0(4) == 1);
    assert(rs.rank0(5) == 2);
    assert(rs.rank0(6) == 3);
  }
  {
    assert(rs.rank1(0) == 0);
    assert(rs.rank1(1) == 1);
    assert(rs.rank1(2) == 1);
    assert(rs.rank1(3) == 2);
    assert(rs.rank1(4) == 3);
    assert(rs.rank1(5) == 3);
    assert(rs.rank1(6) == 3);
  }
  {
    assert(rs.select0(0) == 0);
    assert(rs.select0(1) == 1);
    assert(rs.select0(2) == 4);
    assert(rs.select0(3) == 5);
  }
  {
    assert(rs.select1(0) == 0);
    assert(rs.select1(1) == 0);
    assert(rs.select1(2) == 2);
    assert(rs.select1(3) == 3);
    assert(rs.select1(4) == 6);
  }
  {
    assert(rs.max_rank0() == 3);
    assert(rs.max_rank1() == 4);
  }

  printf("\tdone\n");
}

int main() {
  printf("EXAGGERATE\n");
  test_simple();

  return 0;
}
