
#pragma once

#include <memory>

#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <terark/io/FileStream.hpp>
#include <terark/rank_select.hpp>
#include <terark/idx/terark_zip_index.hpp>

#include "../terark_zip_common.h"

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

using TERARKDB_NAMESPACE::TerarkIndex;

enum DataStored {
  standard_ascend = 0,
  standard_descend,
  standard_allzero,
};

// sorteduint related
extern void test_il256_il256_sorteduint(DataStored);
extern void test_allone_il256_sorteduint(DataStored);
extern void test_fewzero_allzero_sorteduint(DataStored);
extern void test_allone_allzero_sorteduint(DataStored);
extern void test_data_seek_short_target_sorteduint();

// uint related
extern void test_il256_il256_uint(DataStored);
extern void test_allone_il256_uint(DataStored);
extern void test_allone_allzero_uint(DataStored);
extern void test_data_seek_short_target_uint();
extern void test_seek_cost_effective();

// str related
extern void test_il256_il256_str(DataStored);
extern void test_allone_il256_str(DataStored);
extern void test_allone_allzero_str(DataStored);
extern void test_data_seek_short_target_str();


