
#include "index_composite_ut.h"


int main(int argc, char** argv) {
  printf("EXAGGERATE\n");

  test_il256_il256_uint(standard_ascend);
  test_il256_il256_uint(standard_descend);

  test_allone_il256_uint(standard_ascend);
  test_allone_il256_uint(standard_descend);

  test_allone_allzero_uint(standard_allzero);

  test_data_seek_short_target_uint();

  test_select_uint();

  return 0;
}
