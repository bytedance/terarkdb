
#include "index_composite_ut.h"


int main(int argc, char** argv) {
  printf("EXAGGERATE\n");

  printf("/////////////// uint test\n");
  test_il256_il256_uint(standard_ascend);
  test_il256_il256_uint(standard_descend);

  test_allone_il256_uint(standard_ascend);
  test_allone_il256_uint(standard_descend);

  test_allone_allzero_uint(standard_allzero);

  test_data_seek_short_target_uint();

  test_seek_cost_effective();

  printf("///////////// str test\n");
  test_il256_il256_str(standard_ascend);

  test_il256_il256_str(standard_descend);

  test_allone_il256_str(standard_ascend);
  test_allone_il256_str(standard_descend);

  test_allone_allzero_str(standard_allzero);

  test_data_seek_short_target_str();

  return 0;
}
