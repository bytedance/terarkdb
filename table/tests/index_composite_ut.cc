
#include <chrono>
#include "index_composite_ut.h"

int main_impl(int argc, char** argv) {
    printf("EXAGGERATE\n");

    printf("/////////////// sorteduint test\n");
    test_il256_il256_sorteduint(standard_ascend);
    test_il256_il256_sorteduint(standard_descend);

    test_allone_il256_sorteduint(standard_ascend);
    test_allone_il256_sorteduint(standard_descend);

    test_fewzero_allzero_sorteduint(standard_ascend);
    test_fewzero_allzero_sorteduint(standard_descend);

    test_allone_allzero_sorteduint(standard_allzero);

    test_data_seek_short_target_sorteduint();

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

int main(int argc, char** argv) {
  //auto now = std::chrono::system_clock::now;
  //auto end = now() + std::chrono::minutes(2);
  //while (now() < end) {
  main_impl(argc, argv);
  //}
  return 0;
}
