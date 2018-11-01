#include <stdio.h>

int main() {
#ifdef __clang_major__
	printf("clang-%d.%d", __clang_major__, __clang_minor__);
#elif defined(__INTEL_COMPILER)
	printf("icc-%d.%d", __INTEL_COMPILER/100, __INTEL_COMPILER%100);
#elif defined(__GNUC__)
	printf("g++-%d.%d", __GNUC__, __GNUC_MINOR__);
#endif
	return 0;
}