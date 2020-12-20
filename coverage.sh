#!/bin/sh

BASE=$PWD
GCC_VERSION=`gcc -dumpversion | awk -F '.' '{print $1}')`
mkdir -p build

# build
cd $BASE/build && cmake ../ -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_TESTS=ON -DWITH_TOOLS=OFF -DWITH_COVERAGE=ON

cd $BASE/build && make -j $(nproc)

# run tests
cd $BASE/build && ctest -j 20

# collect result
cd $BASE/build && lcov --gcov-tool /usr/bin/gcov-$GCC_VERSION --directory . --capture --output-file lcov_output.info

cd $BASE/build && genhtml lcov_output.info --output=cov_html

# print result
