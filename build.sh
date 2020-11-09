#!/bin/bash
#
# WITH_TESTS=1 ./build_cmake.sh
#

BASE=$PWD
OUTPUT=output
mkdir -p $OUTPUT

BUILD_TYPE=Release
if [ "$WITH_TESTS" == "1" ];then
  BUILD_TYPE=Debug
  WITH_TESTS=ON
else
  WITH_TESTS=OFF
fi

echo "build $BUILD_TYPE, with_tests = $WITH_TESTS"

if test -n "$BUILD_BRANCH"; then
    # this script is run in SCM auto build
    git checkout "$BUILD_BRANCH"
    sudo apt-get update
    sudo apt-get install libaio-dev -y
else
    echo you must ensure libaio-dev have been installed
fi

git submodule update --init --recursive

cd $BASE/$OUTPUT && cmake ../ -DCMAKE_BUILD_TYPE=$BUILD_TYPE -DWITH_TESTS=$WITH_TESTS
cd $BASE/$OUTPUT && make -j $(nproc) && make install
