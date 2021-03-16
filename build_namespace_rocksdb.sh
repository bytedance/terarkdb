#!/bin/bash
#
# WITH_TESTS=1 ./build_cmake.sh
#

BASE=$PWD
OUTPUT=output
mkdir -p $OUTPUT

if [ "$WITH_TESTS" = "1" ];then
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

cd $BASE/$OUTPUT && cmake ../ -DCMAKE_BUILD_TYPE=Release -DWITH_TESTS=${WITH_TESTS} -DWITH_TOOLS=ON -DWITH_TERARK_ZIP=ON -DWITH_TERARKDB_NAMESPACE=rocksdb
cd $BASE/$OUTPUT && make -j $(nproc)
