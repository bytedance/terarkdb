#!/bin/bash
#
# WITH_TESTS=1 ./build.sh
#

BASE=$PWD
OUTPUT=output
mkdir -p $OUTPUT

if test -n "$BUILD_BRANCH"; then
    # this script is run in SCM auto build
    git checkout "$BUILD_BRANCH"
    sudo apt-get update
    sudo apt-get install libaio-dev -y
else
    echo you must ensure libaio-dev have been installed
fi

git submodule update --init --recursive

if [ "$WITH_TESTS" == "1" ]; then
  WITH_TESTS=ON
  echo "build $BUILD_TYPE, with_tests = $WITH_TESTS"
  cd $BASE/$OUTPUT && cmake ../ -DCMAKE_INSTALL_PREFIX=$OUTPUT -DCMAKE_BUILD_TYPE=Debug -DWITH_TESTS=${WITH_TESTS} -DWITH_TOOLS=ON -DWITH_TERARK_ZIP=ON
else
  WITH_TESTS=OFF
  echo "build $BUILD_TYPE, with_tests = $WITH_TESTS"
  cd $BASE/$OUTPUT && cmake ../ -DCMAKE_INSTALL_PREFIX=$OUTPUT -DCMAKE_BUILD_TYPE=Release -DWITH_TOOLS=ON -DWITH_TERARK_ZIP=ON
fi
cd $BASE/$OUTPUT && make -j $(nproc) && make install
