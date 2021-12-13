#!/bin/bash
#
# WITH_TESTS=1 WITH_ZNS=1 ./build.sh
#

set -e

BASE=$PWD
OUTPUT=build
mkdir -p $OUTPUT

if [ -z "$WITH_TESTS" ] || [ -z "${WITH_ZNS}" ]; then
  echo "usage: WITH_TESTS=OFF WITH_ZNS=OFF ./build.sh"
  exit
fi

if test -n "$BUILD_BRANCH"; then
  # this script is run in SCM auto build
  git checkout "$BUILD_BRANCH"
  sudo apt-get update
  sudo apt-get install libaio-dev -y
else
  echo "libaio is required, please make sure you have it!"
fi

git submodule update --init --recursive


if [ "$WITH_TESTS" == "ON" ]; then
  BUILD_TYPE=Debug
  echo "You are building TerarkDB with tests, so debug mode is enabled"
else
  BUILD_TYPE=Release
fi

echo "build = $BUILD_TYPE, with_tests = $WITH_TESTS, with_zns = $WITH_ZNS"

cd $BASE/$OUTPUT && cmake ../ \
  -DCMAKE_INSTALL_PREFIX=$OUTPUT \
  -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
  -DWITH_TESTS=$WITH_TESTS \
  -DWITH_ZENFS=$WITH_ZNS \
  -DWITH_TOOLS=ON \
  -DWITH_TERARK_ZIP=ON $@

# compatibility with macOS
NPROC=$(nproc || sysctl -n hw.logicalcpu)

cd $BASE/$OUTPUT && make -j $NPROC && make install
