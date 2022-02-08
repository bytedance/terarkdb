#!/bin/bash
#
# WITH_TESTS=ON WITH_ZNS=OFF [METRICS_PATH=...] ./build.sh
#
set -e

git submodule update --init --recursive

BASE=$PWD
OUTPUT=build
WITH_BYTEDANCE_METRICS=OFF

if [ -z "$WITH_TESTS" ] || [ -z "${WITH_ZNS}" ]; then
  echo "usage: WITH_TESTS=OFF WITH_ZNS=OFF ./build.sh"
  exit
fi

if [ -z "$METRICS_PATH" ]; then
  echo "build without bytedance metrics reporter"
else
  WITH_BYTEDANCE_METRICS=ON
fi

if [ "$WITH_TESTS" == "ON" ]; then
  BUILD_TYPE=Debug
  echo "You are building TerarkDB with tests, so debug mode is enabled"
else
  BUILD_TYPE=RelWithDebInfo
fi

if test -n "$BUILD_BRANCH"; then
  # this script is run in SCM auto build
  git checkout "$BUILD_BRANCH"
  sudo apt-get update
  sudo apt-get install libaio-dev -y
else
  echo "libaio is required, please make sure you have it!"
fi

echo "build = $BUILD_TYPE, with_tests = $WITH_TESTS, with_zns = $WITH_ZNS"

rm -rf $OUTPUT && mkdir -p $OUTPUT
cd $BASE/$OUTPUT && cmake ../ \
  -DCMAKE_INSTALL_PREFIX=$OUTPUT \
  -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
  -DWITH_TESTS=$WITH_TESTS \
  -DWITH_ZENFS=$WITH_ZNS \
  -DBYTEDANCE_METRICS_PATH=$METRICS_PATH \
  -DWITH_BYTEDANCE_METRICS=$WITH_BYTEDANCE_METRICS \
  -DWITH_TOOLS=ON \
  -DWITH_TERARK_ZIP=OFF $@

# compatibility with macOS
NPROC=$(nproc || sysctl -n hw.logicalcpu)

cd $BASE/$OUTPUT && make -j $NPROC && make install
