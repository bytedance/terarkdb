#!/bin/bash

BASE_DIR=$PWD


mkdir -p verify_tests/build/ && cd verify_tests/build && cmake ../ && make
rm -rf /tmp/rocksdb_test


# test case use this dir as temp directory

echo "start testing..."
export LD_LIBRARY_PATH=$BASE_DIR/output/lib_static
echo $LD_LIBRARY_PATH

cd $BASE_DIR/verify_tests/build
r1=`env TerarkZipTable_localTempDir=$PWD ./basic_test_static`
rm -rf /tmp/rocksdb_test
if [ "$r1" == "basic test success" ]; then
  echo "basic test success"
else
  echo "basic test failed!"
fi
