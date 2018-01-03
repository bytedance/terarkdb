#!/bin/bash

BASE_DIR=/newssd1/temp

echo "start pulling from terark-zip-rocksdb..."
cd ${BASE_DIR}/terark-zip-rocksdb
git pull new-code master
echo "-- done"

echo "start pulling from terark..."
cd ${BASE_DIR}/terark
git pull new-code master
echo "-- done"

echo "start building..."
cd ${BASE_DIR}/terark
sh alldeploy.sh pkg
echo "-- done"

echo "GoodJob"
