#!/bin/bash

BASE=$PWD
OUTPUT=output
METRICS_PATH=$BASE/../metrics2-cmake
mkdir -p $OUTPUT

cd $BASE/$OUTPUT && cmake ../ -DCMAKE_INSTALL_PREFIX=$OUTPUT \
							-DCMAKE_BUILD_TYPE=RelWithDebInfo \
							-DWITH_TOOLS=ON \
							-DWITH_TERARK_ZIP=OFF \
							-DWITH_BYTEDANCE_METRICS=ON \
							-DBYTEDANCE_METRICS_PATH=$METRICS_PATH \
							-DWITH_ZENFS=ON
cd $BASE/$OUTPUT && make -j $(nproc)
