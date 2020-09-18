#!/bin/bash

BASE=$PWD
OUTPUT=output
mkdir -p $OUTPUT
cd $BASE/$OUTPUT && cmake ../ -DCMAKE_BUILD_TYPE=Release -DWITH_TESTS=OFF
cd $BASE/$OUTPUT && make -j $(nproc) && make install
