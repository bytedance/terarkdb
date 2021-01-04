#!/bin/bash

BASE=$PWD
OUTPUT=output
mkdir -p $OUTPUT

git submodule update --init --recursive

cd $BASE/$OUTPUT && cmake ../ -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_TOOLS=ON -DWITH_TERARK_ZIP=ON
cd $BASE/$OUTPUT && make -j $(nproc) && make install
