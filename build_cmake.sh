#!/bin/bash

BASE=$PWD
OUTPUT=output
mkdir -p $OUTPUT

if test -n "$BUILD_BRANCH"; then
    # this script is run in SCM auto build
    git checkout "$BUILD_BRANCH"
    sudo apt-get update
    sudo apt-get install libaio-dev
else
    echo you must ensure libaio-dev have been installed
fi

git submodule update --init --recursive

cd $BASE/$OUTPUT && cmake ../ -DCMAKE_BUILD_TYPE=Release -DWITH_TESTS=OFF
cd $BASE/$OUTPUT && make -j $(nproc) && make install
