#!/bin/bash
set -e

BASE_DIR=$PWD
if [ `uname` == Darwin ]; then
	cpuNum=`sysctl -n machdep.cpu.thread_count`
else
	cpuNum=`nproc`
fi

mkdir -p build && cd build

cmake ../ -DCMAKE_BUILD_TYPE=Debug && make -j $cpuNum
