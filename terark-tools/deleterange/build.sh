#!/bin/bash
set -e

BASE_DIR=$PWD
if [ `uname` == Darwin ]; then
	cpuNum=`sysctl -n machdep.cpu.thread_count`
else
	cpuNum=`nproc`
fi

mkdir -p build && cd build

cmake ../ && make -j $cpuNum