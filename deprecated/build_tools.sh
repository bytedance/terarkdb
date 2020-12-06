#!/bin/bash
set LC_ALL=C

cpuNum=`nproc`

make tools_lib DEBUG_LEVEL=0 -j $cpuNum

make test_libs DEBUG_LEVEL=0 -j $cpuNum

make db_bench DEBUG_LEVEL=0 -j $cpuNum

make trace_analyzer DEBUG_LEVEL=0 -j $cpuNum
