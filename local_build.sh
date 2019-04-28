#!/usr/bin/env bash

# this build script is use for local machine build, use your own terark-core and terark-rocksdb insteads of download online.

if [ `uname` == Darwin ]; then
	cpuNum=`sysctl -n machdep.cpu.thread_count`
else
	cpuNum=`nproc`
fi

git submodule update --init


ROCKSDB_SRC=../terark-rocksdb/output TERARK_CORE_HOME=../terark-core/output make pkg -j $cpuNum


# move all binaries to output/ dir for next CICD steps
WITH_BMI2=`./cpu_has_bmi2.sh`
SYSTEM=`uname -m -s | sed 's:[ /]:-:g'`
tmpfile=`mktemp compiler-XXXXXX`
COMPILER=`gcc tools/configure/compiler.cpp -o $tmpfile.exe && ./$tmpfile.exe && rm -f $tmpfile*`
PLATFORM_DIR=$SYSTEM-$COMPILER-bmi2-$WITH_BMI2

echo $PLATFORM_DIR
