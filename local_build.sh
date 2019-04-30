#!/usr/bin/env bash
# author: guokuankuan@bytedance.com

TERARK_CORE_DIST=../terark-core/output
TERARK_ROCKSDB_DIST=../terark-rocksdb/output

git submodule update --init

# need terark-core
rm -rf terark_core.tar.gz
rm -r terark-core || true
mkdir terark-core

# need terark-rocksdb
rm -rf terark_rocksdb.tar.gz
rm -r terark-rocksdb || true
mkdir terark-rocksdb


if [ `uname` == Darwin ]; then
	cp -r $TERARK_ROCKSDB_DIST/* terark-rocksdb/
	cp -r $TERARK_CORE_DIST/* terark-core/
	cpuNum=`sysctl -n machdep.cpu.thread_count`
else
	cp -rlP $TERARK_ROCKSDB_DIST/* terark-rocksdb/
	cp -rlP $TERARK_CORE_DIST/* terark-core/
	cpuNum=`nproc`
fi


rm -rf pkg
TerarkLibDir=terark-core/lib PKG_WITH_ROCKSDB=0 make pkg -j $cpuNum


# move all binaries to output/ dir for next CICD steps
WITH_BMI2=`./cpu_has_bmi2.sh`
SYSTEM=`uname -m -s | sed 's:[ /]:-:g'`
tmpfile=`mktemp compiler-XXXXXX`
COMPILER=`gcc tools/configure/compiler.cpp -o $tmpfile.exe && ./$tmpfile.exe && rm -f $tmpfile*`
PLATFORM_DIR=$SYSTEM-$COMPILER-bmi2-$WITH_BMI2

rm -rf output
echo pkg/terark-zip-rocksdb-$PLATFORM_DIR
mv pkg/terark-zip-rocksdb-$PLATFORM_DIR output
