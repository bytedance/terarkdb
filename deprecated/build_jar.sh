#!/usr/bin/env bash
#

set -e

VALGRIND=0
PORTABLE=1
WITH_BMI2=1

TERARKDB_ENABLE_METRICS=0
TERARKDB_ENABLE_CONSOLE=0

if [ `uname` == Darwin ]; then
	cpuNum=`sysctl -n machdep.cpu.thread_count`
else
	cpuNum=`nproc`
fi

if test -n "$BUILD_BRANCH"; then
    # this script is run in SCM auto build
    git checkout "$BUILD_BRANCH"
    sudo apt-get update
    sudo apt-get install libaio-dev -y
    sudo apt-get install libcurl4-openssl-dev -y
    sudo apt-get install openjdk-8-jdk -y
else
    echo you must ensure libaio-dev have been installed
fi

if test -z "$NO_INIT"; then
  if [ ! -f "terark-core.got" ]; then
    git submodule update --init --recursive
  fi
fi


export BUNDLE_ALL_TERARK_STATIC=${BUNDLE_ALL_TERARK_STATIC:-1}

# build all related dynamic libraries
make LINK_TERARK=static \
     EXTRA_CXXFLAGS="-DROCKSDB_VALGRIND_RUN=$VALGRIND" \
     BMI2=$WITH_BMI2 \
     DISABLE_WARNING_AS_ERROR=1 \
     DISABLE_JEMALLOC=1 \
     TERARKDB_ENABLE_METRICS=$TERARKDB_ENABLE_METRICS \
     TERARKDB_ENABLE_CONSOLE=$TERARKDB_ENABLE_CONSOLE \
     PORTABLE=$PORTABLE \
     DEBUG_LEVEL=0 shared_lib -j $cpuNum


# pkgdir=output
# rm -f `find $pkgdir -name '*.cc' -o -name '*.d' -o -name '*.o'`

# detect output dir name
# SYSTEM=`uname -m -s | sed 's:[ /]:-:g'`
# tmpfile=`mktemp compiler-XXXXXX`
# COMPILER=`gcc terark-tools/detect-compiler.cpp -o $tmpfile.exe && ./$tmpfile.exe && rm -f $tmpfile*`
# PLATFORM_DIR=$SYSTEM-$COMPILER-bmi2-$WITH_BMI2


# cp -a shared-objects/build/$PLATFORM_DIR/dbg-0/librocksdb* $pkgdir/lib

if test -n "$BUILD_BRANCH"; then
  sudo cp java/rocksjni/jni.h /usr/include/
  sudo cp java/rocksjni/jni_md.h /usr/include/
fi

# build jar
make LINK_TERARK=static \
     EXTRA_CXXFLAGS="-DROCKSDB_VALGRIND_RUN=$VALGRIND" \
     BMI2=$WITH_BMI2 \
     DISABLE_WARNING_AS_ERROR=1 \
     DISABLE_JEMALLOC=1 \
     TERARKDB_ENABLE_METRICS=$TERARKDB_ENABLE_METRICS \
     TERARKDB_ENABLE_CONSOLE=$TERARKDB_ENABLE_CONSOLE \
     PORTABLE=$PORTABLE \
     DEBUG_LEVEL=0 frocksdbjavastaticrelease -j $cpuNum

rm -rf output
mv java/target/frocksdb-release/ output
cp java/RocksDBJava.java output/

echo "build and package successful!"
