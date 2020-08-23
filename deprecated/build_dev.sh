#!/usr/bin/env bash
#
# usage:
#
#   USE_VALGRIND=1 ./build_dev.sh
#

set -e

VALGRIND=0
WITH_BMI2=1

if [ "$USE_VALGRIND" == "1" ]; then
  VALGRIND=1
fi

if [ `uname` == Darwin ]; then
	cpuNum=`sysctl -n machdep.cpu.thread_count`
else
	cpuNum=`nproc`
fi

if test -n "$BUILD_BRANCH"; then
    # this script is run in SCM auto build
    git checkout "$BUILD_BRANCH"
    sudo apt-get update
    sudo apt-get install libaio-dev
else
    echo you must ensure libaio-dev have been installed
fi

if test -z "$NO_INIT"; then
  if [ ! -f "terark-core.got" ]; then
    git submodule update --init --recursive
  fi
fi

# export BUNDLE_ALL_TERARK_STATIC=${BUNDLE_ALL_TERARK_STATIC:-1}

# build targets
# make LINK_TERARK=static \
#      EXTRA_CXXFLAGS="-DROCKSDB_VALGRIND_RUN=$VALGRIND" \
#      BMI2=$WITH_BMI2 \
#      DISABLE_WARNING_AS_ERROR=1 \
#      DEBUG_LEVEL=0 shared_lib -j $cpuNum

make LINK_TERARK=static \
     EXTRA_CXXFLAGS="-DROCKSDB_VALGRIND_RUN=$VALGRIND -Wfatal-errors" \
     BMI2=$WITH_BMI2 \
     DISABLE_WARNING_AS_ERROR=1 \
     DEBUG_LEVEL=2 shared_lib -j $cpuNum

# static library
# make LINK_TERARK=static \
#      EXTRA_CXXFLAGS="-DROCKSDB_VALGRIND_RUN=$VALGRIND" \
#      BMI2=$WITH_BMI2 \
#      DISABLE_WARNING_AS_ERROR=1 \
#      DEBUG_LEVEL=0 static_lib -j $cpuNum

# make LINK_TERARK=static \
#      EXTRA_CXXFLAGS="-DROCKSDB_VALGRIND_RUN=$VALGRIND" \
#      BMI2=$WITH_BMI2 \
#      DISABLE_WARNING_AS_ERROR=1 \
#      DEBUG_LEVEL=2 static_lib -j $cpuNum

pkgdir=output
rm -rf $pkgdir

# copy all header files
mkdir -p $pkgdir
mkdir -p $pkgdir/lib
mkdir -p $pkgdir/lib_static

cp -r include      $pkgdir
cp -r db           $pkgdir/include
cp -r env          $pkgdir/include
cp -r memtable     $pkgdir/include
cp -r port         $pkgdir/include
cp -r table        $pkgdir/include
cp -r util         $pkgdir/include
cp -r utilities    $pkgdir/include
cp -r options      $pkgdir/include
cp -r monitoring   $pkgdir/include

rm -f `find $pkgdir -name '*.cc' -o -name '*.d' -o -name '*.o'`

# detect output dir name
SYSTEM=`uname -m -s | sed 's:[ /]:-:g'`
tmpfile=`mktemp compiler-XXXXXX`
COMPILER=`gcc terark-tools/detect-compiler.cpp -o $tmpfile.exe && ./$tmpfile.exe && rm -f $tmpfile*`
PLATFORM_DIR=$SYSTEM-$COMPILER-bmi2-$WITH_BMI2

#echo build/$PLATFORM_DIR/shared_lib/dbg-0/

# copy terark-rocksdb dynamic lib
# cp -a shared-objects/build/$PLATFORM_DIR/dbg-0/librocksdb* $pkgdir/lib
cp -a shared-objects/build/$PLATFORM_DIR/dbg-2/librocksdb* $pkgdir/lib

echo "build and package successful!"

