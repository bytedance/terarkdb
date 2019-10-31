#!/usr/bin/env bash
# author : guokuankuan@bytedance.com

if [ `uname` == Darwin ]; then
	cpuNum=`sysctl -n machdep.cpu.thread_count`
else
	cpuNum=`nproc`
fi
WITH_BMI2=1

if test -n "$BUILD_BRANCH"; then
    # this script is run in SCM auto build
    git checkout "$BUILD_BRANCH"
    git submodule update --init --recursive
    sudo apt-get update
    sudo apt-get install libaio-dev
else
    echo you must ensure libaio-dev have been installed
fi

export BUNDLE_ALL_TERARK_STATIC=${BUNDLE_ALL_TERARK_STATIC:-1}

# # build targets
make LINK_TERARK=static \
    BMI2=$WITH_BMI2 \
    DISABLE_WARNING_AS_ERROR=1 \
    DEBUG_LEVEL=0 shared_lib -j $cpuNum

make LINK_TERARK=static \
    BMI2=$WITH_BMI2 \
    DISABLE_WARNING_AS_ERROR=1 \
    DEBUG_LEVEL=1 shared_lib -j $cpuNum

make LINK_TERARK=static \
    BMI2=$WITH_BMI2 \
    DISABLE_WARNING_AS_ERROR=1 \
    DEBUG_LEVEL=2 shared_lib -j $cpuNum

# static library
make LINK_TERARK=static \
     BMI2=$WITH_BMI2 \
     DISABLE_WARNING_AS_ERROR=1 \
     DEBUG_LEVEL=0 static_lib -j $cpuNum

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
cp -a shared-objects/build/$PLATFORM_DIR/dbg-0/librocksdb* $pkgdir/lib
cp -a shared-objects/build/$PLATFORM_DIR/dbg-1/librocksdb* $pkgdir/lib
cp -a shared-objects/build/$PLATFORM_DIR/dbg-2/librocksdb* $pkgdir/lib
cp -a librocksdb*.a $pkgdir/lib_static

echo "build and package successful!"
