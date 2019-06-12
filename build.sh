#!/usr/bin/env bash
# author : guokuankuan@bytedance.com

if [ `uname` == Darwin ]; then
	cpuNum=`sysctl -n machdep.cpu.thread_count`
else
	cpuNum=`nproc`
fi
WITH_BMI2=1

# clone terark-zip-rocksdb: terark-rocksdb depends on some header files from zip-rocksdb

# build targets
make BUNDLE_TERARK_ZIP_ROCKSDB=1 LINK_TERARK=static TERARK_CORE_PKG_DIR=terark-core \
     BMI2=$WITH_BMI2 \
     DISABLE_WARNING_AS_ERROR=1 \
     DEBUG_LEVEL=0 shared_lib -j $cpuNum

make BUNDLE_TERARK_ZIP_ROCKSDB=1 LINK_TERARK=static TERARK_CORE_PKG_DIR=terark-core \
     BMI2=$WITH_BMI2 \
     DISABLE_WARNING_AS_ERROR=1 \
     DEBUG_LEVEL=2 shared_lib -j $cpuNum

pkgdir=output
rm -rf $pkgdir

# copy all header files
mkdir -p $pkgdir
mkdir -p $pkgdir/lib

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
cp -a shared-objects/build/$PLATFORM_DIR/dbg-2/librocksdb* $pkgdir/lib

echo "build and package successful!"
