#!/usr/bin/env bash

#make libzstd.a libsnappy.a liblz4.a libbz2.a -j 4
#make shared_lib DEBUG_LEVEL=0 -j 4 DISABLE_WARNING_AS_ERROR=1


pkgdir=pkg
# copy all header files
mkdir -p $pkgdir
cp -r db        $pkgdir
cp -r env       $pkgdir
cp -r include   $pkgdir
cp -r memtable  $pkgdir
cp -r port      $pkgdir
cp -r table     $pkgdir
cp -r util      $pkgdir


# detect output dir name
WITH_BMI2=0
SYSTEM=`uname -m -s | sed 's:[ /]:-:g'`
tmpfile=`mktemp compiler-XXXXXX`
COMPILER=`gcc terark-tools/detect-compiler.cpp -o $tmpfile.exe && ./$tmpfile.exe && rm -f $tmpfile*`
PLATFORM_DIR=$SYSTEM-$COMPILER-bmi2-$WITH_BMI2

echo build/$PLATFORM_DIR/shared_lib/dbg-0/

# copy dynamic lib
mkdir -p $pkgdir/lib
cp build/$PLATFORM_DIR/shared_lib/dbg-0/librocksdb.* $pkgdir/lib

# change directory to fit CICD directory
mv pkg output

