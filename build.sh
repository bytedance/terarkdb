#!/usr/bin/env bash

make libzstd.a libsnappy.a liblz4.a libbz2.a -j 4
make shared_lib DEBUG_LEVEL=0 -j 4 DISABLE_WARNING_AS_ERROR=1


pkgdir=pkg
mkdir -p $pkgdir
cp -r db        $pkgdir
cp -r env       $pkgdir
cp -r include   $pkgdir
cp -r memtable  $pkgdir
cp -r port      $pkgdir
cp -r table     $pkgdir
cp -r util      $pkgdir



# move all binaries to output/ dir for next CICD steps
WITH_BMI2=`./cpu_has_bmi2.sh`
SYSTEM=`uname -m -s | sed 's:[ /]:-:g'`
tmpfile=`mktemp compiler-XXXXXX`
COMPILER=`gcc tools/configure/compiler.cpp -o $tmpfile.exe && ./$tmpfile.exe && rm -f $tmpfile*`
PLATFORM_DIR=$SYSTEM-$COMPILER-bmi2-$WITH_BMI2

echo $PLATFORM_DIR

#mv 'pkg/terark-fsa_all-'$PLATFORM_DIR output

