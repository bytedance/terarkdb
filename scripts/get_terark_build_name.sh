#!/bin/sh
if [ $# -lt 1 ]; then
	echo usage $0 CXX [WITH_BMI2]
	exit 1
fi
CXX=$1
set -e
tmpfile=`mktemp /tmp/compiler-XXXXXX`
dir=`dirname $0`
dir=`cd $dir; pwd`
$CXX $dir/terark-tools/detect-compiler.cpp -o ${tmpfile}.exe
COMPILER=`${tmpfile}.exe`
rm -f ${tmpfile}*
UNAME_MachineSystem=`uname -m -s | sed 's:[ /]:-:g'`
if [ -z ${WITH_BMI2} ]; then
	WITH_BMI2=`bash $dir/cpu_has_bmi2.sh`
fi
BUILD_NAME=${UNAME_MachineSystem}-${COMPILER}-bmi2-${WITH_BMI2}
echo -n $BUILD_NAME
