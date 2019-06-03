#!/bin/bash

mydir=`dirname $0`
$mydir/cpu_features.sh | grep -qs bmi2
bmi2_status=${PIPESTATUS[1]}
if [ $bmi2_status -eq 0 ] # 0 indicate success
then
	echo 1
else
	echo 0
fi

