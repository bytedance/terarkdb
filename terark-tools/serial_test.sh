mkdir -p test-log
rm -f test-log/failed.list
touch test-log/failed.list
for name in $*
do
	echo "Testing $name"
	make DEBUG_LEVEL=2 COMPILE_WITH_ASAN=1 $name -j256 > temp_compile.log 2>&1
	if [ ! -f "$name" ]; then
		mv temp_compile.log test-log/CE_$name.log
	else
       		./$name > test-log/$name.log 2> test-log/$name.errlog
		if [ `grep "FAILED" test-log/$name.log -c` -gt 0 ]; then
			cat "$name\n" >> test-log/failed.list
			echo "FAILED : $name"
		else
			rm -f $name
			echo "PASSED : $name"
		fi
	fi
done
