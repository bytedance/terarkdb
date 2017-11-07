

TerarkDir=/newssd1/temp/terark/src
TerarkLib=/newssd1/zzz/terark-zip-rocksdb/pkg/terark-zip-rocksdb-Linux-x86_64-g++-5.4-bmi2-1
RocksDir=/newssd1/temp/rocksdb
CXX=g++
#CFLAGS=-g3 -std=c++11 -fPIC
CFLAGS=-g -std=c++11
INC=-I. -I.. -I$(TerarkDir) -I$(RocksDir)/include -I$(RocksDir)
LIBPATH=-L. -L.. -L$(TerarkLib)/lib -L$(RocksDir)
LIB=-lpthread -lrocksdb -lterark-core-g++-5.4-r -lterark-fsa-g++-5.4-r -lterark-zbs-g++-5.4-r

#OBJS=terark_zip_common.o terark_zip_config.o terark_zip_index.o
OBJS=terark_zip_common.o terark_zip_index.o


all: test_composite
#all: test_full

test_composite: ${OBJS}
	${CXX} -g3 -std=c++11 ${INC} ${LIBPATH} -o index_composite_ut index_composite_ut.cc ${OBJS} ${LIB}

test_full: ${OBJS}
	${CXX} -g3 -std=c++11 ${INC} ${LIBPATH} -o index_full_ut index_full_ut.cc ${OBJS} ${LIB}

terark_zip_common.o:
	${CXX} ${CFLAGS} ${INC} ${LIBPATH} -c ../terark_zip_common.cc ${LIB}

#terark_zip_config.o:
#	${CXX} ${CFLAGS} ${INC} ${LIBPATH} -c ../terark_zip_config.cc ${LIB}

#terark_zip_table.o:
#	${CXX} ${CFLAGS} ${INC} ${LIBPATH} -c ../terark_zip_table.cc ${LIB}

terark_zip_index.o:
	${CXX} ${CFLAGS} ${INC} ${LIBPATH} -c ../terark_zip_index.cc ${LIB}

clean:
	rm -f ${EXES} *.o *.exe *.a test_add bridge *.so* index_composite_ut index_full_ut


