#DictZipBlobStore_zipThreads=0 numactl --cpubind=$1 --membind=$1 ./rocksdb_test data$1
DictZipBlobStore_zipThreads=0 gdb --args ./rocksdb_test Iter Get Terark RangeDel
