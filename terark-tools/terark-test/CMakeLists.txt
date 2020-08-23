add_executable(rocksdb_test chaos_test.cc)

target_link_libraries(
	rocksdb_test
	rocksdb
	gflags
)
