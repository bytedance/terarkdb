add_executable(sst_scan sst_scan.cc)

target_link_libraries(
  sst_scan
	rocksdb
	gflags
)
