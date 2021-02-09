mac=0 && [ "$(uname)" == "Darwin" ] && mac=1
quit(){
  if [ $? -ne 0 ]; then
    echo "failed"
    exit 0
  fi
}
test(){
  quit
  if [ "$(uname)" == "Darwin" ]; then
    make -j`sysctl -n hw.ncpu`
  elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
   make -j`nproc`
  fi
  quit
}
git submodule update --init --recursive
[ ! -d output ] && mkdir output
cd output
# macos don't support JEMALLOC
cmake -DCMAKE_BUILD_TYPE=Debug -DWITH_JEMALLOC=OFF -DWITH_TESTS=ON -DWITH_TOOLS=ON -DWITH_ASAN=ON -DWITH_TERARK_ZIP=OFF -DWITH_TERARKDB_NAMESPACE:STRING=bytedance .. && test
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_JEMALLOC=OFF -DWITH_TESTS=ON -DWITH_TOOLS=ON -DWITH_ASAN=ON -DWITH_TERARK_ZIP=OFF -DWITH_TERARKDB_NAMESPACE:STRING=bytedance .. && test
cmake -DCMAKE_BUILD_TYPE=Release -DWITH_JEMALLOC=OFF -DWITH_TESTS=OFF -DWITH_TOOLS=ON -DWITH_ASAN=ON -DWITH_TERARK_ZIP=OFF -DWITH_TERARKDB_NAMESPACE:STRING=bytedance .. && test
[ ! $mac ] && cmake -DCMAKE_BUILD_TYPE=Release -DWITH_JEMALLOC=ON -DWITH_TESTS=OFF -DWITH_TOOLS=OFF -DWITH_ASAN=OFF -DWITH_TERARK_ZIP=ON -UWITH_TERARKDB_NAMESPACE .. && test
[ ! $mac ] && cmake -DCMAKE_BUILD_TYPE=Debug -DWITH_JEMALLOC=ON -DWITH_TESTS=ON -DWITH_TOOLS=ON -DWITH_ASAN=OFF -DWITH_TERARK_ZIP=ON -UWITH_TERARKDB_NAMESPACE .. && test
[ ! $mac ] && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_JEMALLOC=ON -DWITH_TESTS=OFF -DWITH_TOOLS=ON -DWITH_ASAN=OFF -DWITH_TERARK_ZIP=ON -UWITH_TERARKDB_NAMESPACE .. && test