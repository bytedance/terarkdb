#!/bin/bash

set -ex

BENCH_HOME=$PWD
TERARKDB_HOME=$(cd ../ && echo $PWD)
CPU_CNT=$(nproc)

echo $TERARKDB_HOME

# clean and build static terarkdb
 cd $TERARKDB_HOME && ./build.sh

# clean and build db_bench
cd $TERARKDB_HOME && DEBUG_LEVEL=0 DISABLE_WARNING_AS_ERROR=1 make db_bench -j $CPU_CNT

cd $BENCH_HOME && rm -rf db_bench && cp ../db_bench .
