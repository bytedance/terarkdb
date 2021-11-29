#!/bin/bash

if [ "$#" -ne 1 ]; then
  echo "Usage: ./prepare_db_bench.sh [device]"
  exit 1 
fi

DEVICE=$1

rm -rf /tmp/zenfs_$DEVICE*
./output/zenfs mkfs --zbd=$DEVICE --aux_path=/tmp/zenfs_$DEVICE --force=true
