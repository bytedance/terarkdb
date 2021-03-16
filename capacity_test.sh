#!/bin/bash
set -e

TEST_LOGFILE="capacity_test_log.txt"
rm -f $TEST_LOGFILE

FS=$1
FS_PATH=$2
SCALE=$3

if [ "$FS" = "posix" ]; then
	ENV_PARAMS="--db=$FS_PATH"
	ZONE_CAP=64
fi

if [ "$FS" = "zenfs" ]; then
	ENV_PARAMS="--fs_uri=zenfs://dev:$FS_PATH"
	CAP_SECTORS=$(blkzone report -c 5 /dev/$FS_PATH | grep -oP '(?<=cap )[0-9xa-f]+' | head -1)
	ZONE_CAP=$(($CAP_SECTORS * 512))
fi

if [ -z $ENV_PARAMS ]; then
	echo "Usage: smoke_test.sh <zenfs/posix> <device/path> <test scale, default 100>"
	exit -1
fi

echo "# Testing filesystem: $FS path: $FS_PATH" | tee -a $TEST_LOGFILE

if [ "$SCALE" = "" ]; then
	SCALE=100
fi

KEY_SIZE=20
VALUE_SIZE=1000
WB_SIZE=$(($ZONE_CAP * 2))
TARGET_FZ_BASE=$(($ZONE_CAP * 2))
TARGET_FILE_SIZE_MULTIPLIER=1
MAX_BYTES_FOR_LEVEL_BASE=$((2 * $TARGET_FZ_BASE))
MAX_BYTES_FOR_LEVEL_MULTIPLIER=8

MAX_BACKGROUND_JOBS=16
MAX_BACKGROUND_COMPACTIONS=4
OPEN_FILES=16

BASE_PARAMS="$ENV_PARAMS -key_size=$KEY_SIZE --value_size=$VALUE_SIZE -max_background_jobs=$MAX_BACKGROUND_JOBS --open_files=$OPEN_FILES --target_file_size_base=$TARGET_FZ_BASE --write_buffer_size=$WB_SIZE --target_file_size_multiplier=$TARGET_FILE_SIZE_MULTIPLIER --max_bytes_for_level_base=$MAX_BYTES_FOR_LEVEL_BASE --max_bytes_for_level_multiplier=$MAX_BYTES_FOR_LEVEL_MULTIPLIER --max_background_compactions=$MAX_BACKGROUND_COMPACTIONS -use_direct_io_for_flush_and_compaction"

NUM=$((100 * 10**4 * $SCALE))
BENCHMARKS="fillrandom"
PARAMS="$BASE_PARAMS --num=$NUM --benchmarks=$BENCHMARKS"
echo "# Running with params: $PARAMS" | tee -a $TEST_LOGFILE
./db_bench $PARAMS | tee -a $TEST_LOGFILE


NUM=$((50 * 10**4 * $SCALE))
BENCHMARKS="overwrite"
PARAMS="$BASE_PARAMS --num=$NUM --benchmarks=$BENCHMARKS --use_existing_db"
echo "# Running with params: $PARAMS" | tee -a $TEST_LOGFILE
./db_bench $PARAMS | tee -a $TEST_LOGFILE

