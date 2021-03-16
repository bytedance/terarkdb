
DEV=nullb1
ZONE_CAP_SECS=$(blkzone report -c 5 /dev/$DEV | grep -oP '(?<=cap )[0-9xa-f]+' | head -1)
ZONE_CAP=$((ZONE_CAP_SECS * 512))

FUZZ=5
BASE_FZ=$(($ZONE_CAP  * (100 - $FUZZ) / 100))
WB_SIZE=$(($BASE_FZ * 2))

TARGET_FZ_BASE=$WB_SIZE
TARGET_FILE_SIZE_MULTIPLIER=2
MAX_BYTES_FOR_LEVEL_BASE=$((2 * $TARGET_FZ_BASE))

./zenfs mkfs --zbd=/dev/$DEV --aux_path=/tmp/zenfs_$DEV --finish_threshold=$FUZZ --force

./db_bench --fs_uri=zenfs://dev:$DEV --key_size=16 --value_size=800 --target_file_size_base=$TARGET_FZ_BASE --write_buffer_size=$WB_SIZE --max_bytes_for_level_base=$MAX_BYTES_FOR_LEVEL_BASE --max_bytes_for_level_multiplier=4 --use_direct_io_for_flush_and_compaction --max_background_jobs=$(nproc) --num=1000000 --benchmarks=fillrandom,overwrite
