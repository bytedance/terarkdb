#!/bin/bash

# 400x 256MB Zones - 100GB
ZONE_SZ=256
SIZE=$(($ZONE_SZ * 400))

modprobe null_blk
cd /sys/kernel/config/nullb &&
    mkdir -p zns_nullb &&
    cd zns_nullb ; echo 0 > power; 
    echo 1 > zoned &&
    echo $ZONE_SZ > zone_size &&
    echo 0 > zone_nr_conv &&
    echo 0 > completion_nsec &&
    echo 4096 > blocksize &&
    echo $SIZE > size &&
    echo 1 > memory_backed &&
    echo 1 > power;
