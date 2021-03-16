
DEV=$1
ZONE_SIZE=$(cat /sys/class/block/$DEV/queue/chunk_sectors)
NR_ZONES=$(cat /sys/class/block/$DEV/queue/nr_zones)
SPLIT_ZONES=$(($NR_ZONES / 2))
SPLIT_SZ=$(($SPLIT_ZONES * $ZONE_SIZE))

echo "0 $SPLIT_SZ linear /dev/$DEV 0" | dmsetup create "$DEV-ONE"
echo "0 $SPLIT_SZ linear /dev/$DEV $SPLIT_SZ" | dmsetup create "$DEV-TWO"

