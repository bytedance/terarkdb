#!/bin/bash

FNAME=/tmp/capacity_stats.txt
grep "used capacity" $1 | grep -oP '(?<=capacity: )[0-9]+' >$FNAME

gnuplot -c plot_zone_capacity_usage.plg $FNAME 


