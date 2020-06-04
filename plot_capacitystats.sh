#!/bin/bash

FNAME=/tmp/capacity_stats.txt
cat $1 |grep Zonestats|awk '{print $(NF-6),"\t",$(NF-5),"\t",$(NF-4)}' >$FNAME

TITLE="set title 'ZenFS capacity';"
LABELS="set xlabel 'Time(s)'; set ylabel 'Capacity(MB)'"
RANGES="set xrange [0:]; set yrange[0:]"
PLOTS="plot '$FNAME' using 1:2 with lines title 'Used','$FNAME' using 1:3 with lines title 'Reclaimable'"

gnuplot -e "$TITLE;$LABELS;$RANGES;$PLOTS;pause -1"
