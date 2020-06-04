#!/bin/bash

FNAME=/tmp/active_stats.txt
cat $1 |grep Zonestats|awk '{print $(NF-6),"\t",$(NF-2)}' >$FNAME

TITLE="set title 'ZenFS active zone usage';"
LABELS="set xlabel 'Time(s)'; set ylabel 'Active zones'"
RANGES="set xrange [0:]; set yrange[0:]"
PLOTS="plot '$FNAME' using 1:2 with lines title ''"

gnuplot -e "$TITLE;$LABELS;$RANGES;$PLOTS;pause -1"
