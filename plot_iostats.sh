ZONEDEV=$1

INPUT=/tmp/iostat_$ZONEDEV.txt
if [ ! -f "$INPUT" ]; then
  echo $INPUT does not exist!
  exit -1
fi

FNAME=/tmp/iostat_$ZONEDEV-filtered.txt
grep $ZONEDEV $INPUT >$FNAME

FIRST_SECOND=$(head -n1 $FNAME |awk '{print $1}')

PLOT_INPUT=/tmp/iostat_$ZONEDEV-plotinput.txt

awk -v first=$FIRST_SECOND '{printf ($1-first)"\t"$2"\t"$3"\t"$4"\t"$5"\t"$6"\n"}' $FNAME >$PLOT_INPUT

echo plotting $PLOT_INPUT


TITLE="set title 'ZenFS IO throughput';"
LABELS="set xlabel 'Time(s)'; set ylabel 'kB/s'"
RANGES="set xrange [0:]; set yrange[0:]"
PLOTS="plot '$PLOT_INPUT' using 1:5 with lines title 'Read','$PLOT_INPUT' using 1:6 with lines title 'Write'"

gnuplot -e "$TITLE;$LABELS;$RANGES;$PLOTS;pause -1"
