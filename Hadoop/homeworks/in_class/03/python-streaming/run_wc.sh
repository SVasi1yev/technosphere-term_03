#!/usr/bin/env bash

set -e

OUTDIR=out/sem3/wc
hadoop fs -rm -r -f $OUTDIR

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
	-file mapper.py -file reducer.py \
	-input '/data/seminar3/texts/*.txt' -output $OUTDIR \
	-mapper mapper.py -reducer reducer.py -combiner reducer.py

echo job finished, see output in $OUTDIR
