#!/usr/bin/env bash

set -e

OUTDIR=out/sem3/wc_combined
hadoop fs -rm -r -f $OUTDIR

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
	-Dmapred.max.split.size=$[32*2**20] \
	-inputformat org.apache.hadoop.mapred.lib.CombineTextInputFormat \
	-file mapper.py -file reducer.py \
	-input '/data/seminar3/texts/*.txt' -output $OUTDIR \
	-mapper mapper.py -reducer reducer.py -combiner reducer.py

echo job finished, see output in $OUTDIR
