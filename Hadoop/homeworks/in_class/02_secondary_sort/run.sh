#!/usr/bin/env bash

set -e
set -o pipefail

Log() {
    echo "[$( date +%c )] $*" >&2
}

OUTDIR=seminar2/out
RESFILE=temps.kml
NREDUCERS=4

Log "BUILDING"
./gradlew jar


Log "REMOVING previous directory"
hadoop fs -rm -r -f $OUTDIR

Log "RUNNING JOB"
hadoop jar ./build/libs/hadoop_sem2.jar SecondarySortDemo \
           -Dmapreduce.job.reduces=$NREDUCERS \
           /data/seminar2/meteo/*.gz $OUTDIR

hadoop fs -text "$OUTDIR/part-r*" | fgrep "$( date +%d.%m )" | ./ids2coords.py >temps.csv
gpsbabel -i csv -f temps.csv -o kml -F $RESFILE

Log "SUCCESS. Result in temps.kml"
