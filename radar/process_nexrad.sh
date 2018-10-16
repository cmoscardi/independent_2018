#!/bin/bash

MONTH="$1"
DAY="$2"


echo "processing ar2v files for $MONTH - $DAY ..."

FPATH="data/$MONTH/$DAY"
FILES="$FPATH/*.ar2v"

for file in $FILES; do
    echo "processing $file..."
    Rscript nexrad_processing.R $file
    rm $file
    rm -f $file.hdf5
done
