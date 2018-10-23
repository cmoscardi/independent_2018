#!/bin/bash
cd data
YEAR=$1
MONTH=$2 
calc(){ awk "BEGIN { print "$*" }"; }

for dir in `ls $YEAR-$MONTH`; do
    n_files=`ls $YEAR-$MONTH/$dir/*[^MDM].ar2v | wc -l`
    n_failures=`wc -l $YEAR-$MONTH/$dir/failures.txt | cut -d ' ' -f 1`
    echo "${dir}: $n_files / $n_failures -- $(echo ${n_failures}/${n_files} | bc -l )"
done
