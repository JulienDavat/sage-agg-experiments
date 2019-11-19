#!/usr/bin/env bash
#!/bin/bash
# Run execution time experiment for reference (TPF)

# in the folder
# example of usage: bash execute-tpf.sh ../../data/queries/queries-wo-construct-test.txt ./test http://localhost:7140/bsbm10

QUERIES=$1 # i.e. a folder that contains SPARQL queries to execute
OUTPUT=$2
SERVER=$3
cpt=1

if [ "$#" -ne 3 ]; then
  echo "Illegal number of parameters."
  echo "Usage: ./run_reference.sh <queries-directory> <output-folder>"
  exit
fi

mkdir -p $OUTPUT/results/
mkdir -p $OUTPUT/errors/

RESFILE="${OUTPUT}/result.csv"

# init results file with headers
echo "query,time,httpCalls,serverTime,transfertSize,errors" > $RESFILE

# read queries
input=$QUERIES
let "q=1"
while IFS= read -r query
do
    echo -n "${q}," >> $RESFILE
    # execution time
    node ./reference.js $SERVER -q "$query" -m $RESFILE > $OUTPUT/results/$q.log 2> $OUTPUT/errors/$q.err
    echo -n "," >> $RESFILE
    # nb errors during query processing
    echo `wc -l ${OUTPUT}/errors/${q}.err | awk '{print $1}'` >> $RESFILE
    let "q++"
done < "$input"

# remove tmp folders
rm -rf $OUTPUT/errors/ $OUPUT/results