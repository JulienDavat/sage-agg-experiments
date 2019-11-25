#!/usr/bin/env bash

CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

QUERIES=$1 # a file where each line contains a sparql query to execute
OUTPUT=$2 # a folder where results will be outputed
SERVER=$3 # the server to query
JAR=$4
BUFFER_SIZE="--optimized --buffer $5"

if [ "null" == $5 ]; then
    BUFFER_SIZE=""
fi

if [ "$#" -ne 5 ]; then
  echo "Illegal number of parameters."
  echo "Usage: bash execute-sage.sh <queries-file> <output-folder> <server> <jar-location> <buffer-size>"
  exit
fi

command -v java >/dev/null 2>&1 || { echo >&2 "Node required for executing queries. Aborting."; exit 1; }

rm -rf $OUTPUT/*
mkdir -p $OUTPUT/results/

touch $OUTPUT/result.csv

# read queries
input=$QUERIES
let "q=1"
while IFS= read -r query
do
  echo "#### Executing sage query-$q ($BUFFER_SIZE) ####"
  java -Xmx6g -jar $JAR query $SERVER --query "$query" --measure="$OUTPUT/result.csv" --time $BUFFER_SIZE --format csv >> "$OUTPUT/results/$q.log"
  let "q++"
done < "$input"

