#!/usr/bin/env bash

CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# example from current folder:
# bash execute-virtuoso.sh ../../data/queries/queries-wo-construct.txt ../../output/virtuoso http://localhost:7130/sparql http://sage.univ-nantes.fr/bsbm10 ../../build/libs/sage-sparql-void-fat-1.0.jar


QUERIES=$1 # a file where each line contains a sparql query to execute
OUTPUT=$2 # a folder where results will be outputed
SERVER=$3 # the server to query
DEFAULT_GRAPH=$4 # the default graph to use
JAR=$5
if [ "$#" -ne 5 ]; then
  echo "Illegal number of parameters."
  echo "Usage: bash execute-virtuoso.sh <queries-file> <output-folder> <server> <default-graph> <jar-location>"
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
  echo "#### Executing virtuoso query-$q ####"
  java -Xmx6g -jar $JAR sparql-endpoint $SERVER "$query" $DEFAULT_GRAPH  --format="text/csv" 1>> "$OUTPUT/results/$q.log" 2>> "$OUTPUT/result.csv"
  let "q++"
done < "$input"

