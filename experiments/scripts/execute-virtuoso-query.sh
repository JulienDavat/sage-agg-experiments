#!/usr/bin/env bash

CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo $CUR

QUERY="$1"
ENDPOINT=$2
DEFAULT_GRAPH=$3
OUTPUT_RESULT=$4
OUTPUT_ERR=$5
JAR=$6

echo "Executing query: $QUERY"
echo "Server Address: $ENDPOINT/$DEFAULT_GRAPH"
rm $OUTPUT_RESULT
touch $OUTPUT_RESULT
echo "Output results (std out): $OUTPUT_RESULT"
rm $OUTPUT_MEASURE
touch $OUTPUT_MEASURE
echo "Output measures (std err) $OUTPUT_MEASURE"
echo "Jar location: $JAR"

java -jar $JAR sparql-endpoint $ENDPOINT "$QUERY" $DEFAULT_GRAPH  1> $OUTPUT_RESULT 2>> $OUTPUT_MEASURE
