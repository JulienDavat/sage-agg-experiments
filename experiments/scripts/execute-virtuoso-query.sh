#!/usr/bin/env bash

CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
QUERY="$1"
ENDPOINT=$2
DEFAULT_GRAPH=$3
OUTPUT_RESULT=$4
OUTPUT_MEASURE=$5
JAR=$6

java -Xms1g -Xmx6g -jar $JAR sparql-endpoint $ENDPOINT "$QUERY" $DEFAULT_GRAPH  --format="text/csv" 1>> "$OUTPUT_RESULT" 2>> "$OUTPUT_MEASURE"
