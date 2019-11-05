#!/usr/bin/env bash

CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
QUERY="$1"
ADDRESS=$2
OUTPUT_RESULT=$3
OUTPUT_MEASURE=$4
JAR=$5
OPTIMIZED=$6

java -Xms1g -Xmx50g -jar $JAR query --query "$QUERY" $ADDRESS --measure=$OUTPUT_MEASURE $OPTIMIZED --format csv >> "$OUTPUT_RESULT"
