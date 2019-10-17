#!/usr/bin/env bash

CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo $CUR

# (relative to the root directory of the repository)
# Example: bash experiments/scripts/execute-sage-query.sh
#   "$(cat ./data/test.rq)"
#   http://localhost:8000/sparql/bsbm1kpostgres
#   ./data/test-result.txt
#   ./data/test-measure.txt
#   build/libs/sage-sparql-void-fat-1.0.jar
#   --optimized

QUERY="$1"
ADDRESS=$2
OUTPUT_RESULT=$3
OUTPUT_MEASURE=$4
JAR=$5
OPTIMIZED=$6

echo "Executing query: $QUERY"
echo "Server Address: $ADDRESS"
rm $OUTPUT_RESULT
touch $OUTPUT_RESULT
echo "Output results: $OUTPUT_RESULT"
rm $OUTPUT_MEASURE
touch $OUTPUT_MEASURE
echo "Output measures $OUTPUT_MEASURE"
echo "Jar location: $JAR"

java -jar $JAR query --query "$QUERY" $ADDRESS --time --measure=$OUTPUT_MEASURE $OPTIMIZED --format csv > $OUTPUT_RESULT
