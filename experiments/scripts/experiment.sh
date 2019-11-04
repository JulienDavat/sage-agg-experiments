#!/usr/bin/env bash

CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo $CUR

case "$1" in
    1) NAME=$1 ;;
    *) NAME="test" ;;
esac



case "$2" in
    1) VIRTUOSO_ADDRESS=$2 ;;
    *) VIRTUOSO_ADDRESS="http://localhost:7130/sparql/" ;;
esac

case "$3" in
    1) SAGE_ADDRESS=$3 ;;
    *) SAGE_ADDRESS="http://localhost:7120/sparql/" ;;
esac

case "$4" in
    1) BUILD_JAR=$4 ;;
    *) BUILD_JAR="$CUR/../../build/" ;;
esac

case "$5" in
    1) VIRTUOSO_DEFAULT_GRAPH=$5 ;;
    *) VIRTUOSO_DEFAULT_GRAPH="http://sage.univ-nantes.fr/bsbm1k" ;;
esac

function run_virtuoso {
    RESULT_VIRTUOSO="$DIR/virtuoso-run-$i.csv"
    touch "$RESULT_VIRTUOSO"
    # read queries
    input="$CUR/../../data/queries/queries.txt"
    let "q=1"
    while IFS= read -r query
    do
      echo "Executing query-$q and appending into: $RESULT_VIRTUOSO"
      let "q++"
      FILE_RESULT="$DIR/virtuoso-run-$i-query-$q-result.txt"
      bash "$CUR/execute-virtuoso-query.sh" "$query" $VIRTUOSO_ADDRESS $VIRTUOSO_DEFAULT_GRAPH $FILE_RESULT $RESULT_VIRTUOSO $BUILD_JAR
    done < "$input"
}

RUNS=3



DIR="$CUR/../../output/$NAME"

mkdir "$DIR"

for i in $(seq 1 1 $RUNS)
do
    echo "Running exp in $DIR: $i..."
    # run the virtuoso experiment
    run_virtuoso
    # run the sage experiment
done