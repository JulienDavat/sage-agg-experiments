#!/usr/bin/env bash

echo $@

# trap ctrl-c and call ctrl_c()
trap ctrl_c INT

function ctrl_c() {
        echo "Killing..."
        exit 1
}

CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
if [[ $# -eq 0 ]] ; then
    NAME="test";
    VIRTUOSO_ADDRESS="http://localhost:7130/sparql/";
    SAGE_ADDRESS="http://localhost:7120/sparql/bsbm1k";
    BUILD_JAR="$CUR/../../build/libs/sage-sparql-void-fat-1.0.jar";
    VIRTUOSO_DEFAULT_GRAPH="http://sage.univ-nantes.fr/bsbm1k";
    QUERIES="$CUR/../../data/queries/queries-wo-construct-test.txt";
    RUNS=1;
else
    NAME=$1;
    VIRTUOSO_ADDRESS=$2;
    SAGE_ADDRESS=$3;
    BUILD_JAR=$4;
    VIRTUOSO_DEFAULT_GRAPH=$5;
    QUERIES=$6;
    RUNS=$7;
fi
echo "Name of the experiment: $NAME"
echo "Virtuoso server address: $VIRTUOSO_ADDRESS"
echo "Sage server address: $SAGE_ADDRESS"
echo "Jar location: $BUILD_JAR"
echo "Virtuoso graph: $VIRTUOSO_DEFAULT_GRAPH"
echo "Queries location: $QUERIES"

function run_virtuoso {
    V_PREFIX="$DIR/virtuoso"
    rm -rf $V_PREFIX*
    RESULT_VIRTUOSO="$V_PREFIX.csv"
    touch "$RESULT_VIRTUOSO"
    # read queries
    input=$QUERIES
    let "q=1"
    while IFS= read -r query
    do
      echo "# Executing virtuoso query-$q and appending into: $RESULT_VIRTUOSO"
      FILE_RESULT="$V_PREFIX-query-$q-result.txt"
      bash "$CUR/execute-virtuoso-query.sh" "$query" $VIRTUOSO_ADDRESS $VIRTUOSO_DEFAULT_GRAPH $FILE_RESULT $RESULT_VIRTUOSO $BUILD_JAR
      let "q++"
    done < "$input"
}

function run_sage {
    S_PREFIX="$DIR/sage-run-$i"
    rm -rf $S_PREFIX*
    RESULT_SAGE="$S_PREFIX.csv"
    touch "$RESULT_SAGE"
    # read queries
    input=$QUERIES
    let "q=1"
    while IFS= read -r query
    do
      echo "# Executing sage query-$q and appending into: $RESULT_SAGE"
      FILE_RESULT="$S_PREFIX-query-$q-result.txt"
      bash "$CUR/execute-sage-query.sh" "$query" $SAGE_ADDRESS $FILE_RESULT $RESULT_SAGE $BUILD_JAR "--time"
      let "q++"
    done < "$input"
}

function run_sage_opt {
    S_PREFIX="$DIR/sage-opt-run-$i"
    rm -rf $S_PREFIX*
    RESULT_SAGE="$S_PREFIX.csv"
    touch "$RESULT_SAGE"
    # read queries
    input=$QUERIES
    let "q=1"
    while IFS= read -r query
    do
      echo "# Executing sage query-$q and appending into: $RESULT_SAGE"
      FILE_RESULT="$S_PREFIX-query-$q-result.txt"
      bash "$CUR/execute-sage-query.sh" "$query" $SAGE_ADDRESS $FILE_RESULT $RESULT_SAGE $BUILD_JAR "--optimized --time"
      let "q++"
    done < "$input"
}

DIR="$CUR/../../output/$NAME"


mkdir -p "$DIR"

command -v python3 >/dev/null 2>&1 || { echo >&2 "python3 required for processing files on csv files. Aborting."; exit 1; }
command -v java >/dev/null 2>&1 || { echo >&2 "java required for executing queries. Aborting."; exit 1; }

number_of_queries=$(wc -l $QUERIES | awk '{print $1}')
echo "Number of queries: " $number_of_queries


# run the virtuoso experiment
run_virtuoso

for i in $(seq 1 1 $RUNS)
do
    echo "===> Running exp in $DIR: $i... <==="
    # run the sage experiment
    run_sage
    # run the sage experiment
    run_sage_opt
    # compute completeness
    rm -rf "$DIR/completeness-sage-run-$i.csv" "$DIR/completeness-sage-opt-run-$i.csv"
    for j in $(seq 1 1 $number_of_queries)
    do
        echo " # Processing completeness test between virtuoso and sage on query $j for run $i..."
        # remove first line of both files
        SORT_V="$DIR/virtuoso-query-$j-result.txt.sorted"
        SORT_S="$DIR/sage-run-$i-query-$j-result.txt.sorted"
        SORT_S_OPT="$DIR/sage-opt-run-$i-query-$j-result.txt.sorted"
        cat "$DIR/virtuoso-query-$j-result.txt" | sed "s/\"//g" | awk 'NF' | sort > $SORT_V
        cat "$DIR/sage-run-$i-query-$j-result.txt" | awk 'NF' | sort > $SORT_S
        cat "$DIR/sage-opt-run-$i-query-$j-result.txt" | awk 'NF' | sort > $SORT_S_OPT
        d=$(diff -b --changed-group-format='%<%>' --unchanged-group-format="" "$SORT_V" "$SORT_S" | wc -l | awk '{print $1}')
        d2=$(diff -b --changed-group-format='%<%>' --unchanged-group-format="" "$SORT_V" "$SORT_S_OPT" | wc -l | awk '{print $1}')
        # remove temp files
        rm $SORT_V $SORT_S $SORT_S_OPT
        # print the result d1, aka: virtuoso vs sage normal
        if [ $d -eq 0 ]; then
            echo "$j, 1" >> "$DIR/completeness-sage-run-$i.csv"
        else
            echo "$j, 0" >> "$DIR/completeness-sage-run-$i.csv"
        fi

        # print the result of d2, aka: virtuoso vs sage optimized
        if [ $d2 -eq 0 ]; then
            echo "$j, 1" >> "$DIR/completeness-sage-opt-run-$i.csv"
        else
            echo "$j, 0" >> "$DIR/completeness-sage-opt-run-$i.csv"
        fi
    done
done

python3 $CUR/average.py -f $DIR/sage-run*.csv -o $DIR/sage-average.csv
python3 $CUR/average.py -f $DIR/sage-opt-run*.csv -o $DIR/sage-opt-average.csv
python3 $CUR/average.py -f $DIR/completeness-sage-run*.csv -o $DIR/completeness-sage-average.csv
python3 $CUR/average.py -f $DIR/completeness-sage-opt-run*.csv -o $DIR/completeness-sage-opt-average.csv

