#!/usr/bin/env bash

# example of usage from this folder
# bash run-tpf.sh 4 ../../output/tpf/ "bsbm10,bsbm100,bsbm1k" "0,1000"

CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# trap ctrl-c and call ctrl_c()
trap ctrl_c INT

function ctrl_c() {
        echo "Killing..."
        exit 1
}

# require Nodejs in order to run the script
command -v python3 >/dev/null 2>&1 || { echo >&2 "Python3 required for executing queries. Aborting."; exit 1; }

RUNS=$1
OUTPUT=$2
DATASETS=()
while read -rd,; do DATASETS+=("$REPLY"); done <<<"$3,";
ADDR=$4
QUERIES=$5

JAR="$CUR/../../build/libs/sage-sparql-void-fat-1.0.jar"

if [ "$#" -ne 5 ]; then
  echo "Illegal number of parameters."
  echo "Usage: bash run-virtuoso.sh <number-of-runs> <output-folder> \"<comma-seperated-list-of-datasets>\" <addr:port> <path-to-queries-to-execute>"
  exit
fi

mkdir -p $OUTPUT

for dataset in "${!DATASETS[@]}"
do
    for i in $(seq 1 1 $RUNS)
    do
        echo "#### (${DATASETS[$dataset]}) Running Virtuoso run $i... into $OUTPUT ####"
        bash $CUR/execute-virtuoso.sh "$QUERIES" "$OUTPUT/run-$i-${DATASETS[$dataset]}/" "http://$ADDR/sparql" "http://sage.univ-nantes.fr/${DATASETS[$dataset]}" $JAR
    done
done


for dataset in "${!DATASETS[@]}"
do
   python3 $CUR/average.py -f $OUTPUT/run-*-${DATASETS[$dataset]}/result.csv -o "$OUTPUT/average-${DATASETS[$dataset]}.csv"
done
