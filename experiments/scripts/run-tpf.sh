#!/usr/bin/env bash

# example of usage from this folder
# bash run-tpf.sh 4 ../../output/tpf/ "bsbm10,bsbm100,bsbm1k"

CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# trap ctrl-c and call ctrl_c()
trap ctrl_c INT

function ctrl_c() {
        echo "Killing: $current"
        kill $current
        exit 1
}

# require Nodejs in order to run the script
command -v python3 >/dev/null 2>&1 || { echo >&2 "Python3 required for executing queries. Aborting."; exit 1; }

RUNS=$1
OUTPUT=$2
DATASETS=()
while read -rd,; do DATASETS+=("$REPLY"); done <<<"$3,";

if [ "$#" -ne 3 ]; then
  echo "Illegal number of parameters."
  echo "Usage: bash run-tpf.sh <number-of-runs> <output-folder> \"<comma-seperated-list-of-datasets>\" "
  exit
fi

mkdir -p $OUTPUT

for dataset in "${!DATASETS[@]}"
do
    for i in $(seq 1 1 $RUNS)
    do
        echo "#### (${DATASETS[$dataset]}) Running tpf run $i... into $OUTPUT ####"
        bash $CUR/execute-tpf.sh "../../data/queries/queries-wo-construct.txt" "$OUTPUT/run-$i-${DATASETS[$dataset]}/" "http://localhost:7140/${DATASETS[$dataset]}"
        current=$!
    done
    python3 $CUR/average.py -f "$OUTPUT/run-*-${DATASETS[$dataset]}" -o "$OUTPUT/average-${DATASETS[$dataset]}.csv"
done

