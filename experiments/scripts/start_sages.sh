#!/usr/bin/env bash

# trap ctrl-c and call ctrl_c()
trap ctrl_c INT

function ctrl_c() {
        echo "Killing $sage1_pid $sage2_pid $sage3_pid"
        kill -9 $sage1_pid $sage2_pid $sage3_pid
        exit 1
}

CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

command -v sage >/dev/null 2>&1 || { echo >&2 "sage required for starting servers.  Start a virtual env and install sage first. Aborting."; exit 1; }
command -v java >/dev/null 2>&1 || { echo >&2 "java required for starting servers.  And run: gradle fatJar in the root directory first. Aborting."; exit 1; }

nohup sage -w 1 -p 7120 "$CUR/../../data/configs/quotas/150.yaml" > sage-150.log &
sage1_pid="$!"
nohup sage -w 1 -p 7121 "$CUR/../../data/configs/quotas/1500.yaml" > sage-1500.log &
sage2_pid="$!"
nohup sage -w 1 -p 7122 "$CUR/../../data/configs/quotas/15000.yaml" > sage-15000.log &
sage3_pid="$!"

echo "Sleeping 5 seconds to start correctly sage servers"
sleep 5

MAIN_FOLDER="$CUR/../../output/bsbm1k"

mkdir -m "$MAIN_FOLDER"
mkdir -p "$MAIN_FOLDER/150" "$MAIN_FOLDER/1500" "$MAIN_FOLDER/15000"
mkdir -p "$MAIN_FOLDER/virtuoso"




QUERIES="$CUR/../../data/queries/to_run.txt"
DATASETS="bsbm1k"
BUFF_SIZE="0"

bash "$CUR/run-virtuoso.sh" 3 "$MAIN_FOLDER/virtuoso" $DATASETS "localhost:7130" $QUERIES
bash "$CUR/run-sage.sh" 3 "$MAIN_FOLDER/150" $DATASETS $BUFF_SIZE "localhost:7120" $QUERIES
bash "$CUR/run-sage.sh" 3 "$MAIN_FOLDER/1500" $DATASETS $BUFF_SIZE "localhost:7121" $QUERIES
bash "$CUR/run-sage.sh" 3 "$MAIN_FOLDER/15000" $DATASETS $BUFF_SIZE "localhost:7122" $QUERIES

kill -9 $sage1_pid $sage2_pid $sage3_pid
