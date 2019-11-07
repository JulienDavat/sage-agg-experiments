#!/usr/bin/env bash
CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

ADDR=$1
RUNS=$2
QUERIES=$3

# bsbm10 experiment
rm -rf $CUR/../../output/bsbm10
bash $CUR/experiment-bis.sh "bsbm10" \
    "http://$ADDR:7130/sparql/" \
    "http://$ADDR:7120/sparql/bsbm10" \
    "$CUR/../../build/libs/sage-sparql-void-fat-1.0.jar" \
    "http://sage.univ-nantes.fr/bsbm10" \
    "$CUR/../../data/queries/queries-wo-construct$QUERIES.txt" \
    $RUNS

# bsbm100 experiment
rm -rf $CUR/../../output/bsbm100
bash $CUR/experiment-bis.sh "bsbm100" \
    "http://$ADDR:7130/sparql/" \
    "http://$ADDR:7120/sparql/bsbm100" \
    "$CUR/../../build/libs/sage-sparql-void-fat-1.0.jar" \
    "http://sage.univ-nantes.fr/bsbm100" \
    "$CUR/../../data/queries/queries-wo-construct$QUERIES.txt" \
    $RUNS

# bsbm1k experiment
rm -rf $CUR/../../output/bsbm1k
bash $CUR/experiment-bis.sh "bsbm1k" \
    "http://$ADDR:7130/sparql/" \
    "http://$ADDR:7120/sparql/bsbm1k" \
    "$CUR/../../build/libs/sage-sparql-void-fat-1.0.jar" \
    "http://sage.univ-nantes.fr/bsbm1k" \
    "$CUR/../../data/queries/queries-wo-construct$QUERIES.txt" \
    $RUNS


