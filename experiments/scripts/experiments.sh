#!/usr/bin/env bash
CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# bsbm10 experiment
rm -rf $CUR/../../output/bsbm10
bash $CUR/experiment-bis.sh "bsbm10" \
    "http://172.16.8.50:7130/sparql/" \
    "http://localhost:7120/sparql/bsbm10" \
    "$CUR/../../build/libs/sage-sparql-void-fat-1.0.jar" \
    "http://sage.univ-nantes.fr/bsbm10" \
    "$CUR/../../data/queries/queries-wo-construct.txt"

# bsbm100 experiment
rm -rf $CUR/../../output/bsbm100
bash $CUR/experiment-bis.sh "bsbm100" \
    "http://localhost:7130/sparql/" \
    "http://localhost:7120/sparql/bsbm100" \
    "$CUR/../../build/libs/sage-sparql-void-fat-1.0.jar" \
    "http://sage.univ-nantes.fr/bsbm100" \
    "$CUR/../../data/queries/queries-wo-construct.txt"

# bsbm1k experiment
rm -rf $CUR/../../output/bsbm1k
bash $CUR/experiment-bis.sh "bsbm1k" \
    "http://localhost:7130/sparql/" \
    "http://localhost:7120/sparql/bsbm1k" \
    "$CUR/../../build/libs/sage-sparql-void-fat-1.0.jar" \
    "http://sage.univ-nantes.fr/bsbm1k" \
    "$CUR/../../data/queries/queries-wo-construct.txt"


