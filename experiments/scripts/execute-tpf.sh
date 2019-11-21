#!/usr/bin/env bash
# Run execution time experiment for reference (TPF)

CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# in the folder
# example of usage: bash execute-tpf.sh ../../data/queries/queries-wo-construct-test.txt ./test http://localhost:7140/bsbm10

QUERIES=$1 # a file where each line contains a sparql query to execute
OUTPUT=$2
SERVER=$3
cpt=1

if [ "$#" -ne 3 ]; then
  echo "Illegal number of parameters."
  echo "Usage: bash execute-tpf.sh <queries-directory> <output-folder> <server>"
  exit
fi

# require Nodejs in order to run the script
command -v node >/dev/null 2>&1 || { echo >&2 "node required for executing queries. Aborting."; exit 1; }

mkdir -p $OUTPUT/results/
mkdir -p $OUTPUT/errors/

RESFILE="${OUTPUT}/result.csv"

# init results file with headers
# echo "query,time,calls,bytes,timeout,errors" > $RESFILE

# read queries
input=$QUERIES
let "q=1"
while IFS= read -r query
do
    echo "Running tpf query $q ..."
    echo -n "${q}," >> $RESFILE
    # execution time
    node --max-old-space-size=6000 "$CUR/comunica/comunica.js" $SERVER -q "$query" -m $RESFILE > $OUTPUT/results/$q.log 2> $OUTPUT/errors/$q.err
    echo -n "," >> $RESFILE
    # nb errors during query processing
    echo `wc -l ${OUTPUT}/errors/${q}.err | awk '{print $1}'` >> $RESFILE
    let "q++"
done < "$input"
