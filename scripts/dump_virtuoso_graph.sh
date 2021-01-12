#!/bin/bash

if (( $# != 4 )); then
    echo "Usage error: dump.sh <graph> <cardinality> <step> <output>"
    exit 1
fi

graph=$1
cardinality=$2
step=$3
offset=0
limit=$step
output=$4

while (( $offset < $cardinality )); do
    sudo /usr/local/virtuoso-opensource/bin/isql exec="set blobs on; sparql define output:format '\"NT\"' construct { ?s ?p ?o } from <$graph> where { ?s ?p ?o } offset ${offset} limit ${limit}" | egrep "^<http" >> $output
    echo "construct { ?s ?p ?o } from <$graph> where { ?s ?p ?o } offset ${offset} limit ${limit}"
    offset=$(($offset + $step))
    echo "Progress: $offset/$cardinality triples"
done