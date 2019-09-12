#!/usr/bin/env bash

CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo $CUR

# exit on any error
set -e

DEFAULT_JVM_OPTS="-Xms4g -Xmx1000g"

SPARQL_ENDPOINT="http://172.16.8.50:7200/sparql/"
DEFAULT_GRAPH="http://sage.univ-nantes.fr/wikidata"

rm -rf "$CUR/../build/"

cd "$CUR/../"

# build sage-sparql-void
gradle -Dhttp.proxyHost=cache.ha.univ-nantes.fr -Dhttp.proxyPort=3128 -Dhttps.proxyHost=cache.ha.univ-nantes.fr -Dhttps.proxyPort=3128 clean build  fatJar

# relative path to the folder we execute the jar, dont forhget the last /
OUTPUTLOCATION="./output/"
# jar location
JAR_LOCATION="$CUR/../build/libs/sage-sparql-void-fat-1.0.jar"

SPORTAL_FILE="$CUR/queries.json"
java $DEFAULT_JVM_OPTS -jar $JAR_LOCATION sportal-sparql-endpoint $SPARQL_ENDPOINT $OUTPUTLOCATION --default-graph=$DEFAULT_GRAPH --sportal-file=$SPORTAL_FILE
