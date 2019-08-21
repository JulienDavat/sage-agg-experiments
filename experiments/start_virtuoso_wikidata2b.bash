#!/usr/bin/env bash

CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo $CUR

# exit on any error
set -e

DEFAULT_JVM_OPTS="-Xms4g -Xmx50g"

sparqlEndpoint="http://172.16.8.50:7200/sparql/"

rm -rf "$CUR/../build/"

cd "$CUR/../"

# build sage-sparql-void
gradle clean fatJar

# relative path to the folder we execute the jar, dont forhget the last /
OUTPUTLOCATION="./output/"
# jar location
JAR_LOCATION="$CUR/../build/libs/sage-sparql-void-fat-1.0.jar"

java $DEFAULT_JVM_OPTS -jar $JAR_LOCATION sportal-sparql-endpoint $sageEndpoint $sparqlEndpoint $OUTPUTLOCATION --default-graph=http://sage.univ-nantes.fr/wikidata
