#!/usr/bin/env bash

CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo $CUR

# exit on any error
set -e

DEFAULT_JVM_OPTS="-Xms50g -Xmx50g"

sageEndpoint="http://172.16.8.50:7120/sparql/wikidata"

rm -rf "$CUR/../build/"

cd "$CUR/../"

# build sage-sparql-void
gradle clean fatJar

# comeback to root project location
cd $CUR

OUTPUTLOCATION="$CUR/../output"
JAR_LOCATION="$CUR/../build/libs/sage-sparql-void-fat-1.0.jar"

java $DEFAULT_JVM_OPTS -jar $JAR_LOCATION dataset $sageEndpoint $sageEndpoint $OUTPUTLOCATION
