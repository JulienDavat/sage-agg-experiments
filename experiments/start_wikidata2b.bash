#!/usr/bin/env bash

CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo $CUR

# exit on any error
set -e

DEFAULT_JVM_OPTS="-Xms5g -Xmx50g"

sageEndpoint="http://172.16.8.50:7120/sparql/wikidata"

rm -rf "$CUR/../build/"

cd "$CUR/../"

# build sage-sparql-void
gradle -Dhttp.proxyHost=cache.ha.univ-nantes.fr -Dhttp.proxyPort=3128 -Dhttps.proxyHost=cache.ha.univ-nantes.fr -Dhttps.proxyPort=3128 clean build  fatJar

# relative path to the folder we execute the jar, dont forhget the last /
OUTPUTLOCATION="./output/"
# jar location
JAR_LOCATION="$CUR/../build/libs/sage-sparql-void-fat-1.0.jar"

java $DEFAULT_JVM_OPTS -jar $JAR_LOCATION dataset $sageEndpoint $sageEndpoint $OUTPUTLOCATION
