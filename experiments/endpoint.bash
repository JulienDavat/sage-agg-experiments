#!/usr/bin/env bash

CUR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo $CUR

# exit on any error
set -e

DEFAULT_JVM_OPTS="-Xms2g -Xmx2g"

sageEndpoint="https://sage.univ-nantes.fr/void/"

rm -rf "$CUR/build/"

# build sage-sparql-void
gradle clean fatJar

# comeback to root project location
cd $CUR

OUTPUTLOCATION="$CUR/output"

java -jar build/libs/sage-sparql-void-fat-1.0.jar endpoint $sageEndpoint output=$OUTPUTLOCATION