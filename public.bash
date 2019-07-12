#!/usr/bin/env bash

gradle clean fatJar

# original
java -jar build/libs/sage-sparql-void-fat-1.0.jar sportal-sparql-endpoint https://query.wikidata.org/sparql ./output/original --sportalFile=data/original-sportal.json &
A=$!
java -jar build/libs/sage-sparql-void-fat-1.0.jar sportal-sparql-endpoint https://dbpedia.org/sparql ./output/original --sportalFile=data/original-sportal.json &
B=$!
wait $A $B

# without construct
java -jar build/libs/sage-sparql-void-fat-1.0.jar sportal-sparql-endpoint https://query.wikidata.org/sparql ./output/wo-construct --sportalFile=data/sportal-wo-construct.json &
A=$!
java -jar build/libs/sage-sparql-void-fat-1.0.jar sportal-sparql-endpoint https://dbpedia.org/sparql ./output/wo-construct --sportalFile=data/sportal-wo-construct.json &
B=$!
wait $A $B

# original corrected
java -jar build/libs/sage-sparql-void-fat-1.0.jar sportal-sparql-endpoint https://query.wikidata.org/sparql ./output/original --sportalFile=data/original-sportal-corrected.json &
A=$!
java -jar build/libs/sage-sparql-void-fat-1.0.jar sportal-sparql-endpoint https://dbpedia.org/sparql ./output/original-corrected --sportalFile=data/original-sportal-corrected.json &
B=$!
wait $A $B

# wo construct corrected
java -jar build/libs/sage-sparql-void-fat-1.0.jar sportal-sparql-endpoint https://query.wikidata.org/sparql ./output/wo-construct-corrected --sportalFile=data/sportal-wo-construct-corrected.json &
A=$!
java -jar build/libs/sage-sparql-void-fat-1.0.jar sportal-sparql-endpoint https://dbpedia.org/sparql ./output/wo-construct-corrected --sportalFile=data/sportal-wo-construct-corrected.json &
B=$!
wait $A $B