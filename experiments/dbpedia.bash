#!/usr/bin/env bash

# original
java -jar ../build/libs/sage-sparql-void-fat-1.0.jar sportal-sparql-endpoint https://dbpedia.org/sparql ../output/original --sportalFile=../data/original-sportal.json

# without construct
java -jar ../build/libs/sage-sparql-void-fat-1.0.jar sportal-sparql-endpoint https://dbpedia.org/sparql ../output/wo-construct --sportalFile=../data/sportal-wo-construct.json

# original corrected
java -jar ../build/libs/sage-sparql-void-fat-1.0.jar sportal-sparql-endpoint https://dbpedia.org/sparql ../output/original-corrected --sportalFile=../data/original-sportal-corrected.json

# wo construct corrected
java -jar ../build/libs/sage-sparql-void-fat-1.0.jar sportal-sparql-endpoint https://dbpedia.org/sparql ../output/wo-construct-corrected --sportalFile=../data/sportal-wo-construct-corrected.json
