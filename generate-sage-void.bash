#!/usr/bin/env bash

baseUri="https://sage.univ-nantes.fr/sparql/"

datasets=("dbpedia-2016-04" "dbpedia-2015-04en" "wikidata-2017-03-13" "dbpedia-3-5-1" "dblp-2017" "sameAs"
    "geonames-2012" "linkedgeodata-2012" "swdf-2017" "swdf-2012" "eventskg-r2" "wiktionary-2012-en" "pathwaycommons-v9")



for dataset in ${datasets[*]}
do
    echo "Processing: $baseURI$dataset"
done