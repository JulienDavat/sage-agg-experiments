#!/usr/bin/env bash

docker exec -it sage-agg sage-postgres-init /opt/data/experiments.yaml bsbm1kpostgres
docker exec -it sage-agg sage-postgres-init /opt/data/experiments.yaml wikidata2017postgres
docker exec -it sage-agg sage-postgres-init /opt/data/experiments.yaml dbpedia351postgres

docker exec -it sage-agg sage-postgres-put --format ttl /opt/data/virtuoso-data/toLoad/bsbm1k.ttl /opt/data/experiments.yaml bsbm1kpostgres
docker exec -it sage-agg sage-postgres-put --format nt /opt/data/virtuoso-data/toLoad/dbpedia.3.5.1_merged.nt /opt/data/experiments.yaml wikidata2017postgres
docker exec -it sage-agg sage-postgres-put --format nt /opt/data/virtuoso-data/toLoad/wikidata-20170313-all-BETA.nt bsbm1k.ttl /opt/data/experiments.yaml dbpedia351postgres