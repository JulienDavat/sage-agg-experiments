#!/usr/bin/env bash

docker exec -t sage-agg sage-postgres-init /opt/data/experiments.yaml bsbm1kpostgres
docker exec -t sage-agg sage-postgres-init /opt/data/experiments.yaml wikidata2017postgres
docker exec -t sage-agg sage-postgres-init /opt/data/experiments.yaml dbpedia351postgres