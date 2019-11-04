#!/usr/bin/env bash

docker exec -t sage-agg sage-postgres-put --format ttl /opt/data/bsbm1k.hdt /opt/data/experiments.yaml bsbm1kpostgres
docker exec -t sage-agg sage-postgres-put --format ttl /opt/data/dbpedia.3.5.1_merged.nt.hdt /opt/data/experiments.yaml dbpedia351postgres
docker exec -t sage-agg sage-postgres-put --format ttl /opt/data/wikidata-20170313-all-BETA.hdt /opt/data/experiments.yaml wikidata2017postgres