#!/usr/bin/env bash

# init
docker exec -t sage-agg sage-postgres-init /opt/data/experiments.yaml bsbm1k
# add
docker exec -t sage-agg sage-postgres-put --format ttl /opt/data/bsbm1k.ttl /opt/data/experiments.yaml bsbm1k

# init
docker exec -t sage-agg sage-postgres-init /opt/data/experiments.yaml bsbm10
# add
docker exec -t sage-agg sage-postgres-put --format ttl /opt/data/bsbm10.ttl /opt/data/experiments.yaml bsbm10

# init
docker exec -t sage-agg sage-postgres-init /opt/data/experiments.yaml bsbm1k
# add
docker exec -t sage-agg sage-postgres-put --format ttl /opt/data/bsbm100.ttl /opt/data/experiments.yaml bsbm100
