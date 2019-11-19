#!/usr/bin/env bash

# init
docker exec -t sage-agg sage-postgres-init /opt/data/experiments.yaml bsbm10
# add
docker exec -t sage-agg sage-postgres-put --format ttl /opt/data/datasets/bsbm10.ttl /opt/data/experiments.yaml bsbm10

# init
docker exec -t sage-agg sage-postgres-init /opt/data/experiments.yaml bsbm100
# add
docker exec -t sage-agg sage-postgres-put --format ttl /opt/data/datasets/bsbm100.ttl /opt/data/experiments.yaml bsbm100


# init
docker exec -t sage-agg sage-postgres-init /opt/data/experiments.yaml bsbm1k
# add
docker exec -t sage-agg sage-postgres-put --format ttl /opt/data/datasets/bsbm1k.ttl /opt/data/experiments.yaml bsbm1k

# init
docker exec -t sage-agg sage-postgres-init /opt/data/experiments.yaml bsbm1k
# add
docker exec -t sage-agg sage-postgres-put --format ttl /opt/data/datasets/bsbm1k.ttl /opt/data/experiments.yaml bsbm1k
