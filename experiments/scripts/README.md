How to init sage-postgres with data?

````bash
# init
docker exec -t sage-agg sage-postgres-init /opt/data/experiments.yaml bsbm1k

# add
docker exec -t sage-agg sage-postgres-put --format ttl /opt/data/bsbm1k.ttl /opt/data/experiments.yaml bsbm1k

````