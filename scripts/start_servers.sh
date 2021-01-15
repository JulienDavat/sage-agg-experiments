#!/bin/bash

# Caution: If you change any port in this file, do not forget to change them on the Snakefile !
# In order to run this script, the SaGe environment must be activated...

VIRTUOSO_DIR=/usr/local/virtuoso-opensource

# Starting SaGe servers
nohup sage -w 1 -p 8080 "configs/sage/correct/150.yaml" > output/log/sage-150.log 2>&1 &
echo -n "$! " > .pids
nohup sage -w 1 -p 8081 "configs/sage/correct/1500.yaml" > output/log/sage-1500.log 2>&1 &
echo -n "$! " >> .pids
nohup sage -w 1 -p 8082 "configs/sage/correct/15000.yaml" > output/log/sage-15000.log 2>&1 &
echo -n "$! " >> .pids
nohup sage -w 1 -p 8083 "configs/sage/approximative/150.yaml" > output/log/sage-approx-150.log 2>&1 &
echo -n "$! " >> .pids
nohup sage -w 1 -p 8084 "configs/sage/approximative/1500.yaml" > output/log/sage-approx-1500.log 2>&1 &
echo -n "$! " >> .pids
nohup sage -w 1 -p 8085 "configs/sage/approximative/15000.yaml" > output/log/sage-approx-15000.log 2>&1 &
echo -n "$! " >> .pids

# Starting Virtuoso
nohup sudo $VIRTUOSO_DIR/bin/virtuoso-t -f -c $VIRTUOSO_DIR/var/lib/virtuoso/db/virtuoso.ini > output/log/virtuoso.log 2>&1 &

# Starting LDF
nohup node_modules/ldf-server/bin/ldf-server configs/ldf/config.json 8000 1 > output/log/ldf.log 2>&1 &
echo -n "$! " >> .pids

# To be sure that all servers have started
echo "Starting all servers... it will takes 30 seconds..."
sleep 30