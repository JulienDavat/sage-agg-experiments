#!/bin/bash

# Caution: If you change any port in this file, do not forget to change them on the Snakefile !
# In order to run this script, the SaGe environment must be activated...

VIRTUOSO_DIR=/usr/local/virtuoso-opensource

# Starting SaGe servers
nohup sage -w 1 -p 8080 "configs/sage/sage-exact-75ms.yaml" > output/log/sage-75.log 2>&1 &
echo -n "$! " > .pids
nohup sage -w 1 -p 8081 "configs/sage/sage-exact-150ms.yaml" > output/log/sage-150.log 2>&1 &
echo -n "$! " > .pids
nohup sage -w 1 -p 8082 "configs/sage/sage-exact-1500ms.yaml" > output/log/sage-1500.log 2>&1 &
echo -n "$! " >> .pids
nohup sage -w 1 -p 8083 "configs/sage/sage-exact-15000ms.yaml" > output/log/sage-15000.log 2>&1 &
echo -n "$! " >> .pids
nohup sage -w 1 -p 8084 "configs/sage/sage-exact-30000ms.yaml" > output/log/sage-30000.log 2>&1 &
echo -n "$! " >> .pids
nohup sage -w 1 -p 8085 "configs/sage/sage-approx-98-75ms.yaml" > output/log/sage-approx-98-75.log 2>&1 &
echo -n "$! " >> .pids
nohup sage -w 1 -p 8086 "configs/sage/sage-approx-98-150ms.yaml" > output/log/sage-approx-98-150.log 2>&1 &
echo -n "$! " >> .pids
nohup sage -w 1 -p 8087 "configs/sage/sage-approx-98-1500ms.yaml" > output/log/sage-approx-98-1500.log 2>&1 &
echo -n "$! " >> .pids
nohup sage -w 1 -p 8088 "configs/sage/sage-approx-98-15000ms.yaml" > output/log/sage-approx-98-15000.log 2>&1 &
echo -n "$! " >> .pids
nohup sage -w 1 -p 8089 "configs/sage/sage-approx-98-30000ms.yaml" > output/log/sage-approx-98-30000.log 2>&1 &
echo -n "$! " >> .pids
nohup sage -w 1 -p 8090 "configs/sage/sage-approx-95-150ms.yaml" > output/log/sage-approx-95-150.log 2>&1 &
echo -n "$! " >> .pids
nohup sage -w 1 -p 8091 "configs/sage/sage-approx-90-150ms.yaml" > output/log/sage-approx-90-150.log 2>&1 &
echo -n "$! " >> .pids

# Starting Virtuoso
nohup sudo $VIRTUOSO_DIR/bin/virtuoso-t -f -c $VIRTUOSO_DIR/var/lib/virtuoso/db/virtuoso.ini > output/log/virtuoso.log 2>&1 &

# Starting LDF
nohup node_modules/ldf-server/bin/ldf-server configs/ldf/config.json 8000 1 > output/log/ldf.log 2>&1 &
echo -n "$! " >> .pids

# To be sure that all servers have started
echo "Starting all servers... it will takes 30 seconds..."
sleep 30