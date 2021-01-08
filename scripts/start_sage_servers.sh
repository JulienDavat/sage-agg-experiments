#!/bin/bash

# Caution: If you change any port in this file, do not forget to change them on the Snakefile !

nohup sage -w 1 -p 8080 "configs/correct/150.yaml" > output/log/sage-150.log 2>/dev/null &
echo -n "$! " > .pids
nohup sage -w 1 -p 8081 "configs/correct/1500.yaml" > output/log/sage-1500.log 2>/dev/null &
echo -n "$! " >> .pids
nohup sage -w 1 -p 8082 "configs/correct/15000.yaml" > output/log/sage-15000.log 2>/dev/null &
echo -n "$! " >> .pids
nohup sage -w 1 -p 8083 "configs/approximative/150.yaml" > output/log/sage-approx-150.log 2>/dev/null &
echo -n "$! " >> .pids
nohup sage -w 1 -p 8084 "configs/approximative/150.yaml" > output/log/sage-approx-1500.log 2>/dev/null &
echo -n "$! " >> .pids
nohup sage -w 1 -p 8085 "configs/approximative/150.yaml" > output/log/sage-approx-15000.log 2>/dev/null &
echo -n "$! " >> .pids
sleep 5