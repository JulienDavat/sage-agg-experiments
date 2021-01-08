#!/bin/bash

nohup sage -w 1 -p 8080 "configs/correct/150.yaml" > output/log/sage-150.log 2>/dev/null &
echo -n "$! " > output/pids
nohup sage -w 1 -p 8081 "configs/correct/1500.yaml" > output/log/sage-1500.log 2>/dev/null &
echo -n "$! " >> output/pids
nohup sage -w 1 -p 8082 "configs/correct/15000.yaml" > output/log/sage-15000.log 2>/dev/null &
echo -n "$! " >> output/pids
sleep 5