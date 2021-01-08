#!/bin/bash

pids=$(cat output/pids)
kill -9 $pids