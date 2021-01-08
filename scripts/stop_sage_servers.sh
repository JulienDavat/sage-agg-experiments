#!/bin/bash

pids=$(cat .pids)
kill -9 $pids
rm .pids