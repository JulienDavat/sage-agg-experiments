#!/bin/bash

VIRTUOSO_DIR=/usr/local/virtuoso-opensource

# Stopping all servers except Virtuoso
pids=$(cat .pids)
kill -9 $pids
rm .pids

# Stopping Virtuoso
$VIRTUOSO_DIR/bin/isql EXEC='shutdown;'

echo "Stopping all servers..."
sleep 15