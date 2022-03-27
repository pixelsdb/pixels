#!/bin/sh

if [ -z "$PIXELS_HOME" ]; then
  echo "ERROR: PIXELS_HOME is not set."
  exit 1
fi

# start coodrinator
echo "Starting Coordinator..."
$PIXELS_HOME/bin/start-coordinator.sh -daemon

sleep 5

echo "Starting DataNode..."
$PIXELS_HOME/bin/start-datanode.sh -daemon

