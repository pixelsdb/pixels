#!/bin/sh

PIXELS_HOME="/home/ubuntu/opt/pixels/"

# start coodrinator
echo "Starting Coordinator..."
$PIXELS_HOME/bin/start-coordinator.sh -daemon

sleep 5

echo "Starting DataNode..."
$PIXELS_HOME/bin/start-datanode.sh -daemon

