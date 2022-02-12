#!/bin/sh

PIXELS_HOME="/home/ubuntu/opt/pixels/"
echo "Stopping DataNode..."
$PIXELS_HOME/bin/stop-datanode.sh

# stop oodrinator
echo "Stopping Coordinator..."
$PIXELS_HOME/bin/stop-coordinator.sh
