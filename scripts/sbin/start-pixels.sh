#!/bin/sh

if [ -z "$PIXELS_HOME" ]; then
  echo "ERROR: PIXELS_HOME is not set."
  exit 1
fi

# start coordinator
$PIXELS_HOME/sbin/start-coordinator.sh -daemon

sleep 5

$PIXELS_HOME/sbin/start-datanode.sh -daemon

