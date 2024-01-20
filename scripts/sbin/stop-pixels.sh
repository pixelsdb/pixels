#!/bin/sh

if [ -z "$PIXELS_HOME" ]; then
  echo "ERROR: PIXELS_HOME is not set."
  exit 1
fi

$PIXELS_HOME/sbin/stop-datanode.sh

$PIXELS_HOME/sbin/stop-coordinator.sh
