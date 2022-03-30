#!/bin/sh

if [ -z "$PIXELS_HOME" ]; then
  echo "ERROR: PIXELS_HOME is not set."
  exit 1
fi

# $PIXELS_HOME/bin/stop-datanode.sh

$PIXELS_HOME/bin/stop-coordinator.sh
