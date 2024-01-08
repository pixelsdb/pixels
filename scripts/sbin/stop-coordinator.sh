#!/bin/bash
if [ -z "$PIXELS_HOME" ]; then
  echo "ERROR: PIXELS_HOME is not set."
  exit 1
fi

$PIXELS_HOME/bin/stop-coordinator.sh
