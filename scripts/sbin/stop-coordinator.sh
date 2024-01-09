#!/bin/bash

if [ -z "$PIXELS_HOME" ]; then
  echo "ERROR: PIXELS_HOME is not set."
  exit 1
fi

HOSTNAME=$(hostname -f)

echo "Stop coordinator on ${HOSTNAME}..."
$PIXELS_HOME/bin/stop-daemon.sh coordinator
