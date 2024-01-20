#!/bin/bash

if [ -z "$PIXELS_HOME" ]; then
  echo "ERROR: PIXELS_HOME is not set."
  exit 1
fi

HOSTNAME=$(hostname -f)

# start coordinator
echo "Starting coordinator on ${HOSTNAME}..."
$PIXELS_HOME/bin/start-daemon.sh coordinator -daemon