#!/bin/sh

if [ -z "$PIXELS_HOME" ]; then
  echo "ERROR: PIXELS_HOME is not set."
  exit 1
fi

echo "Starting vmtouch..."
$PIXELS_HOME/bin/start-vmtouch.sh
