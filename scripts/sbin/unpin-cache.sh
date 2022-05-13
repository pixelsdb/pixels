#!/bin/sh

if [ -z "$PIXELS_HOME" ]; then
  echo "ERROR: PIXELS_HOME is not set."
  exit 1
fi

echo "Stopping vmtouch..."
$PIXELS_HOME/bin/stop-vmtouch.sh"
