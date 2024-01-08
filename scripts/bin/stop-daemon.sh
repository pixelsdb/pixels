#!/bin/bash

if [ -z "$PIXELS_HOME" ]; then
  echo "ERROR: PIXELS_HOME is not set."
  exit 1
fi

MAIN_CLASS="io.pixelsdb.pixels.daemon.DaemonMain"

if [ "xPIXELS_HOME" = "x" ]; then
  export PIXELS_HOME="${base_dir}/../"
fi

ROLE=$1
EXTRA_ARGS="-operation stop -role "$ROLE

exec ${PIXELS_HOME}/bin/run-class.sh ${EXTRA_ARGS} ${MAIN_CLASS}
