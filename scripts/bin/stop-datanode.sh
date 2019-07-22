#!/bin/bash

echo "Stop Pixels Coordinator."

base_dir=$(dirname $0)
MAIN_CLASS="io.pixelsdb.pixels.daemon.DaemonMain"

if [ "xPIXELS_HOME" = "x" ]; then
  export PIXELS_HOME="${base_dir}/../"
fi

EXTRA_ARGS="-role kill"
OPTS="datanode"

exec ${base_dir}/run-class.sh ${EXTRA_ARGS} ${MAIN_CLASS} $OPTS
