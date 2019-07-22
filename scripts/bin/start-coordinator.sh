#!/bin/bash

echo "Start Pixels Coordinator."

base_dir=$(dirname $0)
MAIN_CLASS="io.pixelsdb.pixels.daemon.DaemonMain"

if [ "xPIXELS_HOME" = "x" ]; then
  export PIXELS_HOME="${base_dir}/../"
else
  base_dir=$PIXELS_HOME
fi

EXTRA_ARGS="-role main -name PixelsCoordinator"
OPTS="coordinator"
COMMAND=$1
case ${COMMAND} in
  -daemon)
    EXTRA_ARGS=${EXTRA_ARGS}" -daemon"
    shift
    ;;
  *)
    ;;
esac

exec ${base_dir}/bin/run-class.sh ${EXTRA_ARGS} ${MAIN_CLASS} $OPTS
