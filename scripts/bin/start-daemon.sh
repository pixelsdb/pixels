#!/bin/bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 role [-daemon]"
  echo "role can be coordinator, datanode, or guard."
  exit 1
fi

if [ -z "$PIXELS_HOME" ]; then
  echo "ERROR: PIXELS_HOME is not set."
  exit 1
fi

MAIN_CLASS="io.pixelsdb.pixels.daemon"
EXTRA_ARGS="-operation start"
DAEMON_ROLE=$1
case ${DAEMON_ROLE} in
  coordinator)
    MAIN_CLASS=${MAIN_CLASS}".PixelsCoordinator"
    EXTRA_ARGS=${EXTRA_ARGS}" -role coordinator"
    ;;
  datanode)
    MAIN_CLASS=${MAIN_CLASS}".PixelsDataNode"
    EXTRA_ARGS=${EXTRA_ARGS}" -role datanode"
    ;;
  *)
    echo "invalid DAEMON_ROLE: "$DAEMON_ROLE
    exit 1
    ;;
esac

COMMAND=$2
case ${COMMAND} in
  -daemon)
    EXTRA_ARGS=${EXTRA_ARGS}" -daemon"
    ;;
  *)
    ;;
esac

exec ${PIXELS_HOME}/bin/run-class.sh ${EXTRA_ARGS} ${MAIN_CLASS}
