#!/bin/bash

if [ -z "$PIXELS_HOME" ]; then
  echo "ERROR: PIXELS_HOME is not set."
  exit 1
fi

# Remember to set `hostnames` and `pixels_home[Optional]` in config file `datanodes`
DEFAULT_PIXELS_HOME=$PIXELS_HOME

while read -r datanode home
do
    home="${home:-${DEFAULT_PIXELS_HOME}}"
    REMOTE_SCRIPT="export PIXELS_HOME=${home} && $PIXELS_HOME/bin/start-daemon.sh datanode -daemon"
    echo "Starting datanode on ${datanode}..."
    ssh -n "${datanode}" "${REMOTE_SCRIPT}"
done < $PIXELS_HOME/sbin/datanodes






