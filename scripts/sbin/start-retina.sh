#!/bin/bash

if [ -z "$PIXELS_HOME" ]; then
  echo "ERROR: PIXELS_HOME is not set."
  exit 1
fi

# Remember to set `hostnames` and `pixels_home[Optional]` in config file `$PIXELS_HOME/etc/retina`
DEFAULT_PIXELS_HOME=$PIXELS_HOME

while read -r retina home
do
    [ -z "${retina}" ] && continue # Skip empty lines
    home="${home:-${DEFAULT_PIXELS_HOME}}"

    # Path to the jemalloc shared library
    JEMALLOC_LIB="${home}/lib/libjemalloc.so"

    # REMOTE_SCRIPT explanation:
    # 1. Set LD_LIBRARY_PATH to find pixels libraries.
    # 2. Set LD_PRELOAD to the jemalloc lib. This ensures the symbols are loaded into the process's global scope immediately.
    REMOTE_SCRIPT="export PIXELS_HOME=${home} && \
                   export LD_LIBRARY_PATH=${home}/lib:\$LD_LIBRARY_PATH && \
                   export LD_PRELOAD=${JEMALLOC_LIB} && \
                   \$PIXELS_HOME/bin/start-daemon.sh retina -daemon"

    echo "Starting retina on ${retina} using LD_PRELOAD with jemalloc"
    ssh -n "${retina}" "${REMOTE_SCRIPT}"
done < "$PIXELS_HOME/etc/retina"