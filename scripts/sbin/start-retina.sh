#!/bin/bash

if [ -z "$PIXELS_HOME" ]; then
  echo "ERROR: PIXELS_HOME is not set."
  exit 1
fi

DEFAULT_PIXELS_HOME=$PIXELS_HOME
# Configuration for jemalloc profiling
# lg_prof_interval:30 means a profile is dumped every 2^30 bytes (~1GB) of allocation
MALLOC_OPTS="prof:true,lg_prof_interval:30,prof_prefix:jeprof.out"

while read -r retina home
do
    [ -z "${retina}" ] && continue # Skip empty lines
    home="${home:-${DEFAULT_PIXELS_HOME}}"

    # Path to the jemalloc shared library
    JEMALLOC_LIB="${home}/lib/libjemalloc.so"

    # REMOTE_SCRIPT explanation:
    # 1. Set LD_LIBRARY_PATH to find pixels libraries.
    # 2. Set LD_PRELOAD to the jemalloc lib. Even with 'pixels_' prefix, this ensures
    #    the symbols are loaded into the process's global scope immediately.
    # 3. MALLOC_CONF and PIXELS_MALLOC_CONF are set to ensure the prefixed version
    #    picks up the configuration correctly.
    REMOTE_SCRIPT="export PIXELS_HOME=${home} && \
                   export LD_LIBRARY_PATH=${home}/lib:\$LD_LIBRARY_PATH && \
                   export LD_PRELOAD=${JEMALLOC_LIB} && \
                   export MALLOC_CONF='${MALLOC_OPTS}' && \
                   export PIXELS_MALLOC_CONF='${MALLOC_OPTS}' && \
                   \$PIXELS_HOME/bin/start-daemon.sh retina -daemon"

    echo "Starting retina on ${retina} using LD_PRELOAD with jemalloc (prefix: pixels_)..."
    ssh -n "${retina}" "${REMOTE_SCRIPT}"
done < "$PIXELS_HOME/etc/retina"