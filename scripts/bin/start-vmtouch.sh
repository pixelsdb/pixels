#!/bin/bash

NUMA_INTERLEAVE=""
if type numactl >/dev/null 2>&1; then
  NUMA_INTERLEAVE="numactl --interleave=all"
fi

# keep CACHE_PATH same as the directory of
# cache.location and index.location in PIXELS_HOME/etc/pixels.properties
CACHE_PATH="/mnt/ramfs"

${NUMA_INTERLEAVE} vmtouch -vt ${CACHE_PATH}/pixels.*
${NUMA_INTERLEAVE} vmtouch -dl ${CACHE_PATH}

echo "vmtouch is up and running."