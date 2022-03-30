#!/bin/bash

if [ -z "$PIXELS_HOME" ]; then
  echo "ERROR: PIXELS_HOME is not set."
  exit 1
fi

echo "Removing logs of pixels-cache..."
rm $PIXELS_HOME/logs/pixels-cache.log

echo "Removing backed files of pixels-cache..."
rm /mnt/ramfs/pixels.cache && rm /mnt/ramfs/pixels.index

export ETCDCTL_API=3
HOST_1=localhost
ENDPOINTS=$HOST_1:2379
etcdctl --endpoints=$ENDPOINTS put cache_version 0
etcdctl --endpoints=$ENDPOINTS put layout_version 0
etcdctl --endpoints=$ENDPOINTS del coordinator
etcdctl --endpoints=$ENDPOINTS del --prefix node_
etcdctl --endpoints=$ENDPOINTS del --prefix location_
