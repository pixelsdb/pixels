#!/bin/bash

prefix="dbiir"
start=2
end=9
file_prefix="pixels"

PIXELS_HOME="/home/iir/opt/pixels/"

echo "clean cache logs..."
rm /home/iir/opt/pixels/logs/pixels-cache.log

for ((i=$start; i<=$end; i++))
do
  if [ $i -lt 10 ]
  then
    echo "clean datanode on "$prefix"0"$i
    ssh $prefix"0"$i "rm /home/iir/opt/pixels/logs/pixels-cache.log"
  else
    echo "clean datanode on "$prefix$i
    ssh $prefix$i "rm /home/iir/opt/pixels/logs/pixels-cache.log"
  fi
done

echo "clean cache files..."
rm /dev/shm/pixels.cache && rm /dev/shm/pixels.index

for ((i=$start; i<=$end; i++))
do
  if [ $i -lt 10 ]
  then
    echo "clean datanode on "$prefix"0"$i
    ssh $prefix"0"$i "rm /dev/shm/$file_prefix.cache && rm /dev/shm/$file_prefix.index"
  else
    echo "clean datanode on "$prefix$i
    ssh $prefix$i "rm /dev/shm/$file_prefix.cache && rm /dev/shm/$file_prefix.index"
  fi
done

echo "reset cache state in etcd..."
export ETCDCTL_API=3
HOST_1=dbiir27
ENDPOINTS=$HOST_1:2379
etcdctl --endpoints=$ENDPOINTS put cache_version 0
etcdctl --endpoints=$ENDPOINTS put layout_version 0
etcdctl --endpoints=$ENDPOINTS del coordinator
etcdctl --endpoints=$ENDPOINTS del --prefix node_
etcdctl --endpoints=$ENDPOINTS del --prefix location_


