#!/bin/sh

prefix="dbiir"
start=2
end=9
file_prefix="pixels"

PIXELS_HOME="/home/iir/opt/pixels/"
echo "clean local node"
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

