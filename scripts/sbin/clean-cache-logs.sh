#!/bin/sh

prefix="dbiir"
start=2
end=9

PIXELS_HOME="/home/iir/opt/pixels/"
echo "clean local node"
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

