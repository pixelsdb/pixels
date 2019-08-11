#!/bin/sh

prefix="dbiir"
start=2
end=9

PIXELS_HOME="/home/iir/opt/pixels/"

# stop data nodes
echo "stop datanodes..."
for ((i=$start; i<=$end; i++))
do
  if [ $i -lt 10 ]
  then
    echo "stop datanode on "$prefix"0"$i
    ssh $prefix"0"$i "export PIXELS_HOME='$PIXELS_HOME' && $PIXELS_HOME/bin/stop-datanode.sh"
  else
    echo "stop datanode on "$prefix$i
    ssh $prefix$i "export PIXELS_HOME='$PIXELS_HOME' && $PIXELS_HOME/bin/stop-datanode.sh"
  fi
  echo "------"
done

# stop coodrinator
echo "stop coordinator..."
$PIXELS_HOME/bin/stop-coordinator.sh
echo "------"
