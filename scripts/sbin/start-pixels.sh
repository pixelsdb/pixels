#!/bin/sh

prefix="dbiir"
start=2
end=9

PIXELS_HOME="/home/iir/opt/pixels/"

# start coodrinator
echo "start coordinator..."
$PIXELS_HOME/bin/start-coordinator.sh -daemon
echo "------"

sleep 5

# start data nodes
echo "start datanodes..."
for ((i=$start; i<=$end; i++))
do
  if [ $i -lt 10 ]
  then
    echo "start datanode on "$prefix"0"$i
    ssh $prefix"0"$i "export PIXELS_HOME='$PIXELS_HOME' && $PIXELS_HOME/bin/start-datanode.sh -daemon"
  else
    echo "start datanode on "$prefix$i
    ssh $prefix$i "export PIXELS_HOME='$PIXELS_HOME' && $PIXELS_HOME/bin/start-datanode.sh -daemon"
  fi
  echo "------"
done

