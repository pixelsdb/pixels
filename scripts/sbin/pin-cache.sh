#!/bin/sh

prefix="dbiir"
start=2
end=9

PIXELS_HOME="/home/iir/opt/pixels/"

# start vmtouch
echo "pin cache and index into memory..."
for ((i=$start; i<=$end; i++))
do
  if [ $i -lt 10 ]
  then
    echo "start vm on "$prefix"0"$i
    ssh $prefix"0"$i "export PIXELS_HOME='$PIXELS_HOME' && $PIXELS_HOME/bin/start-vmtouch.sh"
  else
    echo "start vm on "$prefix$i
    ssh $prefix$i "export PIXELS_HOME='$PIXELS_HOME' && $PIXELS_HOME/bin/start-vmtouch.sh"
  fi
done
