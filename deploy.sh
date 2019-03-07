#!/bin/bash

node="dbiir01"

mvn clean package

ssh -tt iir@$node /home/iir/opt/presto-server/sbin/stop-all.sh
scp -r ./pixels-presto/target/pixels-presto-0.1.0-SNAPSHOT iir@$node:~/opt/presto-server/plugin/
ssh -tt iir@$node /home/iir/opt/presto-server/sbin/mv-plugins.sh
