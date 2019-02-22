#!/bin/bash

mvn clean package

ssh -tt iir@dbiir01 /home/iir/opt/presto-server/sbin/stop-all.sh
scp -r ./pixels-presto/target/pixels-presto-0.1.0-SNAPSHOT iir@dbiir01:~/opt/presto-server/plugin/
ssh -tt iir@dbiir01 /home/iir/opt/presto-server/sbin/mv-plugins.sh
