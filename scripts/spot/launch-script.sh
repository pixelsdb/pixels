#!/bin/bash

# author: Tiannan Sha
# script to run right before a new instance becoming in-service.
# usage: add this to template of auto scaling group

# adapted from: https://docs.aws.amazon.com/autoscaling/ec2/userguide/tutorial-lifecycle-hook-instance-metadata.html

function get_instance_id {
    echo $(curl -s http://169.254.169.254/latest/meta-data/instance-id)
}

# start trino node
# replace dummies in configurations
# ~/trino-server-375/etc/config.properties replace coordinator-ip-dummy to <coord_private_ip>
sed -i 's/coordinator-ip-dummy/172.31.33.182/g' /home/ubuntu/opt/trino-server/etc/config.properties

# ~/trino-server-375/etc/node.properties replace instance-id-dummy to <instance id>
instance_id=$(get_instance_id)
sed -i "s/instance-id-dummy/$instance_id/g" /home/ubuntu/opt/trino-server/etc/node.properties

echo "start trino"
sudo /home/ubuntu/opt/trino-server/bin/launcher start

echo "start node_exporter"
screen -d -S node_exporter -m /home/ubuntu/opt/node_exporter/start-node-exporter.sh

# keep checking termination state to activate termination hook for gracefully shutting down trino
nohup /root/termination-handler.sh &
