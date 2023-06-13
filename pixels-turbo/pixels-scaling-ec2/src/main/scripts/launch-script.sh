#!/bin/bash

# author: Tiannan Sha
# script to run right before a new instance becoming in-service.
# usage: add this to template of auto scaling group

# adapted from: https://docs.aws.amazon.com/autoscaling/ec2/userguide/tutorial-lifecycle-hook-instance-metadata.html

function get_instance_id {
    echo $(curl -s http://169.254.169.254/latest/meta-data/instance-id)
}

function get_private_ip {
    echo $(curl -s http://169.254.169.254/latest/meta-data/local-ipv4)
}

# start trino node
# replace dummies in configurations
# ~/opt/trino-server/etc/config.properties replace coordinator-ip-dummy with the private ip of coordinator
sed -i 's/coordinator-ip-dummy/172.31.33.182/g' /home/ubuntu/opt/trino-server/etc/config.properties

# ~/opt/trino-server/etc/node.properties replace instance-id-dummy with the real instance id
instance_id=$(get_instance_id)
sed -i "s/instance-id-dummy/$instance_id/g" /home/ubuntu/opt/trino-server/etc/node.properties

# ~/opt/pixels/pixels.properties replace output-endpoint-dummy with the local minio endpoint
private_ip=$(get_private_ip)
sed -i "s+output-endpoint-dummy+http://$private_ip:9000/+" /home/ubuntu/opt/pixels/pixels.properties

# ~/opt/pixels/pixels.properties replace output-folder-dummy with the local minio folder
sed -i "s+output-folder-dummy+pixels-lambda/output+" /home/ubuntu/opt/pixels/pixels.properties

# ~/opt/pixels/pixels.properties set output.scheme to minio
sed -i "s/output-scheme-dummy/minio/g" /home/ubuntu/opt/pixels/pixels.properties

echo "start trino"
su ubuntu -c "/home/ubuntu/opt/trino-server/bin/launcher start"

echo "start node_exporter"
su ubuntu -c "screen -d -S node_exporter -m /home/ubuntu/opt/node_exporter/start-node-exporter.sh"

echo "start minio_server"
su ubuntu -c "screen -d -S minio_server -m /home/ubuntu/opt/minio-server/minio server --console-address :9090 /home/ubuntu/opt/minio-server/data/"

# keep checking termination state to activate termination hook for gracefully shutting down trino
nohup /root/termination-handler.sh &
