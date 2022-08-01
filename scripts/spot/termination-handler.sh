#!/bin/bash

# author: Tiannan Sha
# This script keep checking the state of the instance, if autoscaling group decides to terminate this instance
# (due to e.g. rebalance recomendation (high spot interruption risk) or scaling), a termination hook will be
# trigerred to gracefully shutdown trino.
# usage: auto start executing this scrip when the instance is boot.

function get_target_state {
    echo $(curl -s http://169.254.169.254/latest/meta-data/autoscaling/target-lifecycle-state)
}

function get_rebalance_recommendation_httpcode {
    echo $(curl -s -w %{http_code} -o /dev/null http://169.254.169.254/latest/meta-data/events/recommendations/rebalance)
}

function complete_lifecycle_action {
    instance_id=$(get_instance_id)
    group_name='ASG-for-spot-vms'
    region='us-east-2'
    hook_name='lifecycle_hook_for_gracefully_shutdown'
    token=`curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600"`
    echo $instance_id
    echo $region
    echo $(aws autoscaling complete-lifecycle-action \
      --lifecycle-token $token \
      --lifecycle-hook-name $hook_name \
      --auto-scaling-group-name $group_name \
      --lifecycle-action-result CONTINUE)
}

while true
do
  #### handle scale in and other terminations not caused by spot interruption
  target_state=$(get_target_state)
  if [ \"$target_state\" = \"Terminated\" ]; then
    echo "gracefully shutdown trino"
    curl -v -X PUT -d '"SHUTTING_DOWN"' -H "Content-type: application/json"  -H "X-Trino-User:user"   http://localhost:8080/v1/info/state
    break
  fi
  echo $target_state

  #### handle re-balancing recommendation, this seems to be unnecessary as we can set ASG to auto
  #### handle re-balancing rec by first start a new instance and then terminate the old one
  #### the old one would use termination hook to gracefully shutdown trino
  rebalance_httpcode=$(get_rebalance_recommendation_httpcode)
  if [ "$rebalance_httpcode" -eq 200 ]; then
    echo "gracefully shutdown trino"
    curl -v -X PUT -d '"SHUTTING_DOWN"' -H "Content-type: application/json"  -H "X-Trino-User:user"   http://localhost:8080/v1/info/state
    break
  fi
  echo $rebalance_httpcode
  sleep 5
done
