#!/bin/bash
# author: Tiannan Sha
# script to run right before a new instance becoming in-service. This script auto starts trino and keep
# checking the state of the instance, if autoscaling group decides to terminate this instance (due to e.g. rebalance recomendation (high spot interruption risk) or scaling), a termination
# hook will be trigerred to gracefully shutdown trino.
# usage: add this to template of auto scaling group


# adapted from: https://docs.aws.amazon.com/autoscaling/ec2/userguide/tutorial-lifecycle-hook-instance-metadata.html


function get_target_state {
    echo $(curl -s http://169.254.169.254/latest/meta-data/autoscaling/target-lifecycle-state)
}

function get_instance_id {
    echo $(curl -s http://169.254.169.254/latest/meta-data/instance-id)
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

#  origninal code
    # echo $instance_id
    # echo $region
    # echo $(aws autoscaling complete-lifecycle-action \
    #   --lifecycle-hook-name $hook_name \
    #   --auto-scaling-group-name $group_name \
    #   --lifecycle-action-result CONTINUE \
    #   --instance-id $instance_id \
    #   --region $region)
}

function main {


    # start trino node
    # replace dummies in configurations
    # ~/trino-server-375/etc/config.properties replace coordinator-ip-dummy to <coord_private_ip>
    sed -i 's/coordinator-ip-dummy/172.31.12.133/g' /home/ubuntu/trino-server-375/etc/config.properties

    # ~/trino-server-375/etc/node.properties replace instance-id-dummy to <instance id>
    instance_id=$(get_instance_id)
    sed -i "s/instance-id-dummy/$instance_id/g" /home/ubuntu/trino-server-375/etc/node.properties

    echo "start trino"
    sudo /home/ubuntu/trino-server-375/bin/launcher start

    # turn off the lambda co processing as we are about to finish lauching new instance
    echo "turn off lambda co-processing"

    # keep checking termination state to activate termination hook for gracefully shutting down trino
    while true
    do
        #### handle scale in and other terminations not caused by spot interruption
        target_state=$(get_target_state)
        if [ \"$target_state\" = \"Terminated\" ]; then
            # # Change hostname
            # export new_hostname="${group_name}-$instance_id"
            # hostname $new_hostname
            # request to gracefully shutdown trino
            echo "gracefully shutdown trino"
            curl -v -X PUT -d '"SHUTTING_DOWN"' -H "Content-type: application/json"  -H "X-Trino-User:user"   http://localhost:8080/v1/info/state

            # no need to call back as hook would timeout (can be set to 30sec to 2hours in aws console) and also trino grace shut down doesn't happen imediately,
            # can pgrep check if there's trino process though, but
            # Send callback
            # complete_lifecycle_action
            break
        fi
        echo $target_state

        #### handle rebalance recommendation, this seems to be unnecessary as we can set ASG to auto
        #### handle rebalance rec by first start a new instance and then terminate the old one
        #### the old one would use termination hook to gracefully shutdown trino
        # rebalance_rec_HTTPCODE=$(get_rebalance_recommendation_httpcode)
        # if [[ "$rebalance_rec_HTTPCODE" -eq 200 ]] ; then
        # # Insert Your Code to Handle Interruption Here
        #     echo "gracefully shutdown Trino"
        #     complete_lifecycle_action
        #     break
        # else
        #     echo $rebalance_rec_HTTPCODE
        # fi

        sleep 5
    done
}

main &