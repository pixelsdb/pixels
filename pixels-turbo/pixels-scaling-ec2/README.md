# Pixels Turbo - Auto Scaling (AWS EC2)

The source code and scripts in this module is to support auto-scaling of the MPP cluster running in AWS EC2.

## Setup Auto Scaling

[Build Pixels](../../README.md#build-pixels) and find scripts (i.e., `launch-script.sh` and `termination-handler.sh`) 
in the `src/main/scripts/` directory of this module.

### Create Coordinator Node

Create an EC2 instance with 32GB disk and install the Pixels components following the instructions [HERE](../../docs/INSTALL.md).
This instance will be used as the coordinator node of the MPP cluster.
Do not use spot instance for this node.

### Create OS Image

Login AWS, create an EC2 instance with 8GB disk, install Pixels components following the instructions [HERE](../../docs/INSTALL.md).
This instance will be used to create the OS image for the worker nodes in the MPP cluster.
You can skip the installation of MySQL, etcd and Grafana, as these components are only needed on the coordinator node.
For Pixels, editing and putting `pixels.properties` into `PIXELS_HOME` is enough if you do not use `pixels-cache`.
Otherwise, fully install Pixels, and ensure you can start it as a datanode using:
```bash
$PIXELS_HOME/bin/start-datanode.sh -daemon
```
For Trino, set `coordinator=false` in `config.properties`, thus Trino will be started as a worker.

Select this instance, click `Actions`-->`Image and templates`-->`Create Image`, fill in the image name (e.g., `Pixels-Worker-AMI`) and description, 
select `Tag image and snapshots together` and then click `Create Image`.

Go to `EC2`-->`Images`-->`AMIs`, you can see the created OS image. Wait for its status becoming `Available`.

### Create Launch Template

Go to `EC2`-->`Instances`-->`Launch Templates`, click `Create launch template`.
Fill in the launch template name (e.g., `Pixels-ASG-Launch-Template`) and the template version description.
Select `Provide guidance to help set set up a template that I can use with EC2 Auto Scaling`.

In `Application and OS Images`, browse and select the OS image `Pixels-Worker-AMI` created above.
Ignore instance type and select the key pair you will use to log in the worker nodes.
In `Network settings`, select the same security group as the coordinator node.
In `Storage`, select 8GB for the root volume.
In `Advanced details`, paste the content of `launch-script.sh` into `User data` and set `coordinator_ip`, 
`executor_output_storage_scheme`, and `executor_output_folder` correctly.
`coordinator_ip` must be set as the private ip of the coordinator node.
Click `Create launch template` to create the launch template.


### Create EC2 Auto Scaling Group

Go to `EC2`-->`Auto Scaling`-->`Auto Scaling Groups`, click `Create Auto Scaling Group`.
Fill in the Auto Scaling group name, and select the launch template `Pixels-ASG-Launch-Template` created above.
Click 'Next'.

In `Instance type requirements`, select `Specify instance type attributes`, fill in the number of vCPUs 
(e.g., 32 for both min and max) and amount of memory in MB (e.g., 12800 min and 13200 max for 128GB),
and optionally exclude some instance types if you do not want the auto-scaling group to create such type of instances.

In `Instance purchase options`, select the percentile of on-demand and spot instances. 
In Pixels, we recommend using 100% spot instances for higher cost-efficiency. 
Pixels cluster can tolerate the reclaim of worker nodes, whereas the coordinator node is not in the auto-scaling group.
In `Allocation strategies`, it is recommended to select `Price capacity optimized` and `Enable Capacity Rebalancing`.

In `Network`, select the same VPC and availability zone as the coordinator node. Click `Next`.
In the next page, keep everything as default and click `Next`.

In `Group size`, set the desired capacity to 0 instance.
In `Scaling`, set the min and max capacity to 0 and the max number of instances your account can create (e.g., 100), respectively.
In `Instance maintenance policy`, select `No policy`. Click `Next`.
