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

Then, copy `termination-handler.sh` into `/root/` of this instance and give its `rwx` permissions to the root user.
Set valid values for `pixels_asg_name` (`Pixels-ASG` in this document) and `pixels_aws_region`.

After that, select this instance, click `Actions`-->`Image and templates`-->`Create Image`, fill in the image name (e.g., `Pixels-Worker-AMI`) and description, 
select `Tag image and snapshots together` and then click `Create Image`.

Go to `EC2`-->`Images`-->`AMIs`, you can see the created OS image. Wait for its status becoming `Available`.

### Create Launch Template

Go to `EC2`-->`Instances`-->`Launch Templates`, click `Create launch template`.
Fill in the launch template name (e.g., `Pixels-ASG-Launch-Template`) and the template version description.
Check `Provide guidance to help set set up a template that I can use with EC2 Auto Scaling`.

In `Application and OS Images`, browse and select the OS image `Pixels-Worker-AMI` created above.
Ignore instance type and select the key pair you will use to log in the worker nodes.
In `Network settings`, select the same security group as the coordinator node.
In `Storage`, select 8GB for the root volume.
In `Advanced details`, paste the content of `launch-script.sh` into `User data` and set `coordinator_ip`, 
`executor_output_storage_scheme`, and `executor_output_folder` correctly.
`coordinator_ip` must be set as the private ip of the coordinator node.
Click `Create launch template` to create the launch template.

### Create CloudWatch Alarms

Execute the unit test `testSingle` in `pixels-scaling-ec2/src/test/java/io/pixelsdb/pixels/scaling/ec2/TestCloudWatchMetrics`
to create a custom metric in CloudWatch. The namespace, dimension name, and dimension value are set by the following three
properties in `$PIXELS_HOME/pixels.properties` (if the unit test is executed with a valid `$PIXELS_HOME` environment variable):
```properties
cloud.watch.metrics.namespace=Pixels
cloud.watch.metrics.dimension.name=cluster
cloud.watch.metrics.dimension.value=01
```
Go to `CloudWatch`-->`Alarms`-->`All Alarms`, create two alarms as follows.

(1) The lower bound alarm. Select `Pixels/cluster/01` as the metric, then select `1 minute` as `Period` and 
`Static, Lower/Equal than 0.75` as `Conditions`. In `additional configuration`, set `1 out of 1` for `datapoints to alarm` and select
`Treat missing data as ignore (maintain the alarm state)` for `Missing data treatment`. In the next page, set notification if needed
and keep other setting as default. In the next page, fill in the alarm name `Pixels-Query-Concurrency-Lower-Bound`, review the settings
in the next page and click `Create alarm`.

(2) The upper bound alarm. Select `Pixels/cluster/01` as the metric, then select `30 seconds` as `Period` and
`Static, Greater/Equal than 3` as `Conditions`. The other settings are the same as above, except the alarm name should be
`Pixels-Query-Concurrency-Upper-Bound`.

### Create EC2 Auto Scaling Group

Go to `EC2`-->`Auto Scaling`-->`Auto Scaling Groups`, click `Create Auto Scaling Group`.
Fill in the Auto Scaling group name (e.g., `Pixels-ASG`), and select the launch template `Pixels-ASG-Launch-Template` created above. 
Click 'Next'.

In `Instance type requirements`, select `Specify instance type attributes`, fill in the number of vCPUs 
(e.g., 32 for both min and max) and amount of memory (e.g., 128GB for both min and max), select Intel and AMD as the CPU manufacture 
(Pixels may work on Amazon's ARM CPUs, but it is tested),
and optionally exclude some instance types if you do not want the auto-scaling group to create such type of instances.

In `Instance purchase options`, select the percentile of on-demand and spot instances. 
In Pixels, we recommend using 100% spot instances for higher cost-efficiency. 
Pixels cluster can tolerate the reclaim of worker nodes, whereas the coordinator node is not in the auto-scaling group.
In `Allocation strategies`, it is recommended to select `Price capacity optimized` and `Enable Capacity Rebalancing`.

In `Network`, select the same VPC and availability zone as the coordinator node. Click `Next`.
In the next page, keep everything as default and click `Next`.

In `Group size`, set the desired capacity to 0 instance.
In `Scaling`, set the min and max capacity to 0 and the max number of instances your account can create (e.g., 100), respectively.
And select `No scaling policies` for `Automatic scaling` as we do not use `target tracking policy` in our auto-scaling group.
In `Instance maintenance policy`, select `No policy`. Click `Next`. Add notifications if needed and click `Next`.
Add tags if needed and click `Next`. Review the settings and click `Create Auto Scaling group`.

Click and enter `Pixels-ASG` in `EC2`-->`Auto Scaling`-->`Auto Scaling Groups`, open the tab `Automatic scaling`, create two dynamic scaling policies
as follows.

(1) The scaling-in policy. Fill in scaling policy name `Pixels-MPP-Scaling-In-Policy`, select `Step scaling` as policy type, select `Pixels-Query-Concurrency-Lower-Bound` as the
CloudWatch alarm, and select take action `Remove 50 percent of group when 0.75 >= query concurrency > -Infinity` and `Remove instances in increments of at least 1 capacity units`.
Click `Create` to create this scaling policy.

(2) The scaling-out policy. Fill in scaling policy name `Pixels-MPP-Scaling-Out-Policy`, select `Step scaling` as policy type, select `Pixels-Query-Concurrency-Upper-Bound` as the
CloudWatch alarm, and select take action `Add 100 percent of group when 3 <= query concurrency < +Infinity`, `Add capacity units in increments of at least 1 capacity units`, 
and `Instance warmup 120 seconds`. Click `Create` to create this scaling policy.

Now, the auto-scaling group has been set up for Pixels. You can execute the aforementioned unit test `testSingle` to update the query concurrency metric
in CloudWatch, and it will trigger the scaling event to create or release EC2 instances for the MPP cluster if the metric value is beyond the range (0.75, 3).
