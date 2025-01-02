# Pixels Turbo - Serverless Worker (AWS Lambda)

`pixels-worker-lambda` implements the serverless query-execution workers running in AWS Lambda.

## Deployment

After [building Pixels](https://github.com/pixelsdb/pixels#build-pixels), we can find
two target files under `pixels-turbo/pixels-worker-lambda/target/`:
* `pixels-worker-lambda-deps.zip`, it contains the 3rd-party dependencies required by the workers.
* `pixels-worker-lambda.jar`, it contains the implementation of the workers.

> Note that AWS Lambda currently only supports Java 8 and Java 11. Therefore, we must build Pixels using JDK 8 or 11
> according to the Java version in the configuration of the Lambda functions.

To deploy the workers:
- Create a `layer` in AWS Lambda for `pixels-worker-lambda-deps.zip`. 
The java runtime can be either java-8 or java-11. The architecture can be either x86-64 or arm64.
- Create another `layer` in AWS Lambda that contains the configuration file `pixels.properties` of Pixels.
A template of this file can be found in `pixels-common/main/resources/`.
Create a folder named `pixels/etc`, put `pixels.properties` in this folder, and compress
this folder into a zip file. Then, use the zip file to create the layer.
For the serverless workers running in AWS Lambda, only the following configuration properties related to 
file storage and I/O are used:
```properties
### file storage and I/O ###
# the scheme of the storage systems that are enabled, e.g., hdfs,file,s3,gcs,minio,redis
enabled.storage.schemes=s3,minio,redis
# which scheduler to use for read requests, valid values: noop, sortmerge, ratelimited
read.request.scheduler=ratelimited
read.request.merge.gap=0
# rate limits only work for s3+ratelimited
read.request.rate.limit.rps=4000
read.request.rate.limit.mbps=200
read.request.enable.retry=true
read.request.max.retry.num=3
# the interval in milliseconds of retry queue checks
read.request.retry.interval.ms=1000
s3.enable.async=true
s3.use.async.client=true
s3.connection.timeout.sec=3600
s3.connection.acquisition.timeout.sec=3600
s3.client.service.threads=8
s3.max.request.concurrency=500
s3.max.pending.requests=10000
```
The above property values should be good defaults for AWS Lambda as of early 2023.

After creating the two layers containing the dependencies and configurations of Pixels, we create the
one Lambda functions for each operator (e.g., scan, broadcast join) in our serverless executor.
In AWS Lambda console, click `create function`, select `Author from scratch`, fill in the function name,
e.g., `ScanWorker`, runtime and architecture are the dependency layer, select or create a `role` with
read-write permissions of the S3 buckets used for storing tables and intermediate results.
In advanced settings, enable VPC and select the same VPC of your EC2 instances.

> Note: If your VPC cannot access S3, you need to add an S3 Gateway Endpoint in the VPC. In the `VPC->EndPoints` section, create an `Endpoint`, select the `Amazon Web Services services` service type, add an `S3 Service` of type `Gateway`, then add it to your VPC and grant `Full access` permission.

Then, upload `pixels-worker-lambda.jar` as the code source of the Lambda function.
In `Runtime settings`, select the handler class of the operator, e.g., `io.pixelsdb.pixels.worker.lambda.ScanWorker`.
In `Layers`, add the two layers created above.
In `Configuration->General configuration`, set timeout to 15 minutes and memory to 10240 MB.
In `Configuration->Environment variables`, create an environment variable named `PIXELS_HOME` with the value `/opt/pixels`.
`/opt/` is the path where the layers are mounted on by default.

We create the other six workers in a similar way:
* `BroadcastChainJoinWorker` with the handler `io.pixelsdb.pixels.worker.lambda.BroadcastChainJoinWorker`;
* `AggregationWorker` with the handler `io.pixelsdb.pixels.worker.lambda.AggregationWorker`;
* `PartitionedJoinWorker` with the handler `io.pixelsdb.pixels.worker.lambda.PartitionedJoinWorker`;
* `PartitionWorker` with the handler `io.pixelsdb.pixels.worker.lambda.PartitionWorker`;
* `PartitionedChainJoinWorker` with the handler `io.pixelsdb.pixels.worker.lambda.PartitionedChainJoinWorker`;
* `BroadcastJoinWorker` with the handler `io.pixelsdb.pixels.worker.lambda.BroadcastJoinWorker`.

> Note: the names of the workers should be consistent with the worker names in `pixels.properties`:
> ```properties
> scan.worker.name=ScanWorker
> partition.worker.name=PartitionWorker
> broadcast.join.worker.name=BroadcastJoinWorker
> broadcast.chain.join.worker.name=BroadcastChainJoinWorker
> partitioned.join.worker.name=PartitionedJoinWorker
> partitioned.chain.join.worker.name=PartitionedChainJoinWorker
> aggregation.worker.name=AggregationWorker
> ```

Now, the deployment of `pixels-lambada-worker` is done. The deployed serverless workers can be used by `Pixels Turbo` 
to execute unpredictable workload spikes.
Currently, Pixels Turbo uses Trino as the query execution engine in the MPP cluster 
(while using the above serverless workers as the query execution engine in cloud functions).
The adaptive and transparent switch between the two types of query execution engines is implemented in 
[`Pixels-Trino`](https://github.com/pixelsdb/pixels-trino). See the instructions of [Install Trino](../../docs/INSTALL.md#install-trino)
to enable this feature.