# pixels-lambda-worker

`pixels-lambda-worker` implements the serverless query-execution workers running in AWS Lambda.

## Deployment

After [building Pixels](https://github.com/pixelsdb/pixels#build-pixels), we can find
two target files under `pixels-turbo/pixels-lambda-worker/target/`:
* `pixels-lambda-worker-deps.zip`, it contains the 3rd-party dependencies required by the workers.
* `pixels-lambda-worker.jar`, it contains the implementation of the workers.

To deploy the workers:
- Create a `layer` in AWS Lambda for `pixels-lambda-worker-deps.zip`. 
The java runtime can be either java-8 or java-11. The architecture can be either x86-64 or arm64.
- Create another `layer` in AWS Lambda that contains the configuration file `pixels.properties` of Pixels.
A template of this file can be found in `pixels-common/main/resources/`.
See [here](https://github.com/pixelsdb/pixels#build-pixels) for more details of the properties in this file.
We can create a folder named `pixels`, put `pixels.properties` in this folder, and compress
this folder into a zip file. Then, use the zip file to create the layer.

After creating the two layers containing the dependencies and configurations of Pixels, we create the
one Lambda functions for each operator (e.g., scan, broadcast join) in our serverless executor.
In AWS Lambda console, click `create function`, select `Author from scratch`, fill in the function name,
e.g., `ScanWorker`, runtime and architecture are the dependency layer, select or create a role that has
S3 read-write permissions.
In advanced settings, enable VPC and select the same VPC of our EC2 instances.

Then, upload `pixels-lambda-worker.jar` as the code source of the Lambda function.
In `Runtime settings`, select the handler class of the operator, e.g., `io.pixelsdb.pixels.lambda.worker.ScanWorker`.
In `Layers`, add the two layers created above.
In `Configuration->General configuration`, set timeout to 15 minutes and memory to 10240 MB.
In `Configuration->Environment variables`, create an environment variable named `PIXELS_HOME` with the value `/opt/pixels`.
`/opt/` is the path where the layers mounted on by default.

We create the other six workers in a similar way:
* `BroadcastChainJoinWorker` with the handler `io.pixelsdb.pixels.lambda.worker.BroadcastChainJoinWorker`;
* `AggregationWorker` with the handler `io.pixelsdb.pixels.lambda.worker.AggregationWorker`;
* `PartitionedJoinWorker` with the handler `io.pixelsdb.pixels.lambda.worker.PartitionedJoinWorker`;
* `PartitionWorker` with the handler `io.pixelsdb.pixels.lambda.worker.PartitionWorker`;
* `PartitionedChainJoinWorker` with the handler `io.pixelsdb.pixels.lambda.worker.PartitionedChainJoinWorker`;
* `BroadcastJoinWorker` with the handler `io.pixelsdb.pixels.lambda.worker.BroadcastJoinWorker`.

> Note: the names of the workers should be consistent with the worker names in `pixels.properties`.

Now, the deployment of `pixels-lambada-worker` is done. The deployed serverless workers can be used by `Pixels-Turbo` 
to execute unpredictable workload spikes.