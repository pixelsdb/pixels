# pixels-lambda-worker

`pixels-lambda-worker` is the serverless query accelerator running in AWS Lambda.

## Deployment

After [building Pixels](https://github.com/pixelsdb/pixels#build-pixels) using maven, we can find
two jar files under `pixels-turbo/pixels-lambda-worker/target/`:
* `pixels-lambda-worker-deps.jar`, this jar file contains the 3rd-party dependencies used by pixels-lambda-worker.
* `pixels-lambda-worker.jar`, this jar file contains the executor classes.

To deploy pixels-lambda-worker, create a path named `java/lib`, and put `pixels-lambda-worker-deps.jar` in this path.
Then, compress `pixels/lib` into a zip file, and create a `layer` in AWS Lambda for this zip file.
The java runtime can be either java-8 or java-11. The architecture can be either x86-64 or arm64.

Then, create another `layer` in AWS Lambda that contains the configuration file `pixels.properties` of Pixels.
A template of this file can be found in `pixels-common/main/resources/`.
See [here](https://github.com/pixelsdb/pixels#build-pixels) for more details of the properties in this file.
We can create a folder named `pixels`, put `pixels.properties` in this folder, and compress
this folder into a zip file. Then, use the zip file to create the layer.

After creating the two layers containing the configuration and dependencies of Pixels, we create the
one Lambda functions for each operator (e.g., scan, broadcast join) in our serverless executor.
In AWS Lambda console, click `create function`, select `Author from scratch`, fill in the function name,
e.g., `ScanWorker`, runtime and architecture are the same as above, select or create the role that has
S3 read-write permissions.
In advanced settings, enable VPC and select the same VPC of our EC2 instances.

Then, upload `pixels-lambda-worker.jar` as the code source of the Lambda function.
In `Runtime settings`, select the handler class of the operator, e.g., `io.pixelsdb.pixels.lambda.worker.ScanWorker`.
In `Layers`, add the two layers created above.
In `Configuration->General configuration`, set timeout to 15 minutes and memory to 10240 MB.
In `Configuration->Environment variables`, create an environment variable named `PIXELS_HOME` with the value `/opt/pixels`.
`/opt/` is the path where the layers mounted on by default.

In addition to the ScanWorker, we need to create other six operator functions:
* `BroadcastChainJoinWorker` with the handler `io.pixelsdb.pixels.lambda.worker.BroadcastChainJoinWorker`;
* `AggregationWorker` with the handler `io.pixelsdb.pixels.lambda.worker.AggregationWorker`;
* `PartitionedJoinWorker` with the handler `io.pixelsdb.pixels.lambda.worker.PartitionedJoinWorker`;
* `PartitionWorker` with the handler `io.pixelsdb.pixels.lambda.worker.PartitionWorker`;
* `PartitionedChainJoinWorker` with the handler `io.pixelsdb.pixels.lambda.worker.PartitionedChainJoinWorker`;
* `BroadcastJoinWorker` with the handler `io.pixelsdb.pixels.lambda.worker.BroadcastJoinWorker`.

> Note: the names of the operator functions should be consistent with the worker names in `pixels.properties`.

Now, the deployment of Pixels Lambda is done. This serverless executor can be used in the Pixels connector for Trino.
In Pixels connector for Presto, we can use ScanWorker in a similar way as Redshift Spectrum, but the other operators are not supported.