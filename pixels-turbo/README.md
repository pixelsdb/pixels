# Pixels Turbo

`Pixels-Turbo` is the query engine in Pixels that aims at improving the elasticity, performance, and cost-efficiency of
query processing. 
It prioritizes query processing in an auto-scaling MPP cluster running in spot VMs,
and automatically invokes cloud functions to process unpredictable workload spikes when the spot VMs do not have
enough resources.

## Components
Currently, `Pixels-Turbo` uses Trino as the query entry-point and the query processor in the MPP cluster, 
and implements the serverless query accelerator from scratch.
`Pixels-Turbo` is composed of the following components:
- [`pixels-trino`](https://github.com/pixelsdb/pixels-trino). In addition to reading data and metadata, 
this component automatically pushes down query operators in the Trino query plan into a logical sub-plan when the Trino cluster
does not have enough resources to process the query.
- [`pixels-planner`](../pixels-planner) accepts the logical sub-plan and compiles it into a physical sub-plan. During the planning, pixels-planner
selects the physical query operators and the parameters of the physical query operators. The root node of the 
physical plan provides the methods to invoke the serverless workers to execute the plan.
- `pixels-invoker-[service]` implements the invokers to be used by the physical plan to invoker the serverless workers in a 
specific cloud function service (e.g., AWS Lambda).
- `pixels-worker-[service]` implements the serverless workers in a specific cloud function service (e.g., AWS Lambda).
- `pixels-worker-common` has the common logics and the base implementations of the serverless workers.
- [`pixels-executor`](../pixels-executor) implements the basic executors of relational operations, such as projection, filter, join, and aggregation.
These executors are used in the serverless workers to execute the assigned physical operator.
- `pixels-scaling-[service]` implements the auto-scaling metrics collector for the specific virtual machine service (e.g., AWS EC2). 
It reports the performance metrics, such as CPU/memory usage and query concurrency, in the MPP cluster. These metrics are 
consumed by the scaling manager (e.g., AWS EC2 Autoscaling Group) in the cloud platform to make scaling decisions.

## Installation

Install `Pixels-Trino` following the instructions [HERE](../docs/INSTALL.md).

To use Pixels-Turbo, we need to set the following properties in `PIXELS_HOME/pixels.properties`:
```properties
executor.input.storage.scheme=s3
executor.intermediate.storage.scheme=s3
executor.intermediate.folder=/pixels-turbo/intermediate/
executor.output.storage.scheme=output-storage-scheme-dummy
executor.output.folder=output-folder-dummy
```
These storage schemes are used to access the input data (the storage scheme of the base tables defined by
`CREATE TABLE` statements), the intermediate data (intermediate results generated during query execution), and the
output data (the result of the sub-plan executed in the serverless workers), respectively.
If any of these storage schemes is `minio` or `redis`, we also need to set the properties for the corresponding storage:
```properties
minio.region=eu-central-2
minio.endpoint=http://localhost:9000
minio.access.key=minio-access-key-dummy
minio.secret.key=minio-secret-key-dummy
redis.endpoint=localhost:6379
redis.access.key=redis-user-dummy
redis.secret.key=redis-password-dummy
```
Ensure they are valid so that the serverless workers can access the corresponding storage systems.
Especially, the `executor.input.storage.scheme` must be consistent with the storage scheme of the base
tables. This is checked during query-planning for Pixels-Turbo.
In addition, the `executor.intermediate.folder` and `executor.output.folder` are the base path where the intermediate
and output data are stored. They also need to be valid and accessible for the serverless workers.

Then, deploy the serverless workers following the instructions in the README.md of `pixels-worker-[service]`.
Currently, we support AWS Lambda and vHive. So the worker service can be `pixels-worker-lambda` or `pixels-worker-vhive`.
More platform integrations will be provided in the future.

If the MPP cluster is running in a cloud machine service such as AWS EC2, follow the instructions in the README.md of `pixels-scakubg-[service]`
to deploy the auto-scaling manager for the MPP cluster. Currently, we support auto-scaling in AWS EC2. So `pixels-scaling-ec2` is the only option.
It can be used as an example to implement the auto-scaling manager for other cloud platforms.

## ## Start Pixels (with Turbo)

In `etc/catalogpixels.properties` under the installation directory of Trino, set `cloud.function.switch` to `auto` if you have installed the auto-scaling manager and 
want to enable auto-scaling of MPP cluster and adaptive invocation of serverless workers; or set it to `on` if you want to always push the queries into serverless workers.
```properties
# serverless config
# it can be on, off, auto
cloud.function.switch=auto
local.scan.concurrency=0
clean.intermediate.result=true
```
`local.scan.concurrency` is the number of concurrent scan tasks that can be executed locally (not in the serverless workers) in the MPP cluster.
It only takes effects for the table scan operator when the query is pushed down into serverless workers.
By setting this number to a value larger than 0, the MPP worker and the serverless workers can work in parallel to improve table scan throughput.
`clean.intermediate.result` controls whether delete the intermediate results when the queries executed in serverless workers are finished.

After that, start Trino and run queries. Pixels will automatically push the queries into the serverless workers when Trino 
is too busy to process the new coming queries (in `auto` mode).
