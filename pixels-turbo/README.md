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

## Usage

Install `Pixels + Trino` following the instructions [HERE](../docs/INSTALL.md).
Then, deploy the serverless workers following the instructions in the README.md of `pixels-worker-[service]`.
Currently, we support AWS Lambda and vHive. So the worker service can be `pixels-worker-lambda` or `pixels-worker-vhive`.
More platform integrations will be provided in the future.

After that, start Trino and run queries. Pixels will automatically push the queries into the serverless workers when Trino 
is too busy to process the new coming queries.
