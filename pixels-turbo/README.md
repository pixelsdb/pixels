# pixels-turbo

`Pixels-Turbo` is the query engine in Pixels that aims at improving the elasticity, performance, and cost-efficiency of
query processing. 
It prioritizes query processing in an auto-scaling MPP cluster running in spot VMs,
and automatically invokes cloud functions to process unpredictable workload spikes when the spot VMs do not have
enough resources.

## Components
Currently, `Pixels-Turbo` uses Trino as the query entry-point and the query processor in the MPP cluster, 
and implements the serverless query accelerator from scratch.
`Pixels-Turbo` is composed of the following components:
- `pixels-trino`([link](https://github.com/pixelsdb/pixels-trino)). In addition to reading data and metadata, 
this component automatically pushes down query operators in the Trino query plan into a logical sub-plan when the Trino cluster
does not have enough resources to process the query.
- `pixels-planner` accepts the logical sub-plan and compiles it into a physical sub-plan. During the planning, pixels-planner
selects the physical query operators and the parameters of the physical query operators. The root node of the 
physical plan provides the methods to invoke the serverless workers to execute the plan.
- `pixels-[platform]-invoker` implements the invokers to be used by the physical plan to invoker the serverless workers in a 
specific serverless computing platform (e.g., AWS Lambda).
- `pixels-[platform]-worker` implements the serverless workers in a specific serverless computing platform (e.g., AWS Lambda).
- `pixels-executor` implements the basic executors of relational operations, such as scan, filter, join, and aggregation.
These executors are used in the serverless workers to execute the assigned physical operator.
- `pixels-[platform]-scaling` implements the auto-scaling metrics collector for the specific cloud platform (e.g., AWS). 
It reports the performance metrics, such as CPU/memory usage and query concurrency, in the MPP cluster. These metrics are 
consumed by the scaling manager (e.g., AWS EC Autoscaling) in the cloud platform to make scaling decisions.

## Usage

Deploy `pixels-trino` following the instructions [here](https://github.com/pixelsdb/pixels#installation-in-aws).
Then, deploy the serverless workers following the instructions in the README.md of `pixels-[platform]-worker`.
Currently, we only support AWS Lambda. So `pixels-lambda-worker` is the only option. More platform integrations
will be provided in the future.

After that, start Trino and run queries. Pixels will automatically push the queries into the serverless workers when Trino 
is too busy to process the new coming queries.
