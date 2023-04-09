# pixels-turbo

`Pixels-Turbo` is a query engine that exploits serverless computing resources (e.g, AWS Lambda) to accelerate the processing of workload spikes.
It prioritizes query processing in an auto-scaling MPP cluster (currently based on Trino) running in spot VMs,
and automatically invokes cloud functions to process workload spikes when the spot VMs could not scale out in time.

Currently, we support deploying `Pixels-Turbo` in AWS, exploiting EC2 spot and Lambda.
Integrations with other platforms might be developed in the future.
