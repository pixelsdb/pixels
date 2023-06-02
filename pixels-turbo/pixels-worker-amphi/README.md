# Pixels-Worker-Amphi

`pixels-worker-amphi` is the on-premises worker in `pixels-amphi` that aims at submitting queries to 
`pixels-server` and receiving the coordinator decision. According to the trade-offs in computation cost, 
the coordinator either execute the query on the cloud and send back the results, or synchronize the 
necessary data and execute the query locally.

## Basic Functionalities

- Communicate with `pixels-server` on cloud side with gRPC. This project shares proto files with Pixels project.
See `../../protos` for the proto definitions.
- Send SQL queries to the dialect transpilation service, and decide corresponding queries for both the 
in-cloud query engine (Trino) and on-premise query engine (DuckDB).
- Submit the transpiled query to the coordinator service, to receive either the 
exact query result from cloud compute, or the URL to download the required data.
- Based on the feedback of coordinator, the worker maintains the cache of local data. Afterwards, 
it synchronizes the catalog with the coordinator.
- The worker runs DuckDB engine to consume the local data and execute the query.

## Dependencies

- `gRPC-1.54.0` and `protobuf` for communication with `pixels-server`
- `DuckDB-0.8.0` for on-premise in-process database
- `googletest-1.13.0` for unit testing
- `spdlog-1.11.0` for logging
- `yaml-cpp-0.7.0` for configuration
- `cli-2.0` for interactive cli

## Build with Docker

`Dockerfile` includes the configuration to run the worker in a docker container:
```
>> docker build -t worker .
... (Installing dependencies and creating docker image)

>> docker run -it worker
... (Running application in container)

root@aebf1e57cbe0:/pixels-worker-amphi# >> make
... (Building the CMake project)

worker-cli > 
```

TODO: update Dockerfile and specify dependency version

## CLI Usage

The system is designed with a user-friendly interactive command line interface,
where the user can view the metadata and execute queries.

TODO: GUI for visualizing the system status (both local and cloud)

## Test and benchmark

In the current stage, we are still focusing on the separate functionality
and test them with unit tests. After the project built, find the executable
`build/tests/unit_tests` to run all unit tests.

TODO: finish all functionalities and **set up the benchmark**