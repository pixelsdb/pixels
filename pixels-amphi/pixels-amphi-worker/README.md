# Pixels-Worker-Amphi

`pixels-worker-amphi` is the on-premises worker in `pixels-amphi` that aims at submitting queries to 
`pixels-server` and receiving the coordinator decision. According to the trade-offs in computation cost, 
the coordinator either execute the query on the cloud and send back the results, or execute the query 
locally with cached columnar data.

## Basic Functionalities

- Communicate with `pixels-server` on cloud side with gRPC. This project shares proto files with Pixels project.
See `protos` in pixels root directory for the definitions.
- Send SQL queries to the dialect transpilation service, and decide corresponding queries for both the 
in-cloud query engine (Trino) and on-premise query engine (DuckDB).
- Submit the transpiled query to the coordinator service, to receive either the 
exact query result from cloud compute, or instructed to perform on-premises execution.
- With the Columnar Cache Planner implemented in `benchmark/scripts/cache_algorithm.py`
and `pixels-amphi/downloader`, the worker can produce a cache plan and download Pixels data as parquet files.
- The worker runs DuckDB engine to create views on parquet files and perform the corresponding query.

## Dependencies

- `gRPC-1.54.0` and `protobuf` for communication with `pixels-server`
- `DuckDB-0.8.0` for on-premise in-process database
- `googletest-1.13.0` for unit testing
- `spdlog-1.11.0` for logging
- `yaml-cpp-0.7.0` for configuration
- `cli-2.0` for interactive cli
- `nlohmann_json-3.11.2` for parsing json files

## Quick start
```
>> make clean
>> make
... (Compiling the worker project and starting the cli)

>> cd build/tests
>> ./unit_tests
... (Performing unit tests)

>> cd build/benchmark
>> ./benchmark /path_to_worker_config /path_to_experiment_config
... (Running the benchmark experiment)
```

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