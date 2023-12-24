# Pixels Amphi

`Pixels-Amphi` is the query coordinator in Pixels that aims at improving flexibility and cost-efficiency of
query processing.
It serves as an adaptive framework to leverage the on-premises resources and allocate 
compute workload more cost-efficiently.

## Components

Currently, `Pixels-Amphi` uses Trino as the query entry-point and the query processor in the cloud,
and use DuckDB as the engine for the local worker. The server-client communication is implemented with gRPC.
`Pixels-Amphi` is composed of the following two components:
- [`pixels-amphi`](./pixels-amphi) works inside the `pixels-server` to provide coordinating decision. 
Coordinator analyzes the SQL query and decides if the worker has cached the data required to execute the query.
- [`pixels-amphi-worker`](../cpp/pixels-amphi-worker) is an independent client process to handle with the OLAP workload.
Given the workload, the worker will download partial columns data in the storage first. 
Then it submits the query to pixels server endpoint and processes the coordinator response.

## Usage

On the server side, install `Pixels + Trino` following the instructions [HERE](../docs/INSTALL.md).

On the worker/client side, build `pixels-amphi-worker` from source or build a Docker container. 
See the instructions [HERE](../cpp/pixels-amphi-worker/README.md).

### 1. Custom schema

`pixels-amphi` uses `Apache Avro` to specify the schema and uses `avro-parquet` as the parquet file writer.
Before downloading the partial data into the local storage, make sure that the schema has been specified in the
[schema folder](./pixels-amphi/src/main/resources/schema).

First create a folder with the schema name (e.g., `tpch`, `clickbench`), then create the avsc files for each table.
Remember to register the path in `schemas.txt` so that `PartialSchemaLoader` is aware of them.

Note that avro schema does not have built-in type for some SQL type like `decimal`, `date`, `timestamp`.
They have to be converted to other types and claimed as `logicalType` in avsc files. For example:

````
// decimal type
    {
      "name": "l_tax",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 15,
        "scale": 2
      }
    }
    
// date type
    {
      "name": "l_shipdate",
      "type": {
        "type": "int",
        "logicalType": "date"
      }
    }
    
// timestamp type
    {
      "name": "EventTime",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      }
    }
````

### 2. Experiment configuration

`Pixels-amphi` provides different strategies to generate the column-level cache plan for the worker.
Given the metadata of the database and workload, the algorithm will decide which columns to cache in the local storage.

First, we need a configuration file to specify the relevant information. We provide the example for the benchmarks: [TPC-H](../cpp/pixels-amphi-worker/benchmark/config/tpch.yaml):

```yaml
## Experiment level settings
exp_name: tpch_template
schema_name: tpch
benchmark_path: /home/ubuntu/opt/pixels/cpp/pixels-amphi-worker/benchmark/
init_sql_path: sql/tpch.sql
cache_data_path: /home/ubuntu/data/tpch/

## Parameters for cache plan generation (*path relative to benchmark*)
schema_path: schema/tpch.json
table_stat_path: schema/tpch_stat.json
workload_path: workload/tpch.txt
cache_plan_path: results/tpch_plan.json
workload_cost_path: stats/tpch_cost.json

# Strategy options
strategy: cost_optimal_columns
storage_restriction: 1541191900
```

Here is the explanation of all the parameters:
- `exp_name`: the name of the experiment, will be used as the prefix of the output files
- `schema_name`: the name of the schema, should be the same as specified in the schema folder (as mentioned above in [Custom schema](#1-custom-schema))
- `benchmark_path`: the absolute path to the benchmark folder
- `init_sql_path`: the path to the SQL file that contains the initialization queries in the local engine (i.e., DuckDB)
- `cache_data_path`: the absolute path to the local cache storage, so that the worker engine can query the data from the local storage
- `schema_path`: the path to the schema file, json file with table name and relevant columns (e.g., [TPC-H](../cpp/pixels-amphi-worker/benchmark/schema/tpch.json))
- `table_stat_path`: the path to the table statistics file, containing all the column size in bytes (e.g., [TPC-H](../cpp/pixels-amphi-worker/benchmark/schema/tpch_stat.json))
- `workload_path`: the path to the workload file, containing all the queries to be executed, one SQL query per line (e.g., [TPC-H](../cpp/pixels-amphi-worker/benchmark/workload/tpch.txt))
- `cache_plan_path`: the path to the cache plan file, containing the column-level cache plan (e.g., [TPC-H](../cpp/pixels-amphi-worker/benchmark/results/tpch_plan.json))
- `workload_cost_path`: the path to the workload cost file, containing the estimated cost of each query (e.g., [TPC-H](../cpp/pixels-amphi-worker/benchmark/stats/tpch_cost.json))
- `strategy`: the strategy to generate the cache plan, currently we support `most_number_columns`, `most_frequent_columns`, `rate_greedy_columns`, `most_coverage_columns`, and `cost_optimal_columns` (see [Generate cache plan](#3-generate-cache-plan) for more details)
- `storage_restriction`: the storage restriction in bytes, the algorithm will ensure that the total size of the cached columns will not exceed this restriction (e.g., 1541191900 means that the storage restriction is ~1.5GB)

### 3. Generate cache plan

Given the configuration file, we aim to cache a selected set of columns in the local storage to optimize the overall cost. The Python [script](../cpp/pixels-amphi-worker/benchmark/scripts/cache_algorithm.py) will generate the plan and store it in `cache_plan_path`, with specified strategy and storage restriction. The options of the strategy are as follows:
- `most_number_columns`: cache the columns with the most number of columns
- `most_frequent_columns`: cache the columns with the most frequent use in the workload
- `rate_greedy_columns`: cache the columns with the highest ratio of frequency versus column size
- `most_coverage_columns`: cache the columns with the most coverage of the workload
- `cost_optimal_columns`: cache the columns with the lowest estimated cost (specified in `workload_cost_path`)

The later two strategies, `most_coverage_columns` and `cost_optimal_columns`, are driven by integer programming, as we can easily formulate the caching problem as an [0-1 knapsack problem](https://en.wikipedia.org/wiki/Knapsack_problem). It means that, if we assume that the estimates of workload cost are precise enough, the algorithm will generate the optimal cache plan.

To run the script, simply execute the following command:
```bash
python cache_algorithm.py --config <config_file_path>
```
where the `config_file_path` is the path to the configuration file (see [Experiment configuration](#2-experiment-configuration)).

### 4. Download cached data

Based on the generated cache plan, the worker will download the partial data from the cloud storage to the local storage. The cloud data is of pixels format and the local data is of parquet format. Our worker query engine DuckDB can efficiently query the parquet data (see [DuckDB Parquet Loading](https://duckdb.org/docs/data/parquet/overview.html)).

After building the pixels project with `mvn clean install`, you will find `pixels-amphi-*.jar` in the target folder
of [`pixels-amphi`](./pixels-amphi). It accepts the configuration file as the command parameter.
Refer to the [example](./pixels-amphi/src/test/resources/config/tpch.json) in the test folder.
The `SchemaData` field comes from the cache plan generated in the previous step.

To start the download process, simply run the following command:
```bash
java -jar pixels-amphi-*.jar --config <config_file_path>
```

Please note that depended on the network bandwidth, the download process may take a while. To quickly test the system, you can also directly download the full dataset and skip this step. The system will still work as if only the cached columns are stored in the local storage.

### 5. Perform the task

After the preparations in the previous steps, now we have:
- the local storage with the cached column data
- pixels server running in the cloud
- pixels amphi worker to perform adaptive query processing

We have add the executable to run the benchmark task ([benchmark source code](../cpp/pixels-amphi-worker/benchmark/benchmark.cpp)). We can now perform the task by running: 
```bash
./benchmark <worker_config_path> <experiment_config_path>
```
where the `worker_config_path` is the path to the [amphi worker configuration file](../cpp/pixels-amphi-worker/config.yaml), and `experiment_config_path` is the configuration mentioned in [Experiment configuration](#2-experiment-configuration). The results of the task will be logged in the `cpp/pixels-amphi-worker/benchmark/results` folder.