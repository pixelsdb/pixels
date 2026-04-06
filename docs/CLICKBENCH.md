# ClickBench Evaluation

After the installation of [Pixels + Trino](INSTALL.md), we can evaluate the performance of Pixels using ClickBench.

## Prepare ClickBench

[Download](https://github.com/ClickHouse/ClickBench?tab=readme-ov-file#data-loading) 
and decompress ClickBench data file (e.g., tsv format) into a local path (e.g., `/data/clickbench/hits/`) with at least 100GB free space.
It is recommended to split the data file into multiple splits so that Pixels can load them in parallel.
The number of splits can be n times of the number of parallel loading tasks.
For example, if you want to load the data using 40 cores (threads), you can download and split the data file link this:
```bash
mkdir -p /data/clickbench/hits/
cd /data/clickbench/
wget https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz
gunzip hits.tsv.gz
split -l 2500000 hits.tsv ./hits/hits-tsv-
```
There are 100 million rows in ClickBench data file, so we have each split contains 2.5 million rows.

## Create ClickBench Database
Log in trino-cli and use the SQL statements in `scripts/sql/clickbench_schema.sql` to create the ClickBench database in Pixels.
The usage of the `storage` and `paths` table properties in the `CREATE TABLE` statement is same as in [TPC-H](TPC-H.md#create-tpc-h-database).

Create the container to store the `hits` table in S3. The container name is the same as the hostname
(e.g., `pixels-clickbench`) in the `paths` property of the table.

During data loading, Pixels will automatically create the folders in the bucket to store the table's files.

## Load Data

Same as for [TPC-H Load Data](TPC-H.md#load-data), run pixels-cli in `PIXELS_HOME`:
```bash
java -jar ./sbin/pixels-cli-*-full.jar
```

Then use the following commands in pixels-cli to load data into table `hits`:
```bash
LOAD -o file:///data/clickbench/hits -s clickbench -t hits -n 156250 -r \t -c 40
```
Parameter `-n` is the number of lines in each row-group, and parameter`-c` is the maximum number of threads used for loading data.
See [TPC-H Load Data](TPC-H.md#load-data) for the usage of other parameters.
It may take a few minutes to load the data using 40 threads (and 40 CPU cores).

Refer to [Import Existing Files](TPC-H.md#import-existing-files) if the table was previously loaded by a legacy pixels-cli.

## Run Queries
Connect to trino-cli:
```bash
cd ~/opt/trino-server
./bin/trino --server localhost:8080 --catalog pixels --schema clickbench
```
Same as for [TPC-H Run Queries](TPC-H.md#run-queries), select the ordered data layout by setting the two session properties in trino-cli:
```sql
set session pixels.ordered_path_enabled=true
set session pixels.compact_path_enabled=false
```
After selecting the data layout, execute the ClickBench queries in trino-cli.

## Data Compaction*
This is optional. It is only needed if we want to test the query performance on the compact layout.
In pixels-cli, use the following commands to compact the files in the ordered path of the table:
```bash
COMPACT -s clickbench -t hits -n no -c 40
```
The last parameter `-c` of `COMPACT` command is the maximum number of threads used for data compaction.
This may take a few seconds to a minute, depending on the write bandwidth of S3 on your EC2 VM.

> `compact.factor` in `PIXELS_HOME/etc/pixels.properties` determines how many row groups are compacted into a single
> file. The default value is 32, which is appropriate in most conditions. An experimental evaluation of the effects
> of compact factor on AWS S3 can be found in our [ICDE'22](https://ieeexplore.ieee.org/document/9835615) paper.

To avoid scanning the small files in the ordered path during query execution, disable the ordered path and enable the compact path before executing queries.

## Statistics Collection*
This is optional. Data statistics enable cost-based query optimization for the queries.
Start Pixels and Trino, make sure that Trino can execute queries on `clickbench` schema and `presto.jdbc.url`
in `PIXELS_HOME/etc/pixels.properties` points to the JDBC endpoint of your Trino instance.

In pixels-cli, use the following commands to collect the data statistics for the columns in the table.
```bash
STAT -s clickbench -t hits
```
Note that `STAT` command issues queries to Trino to collect some statistics. Set the following two properties in `PIXELS_HOME/etc/pixels.properties` as needed before executing this command:
```properties
executor.ordered.layout.enabled=false
executor.compact.layout.enabled=true
```
By setting `executor.compact.layout.enabled=true`, the compact layout is used for the statistic collection.

When it is finished successfully, set `splits.index.type=cost_based` and restart Trino to benefit from cost-based data splitting (determining the number of tasks to scan a base table).
