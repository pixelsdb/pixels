# TPC-H Evaluation

After the installation of [Pixels + Trino](INSTALL.md), we can evaluate the performance of Pixels using TPC-H.

## Prepare TPC-H

Attach a volume that is larger than the scale factor (e.g., 150GB for SF100) to the EC2 instance.
Mount the attached volume to a local path (e.g., `/data/tpch`).
Download tpch-dbgen to the instance, build it, and generate the dataset and queries into the attached volume.
Here, we put the dataset in `/data/tpch/100g/`.
The file(s) of each table are stored in a separate directory named by the table name.

## Create TPC-H Database
Log in trino-cli and use the SQL statements in `scripts/sql/tpch_schema.sql` to create the TPC-H database in Pixels.
In each `CREATE TABLE` statement, the table property `storage` defines the type of storage system used to store the
files in this table, whereas the table property `paths` defines the URIs of the paths in which the table files are stored.
Multiple URIs can be listed one after another, seperated by semicolon, in `paths`.
> Note that the URIs in `paths` can have no storage scheme or their storage scheme must be consistent with `storage`. 

Then, use `SHOW SCHEMAS` and `SHOW TABLES` statements to check if the tpch database has been
created successfully.

Create the container to store the tables in S3. The container name is the same as the hostname
(e.g., `pixels-tpch`) in the `paths` of each table.
Change the bucket name if it already exists.

During data loading, Pixels will automatically create the folders in the bucket to store the files in each table.

## Load Data

We use `pixels-cli` to load data into Pixels tables.

Under `PIXELS_HOME`, run pixels-cli:
```bash
java -jar ./sbin/pixels-cli-*-full.jar
```

Then use the following commands in pixels-cli to load data for the TPC-H tables:
```bash
LOAD -o file:///data/tpch/100g/customer -s tpch -t customer -n 319150 -r \| -c 1
LOAD -o file:///data/tpch/100g/lineitem -s tpch -t lineitem -n 600040 -r \| -c 1
LOAD -o file:///data/tpch/100g/nation -s tpch -t nation -n 100 -r \| -c 1
LOAD -o file:///data/tpch/100g/orders -s tpch -t orders -n 638300 -r \| -c 1
LOAD -o file:///data/tpch/100g/part -s tpch -t part -n 769240 -r \| -c 1
LOAD -o file:///data/tpch/100g/partsupp -s tpch -t partsupp -n 360370 -r \| -c 1
LOAD -o file:///data/tpch/100g/region -s tpch -t region -n 10 -r \| -c 1
LOAD -o file:///data/tpch/100g/supplier -s tpch -t supplier -n 333340 -r \| -c 1
```
It may take half an hour if data is loaded in a single thread. The last parameter `-c` of the `LOAD` command is the maximum number
of threads used for loading data. It only effects when the input directory (specified by `-o`)
contains multiple input files. In case that the TPC-H table has multiple parts, you can set
`-c` to the number of parts to improve the data loading performance.
Optionally, we can use `-e` to specify the encoding level (ranges 0 - 2) for data loading.
The default encoding level is 2, which is the highest encoding level that provides the best compression ratio.
Encoding level 0 disables encoding. It is also possible to use the `-l` parameter to load the data into other
paths instead of the table's paths defined in the `CREATE TABLE` DDL statement.

As we don't use pixels-cache for TPC-H, there is no need to load the cache.
Otherwise, we can load the cached table into pixels-cache using:
```bash
./sbin/load-cache.sh layout_version
```
`layout_version` is the version of the table's layout that specifies the columns we want to cache.

## Run Queries
Connect to trino-cli:
```bash
cd ~/opt/trino-server
./bin/trino --server localhost:8080 --catalog pixels --schema tpch
```
In trino-cli, select the ordered data layout by setting the two session properties:
```sql
set session pixels.ordered_path_enabled=true
set session pixels.compact_path_enabled=false
```
By default, both paths are enabled. You can also enable the compact path and disable the ordered path when [data compaction](#data-compaction) is done.
After selecting the data layout, execute the TPC-H queries in trino-cli.

If you are using trino JDBC, you can set these session properties in the JDBC [connection properties](https://trino.io/docs/405/client/jdbc.html#connection-parameters):
```java
properties.setProperty("sessionProperties", "pixels.ordered_path_enabled:true;pixels.compact_path_enabled:false");
```

## Data Compaction*
This is optional. It is only needed if we want to test the query performance on the compact layout.
In pixels-cli, use the following commands to compact the files in the ordered path of each table:
```bash
COMPACT -s tpch -t customer -n no -c 2
COMPACT -s tpch -t lineitem -n no -c 16
COMPACT -s tpch -t nation -n no -c 1
COMPACT -s tpch -t orders -n no -c 8
COMPACT -s tpch -t part -n no -c 1
COMPACT -s tpch -t partsupp -n no -c 8
COMPACT -s tpch -t region -n no -c 1
COMPACT -s tpch -t supplier -n no -c 1
```
The last parameter `-c` of `COMPACT` command is the maximum number of threads used for data compaction.
For large tables such as `lineitem`, you can increase `-c` to improve the compaction performance.
Compaction does not re-encode data, hence it should be much faster than loading.

> `compact.factor` in `$PIXELS_HOME/pixels.properties` determines how many row groups are compacted into a single
> file. The default value is 32, which is appropriate in most conditions. An experimental evaluation of the effects
> of compact factor on AWS S3 can be found in our [ICDE'22](https://ieeexplore.ieee.org/document/9835615) paper.

To avoid scanning the small files in the ordered path during query execution, disable the ordered path and enable the compact path before executing queries.

## Statistics Collection*
This is optional. Data statistics enable cost-based query optimization for the queries.
Start Pixels and Trino, make sure that Trino can execute queries on `tpch` schema and `presto.jdbc.url`
in `$PIXELS_HOME/properties` points to the JDBC endpoint of your Trino instance.

In pixels-cli, use the `STAT` command to collect the data statistics for each table.
```bash
STAT -s tpch -t nation
STAT -s tpch -t region
STAT -s tpch -t supplier
STAT -s tpch -t customer
STAT -s tpch -t part
STAT -s tpch -t partsupp
STAT -s tpch -t orders
STAT -s tpch -t lineitem
```
Note that `STAT` command issues queries to Trino to collect some statistics. Set the following two properties in `$PIXELS_HOME/pixels.properties` as needed before executing this command:
```properties
executor.ordered.layout.enabled=false
executor.compact.layout.enabled=true
```
By setting `executor.compact.layout.enabled=true`, the compact layout is used for the statistic collection.

When it is finished successfully, set `splits.index.type=cost_based` and restart Trino to benefit from cost-based data splitting (determining the number of tasks to scan a base table).
