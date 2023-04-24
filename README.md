Pixels
=======

The core of Pixels is a columnar storage engine designed for data lakes and warehouses.
It is optimized for analytical tables stored in on-premises and cloud-native storage systems,
including S3, GCS, HDFS, Redis, and local file systems.
Pixels outperforms Parquet, which is the most widely used columnar format in today's lakehouses, by up to two orders of magnitude.

We have integrated Pixels with popular query engines including Trino (405), Presto (0.279), and Hive (2.3+). These
integrations are opensourced in separate repositories:
* [Pixels Connector for Trino](https://github.com/pixelsdb/pixels-trino)
* [Pixels Connector for Presto](https://github.com/pixelsdb/pixels-presto)
* [Pixels SerDe for Hive](https://github.com/pixelsdb/pixels-hive)

Pixels also has its own query engine [Pixels-Turbo](https://github.com/pixelsdb/pixels/tree/master/pixels-turbo).
It processes sustained queries in an autoscaling MPP cluster (currently based on Trino) and exploits serverless functions (e.g, AWS Lambda) to accelerate the processing of workload spikes.
With `Pixels-Turbo`, we can achieve better performance and cost-efficiency for sustained workloads, while not compromising elasticity
for workload spikes.


## Publications

Pixels is an academic system aims at providing production-grade quality. It supports all the functionalities required by TPC-H and
is compatible with the mainstream data analytic ecosystems.
The key ideas and insights in Pixels are elaborated in the following publications.

> `SIGMOD'23` Using Cloud Function as Accelerator for Elastic Data Analytics\
> Haoqiong Bian, Tiannan Sha, Anastasia Ailamaki

> `EDBT'22` Columnar Storage Optimization and Caching for Data Lakes (short) [[Cite]](https://dblp.org/rec/conf/edbt/JinBC022.html?view=bibtex)\
> Guodong Jin, Haoqiong Bian, Yueguo Chen, Xiaoyong Du

> `ICDE'22` Pixels: An Efficient Column Store for Cloud Data Lakes [[Cite]](https://dblp.org/rec/conf/icde/BianA22.html?view=bibtex)\
> Haoqiong Bian, Anastasia Ailamaki

> `CIDR'20` Pixels: Multiversion Wide Table Store for Data Lakes (abstract) [[Cite]](https://dblp.org/rec/conf/cidr/Bian20.html?view=bibtex)\
> Haoqiong Bian

> `ICDE'18` Rainbow: Adaptive Layout Optimization for Wide Tables (demo) [[Cite]](https://dblp.org/rec/conf/icde/BianTJCQD18.html?view=bibtex)\
> Haoqiong Bian, Youxian Tao, Guodong Jin, Yueguo Chen, Xiongpai Qin, Xiaoyong Du

> `SIGMOD'17` Wide Table Layout Optimization by Column Ordering and Duplication [[Cite]](https://dblp.org/rec/conf/sigmod/BianYTCCDM17.html?view=bibtex)\
> Haoqiong Bian, Ying Yan, Wenbo Tao, Liang Jeff Chen, Yueguo Chen, Xiaoyong Du, Thomas Moscibroda


## Build Pixels

Install JDK 17.0.3 or above, and clone the Pixels codebase into any `SRC_BASE` directory:
```bash
git clone https://github.com/pixelsdb/pixels.git
```
Enter `SRC_BASE/pixels`, use `mvn install` to build and install it to the local Maven repository.
> JDK 17.0.3+ is required by Trino. To run Pixels in Presto or other query engines, please build Pixels and the corresponding connector
> using the Java version required by the query engine. Pixels by itself is compatible with Java 8 or above.

It may take a couple of minutes to complete. After that, find `pixels-daemon-*-full.jar` in `pixels-daemon/target`, which is the jar to run Pixels daemons.
Also find `pixels-cli-*-full.jar` in `pixels-cli/target`, which is the jar of Pixels command line tool.
They will be used in the installation progress.

Pixels is compatible with different query engines, such as Trino, Presto, and Hive.
However, for simplicity, we use Trino as an example to illustrate how Pixels works with query engines in the data lakes.

To use Pixels in Trino, download [pixels-trino](https://github.com/pixelsdb/pixels-trino), 
and build it using `mvn package`.

Find the following zip files in the build target directories:
* `pixels-trino-listener-*.zip`, this is the event listener plugin for Trino.
* `pixels-trino-connector-*.zip`, this is the connector for Trino.

They will be used in the installation.

> If you want to run the unit tests or the main classes in Intellij for debugging purpose, set the `PIXELS_HOME` environment
> variable for `Junit` or `Application` in `Run` -> `Edit Configurations` -> `Edit Configuration Templetes`.
> Ensure that the `PIXELS_HOME` directory exists and follow the instructions in [Install Pixels](#Install-Pixels) to put
> the `pixels.properties` into `PIXELS_HOME` and create the `logs` directory where the log files will be
> written.


## Development Environment*

If you want to develop Pixels in Intellij, open `SRC_BASE/pixels` as a maven project.
When the project is fully indexed and the dependencies are successfully downloaded,
add a `PIXELS_HOME` environment variable in *Run -> Edit Configurations -> Edit configuration templates*,
point it to your Pixels home that contains the configuration file (i.e., `pixels.properties`) and stores the runtime logs.

Now, in Intellij, you can build Pixels using the maven plugin, run and debug unit tests, and debug Pixels by
setting up a *Remote JVM Debug*.


## Installation in AWS

Create an EC2 Ubuntu-20.04 or 22.04 instance with x86 arch and at least 20GB root volume. 8GB or larger memory is recommended. Login the instance as `ubuntu` user, 
and install the following components.

### Install JDK
Install JDK 17.0 in the EC2 instance:
```bash
sudo apt install openjdk-17-jdk openjdk-17-jre
```
Check the java version:
```bash
java -version
```
If the other version of JDK is in use, switch to JDK 17:
```bash
update-java-alternatives --list
sudo update-java-alternatives --set /path/to/jdk-17.0
```
Oracle JDK 17.0, Azul Zulu JDK 17, or GraalVM 22 for Java 17 also works.

### Setup AWS Credentials
If we use S3 as the underlying storage system, we have to configure the AWS credentials.

Currently, we do not configure the `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_DEFAULT_REGION` from Pixels.
Therefore, we have to configure these credentials using 
[environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html#envvars-set) or 
[credential files](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

### Install Pixels

To build Pixels from source and install it locally, you can simply run:

```bash 
# You may also want to append this line into `~/.bashrc`
export PIXELS_HOME=$HOME/opt/pixels/
./install.sh
```

But you still need to:
- Put the jdbc connector of MySQL into `PIXELS_HOME/lib`.
- Modify `pixels.properties` to ensure the following properties are valid:
```properties
pixels.var.dir=/home/pixels/opt/pixels/var/
metadata.db.driver=com.mysql.jdbc.Driver
metadata.db.user=pixels
metadata.db.password=password
metadata.db.url=jdbc:mysql://localhost:3306/pixels_metadata?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull
metadata.server.port=18888
metadata.server.host=localhost
trans.server.port=18889
trans.server.host=localhost
etcd.hosts=localhost
etcd.port=2379
metrics.node.text.dir=/home/pixels/opt/node_exporter/text/
presto.pixels.jdbc.url=jdbc:trino://localhost:8080/pixels/tpch
hdfs.config.dir=/opt/hadoop-2.7.3/etc/hadoop/
```
The hostnames, ports, paths, usernames, and passwords in these properties are to be configured in the following steps of installation.

> **Note:** optionally, you can also set the `PIXEL_CONFIG` system environment variable
> to specify a different location of `pixels.properties`. This can be a http or https URL
> to a remote location.

Optionally, to install it step-by-step, please see the guidance below.

### Install Step-by-Step*

Here, we install Pixels and other binary packages into the `~/opt` directory:
```bash
mkdir ~/opt
```

Create the home `~/opt/pixels` for Pixels:
```bash
cd ~/opt
mkdir pixels
```
Append this line into `~/.bashrc` and source it:
```bash
export PIXELS_HOME=$HOME/opt/pixels/
```
Create the following directories in `PIXELS_HOME`:
```bash
cd $PIXELS_HOME
mkdir bin
mkdir lib
mkdir listener
mkdir logs
mkdir sbin
mkdir var
```
Put the sh scripts in `scripts/bin` and `scripts/sbin` into `PIXELS_HOME/bin` and `PIXELS_HOME/sbin` respectively.
Put `pixels-daemon-*-full.jar` into `PIXELS_HOME`.
Put `pixels-cli-*-full.jar` into `PIXELS_HOME/sbin`
Put the jdbc connector of MySQL into `PIXELS_HOME/lib`.
Put `pixels-common/src/main/resources/pixels.properties` into `PIXELS_HOME`.
Modify `pixels.properties` to ensure that the URLs, ports, paths, usernames, and passwords are valid.
Leave the other config parameters as default.

Optionally, to use the columnar cache in Pixels (i.e., pixels-cache), create and mount an in-memory file system:
```bash
sudo mkdir -p /mnt/ramfs
sudo mount -t tmpfs -o size=1g tmpfs /mnt/ramfs
```
The path `/mnt/ramfs` is determined by `cache.location` and `index.location` in `PIXELS_HOME/pixels.properties`.
The `size` parameter of the mount command should be larger than or equal to the sum of `cache.size` and `index.size` in 
`PIXELS_HOME/pixels.properties`. And it must be smaller than the physical memory size.
Set the schema name and table name of the cached table using `cache.schema` and `cache.table`, respectively.
Set `cache.storage.scheme` to be the name of the storage system where the cached table is stored.
**Finally**, set `cache.enabled` and `cache.read.direct` to `true` in `PIXELS_HOME/pixels.properties`.

Set `cache.enabled` to `false` if you don't use pixels-cache.

### Install MySQL
MySQL and etcd are used to store the metadata and states of Pixels. To install MySQL:
```bash
sudo apt update
sudo apt install mysql-server
sudo mysql_secure_installation
```
Login MySQL and create a user and a metadata database for Pixels:
```mysql
CREATE USER 'pixels'@'%' IDENTIFIED BY 'password';
CREATE DATABASE pixels_metadata;
GRANT ALL PRIVILEGES ON pixels_metadata.* to 'pixels'@'%';
FLUSH PRIVILEGES;
```
Ensure that MySQL server can be accessed remotely. Sometimes the default MySQL configuration
binds the server to localhost thus declines remote connections.

Use `scrpts/sql/metadata_schema.sql` to create tables in `pixels_metadata`.

### Install etcd

First install go-lang:
```bash
sudo apt install golang
```

Copy `scripts/tars/etcd-v3.3.4-linux-amd64.tar.xz` into `~/opt` and decompress it.
Append the following lines into `~/.bashrc` and source it:
```bash
export ETCDCTL_API=3
export ETCD=$HOME/opt/etcd-v3.3.4-linux-amd64-bin
export PATH=$PATH:$ETCD
```
All the following commands are executed under `~/opt` by default.
Create the link and start etcd:
```bash
ln -s etcd-v3.3.4-linux-amd64-bin etcd
cd etcd
./start-etcd.sh
```
You can use `screen` or `nohup` to run it in the background.

### Install Hadoop*
Hadoop is optional. It is only needed if you want to use HDFS as an underlying storage.

Pixels has been tested to be compatible with Hadoop-2.7.3 and Hadoop-3.3.1.
Follow the official docs to install Hadoop.

Modify `hdfs.config.dir` in `PIXELS_HOME/pixels.properties`
and point it to the `etc/hadoop` directory under the home of Hadoop.
Pixels will read the Hadoop configuration files `core-site.xml` and `hdfs-site.xml` from this directory.

> Note that some default ports used by Hadoop
> may conflict with the default ports used by Trino. In this case, modify the default port configuration
> of either system.

### Install Trino
Trino is the recommended query engine that works with Pixels. Currently, Pixels is compatible with Trino-405.
Download and install Trino-405 following the instructions [here](https://trino.io/docs/405/installation/deployment.html).

Here, we install Trino to `~/opt/trino-server-405` and create a link for it:
```bash
ln -s trino-server-405 trino-server
```
Then download [trino-cli](https://trinodb.github.io/docs.trino.io/405/client/cli.html) into `~/opt/trino-server/bin/`
and give executable permission to it.

There are two important directories in the home of trino-server: `etc` and `plugin`.
Decompress `pixels-trino-listener-*.zip` and `pixels-trino-connector-*.zip` into the `plugin` directory.
The `etc` directory contains the configuration files of Trino.
In addition to the configurations mentioned in the official docs, add the following configurations
for Pixels:
* Create the listener config file named `event-listener.properties` in the `etc` directory, with the following content:
```properties
event-listener.name=pixels-event-listener
enabled=true
listened.user.prefix=none
listened.schema=pixels
listened.query.type=SELECT
log.dir=/home/ubuntu/opt/pixels/listener/
```
`log-dir` should point to
an existing directory where the listener logs will appear.

* Create the catalog config file named `pixels.properties` for Pixels in the `etc/catalog` directory,
with the following content:
```properties
connector.name=pixels
pixels.config=/home/ubuntu/opt/pixels/pixels.properties

# serverless config
# lambda.switch can be on, off, auto
lambda.switch=auto
local.scan.concurrency=40
clean.local.result=true
output.scheme=output-scheme-dummy
output.folder=output-folder-dummy
output.endpoint=output-endpoint-dummy
output.access.key=lambda
output.secret.key=password
```
`pixels.config` is used to specify the config file for Pixels, and has a higher priority than the config file under `PIXELS_HOME`.
**Note** that `etc/catalog/pixels.proterties` under Trino's home is different from `PIXELS_HOME/pixels.properties`.
The other properties are related to serverless execution.
In Trino, Pixels can projection, selection, join, and aggregation into AWS Lambda
This feature can be turned on by setting `lambda.switch` to `auto` (adaptively enabled) or `on` (always enabled), `output.scheme` to the storage scheme of the intermediate files (e.g. s3),
`output.folder` to the directory of the intermediate files, `output.endpoint` to the endpoint of the intermediate storage,
and `output.access/secret.key` to the access/secret key of the intermediate storage.

Append the following two lines into `etc/jvm.config`:
```config
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
```
Thus, pixels can reflect internal or low-level classes to improve performance. This is only needed for Java 9+.

Some scripts in Trino may require python:
```bash
sudo apt-get install python
```

### Install Prometheus and Grafana*
Prometheus and Grafana are optional. We can install them to monitor the
performance metrics of the whole system.

To install Prometheus, copy `node_exporter-1.5.0.linux-amd64.tar.xz`, `jmx_exporter-0.18.0.tar.xz`, and `prometheus-2.43.0.linux-amd64.tar.xz`
under `scripts/tars` into the `~/opt` directory and decompress them.

Create links:
```bash
ln -s jmx_exporter-0.18.0/ jmx_exporter
ln -s node_exporter-1.5.0.linux-amd64 node_exporter
ln -s prometheus-2.43.0.linux-amd64 prometheus
```

Append the following lines into `~/.bashrc`:
```bash
export PROMETHEUS_HOME=$HOME/opt/prometheus/
export NODE_EXPORTER_HOME=$HOME/opt/node_exporter
export PATH=$PATH:$PROMETHEUS_HOME:$NODE_EXPORTER_HOME
```

Enter the home of Trino-server. Append this line to `etc/jvm.config`:
```bash
-javaagent:/home/ubuntu/opt/jmx_exporter/jmx_prometheus_javaagent-0.18.0.jar=9101:/home/ubuntu/opt/jmx_exporter/pixels-jmx.yml
```

Start `node_exporter` and `prometheus` respectively, using the `start-*.sh` in their directories.
You can use `screen` or `nohup` to run them in the background.

Then, follow the instructions [here](https://grafana.com/docs/grafana/latest/installation/debian/)
to install Grafana.

Log in Grafana, create a Prometheus data source named `cluster-monitor`.
Set `URL` to `http://localhost:9090`, `Scrape Interval` to `5s`, and `HTTP Method` to `GET`. Other configurations remains default.

Import the json dashboard configuration files under `scripts/grafana` into Grafana.
Then we get three dashboards `Node Exporter` and `JVM Exporter` in Grafana.
These dashboards can be used to monitor the performance metrics of the instance.

## Start Pixels
Enter `PIXELS_HOME`.
If pixels-cache is enabled, set up the cache before starting Pixels:
```bash
./sbin/reset-cache.sh
sudo ./sbin/pin-cache.sh
```
`reset-cache.sh` is only needed for the first time of using pixels-cache. 
It initializes some states in etcd for the cache.

Even if pixels-cache is disabled, `reset-cache.sh` is needed for the first time of starting Pixels.
Then, start the daemons of Pixels using:
```bash
./sbin/start-pixels.sh
```
The metadata server, coordinator, node manager, and metrics server, are running in the daemons.

After starting Pixels, enter the home of trino-server and start Trino:
```bash
./bin/launcher start
```

Connect to trino-server using trino-cli:
```bash
./bin/trino --server localhost:8080 --catalog pixels
```
Run `SHOW SCHEMAS` in trino-cli, the result should be as follows if everything is installed correctly.
```sql
       Schema       
--------------------
 information_schema 
(1 row)
```

By now, Pixels has been installed in the EC2 instance. The aforementioned instructions should also
work in other kinds of VMs or physical servers.

Run the script `$PIXELS_HOME/sbin/stop-pixels.sh` to stop Pixels when needed.
If pixels-cache is used, also run the `$PIXELS_HOME/sbin/unpin-cache.sh` to release the memory that is 
pinned by the cache.

## TPC-H Evaluation

After starting Pixels and Trino, we can evaluate the performance of Pixels using TPC-H.

### Prepare TPC-H

Attach a volume that is larger than the scale factor (e.g., 150GB for SF100) to the EC2 instance.
Mount the attached volume to a local path (e.g., `/data/tpch`).
Download tpch-dbgen to the instance, build it, and generate the dataset and queries into the attached volume.
Here, we put the dataset in `/data/tpch/100g/`.
The file(s) of each table are stored in a separate directory named by the table name.

### Create TPC-H Database
Log in trino-cli and use the SQL statements in `scripts/sql/tpch_schema.sql` to create the TPC-H database in Pixels.
Change the value of the `storage` table property in the create-table statement to `hdfs` if HDFS is used as the 
underlying storage system instead of S3.
> Note that trino-cli can execute only one SQL statement at each time.

Then, use `SHOW SCHEMAS` and `SHOW TABLES` statements to check if the tpch database has been
created successfully.

Connect to MySQL using the user `pixels`, and execute the SQL statements in `scripts/sql/tpch_layouts.sql`
to create the table layouts for the tables in the TPC-H database in Pixels. Actually, these layouts
should be created by the storage layout optimizer ([Rainbow](https://ieeexplore.ieee.org/document/8509421)).
However, we directly load the layouts here for simplicity.

Create the container to store the tables in S3. The container name is the same as the hostname
(e.g., `pixels-tpch`) in the `LAYOUT_ORDER_PATH` and `LAYOUT_COMPACT_PATH` of each table layout.
Change the bucket name if it already exists.

During data loading, Pixels will automatically create the folders in the bucket to store the files in each table.

### Load Data

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
It may take about one hour. The last parameter `-c` of the `LOAD` command is the maximum number
of threads used for loading data. It only effects when the input directory (specified by `-o`)
contains multiple input files. In case that the TPC-H table has multiple parts, you can set 
`-c` to the number of parts to improve the data loading performance.

As we don't use pixels-cache for TPC-H, there is no need to load the cache.
Otherwise, we can load the cached table into pixels-cache using:
```bash
./sbin/load-cache.sh layout_version
```
`layout_version` is the version of the table's layout that specifies the columns we want to cache.

### Run Queries
Connect to trino-cli:
```bash
cd ~/opt/trino-server
./bin/trino --server localhost:8080 --catalog pixels --schema tpch
```
Execute the TPC-H queries in trino-cli.

### Data Compaction*
This is optional. It is only needed if we want to test the query performance on the compact layout.
In pixels-cli, use the following commands to compact the files in the ordered path of each table:
```bash
COMPACT -s tpch -t customer -n no -c 2
COMPACT -s tpch -t lineitem -n no -c 16
COMPACT -s tpch -t orders -n no -c 8
COMPACT -s tpch -t part -n no -c 1
COMPACT -s tpch -t partsupp -n no -c 8
COMPACT -s tpch -t supplier -n no -c 1
```
The tables `nation` and `region` are too small, no need to compact them.
The last parameter `-c` of `COMPACT` command is the maximum number
of threads used for data compaction. For large tables such as `lineitem`, you can increase `-c` to 
improve the compaction performance. Compaction is normally faster than loading with same number of threads.

To avoid scanning the small files in the ordered path during query execution,
create an empty bucket in S3 and change the ordered path in the metadata database
to the empty bucket.

### Statistics Collection*
This is optional. Data statistics enable cost-based query optimization for the queries.
Start Pixels and Trino, make sure that Trino can execute queries on `tpch` schema.
In pixels-cli, use the following commands to collect the data statistics for the columns in each table.
```bash
STAT -s tpch -t nation -o false -c true
STAT -s tpch -t region -o false -c true
STAT -s tpch -t supplier -o false -c true
STAT -s tpch -t customer -o false -c true
STAT -s tpch -t part -o false -c true
STAT -s tpch -t partsupp -o false -c true
STAT -s tpch -t orders -o false -c true
STAT -s tpch -t lineitem -o false -c true
```
After it is finished, statistics of each tpch column can be found in the `pixels_metadata.COLS` metadata table.
Finally, manually update the row count for each tpch table in `pixels_metadata.TBLS.TBL_ROW_COUNT`.

Set `splits.index.type=cost_based` and restart Trino to benefit from cost-based query optimization.
