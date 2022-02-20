Pixels
=======

Pixels is a columnar storage engine for data lakes. It is optimized for data analytics on tables that are stored in HDFS and S3-like file/object storage systems, and provides much higher performance than existing columnar formats such as Parquet.
Moreover, all the storage optimizations in Pixels, including data layout reordering, columnar caching, and I/O scheduling, are transparent to query engines and underlying file/object storage systems.
Thus, it does not affect the maintainability and portability of the storage layer in data lakes.

## Build Pixels
Install JDK 8.0.
Open Pixels as maven project in Intellij. When the project is fully indexed and the dependencies are successfully downloaded,
use the maven's `package` command to build it. Some test params are missing for the unit tests, you can simply create arbitrary values for them.
Ensure that Pixels is built using language level 1.8, for the Presto and Hive versions we use are compatible with JDK 8.0 only.

It may take about one minute to complete. After that, find the following jar/zip files that will be used in the installation:
* `pixels-daemon-*-full.jar` in `pixels-daemon/target`, this is the jar to run Pixels daemons;
* `pixels-listener-*.zip` in `pixels-listener/target`, this is the listener plugin for Presto;
* `pixels-presto-*.zip` in `pixels-presto/target`, this is the connector for Presto;
* `pixels-load-*-full.jar` in `pixels-load/target`, this is the jar to load data for Pixels.

## Installation in AWS

Create an EC2 Ubuntu-20.04 instance with x86 arch and at least 20GB root volume. Memory of 8GB or larger is recommended. Log in the instance as `ubuntu` user, 
and install the following components.

### Install JDK
Install JDK 8.0 in the EC2 instance:
```bash
sudo apt install openjdk-8-jdk openjdk-8-jre
```
Check the java version:
```bash
java -version
```
If the other version of JDK is in use, switch to JDK 8.0:
```bash
update-java-alternatives --list
sudo update-java-alternatives --set /path/to/jdk-8.0
```
Oracle JDK 8.0 also works.

### Install Pixels
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
Put `pixels-daemon-*-full.jar` into `PIXELS_HOME` and `pixels-load-*-full.jar` into `PIXELS_HOME/sbin`.
Put the jdbc connector of MySQL into `PIXELS_HOME/lib`.
Put `pixels-common/src/main/resources/pixels.properties` into `PIXELS_HOME`.
Modify `pixels.properties` to ensure that the URLs, paths, and passwords are valid.
Leave the other config parameters as default.

To use the columnar cache in Pixels (i.e., pixels-cache), create and mount an in-memory file system:
```bash
sudo mkdir -p /mnt/ramfs
sudo mount -t tmpfs -o size=1g tmpfs /mnt/ramfs
```
The path `/mnt/ramfs` is determined by `cache.location` and `index.location` in `PIXELS_HOME/pixels.properties`.
The `size` parameter of the mount command should be larger than or equal to the sum of `cache.size` and `index.size` in 
`PIXELS_HOME/pixels.properties`. And it must be smaller than the physical memory size.
Also ensure that `cache.enabled` and `cache.read.direct` are set to `true` in `PIXELS_HOME/pixels.properties`.
Set `cache.enabled` to `false` if you don't want to use pixels-cache. 
But **NOTE** that the path `/mnt/ramfs` must exist even if pixels-cache is disabled, as Pixels checks this path when starts.

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

### Install Presto
Presto is the recommended query engine that works with Pixels. Currently, Pixels is compatible with Presto-0.215.
Download and install Presto-0.215 following the instructions [here](https://prestodb.io/docs/0.215/installation/deployment.html).

Here, we install Presto to `~/opt/presto-server-0.215` and create a link for it:
```bash
ln -s presto-server-0.215 presto-server
```
Then download [presto-cli](https://prestodb.io/docs/0.215/installation/cli.html) into `~/opt/presto-server/bin/`
and give executable permission to it.

There are two important directories in the home of presto-server: `etc` and `plugin`.
Decompress `pixels-listener-*.zip` and `pixels-presto-*.zip` into the `plugin` directory.
The `etc` directory contains the configuration files of Presto.
In addition to the configurations mentioned in the official docs, add the following configurations
for Pixels:
* Create the listener config file named `event-listener.properties` with the following content:
```properties
event-listener.name=pixels-event-listener
enabled=true
listened.user.prefix=none
listened.schema=pixels
listened.query.type=SELECT
log.dir=/home/ubuntu/opt/pixels/listener/
```
`log-dir` should point to
an existing directory where the listener logs will appear

* Create the catalog config file named `pixels.properties` for Pixels in the `catalog` subdirectory,
with the following content:
```properties
connector.name=pixels
pixels.home=/home/ubuntu/opt/pixels/
```
`pixels.home` should be the same as `PIXELS_HOME`.
**Note** that this `pixels.properties` is in the `etc/catalog` directory of Presto's home, and is different from `PIXELS_HOME/pixels.properties`.

Some scripts in Presto may also require python:
```bash
sudo apt-get install python
```

### Install Prometheus and Grafana*
Prometheus and Grafana are optional. We can install them to monitor the
performance metrics of the whole system.

To install Prometheus, copy `node_exporter-0.15.2.linux-amd64.tar.xz`, `jmx_exporter-0.11.0.tar.gz`, and `prometheus-2.1.0.linux-amd64.tar.xz`
under `scripts/tars` into the `opt` directory and decompress them.

Create links:
```bash
ln -s jmx_exporter-0.11.0/ jmx_exporter
ln -s node_exporter-0.15.2.linux-amd64 node_exporter
ln -s prometheus-2.1.0.linux-amd64 prometheus
```

Append the following lines into `~/.bashrc`:
```bash
export PROMETHEUS_HOME=$HOME/opt/prometheus/
export PATH=$PATH:$PROMETHEUS_HOME
```

Enter the `etc` directory under the home of presto-server. Append this line to `jvm.config`:
```bash
-javaagent:/home/ubuntu/opt/jmx_exporter/jmx_prometheus_javaagent-0.11.0.jar=9101:/home/ubuntu/opt/jmx_exporter/presto-jmx.yml
```

Start `node_exporter` and `prometheus` respectively, using the `start-*.sh` in their directories.
You can use `screen` or `nohup` to run them in the background.

Then, follow the instructions [here](https://grafana.com/docs/grafana/latest/installation/debian/)
to install Grafana.

Log in Grafana, create a Prometheus data source named `cluster-monitor`.
Set URL to `http://localhost:9090`, Scrape Interval to `5s`, and HTTP Method to `GET`. Other configurations remains default.

Import the json dashboard configuration files under `scripts/grafana` into Grafana.
Then we get three dashboards `Cluster Exporter`, `JVM Exporter`, and `Node Exporter` in Grafana.
These dashboards can be used to monitor the performance metrics of the instance.

### Install Hadoop*
Hadoop is optional. It is only needed if you want to use HDFS as an underlying storage.
It is tested that Pixels works well with Hadoop-2.7.3 and Hadoop-3.3.1.
Follow the official docs to install Hadoop if needed.

Note that some default ports used by Hadoop
may conflict with the default ports used by Presto. In this case, modify the default port configuration
of either system.

However, **even if HDFS is not used**, Pixels has to read Hadoop configuration files `core-site.xml` and `hdfs-site.xml` from the path that
is specified by `hdfs.config.dir` in `PIXELS_HOME/pixels.properties`. Therefore, make sure that these two file
exist in `hdfs.config.dir`.

### AWS Credentials
If we use S3 as the underlying storage system, we have to configure the AWS credentials.

Currently, we do not configure the `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_DEFAULT_REGION` from Pixels.
Therefore, we have to configure these credentials using 
[environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html#envvars-set) or 
[credential files](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html). 

## Start Pixels
Enter `PIXELS_HOME` and start the daemons of Pixels using:
```bash
./sbin/start-pixels.sh
```
Enter the home of presto-server and start Presto:
```bash
./bin/launcher start
```

Connect to presto-server using presto-cli:
```bash
./bin/presto --server localhost:8080 --catalog pixels
```
Run `SHOW SCHEMAS` in presto-cli, the result should be as follows if everything is installed correctly.
```sql
       Schema       
--------------------
 information_schema 
(1 row)
```

By now, Pixels has been installed in the EC2 instance. The aforementioned instructions should also
work in other kinds of VMs or physical servers.

## TPC-H Evaluation

### Prepare TPC-H

Attach a volume that is larger than the scale factor (e.g., 150GB for SF100) to the EC2 instance.
Mount the attached volume to a local path (e.g., `/data/tpch`).
Download tpch-dbgen to the instance, build it, and generate the dataset and queries into the attached volume.
Here, we put the dataset in `/data/tpch/100g/`.
The file(s) of each table are stored in a separate directory named by the table name.

### Create TPC-H Database
Log in presto-cli and use the SQL statements in `scripts/sql/tpch_schema.sql` to create the TPC-H database in Pixels.
Change the value of the `storage` table property in the create-table statement to `hdfs` if HDFS is used as the 
underlying storage system instead of S3.
Note that presto-cli can execute only one SQL statement at each time.

Then, use `SHOW SCHEMAS` and `SHOW TABLES` statements to check if the tpch database has been
created successfully.

Connect to MySQL using the user `pixels`, and execute the SQL statements in `scripts/sql/tpch_layouts.sql`
to create the table layouts for the tables in the TPC-H database in Pixels. Actually, these layouts
should be created by the storage layout optimizer ([Rainbow](https://ieeexplore.ieee.org/document/8509421)).
However, we directly load the layouts here for simplicity.

Create the containers for the tables layouts in S3. The container name is the same as the hostname
(e.g., `pixels-tpch-customer-v-0-order`) in the `LAYOUT_ORDER_PATH` and `LAYOUT_COMPACT_PATH` of each table layout.
Change the paths in the table layouts if the container names are already used.

### Load Data
Under `PIXELS_HOME`, run pixels-load:
```bash
java -jar pixels-load-*-full.jar
```

Then use the following command in pixels-load to load data for the TPC-H tables:
```bash
LOAD -f pixels -o file:///data/tpch/100g/customer -d tpch -t customer -n 319150 -r \| -c 1
LOAD -f pixels -o file:///data/tpch/100g/lineitem -d tpch -t lineitem -n 600040 -r \| -c 1
LOAD -f pixels -o file:///data/tpch/100g/nation -d tpch -t nation -n 100 -r \| -c 1
LOAD -f pixels -o file:///data/tpch/100g/orders -d tpch -t orders -n 638300 -r \| -c 1
LOAD -f pixels -o file:///data/tpch/100g/part -d tpch -t part -n 769240 -r \| -c 1
LOAD -f pixels -o file:///data/tpch/100g/partsupp -d tpch -t partsupp -n 360370 -r \| -c 1
LOAD -f pixels -o file:///data/tpch/100g/region -d tpch -t region -n 10 -r \| -c 1
LOAD -f pixels -o file:///data/tpch/100g/supplier -d tpch -t supplier -n 333340 -r \| -c 1
```
This may take a few hours.

### Run Queries
Connect to presto-cli:
```bash
cd ~/opt/presto-server
./bin/presto --server localhost:8080 --catalog pixels --schema tpch
```
Execute the TPC-H queries in presto-cli.

### Data Compaction*
This is optional. It is only needed if we want to test the query performance on the compact layout.
In pixels-load, use the following command to compact the files in the ordered path of each table:
```bash
COMPACT -s tpch -t customer -l 1 -n no
COMPACT -s tpch -t lineitem -l 2 -n no
COMPACT -s tpch -t orders -l 4 -n no
COMPACT -s tpch -t part -l 5 -n no
COMPACT -s tpch -t partsupp -l 6 -n no
COMPACT -s tpch -t supplier -l 8 -n no
```
The tables `nation` and `region` are too small, no need to compact them.
Compaction is faster than loading.

To avoid scanning the small files in the ordered path during query execution,
create an empty bucket in S3 and change the ordered path in the metadata database
to the empty bucket.