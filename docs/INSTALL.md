# Basic Installation

In this document, we show an example of installing the core components of Pixels in AWS EC2.
Installation in other cloud or on-premises environments is almost the same.
In this example, we use Trino as the query engine. To use other query engines, skip [Install Trino](#install-trino)
and check the instructions for the other query engine:
* [Install Pixels + Presto](https://github.com/pixelsdb/pixels-presto)
* [Install Pixels + Hive](https://github.com/pixelsdb/pixels-hive)
* [Install Pixels + DuckDB](../cpp/README.md)

Here, we only install and configure the essential components for query processing.
To use the following optional components, follow the instructions in the corresponding README.md after the basic installation:
* [Pixels Cache](../pixels-cache/README.md): The distributed columnar cache to accelerate query processing.
* [Pixels Turbo](../pixels-turbo/README.md): The hybrid query engine that invokes serverless resources to help process unpredictable workload spikes.
* [Pixels Amphi](../pixels-amphi/README.md): The adaptive query scheduler that enables cost-efficient query processing in both on-perm and in-cloud environments.

In AWS EC2, create an Ubuntu-20.04 or 22.04 instance with x86 arch and at least 4GB memory and 20GB root volume. 
8GB or larger memory is recommended for performance evaluations on datasets larger than 10GB. 
Login the instance as `ubuntu` user, and install the following components.

> Installation steps marked with `*` are optional.

## Install JDK
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

## Install Maven

Pixels requires maven 3.8 or later to build the source code (some early maven versions may work, but we haven't test them yet). On some operating systems, the maven installed by `apt` or `yum` might be incompatible with new JDKs such as 17. 
In this case, manually install a newer maven compatible with your JDK.

## Setup AWS Credentials*

To use S3 as the underlying storage system, we have to configure the AWS credentials.

Currently, we do not configure the `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_DEFAULT_REGION` from Pixels.
Therefore, configure these credentials using
[environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html#envvars-set) or
[credential files](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

## Install Pixels

To build Pixels from source and install it locally, you can simply run:

```bash 
echo 'export PIXELS_HOME=$HOME/opt/pixels/' >> ~/.bashrc
source ~/.bashrc
./install.sh
```

But you still need to:
- Put the [MySQL JDBC connector](https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar) into `PIXELS_HOME/lib`.
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
# query scheduling server for pixels-turbo
query.schedule.server.port=18893
query.schedule.server.host=localhost
etcd.hosts=localhost
etcd.port=2379
metrics.node.text.dir=/home/pixels/opt/node_exporter/text/
presto.pixels.jdbc.url=jdbc:trino://localhost:8080/pixels/tpch
```
The hostnames, ports, paths, usernames, and passwords in these properties are to be configured in the following steps of installation.

> **Note:** optionally, you can also set the `PIXEL_CONFIG` system environment variable
> to specify a different location of `pixels.properties`. This can be a http or https URL
> to a remote location.

If you are installing a cluster, install Pixels on each node of the cluster.

Optionally, to install Pixels step-by-step, see the guidance below.

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
Put the sh scripts in `scripts/bin` and `scripts/sbin` into `PIXELS_HOME/bin` and `PIXELS_HOME/sbin`, respectively.
Put `pixels-daemon-*-full.jar` into `PIXELS_HOME`.
Put `pixels-cli-*-full.jar` into `PIXELS_HOME/sbin`
Put the jdbc connector of MySQL into `PIXELS_HOME/lib`.
Put `pixels-common/src/main/resources/pixels.properties` into `PIXELS_HOME`.
Modify `pixels.properties` to ensure that the URLs, ports, paths, usernames, and passwords are valid.
Leave the other config parameters as default.

Set `cache.enabled` to `false` in `$PIXELS_HOME/pixels.properties` if you don't use pixels-cache.

## Install MySQL
MySQL and etcd are used to store the metadata and states of Pixels. MySQL/MariaDB 5.5 or later has been tested. Other forks or variants may also work.
You only need to install one etcd and one MySQL instance, even in a cluster.

To install MySQL:
```bash
sudo apt update
sudo apt install mysql-server
sudo mysql_secure_installation
```

> Note that for mysql 8+, you may have to set a native password for mysql `root` user before running `mysql_secure_installation`, for example:
> ```bash
> sudo mysql
> mysql> ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'your_root_password';
> mysql> exit
> sudo mysql_secure_installation
> ```

Login MySQL and create a user and a metadata database for Pixels:

```mysql
CREATE USER 'pixels'@'%' IDENTIFIED BY 'password';
CREATE DATABASE pixels_metadata;
GRANT ALL PRIVILEGES ON pixels_metadata.* to 'pixels'@'%';
FLUSH PRIVILEGES;
```
Ensure that MySQL server can be accessed remotely. Sometimes the default MySQL configuration
binds the server to localhost thus declines remote connections.

Use `scripts/sql/metadata_schema.sql` to create tables in `pixels_metadata`.

## Install etcd

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

The default `etcd/conf.yml` is good for the default Ubuntu user in AWS Ubuntu instances.
If you are using your own OS installation or a different user, please modify the settings in `conf.yml` accordingly, 
especially for the `data-dir` and `*-urls`.

## Install Hadoop*
Hadoop is optional. It is only needed if you want to use HDFS as an underlying storage.

Pixels has been tested to be compatible with Hadoop-2.7.3 and Hadoop-3.3.1.
Follow the official docs to install Hadoop.

Modify `hdfs.config.dir` in `PIXELS_HOME/pixels.properties`
and point it to the `etc/hadoop` directory under the home of Hadoop.
Pixels will read the Hadoop configuration files `core-site.xml` and `hdfs-site.xml` from this directory.

> Note:
> (1) Some default ports used by Hadoop
> may conflict with the default ports used by Trino. In this case, modify the default port configuration
> of either system.
> (2) Hadoop 2.7.x and 3.3.x are not compatible with high-version JDKs such as JDK 17. Please configure
> Hadoop to use JDK 8.

## Install Trino
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

# serverless config
# it can be on, off, auto, or session
cloud.function.switch=off
clean.intermediate.result=true
```
**Note** that `etc/catalog/pixels.proterties` under Trino's home is different from `PIXELS_HOME/pixels.properties`.
In Trino, Pixels can push projections, filters, joins, and aggregations into serverless computing services (e.g., AWS Lambda).
This feature is named `Pixels-Turbo` and can be turned on by setting `cloud.function.switch` to `auto` (adaptively enabled) or `on` (always enabled).
Turn it `off` to only use Trino workers for query processing.
We can also set it to `session` so that this switch can be dynamically turned on or off by the session properties `pixels.cloud_function_enabled`.
This allows `pixels-server` choosing whether to execute the query with cloud functions enabled.

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

## Install Prometheus and Grafana*
Prometheus and Grafana are optional. We can install them to monitor the
performance metrics of the whole system.

To install Prometheus, copy `node_exporter-1.5.0.linux-amd64.tar.xz`, `jmx_exporter-0.18.0.tar.xz`, and `prometheus-2.43.0.linux-amd64.tar.xz`
under `scripts/tars` into the `~/opt` directory and decompress them.
Node exporter and jmx exporter are needed for every node in a cluster, whereas Prometheus and Grafana are only needed on the node where you want
to store the performance metrics and provide visualization.

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
Enter `PIXELS_HOME`, execute `./sbin/reset-cache.sh` for the first time of starting Pixels if pixels-cache is enabled.

Then, start the daemons of Pixels using:
```bash
./sbin/start-pixels.sh
```
The essential services of Pixels, such as the metadata server, transaction manager, cache coordinator, cache manager, and metrics server, are running in these daemons.

To start Pixels in a cluster, edit `PIXELS_HOME/sbin/workers` and list all the worker nodes in this file.
Each line in this file is in the following format:
```config
hostname_of_the_worker pixels_home_of_the_worker
```
`pixels_home_of_the_worker` is optional if the worker has the same `PIXELS_HOME` as the coordinator node where you run
`start_pixels.sh`.

You can also start Pixels coordinator and workers separately using `PIXELS_HOME/start-coordinator` and `PIXELS_HOME/start-workers.sh`.

> Note: You can also add JVM OPTS for Pixels daemons in `PIXELS_HOME/bin/jvm.config`. This is useful for profiling and remote debugging.

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

Run the script `PIXELS_HOME/sbin/stop-pixels.sh` to stop Pixels, or run `PIXELS_HOME/stop-coordinator` and `PIXELS_HOME/stop-workers.sh` to stop the coordinator and workers, respectively.
