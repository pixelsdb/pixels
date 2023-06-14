Pixels
=======

The core of Pixels is a columnar storage engine designed for data lakes and warehouses.
It is optimized for analytical tables stored in on-premises and cloud-native storage systems,
including S3, GCS, HDFS, Redis, and local file systems.
Pixels outperforms Parquet, which is the most widely used columnar format in today's lakehouses, by up to two orders of magnitude.

We have integrated Pixels with popular query engines including DuckDB, Trino (405), Presto (0.279), and Hive (2.3+).

The DuckDB integration and the C++ implementation of Pixels are in the [cpp](cpp) folder.
The other integrations are opensourced in separate repositories:
* [Pixels Connector for Trino](https://github.com/pixelsdb/pixels-trino)
* [Pixels Connector for Presto](https://github.com/pixelsdb/pixels-presto)
* [Pixels SerDe for Hive](https://github.com/pixelsdb/pixels-hive)

Pixels also has its own query engine [Pixels-Turbo](pixels-turbo).
It prioritizes processing queries in an autoscaling MPP cluster (currently based on Trino) and exploits serverless functions 
(e.g, [AWS Lambda](https://aws.amazon.com/lambda/) or [vHive / Knative](https://github.com/vhive-serverless/vHive)) 
to accelerate the processing of workload spikes. With `Pixels-Turbo`, we can achieve better performance and cost-efficiency 
for continuous workloads while not compromising elasticity for workload spikes.

On the basis of Pixels-Turbo, we are also building a pure cloud-function-based serverless query engine, and exploring how to improve the query execution 
efficiency in cloud functions.

## Build Pixels

Pixels is mainly implemented in Java, while there is a C++ implementation of the Pixels Reader.
The [C++ document](cpp/README.md) gives the instructions of using the C++ reader.
Here we explain how to build and use the Java components.

Install JDK 17.0.3 or above, and clone the Pixels codebase into any `SRC_BASE` directory:
```bash
git clone https://github.com/pixelsdb/pixels.git
```
Enter `SRC_BASE/pixels`, use `mvn install` to build and install it to the local Maven repository.
> JDK 17.0.3+ is required by Trino. To run Pixels in Presto or other query engines, please build Pixels and the corresponding connector
> using the Java version required by the query engine. Pixels by itself is compatible with Java 8 or above.

It may take a couple of minutes to complete. After that, find jar files:
* `pixels-daemon-*-full.jar` in `pixels-daemon/target`,this is the jar to run Pixels daemons.
* `pixels-cli-*-full.jar` in `pixels-cli/target`, this is the jar of Pixels command line tool.

They will be used in the installation of Pixels.

Pixels is compatible with different query engines, such as Trino, Presto, and Hive.
The query engine integrations can be easily built using maven.
For example, to build the Trino integration for Pixels, just git clone [pixels-trino](https://github.com/pixelsdb/pixels-trino), 
and build it using `mvn package` in the local git repository.

After building `pixels-trino`, find the following zip files in the build target directories:
* `pixels-trino-listener-*.zip`, this is the event listener plugin for Trino.
* `pixels-trino-connector-*.zip`, this is the connector for Trino.

They will be used in the installation of the integration.


## Develop Pixels in IntelliJ

If you want to develop Pixels in Intellij, open `SRC_BASE/pixels` as a maven project.
When the project is fully indexed and the dependencies are successfully downloaded, 
you can build Pixels using the maven plugin (as an alternative of `mvn package`), run and debug unit tests, and debug Pixels by
setting up a *Remote JVM Debug*.

> To run or debug the unit tests or the main classes of Pixels in Intellij, set the `PIXELS_HOME` environment
> variable for `Junit` or `Application` in `Run` -> `Edit Configurations` -> `Edit Configuration Templetes`.
> Ensure that the `PIXELS_HOME` directory exists and follow the instructions in [Install Pixels](docs/INSTALL.md#install-pixels) to put
> the `pixels.properties` into `PIXELS_HOME` and create the `logs` directory where the log files will be
> written into.


## Deploy and Evaluate Pixels

You can follow the [Installation](docs/INSTALL.md) instructions to deploy Pixels in a cluster,
and learn how to use Pixels and evaluate its performance following [TPC-H Evaluation](docs/TPC-H.md).


## Contributing

We welcome contributions to Pixels and its subprojects. If you are interested in contributing to Pixels, 
please read our [Git Workflow](https://github.com/pixelsdb/pixels/wiki/Git-Workflow).


## Publications

Pixels is an academic system aims at providing production-grade quality. It supports all the functionalities required by TPC-H and
is compatible with the mainstream data analytic ecosystems.
The key ideas and insights in Pixels are elaborated in the following publications.

> `SIGMOD'23` Using Cloud Function as Accelerator for Elastic Data Analytics\
> Haoqiong Bian, Tiannan Sha, Anastasia Ailamaki

> `EDBT'22` Columnar Storage Optimization and Caching for Data Lakes (short) [[BibTeX]](https://dblp.org/rec/conf/edbt/JinBC022.html?view=bibtex)\
> Guodong Jin, Haoqiong Bian, Yueguo Chen, Xiaoyong Du

> `ICDE'22` Pixels: An Efficient Column Store for Cloud Data Lakes [[BibTeX]](https://dblp.org/rec/conf/icde/BianA22.html?view=bibtex)\
> Haoqiong Bian, Anastasia Ailamaki

> `CIDR'20` Pixels: Multiversion Wide Table Store for Data Lakes (abstract) [[BibTeX]](https://dblp.org/rec/conf/cidr/Bian20.html?view=bibtex)\
> Haoqiong Bian

> `ICDE'18` Rainbow: Adaptive Layout Optimization for Wide Tables (demo) [[BibTeX]](https://dblp.org/rec/conf/icde/BianTJCQD18.html?view=bibtex)\
> Haoqiong Bian, Youxian Tao, Guodong Jin, Yueguo Chen, Xiongpai Qin, Xiaoyong Du

> `SIGMOD'17` Wide Table Layout Optimization by Column Ordering and Duplication [[BibTeX]](https://dblp.org/rec/conf/sigmod/BianYTCCDM17.html?view=bibtex)\
> Haoqiong Bian, Ying Yan, Wenbo Tao, Liang Jeff Chen, Yueguo Chen, Xiaoyong Du, Thomas Moscibroda
