# Pixels Retina

Retina (<ins>re</ins>al-<ins>ti</ins>me a<ins>na</ins>lytics) is the real-time data synchronization framework in Pixels.
It supports replaying data-change operations from a log-based CDC (change-data-capture) source as mirror transactions on the columnar table data.
It proposes a light-weight MVCC mechanism and corresponding version storage to support parallel mirror transaction replay and
concurrent analytical query processing, and a vectorized-filter-on-read (VFoR) approach for analytical queries to read consistent
data snapshots.

Compared to the merge-on-read (MoR) based on catalog snapshots in existing lakehouse systems, 
such as Apache Iceberg and Apache Paimon, Retina supports real row-granular (instead of batch-granular) transactional
data change replay without the expensive version merging and data compaction mechanisms.
Evaluations show that Retina simultaneously provides 10-ms-level data freshness and over 3.2M row/s scalable data-change 
replay throughput, without compromising query performance or resource cost-efficiency, 
significantly outperforming state-of-the-art lakehouses, Iceberg and Paimon, which provides minute-level data freshness
and one order of magnitude lower data-change throughput.

## Retina Components

The components related to Retina are:

- Sink: It connects to CDC streams from Debezium, reconstructs the data-change messages in the
CDC stream into mirror transactions, and send the data-change operations in mirror transaction through stream RPC to Pixels-Retina.
[Source code](https://github.com/pixelsdb/pixels-sink);
- Replayer: It receives the data-change operations from Pixels-Sink and replays them on the columnar data tables.
Source code: 
[core data structures and operations](../cpp/pixels-retina), 
[top-level replay and garbage collection](.), 
[client](../pixels-common/src/main/java/io/pixelsdb/pixels/common/retina),
and [server](../pixels-daemon/src/main/java/io/pixelsdb/pixels/daemon/retina).
RPC handling are in this directory and .
- Transaction Service: It allocates transaction timestamps for mirror transactions and analytical queries, and manages the timestamp watermarks
for the MVCC protocol of Retina.
Source code: 
[client](../pixels-common/src/main/java/io/pixelsdb/pixels/common/transaction) and
[server](../pixels-daemon/src/main/java/io/pixelsdb/pixels/daemon/transaction).
- Index Service: It is a multi-version index that mapping the index key (e.g., primary key or secondary key) to row location. 
Replayer looks up and updates the primary index during data-change replay.
Source code: 
[framework and interfaces](../pixels-common/src/main/java/io/pixelsdb/pixels/common/index),
[pluggable implementations](../pixels-index), 
[clients](../pixels-common/src/main/java/io/pixelsdb/pixels/common/index/service)
[server](../pixels-daemon/src/main/java/io/pixelsdb/pixels/daemon/index)
- Catalog Service (i.e., metadata service): It manages the schema, statistics, and data catalog of tables.
Source code: [client](../pixels-common/src/main/java/io/pixelsdb/pixels/common/metadata) and
[server](../pixels-daemon/src/main/java/io/pixelsdb/pixels/daemon/metadata).
- Columnar file format: It provides the file format definition, reader, writer of the Pixels file format. [Source code](../pixels-core).
- Trino connector: It runs inside the Trino cluster to access the services of Retina/Pixels, and calls the file reader to read data.
[Source code](https://github.com/pixelsdb/pixels-trino).

## Usage
