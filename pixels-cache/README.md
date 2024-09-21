# Pixels Cache
Pixels cache is the distributed columnar cache that co-locates with the query (compute) engine.
It consists of a 'cache coordinator' on the master node and a 'cache manager' on each worker node of the query engine cluster.
Their implementation can be found in `pixels-daemon`.

The cache coordinator maintains the cache plan that decides which column chunk in which row group is caches on which worker node.
Whereas the cache manager on each worker node listens to the update of the cache plan and replaces the cache content on this worker node accordingly.

## How It Works
The cache plan is stored in etcd and have the following data model:
1. layout_version -> version: data layout version, incremented each time new data layouts are created.
2. cache_version -> version: caching version, incremented each time caches are updated.
3. cache_location_%{version}_%{node_id} -> files: recording the array of files cached on the specified node (cache manager) under the specified caching version.

The cache is read and updated as follows:
1. When the `layout optimizer` generates a new layout, it writes the new layout (with a new `layout_version`) into Etcd.
2. The `cache coordinator` constantly checks the value of `layout_version`, once it finds a newer `layout_version`, it allocates a new version of cache plan in Etcd, then updates the `cache_version` to the latest `layout_version`.
3. Once a cache manager finds a new `cache_version`, it begins to read the new cache plan and finds the new column chunks to be cached by itself, and sets itself as `busy` in Etcd.
4. When a query comes, Presto/Trino Coordinator checks Etcd for the cache plan, thus find available caches for its query splits.
5. Each Presto/Trino WorkerNode executes query splits with caching information (whether the column chunks in the query split are cached or not), and calls `PixelsCacheReader` to read the cached column chunks locally (if any).

## Installation
Install Pixels following the instructions [here](../docs/INSTALL.md), but do not start Pixels before finishing the following configurations.

Check the following settings related to pixels-cache in `$PIXELS_HOME/pixels.properties` on each node:
```properties
# the location of the cache content file of pixels-cache
cache.location=/mnt/ramfs/pixels.cache
# the size of the cache content file of pixels-cache in bytes
cache.size=68719476736
# the location of the index file of pixels-cache
index.location=/mnt/ramfs/pixels.index
# the size of the index file of pixels-cache in bytes
index.size=1073741824
# the scheme of the storage system to be cached
cache.storage.scheme=hdfs
# which schema to be cached
cache.schema=pixels
# which table to be cached
cache.table=test_105
# lease ttl must be larger than heartbeat period
lease.ttl.seconds=20
# heartbeat period must be larger than 0
heartbeat.period.seconds=10
# set to false if storage.scheme is S3
enable.absolute.balancer=true
# set to true to enable pixels-cache
cache.enabled=true
# set to true to read cache without memory copy
cache.read.direct=true
```
The above values are a good default setting for each node to cache up-to 64GB data of table `pixels.test_105` stored on HDFS.
Change the `cache.schema`, `cache.table`, and `cache.storage.scheme` to cache a different table that is stored in a different storage system.

On each worker node, create and mount an in-memory file system with 65GB capacity:
```bash
sudo mkdir -p /mnt/ramfs
sudo mount -t tmpfs -o size=65g tmpfs /mnt/ramfs
```
The `size` parameter of the mount command should be larger than or equal to the sum of `cache.size` and `index.size` in
`PIXELS_HOME/pixels.properties`, but must be smaller than the available physical memory size.

Set up the cache before starting Pixels:
```bash
./sbin/reset-cache.sh
```
`reset-cache.sh` is only needed for the first time of initializing pixels-cache.
It initializes some states in etcd for the cache.
If you have modified the `etcd` urls, please change the `ENDPOINTS` property in `reset-cache.sh` as well.

## Start Pixels (with Cache)

Enter `PIXELS_HOME` and start all Pixels daemons using:
```bash
./sbin/start-pixels.sh
```
If starting the daemons in a cluster of multiple nodes, set the hostnames of the worker nodes in `$PIXELS_HOME/sbin/workers`
and run `start-pixels.sh` on the coordinator node.

On each worker node, pin the cache in memory using:
```bash
sudo -E ./sbin/pin-cache.sh
```

Then create a new data layout for the cached table, and update `layout_version` in Etcd to trigger cache building or replacement:
```bash
./sbin/load-cache.sh {layout-version}
```

To stop Pixels, run `./sbin/stop-pixels.sh` to stop all Pixels daemons on the coordinator node, 
and run `sudo -E ./sbin/unpin-cache.sh` to release the memory pinned for the cache on each worker node.