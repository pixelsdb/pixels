# Pixels Cache
Pixels cache is the distributed columnar cache that co-locates with the query (compute) engine.
It consists of a 'cache coordinator' on the master node and a 'cache manager' on each worker node of the query engine cluster.
Their implementation can be found in `pixels-daemon`.

The cache coordinator maintains the cache plan that decides which column chunk in which row group is caches on which worker node.
Whereas the cache manager on each worker node listens to the update of the cache plan and replaces the cache content on this worker node accordingly.

## How It Works
The cache plan is stored in etcd and have the following data model:
1. layout_version -> {schema_name}.{table_name}:{layout_version}: data layout version, updated by the user or program that want to trigger cache loading or replacement. Only increasing layout versions are accepted.
2. cache_version -> {schema_name}.{table_name}:{layout_version}: cache version, set by the cache coordinator to notify the cache workers for cache loading or replacement when the cache tasks are ready in etcd.
3. cache_location_{layout_version}_{worker_hostname} -> files: recording the array of files cached on the specified node (cache manager) under the specified caching version.

The cache is read and updated as follows:
1. When the `layout optimizer` generates a new layout, it writes the new layout (with a new `layout_version`) into Etcd.
2. The `cache coordinator` monitors the new values of `layout_version`, once it finds a newer `layout_version`, it creates the corresponding cache tasks for each cache worker in Etcd, then updates the `cache_version` to the latest `layout_version`.
3. The `cache workers` monitor `cache_version`. When a cache worker finds a new `cache_version`, it reads its cache task from `cache_location_{layout_version}_{worker_hostname}` and sets itself to `busy` to avoid concurrent cache updating. After than, the cache worker begins to load or replace the cache content.
4. When a query comes, Presto/Trino Coordinator checks Etcd for the cache plan, thus find available caches for its query splits.
5. Each Presto/Trino WorkerNode executes query splits with caching information (whether the column chunks in the query split are cached or not), and calls `PixelsCacheReader` to read the cached column chunks locally (if any).

## Installation

### Install vmtouch
Find `vmtouch-1.3.1.tar.xz` in `scripts/tars` under the Pixels source code folder and decompress it to anywhere.
Enter the decompressed folder, run:
```bash
make install
```
to build and install vmtouch to the operating system.

### Install Pixels
Install Pixels following the instructions [HERE](../docs/INSTALL.md), but do not start Pixels before finishing the following configurations.

Check the following settings related to pixels-cache in `PIXELS_HOME/etc/pixels.properties` on each node:
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
# set to true if cache.storage.scheme is a locality sensitive storage such as hdfs
cache.absolute.balancer.enabled=true
# set to true to enable pixels-cache
cache.enabled=true
# set to true to read cache without memory copy
cache.read.direct=true

# heartbeat lease ttl must be larger than heartbeat period
heartbeat.lease.ttl.seconds=20
# heartbeat period must be larger than 0
heartbeat.period.seconds=10
```
The above values are a good default setting for each node to cache up-to 64GB data of table `pixels.test_105` stored on `HDFS`.
Change `cache.storage.scheme` to cache the data stored in a different storage system.

### Mount In-memory File System
On each worker node, create and mount an in-memory file system with 65GB capacity:
```bash
sudo mkdir -p /mnt/ramfs
sudo mount -t tmpfs -o size=65g tmpfs /mnt/ramfs
```
The `size` parameter of the mount command should be larger than or equal to the sum of `cache.size` and `index.size` in
`PIXELS_HOME/etc/pixels.properties`, but must be smaller than the available physical memory size.


## Start Pixels (with cache)

Enter `PIXELS_HOME` and start all Pixels daemons using:
```bash
./sbin/start-pixels.sh
```
If starting the daemons in a cluster of multiple nodes, set the hostnames of the worker nodes in `PIXELS_HOME/sbin/workers`
and run `start-pixels.sh` on the coordinator node. Each line in `PIXELS_HOME/sbin/workers` is the hostname of a
worker node. If the worker node has a different `PIXELS_HOME` environment variable than the coordinator node, append
the `PIXELS_HOME` variable to the hostname, separate by a space like this:
```properties
worker1 /home/pixels/worker1_pixels_home
worker2 /home/pixels/worker2_pixels_home
...
```

On each worker node, pin the cache in memory using:
```bash
sudo -E ./sbin/pin-cache.sh
```
Modify `CACHE_PATH` if it is not consistent with the mount point of the in-memory file system storing
the cache and index files.

Then create a new data layout for the cached table, and update `layout_version` of the cached table in etcd to trigger 
cache loading or replacement:
```bash
./sbin/load-cache.sh {schema_name}.{table_name}:{layout_version}
# e.g., ./sbin/load-cache.sh tpch.lineitem:1
```
`schema_name` and `table_name` specifies which table to cache.
Whereas `layout_version` specifies which layout version of the table to cache.
Note that pixels-cache only caches data in the compact path of the layout, so ensure the table is compacted on the layout.
See examples of compacting tables [HERE](../docs/TPC-H.md#data-compaction).
Currently, we only cache the full compact files with the same number of row groups defined by 
`numRowGroupInFile` in the `LAYOUT_COMPACT` field of the layout in metadata. The tail compact file 
(if exists) with less row groups than `numRowGroupInFile` will be ignored in cache loading or replacement.

If you have modified the `etcd` hostname and port in `PIXELS_HOME/etc/pixels.properties`, change the `ENDPOINTS` property
in `load-cache.sh` as well.

## Stop Pixels and clear cache
To stop Pixels, run:
```bash
./sbin/stop-pixels.sh
```
on the coordinator node to stop all Pixels daemons in the cluster.

The cache does not lost when Pixels is stopped. And it can be reused the next time Pixels is started.

To clear the cache and free the memory, run:
```bash
sudo -E ./sbin/unpin-cache.sh
```
on each worker node to release the memory pinned for the cache.
After than, you can delete the shared-memory files at `cache.location` and `index.location` on each worker node to
finally release the memory occupied by the cache.
You can also umount the in-memory file system. This is optional. The in-memory file system will be
automatically umount when the operating system is restarted.

Then, run:
```bash
./sbin/reset-cache.sh
```
on any node in the cluster to reset the states related to pixels-cache in etcd.
If you have modified the `etcd` hostname and port in `PIXELS_HOME/etc/pixels.properties`, change the `ENDPOINTS` property
in `reset-cache.sh` as well.