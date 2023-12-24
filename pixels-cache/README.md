# Pixels Cache
Pixels cache is the distributed columnar cache that co-locates with the query (compute) engine.
It consists of a 'cache coordinator' on the master node and a 'cache manager' on each worker node of the query engine cluster.
Their implementation can be found in `pixels-daemon`.

The cache coordinator tracks the states of the cache managers and maintains the cache plan that decides which column chunk in which row group is caches on which worker node.
Whereas the cache manager on each worker node listens to the update of the cache plan and replaces the cache content on this worker node accordingly.

## How It Works
The cache plan and the status of the cache managers are stored in etcd and have the following data model:
1. node_${id} -> status: recording status of each cache manager.
2. location_%{version}_%{node_id} -> files: recording the array of files cached on the specified node (cache manager) under the specified caching version.
3. layout_%{version} -> layout: recording layout of the specified version.
4. cache_version -> version: caching version, incremented each time caches are updated.

The cache is read and updated as follows:
1. When the `layout optimizer` generates a new layout, it writes the new layout (with a new `layout_version`) into Etcd.
2. The `cache coordinator` constantly checks the value of `layout_version`, once it finds a newer `layout_version`, it allocates a new version of cache plan in Etcd, then updates the `cache_version` to the latest `layout_version`.
3. Once a cache manager finds a new `cache_version`, it begins to read the new cache plan and finds the new column chunks to be cached by itself, and sets itself as `busy` in Etcd.
4. When a query comes, Presto/Trino Coordinator checks Etcd for the cache plan and the cache manager status, thus find available caches for its query splits.
5. Each Presto/Trino WorkerNode executes query splits with caching information (whether the column chunks in the query split are cached or not), and calls `PixelsCacheReader` to read the cached column chunks locally (if any).
6. The `cache coordinator` and the `cache managers` sync their status through Etcd.