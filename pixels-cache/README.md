# Pixels Cache

## Interface
Main interfaces of pixels cache includes:

- **PixelsCache** provides the instance of a pixels cache. It holds a PixelsManager.
- **PixelsCacheManager** is the backend thread for cache management, it is the cache manager on a single worker.
- **PixelsCacheReader** provides the read and search functions of pixels cache.
- **MemoryMappedFile** provides basic read and write functions of shared memory.
- **MappedBusReader** and **MappedBusWriter** provides message queues on top of shared memory.
- **PixelsMetaHolder** is the metadata cache for pixels.

In our previous design, the pixels cache are managed independently by the cache manager on each worker node. In the query planning phase, cache location is not considered for improving I/O performance. The location of a split/task is only determined by which node the corresponding HDFS block is located in. The cache manager on each node works in a passive manner - waiting for a split/task been assigned to this node and managing a cache for the columnlet accessed by that task.

This is not an efficient manner, for that 
1) pre-caching and global cache scheduling is not possible; 
2) multiple nodes may have the cache of the same split, which is a waste of memory space.

So we have to design a distributed cache coordinator to solve this problem. The coordinator and the node cache manager share an etcd cluster as the communication medium.

The etcd cluster is also used to store distributed locks for Issue #38.

There are some tasks to be done to resolve this issue: 
1) add etcd util into pixels-common; 
2) implement PixelsCacheCoordinator in pixels-cache; 
3) continue implementing PixelsCacheManager in pixels-cache; 
4) implement CacheManager in pixels-daemon, CacheManager manages pixels cache on each worker node; 
5) implement CacheCoordinator in pixels-daemon, CacheCoordinator coordinates the cache distribution for the whole cluster.

## Data Model in ETCD
1. caching node -> status
2. file -> caching location (each version maintains a separate set of file caching locations)
3. layout_version -> layout_id
4. cache_version -> version_id

## Caching Flow
1. When `Layout Optimizer` generates a new layout, it writes the new layout (with a new `layout_id`) into ETCD.
2. `CacheCoordinator` constantly checks the value of `cache_version` and `layout_version`, once it finds a newer `layout_version`, it allocates a new version of file caching locations in ETCD, then updates the `cache_version` to the newest `layout_version`.
3. Once a caching node(CacheManager) finds a new `cache_version`, it begins to update itself according to the new file caching locations, and sets itself as `busy` in the ETCD.
4. When a query comes, Presto Coordinator checks ETCD for file caching locations, and checks caching node status, thus find available caches for its source splits.
5. Each Presto DataNode executes query splits with caching information (whether cached or not), and calls `PixelsCacheReader` to read caching data locally, whose implementation is encapsulated in the `PixelsReader`.
6. `CacheCoordinator` constantly sends heartbeats to check running status of caching nodes, and sync their status through ETCD.