# Pixels Cache

## Interface
Main interfaces of pixels cache includes:

- **PixelsCache** provides the instance of a pixels cache. It holds a PixelsManager.
- **PixelsCacheManager** is the backend thread for cache management, it is the cache manager on a single worker..
- **PixelsReader** provides the read and search functions of pixels cache.
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