package cn.edu.ruc.iir.pixels.cache;

/**
 * PixelsCacheCoordinator is responsible for the following tasks:
 * 1. caching balance. It assigns each file a caching location, which are updated into etcd for global synchronization, and maintains a dynamic caching balance in the cluster.
 * 2. cache update trigger. It triggers an update of caching locations in the cluster when new optimized layouts are available.
 * 3. caching node monitor. It monitors all caching nodes(CacheManager) in the cluster, and update running status(available, busy, dead, etc.) of caching nodes.
 */
public class PixelsCacheCoordinator extends Thread
{
    private PixelsCacheCoordinator ()
    {

    }

    @Override
    public void run()
    {
        super.run();
    }


}
