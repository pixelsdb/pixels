package io.pixelsdb.pixels.daemon.cache;

/**
 * pixels
 *
 * @author guodong
 */
public class MockCacheCoordinator
{
    public static void main(String[] args)
    {
        CacheCoordinator cacheCoordinator = new CacheCoordinator();
        cacheCoordinator.run();
    }
}
