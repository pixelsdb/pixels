package io.pixelsdb.pixels.daemon.cache;

/**
 * pixels
 *
 * @author guodong
 */
public class MockCacheManager
{
    public static void main(String[] args)
    {
        CacheManager cacheManager = new CacheManager();
        cacheManager.run();
    }
}
