package io.pixelsdb.pixels.cache;

import org.junit.Test;

import java.io.IOException;

public class TestCacheContentReader {
    @Test
    public void testNativeDiskCacheContentReader() throws IOException, NoSuchFieldException, IllegalAccessException {
        CacheContentReader reader = new NativeDiskCacheContentReader("/mnt/nvme1n1/pixels.cache");
    }
}
