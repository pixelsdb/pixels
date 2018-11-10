package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsCacheConfig
{
    private final ConfigFactory configFactory;

    public PixelsCacheConfig()
    {
        this.configFactory = ConfigFactory.Instance();
    }

    public String getIndexLocation()
    {
        return configFactory.getProperty("index.location");
    }

    public long getIndexSize()
    {
        return Long.parseLong(configFactory.getProperty("index.size"));
    }

    public String getCacheLocation()
    {
        return configFactory.getProperty("cache.location");
    }

    public long getCacheSize()
    {
        return Long.parseLong(configFactory.getProperty("cache.size"));
    }

    public String getMetaHost()
    {
        return configFactory.getProperty("metadata.server.host");
    }

    public int getMetaPort()
    {
        return Integer.parseInt(configFactory.getProperty("metadata.server.port"));
    }

    public String getSchema()
    {
        return configFactory.getProperty("cache.schema");
    }

    public String getTable()
    {
        return configFactory.getProperty("cache.table");
    }

    public String getHDFSConfigDir()
    {
        return configFactory.getProperty("hdfs.config.dir");
    }

    public int getNodeLeaseTTL()
    {
        return Integer.parseInt(configFactory.getProperty("lease.ttl.seconds"));
    }

    public String getHostAddress()
    {
        return configFactory.getProperty("cache.host");
    }
}
