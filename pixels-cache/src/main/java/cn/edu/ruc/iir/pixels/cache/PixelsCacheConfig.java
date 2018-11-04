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
        return "";
    }

    public long getIndexSize()
    {
        return 0L;
    }

    public String getCacheLocation()
    {
        return "";
    }

    public long getCacheSize()
    {
        return 0L;
    }

    public String getMetaHost()
    {
        return "";
    }

    public int getMetaPort()
    {
        return 0;
    }

    public String getSchema()
    {
        return "";
    }

    public String getTable()
    {
        return "";
    }

    public String getHDFSConfigDir()
    {
        return "";
    }

    public int getNodeLeaseTTL()
    {
        return 0;
    }

    public String getNodeId()
    {
        return "";
    }
}
