package cn.edu.ruc.iir.pixels.core.utils;

import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsCoreConfig
{
    private final static ConfigFactory config = ConfigFactory.Instance();

    public String getMetricsDir()
    {
        return config.getProperty("metric.reader.json.dir");
    }

    public float getMetricsCollectProb()
    {
        return Float.valueOf(config.getProperty("metric.reader.collect.prob"));
    }
}
