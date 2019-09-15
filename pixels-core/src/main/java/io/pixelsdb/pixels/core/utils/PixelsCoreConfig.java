package io.pixelsdb.pixels.core.utils;

import io.pixelsdb.pixels.common.utils.ConfigFactory;

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
