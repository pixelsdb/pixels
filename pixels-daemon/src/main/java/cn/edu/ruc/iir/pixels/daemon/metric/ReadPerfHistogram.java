package cn.edu.ruc.iir.pixels.daemon.metric;

import cn.edu.ruc.iir.pixels.common.ConfigFactory;

public class ReadPerfHistogram
{
    private static final int Interval;

    static
    {
        Interval = Integer.parseInt(ConfigFactory.Instance().getProperty("metric.bytesms.interval"));
    }
}
