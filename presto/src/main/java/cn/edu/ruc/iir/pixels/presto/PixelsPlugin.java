package cn.edu.ruc.iir.pixels.presto;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new PixelsConnectorFactory());
    }
}
