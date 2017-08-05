package cn.edu.ruc.iir.pixels.presto;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;

import java.util.Map;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "pixels";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return null;
    }

    @Override
    public Connector create(String s, Map<String, String> map, ConnectorContext connectorContext)
    {
        return null;
    }
}
