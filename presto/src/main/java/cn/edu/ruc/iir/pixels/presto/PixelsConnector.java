package cn.edu.ruc.iir.pixels.presto;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsConnector
    implements Connector
{
    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean b)
    {
        return null;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle connectorTransactionHandle)
    {
        return null;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return null;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return null;
    }

    @Override
    public void shutdown()
    {

    }
}
