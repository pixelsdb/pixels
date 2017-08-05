package cn.edu.ruc.iir.pixels.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.List;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsPageSourceProvider
    implements ConnectorPageSourceProvider
{
    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle connectorTransactionHandle, ConnectorSession connectorSession, ConnectorSplit connectorSplit, List<ColumnHandle> list)
    {
        return null;
    }
}
