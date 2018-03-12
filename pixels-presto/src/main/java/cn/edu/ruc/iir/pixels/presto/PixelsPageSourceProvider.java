package cn.edu.ruc.iir.pixels.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Provider Class for Pixels Page Source class.
 */
public class PixelsPageSourceProvider implements ConnectorPageSourceProvider {

    private PixelsRecordSetProvider pixelsRecordSetProvider;

    @Inject
    public PixelsPageSourceProvider(PixelsRecordSetProvider pixelsRecordSetProvider) {
        this.pixelsRecordSetProvider = requireNonNull(pixelsRecordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
                                                ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns) {
        return new PixelsPageSource(pixelsRecordSetProvider.getRecordSet(transactionHandle, session, split, columns));
    }
}
