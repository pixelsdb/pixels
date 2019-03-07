package cn.edu.ruc.iir.pixels.presto;

import cn.edu.ruc.iir.pixels.cache.MemoryMappedFile;
import cn.edu.ruc.iir.pixels.common.physical.FSFactory;
import cn.edu.ruc.iir.pixels.presto.impl.PixelsPrestoConfig;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Provider Class for Pixels Page Source class.
 */
public class PixelsPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger logger = LogManager.getLogger(PixelsPageSourceProvider.class);
    private final String connectorId;
    private final FSFactory fsFactory;
    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;

    @Inject
    public PixelsPageSourceProvider(PixelsConnectorId connectorId, PixelsPrestoConfig config)
            throws Exception
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.fsFactory = requireNonNull(config.getFsFactory(), "fsFactory is null");
        if (config.getConfigFactory().getProperty("cache.enabled").equalsIgnoreCase("true")) {
            this.cacheFile = new MemoryMappedFile(
                    config.getConfigFactory().getProperty("cache.location"),
                    Long.parseLong(config.getConfigFactory().getProperty("cache.size")));
            this.indexFile = new MemoryMappedFile(
                    config.getConfigFactory().getProperty("index.location"),
                    Long.parseLong(config.getConfigFactory().getProperty("index.size")));
        }
        else {
            this.cacheFile = null;
            this.indexFile = null;
        }
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
                                                ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns) {
        List<PixelsColumnHandle> pixelsColumns = columns.stream()
                .map(PixelsColumnHandle.class::cast)
                .collect(toList());
        requireNonNull(split, "split is null");
        PixelsSplit pixelsSplit = (PixelsSplit) split;
        checkArgument(pixelsSplit.getConnectorId().equals(connectorId), "connectorId is not for this connector");

        return new PixelsPageSource(pixelsSplit, pixelsColumns, fsFactory, cacheFile, indexFile, connectorId);
    }
}
