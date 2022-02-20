/*
 * Copyright 2018 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.presto;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.*;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.transaction.QueryTransInfo;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;
import io.pixelsdb.pixels.presto.impl.PixelsPrestoConfig;
import io.pixelsdb.pixels.presto.properties.PixelsSessionProperties;
import io.pixelsdb.pixels.presto.properties.PixelsTableProperties;

import javax.inject.Inject;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class PixelsConnector
        implements Connector {
    private static final Logger logger = Logger.get(PixelsConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final PixelsMetadata metadata;
    private final PixelsSplitManager splitManager;
    private final boolean recordCursorEnabled;
    private final PixelsPageSourceProvider pageSourceProvider;
    private final PixelsRecordSetProvider recordSetProvider;
    private final PixelsSessionProperties sessionProperties;
    private final PixelsTableProperties tableProperties;
    private TransService transService;

    @Inject
    public PixelsConnector(
            LifeCycleManager lifeCycleManager,
            PixelsMetadata metadata,
            PixelsSplitManager splitManager,
            PixelsPrestoConfig config,
            PixelsPageSourceProvider pageSourceProvider,
            PixelsRecordSetProvider recordSetProvider,
            PixelsSessionProperties sessionProperties,
            PixelsTableProperties tableProperties) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "recordSetProvider is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
        requireNonNull(config, "config is null");
        this.recordCursorEnabled = Boolean.parseBoolean(config.getConfigFactory().getProperty("record.cursor.enabled"));
        this.transService = new TransService(config.getConfigFactory().getProperty("trans.server.host"),
                Integer.parseInt(config.getConfigFactory().getProperty("trans.server.port")));
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        /**
         * Issue #172:
         * Be careful that Presto does not set readOnly to true for normal queries.
         */
        QueryTransInfo info;
        try
        {
            info = this.transService.getQueryTransInfo();
        } catch (TransException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_TRANS_SERVICE_ERROR, e);
        }
        TransContext.Instance().beginQuery(info);
        return new PixelsTransactionHandle(info.getQueryId(), info.getQueryTimestamp());
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        if (transactionHandle instanceof PixelsTransactionHandle)
        {
            PixelsTransactionHandle handle = (PixelsTransactionHandle) transactionHandle;
            TransContext.Instance().commitQuery(handle.getTransId());
        } else
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_TRANS_HANDLE_TYPE_ERROR,
                    "The transaction handle is not an instance of PixelsTransactionHandle.");
        }
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        if (transactionHandle instanceof PixelsTransactionHandle)
        {
            PixelsTransactionHandle handle = (PixelsTransactionHandle) transactionHandle;
            TransContext.Instance().rollbackQuery(handle.getTransId());
        } else
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_TRANS_HANDLE_TYPE_ERROR,
                    "The transaction handle is not an instance of PixelsTransactionHandle.");
        }
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support reading tables page at a time
     */
    @Override
    public PixelsPageSourceProvider getPageSourceProvider() {
        if (this.recordCursorEnabled)
        {
            throw new UnsupportedOperationException();
        }
        return pageSourceProvider;
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support reading tables record at a time
     */
    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        if (this.recordCursorEnabled)
        {
            return recordSetProvider;
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public final void shutdown() {
        try {
            lifeCycleManager.stop();
        } catch (Exception e) {
            logger.error(e, "error in shutting down connector");
            throw new PrestoException(PixelsErrorCode.PIXELS_CONNECTOR_ERROR, e);
        }
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties.getSessionProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties.getTableProperties();
    }
}
