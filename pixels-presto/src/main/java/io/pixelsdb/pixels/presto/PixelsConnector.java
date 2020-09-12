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
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class PixelsConnector
        implements Connector {
    private static final Logger logger = Logger.get(PixelsConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final PixelsMetadata metadata;
    private final PixelsSplitManager splitManager;
    private final PixelsPageSourceProvider pageSourceProvider;

    @Inject
    public PixelsConnector(
            LifeCycleManager lifeCycleManager,
            PixelsMetadata metadata,
            PixelsSplitManager splitManager,
            PixelsPageSourceProvider pageSourceProvider) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        return PixelsTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public PixelsPageSourceProvider getPageSourceProvider() {
        return pageSourceProvider;
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
}
