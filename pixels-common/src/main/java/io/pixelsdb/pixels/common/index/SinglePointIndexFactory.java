/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.common.index;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.ShutdownHookManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2025-02-08
 */
public class SinglePointIndexFactory
{
    private static final Logger logger = LogManager.getLogger(SinglePointIndexFactory.class);
    private final Map<TableIndex, Map<Integer, SinglePointIndex>> singlePointIndexImpls = new ConcurrentHashMap<>();
    private final Set<SinglePointIndex.Scheme> enabledSchemes = new ConcurrentSkipListSet<>();
    private final Map<Long, TableIndex> indexIdToTableIndex = new ConcurrentHashMap<>();
    private final Lock lock = new ReentrantLock();
    /**
     * The providers of the enabled single point index schemes.
     */
    private final ImmutableMap<SinglePointIndex.Scheme, SinglePointIndexProvider> singlePointIndexProviders;

    private SinglePointIndexFactory()
    {
        String value = ConfigFactory.Instance().getProperty("enabled.single.point.index.schemes");
        requireNonNull(value, "enabled.single.point.index.schemes is not configured");
        String[] schemeNames = value.trim().split(",");
        checkArgument(schemeNames.length > 0,
                "at least one single point index scheme must be enabled");

        ImmutableMap.Builder<SinglePointIndex.Scheme, SinglePointIndexProvider> providersBuilder = ImmutableMap.builder();
        ServiceLoader<SinglePointIndexProvider> providerLoader = ServiceLoader.load(SinglePointIndexProvider.class);
        for (String name : schemeNames)
        {
            SinglePointIndex.Scheme scheme = SinglePointIndex.Scheme.from(name);
            this.enabledSchemes.add(scheme);
            boolean providerExists = false;
            for (SinglePointIndexProvider singlePointIndexProvider : providerLoader)
            {
                if (singlePointIndexProvider.compatibleWith(scheme))
                {
                    providersBuilder.put(scheme, singlePointIndexProvider);
                    providerExists = true;
                    break;
                }
            }
            if (!providerExists)
            {
                // only log a warning, do not throw exception.
                logger.warn("no single point index provider exists for scheme: {}", scheme.name());
            }
        }
        this.singlePointIndexProviders = providersBuilder.build();
    }

    private static volatile SinglePointIndexFactory instance = null;

    public static SinglePointIndexFactory Instance()
    {
        if (instance == null)
        {
            synchronized (SinglePointIndexFactory.class)
            {
                if (instance == null)
                {
                    instance = new SinglePointIndexFactory();
                    ShutdownHookManager.Instance().registerShutdownHook(SinglePointIndexFactory.class, false, () -> {
                        try
                        {
                            instance.closeAll();
                        }
                        catch (SinglePointIndexException e)
                        {
                            logger.error("Failed to close all single point index instances.", e);
                            e.printStackTrace();
                        }
                    });
                }
            }
        }
        return instance;
    }

    public List<SinglePointIndex.Scheme> getEnabledSchemes()
    {
        return ImmutableList.copyOf(this.enabledSchemes);
    }

    public boolean isSchemeEnabled(SinglePointIndex.Scheme scheme)
    {
        return this.enabledSchemes.contains(scheme);
    }

    /**
     * Get the single point index instance.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @return the single point index instance
     * @throws SinglePointIndexException
     */
    public SinglePointIndex getSinglePointIndex(long tableId, long indexId, IndexOption indexOption) throws SinglePointIndexException
    {
        TableIndex tableIndex = this.indexIdToTableIndex.get(indexId);
        if (tableIndex == null)
        {
            this.lock.lock();
            try
            {
                // double check to avoid redundant tableIndex creation
                tableIndex = this.indexIdToTableIndex.get(indexId);
                if (tableIndex == null)
                {
                    io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex spi =
                            MetadataService.Instance().getSinglePointIndex(indexId);
                    if (spi == null)
                    {
                        throw new SinglePointIndexException("single point index with id " + indexId + " does not exist");
                    }
                    tableIndex = new TableIndex(tableId, indexId, spi.getIndexScheme(), spi.isUnique());
                    this.indexIdToTableIndex.put(indexId, tableIndex);
                }
            } catch (MetadataException e)
            {
                throw new SinglePointIndexException("failed to query single point index information from metadata", e);
            }
            finally
            {
                this.lock.unlock();
            }
        }
        return getSinglePointIndex(tableIndex, indexOption);
    }

    /**
     * Get the single point index instance.
     * @param tableIndex the object contains the table id, index id, and index scheme
     * @return the single point index instance
     * @throws SinglePointIndexException
     */
    public SinglePointIndex getSinglePointIndex(TableIndex tableIndex, IndexOption indexOption) throws SinglePointIndexException
    {
        requireNonNull(tableIndex, "tableIndex is null");
        checkArgument(this.enabledSchemes.contains(tableIndex.scheme), "single point index scheme '" +
                tableIndex.scheme.toString() + "' is not enabled.");

        this.indexIdToTableIndex.putIfAbsent(tableIndex.indexId, tableIndex);

        Map<Integer, SinglePointIndex> vNodeMap = this.singlePointIndexImpls.computeIfAbsent(
                tableIndex, k -> new ConcurrentHashMap<>());

        int vNodeId = indexOption.getVNodeId();
        SinglePointIndex singlePointIndex = vNodeMap.get(vNodeId);

        if (singlePointIndex == null)
        {
            this.lock.lock();
            try
            {
                // double check to avoid redundant creation of singlePointIndex
                singlePointIndex = vNodeMap.get(vNodeId);
                if (singlePointIndex == null)
                {
                    logger.info("Creating SinglePointIndex instance for tableId: {}, indexId: {}, vNodeId: {}",
                            tableIndex.tableId, tableIndex.indexId, vNodeId);
                    singlePointIndex = this.singlePointIndexProviders.get(tableIndex.getScheme()).createInstance(
                            tableIndex.tableId, tableIndex.indexId, tableIndex.scheme, tableIndex.isUnique(), indexOption);
                    vNodeMap.put(vNodeId, singlePointIndex);
                }
            }
            finally
            {
                this.lock.unlock();
            }
        }
        return singlePointIndex;
    }

    /**
     * Close all the opened single point index instances.
     * @throws IOException
     */
    public void closeAll() throws SinglePointIndexException
    {
        this.lock.lock();
        try
        {
            for (Map.Entry<TableIndex, Map<Integer, SinglePointIndex>> outerEntry : this.singlePointIndexImpls.entrySet())
            {
                TableIndex tableIndex = outerEntry.getKey();
                Map<Integer, SinglePointIndex> vNodeMap = outerEntry.getValue();

                if (vNodeMap != null)
                {
                    // Iterate through all SinglePointIndex instances for each vNodeId
                    for (SinglePointIndex indexImpl : vNodeMap.values())
                    {
                        try
                        {
                            if (indexImpl != null)
                            {
                                indexImpl.close();
                            }
                        }
                        catch (IOException e)
                        {
                            // Note: As per original logic, an exception here stops the closing process
                            throw new SinglePointIndexException(
                                    "failed to close single point index with id " + tableIndex.indexId, e);
                        }
                    }
                }
            }

            // Clear all tracking maps after successful closing
            this.singlePointIndexImpls.clear();
            this.indexIdToTableIndex.clear();
        }
        finally
        {
            this.lock.unlock();
        }
    }

    /**
     * Close the single point index.
     * @param tableId the table id of the single point index
     * @param indexId the index id of the single point index
     * @param closeAndRemove remove the index storage after closing if true
     * @throws SinglePointIndexException
     */
    public void closeIndex(long tableId, long indexId, boolean closeAndRemove, IndexOption indexOption) throws SinglePointIndexException
    {
        // indexOption is ignored as per requirement to close all vNodes for this index
        this.lock.lock();
        try
        {
            // 1. Identify the TableIndex
            TableIndex tableIndex = this.indexIdToTableIndex.remove(indexId);
            if (tableIndex == null)
            {
                // Fallback: create a dummy TableIndex if not found in the tracker
                tableIndex = new TableIndex(tableId, indexId, null, false);
            }

            // 2. Remove the entire inner map (all vNodes) from the implementation map
            Map<Integer, SinglePointIndex> vNodeMap = this.singlePointIndexImpls.remove(tableIndex);

            if (vNodeMap != null)
            {
                // 3. Iterate through all SinglePointIndex instances for all vNodes
                for (Map.Entry<Integer, SinglePointIndex> entry : vNodeMap.entrySet())
                {
                    int vNodeId = entry.getKey();
                    SinglePointIndex indexImpl = entry.getValue();

                    if (indexImpl != null)
                    {
                        try
                        {
                            if (closeAndRemove)
                            {
                                indexImpl.closeAndRemove();
                            }
                            else
                            {
                                indexImpl.close();
                            }
                        }
                        catch (IOException e)
                        {
                            // Note: If one vNode fails to close, we still throw to notify the caller,
                            // but the mappings have already been removed from the factory.
                            throw new SinglePointIndexException(
                                    "failed to close single point index with id " + indexId + " for vNode " + vNodeId, e);
                        }
                    }
                }
            }
            else
            {
                logger.warn("No active index instances found for indexId {} during close operation", indexId);
            }
        }
        finally
        {
            this.lock.unlock();
        }
    }

    public static class TableIndex
    {
        private final long tableId;
        private final long indexId;
        private final SinglePointIndex.Scheme scheme;
        private final boolean unique;

        public TableIndex(long tableId, long indexId, SinglePointIndex.Scheme scheme, boolean unique)
        {
            this.tableId = tableId;
            this.indexId = indexId;
            this.scheme = scheme;
            this.unique = unique;
        }

        public long getTableId()
        {
            return tableId;
        }

        public long getIndexId()
        {
            return indexId;
        }

        public SinglePointIndex.Scheme getScheme()
        {
            return scheme;
        }

        public boolean isUnique()
        {
            return unique;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableId, indexId);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof TableIndex))
            {
                return false;
            }
            TableIndex that = (TableIndex) obj;
            return this.indexId == that.indexId && this.tableId == that.tableId;
        }
    }
}
