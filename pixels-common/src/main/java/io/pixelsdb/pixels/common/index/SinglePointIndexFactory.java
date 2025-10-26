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
    private final Map<TableIndex, SinglePointIndex> singlePointIndexImpls = new ConcurrentHashMap<>();
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
                "at lease one single point index scheme must be enabled");

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
                    Runtime.getRuntime().addShutdownHook(new Thread(()->
                    {
                        try
                        {
                            instance.closeAll();
                        }
                        catch (SinglePointIndexException e)
                        {
                            logger.error("Failed to close all single point index instances.", e);
                            e.printStackTrace();
                        }
                    }));
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
    public SinglePointIndex getSinglePointIndex(long tableId, long indexId) throws SinglePointIndexException
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
        return getSinglePointIndex(tableIndex);
    }

    /**
     * Get the single point index instance.
     * @param tableIndex the object contains the table id, index id, and index scheme
     * @return the single point index instance
     * @throws SinglePointIndexException
     */
    public SinglePointIndex getSinglePointIndex(TableIndex tableIndex) throws SinglePointIndexException
    {
        requireNonNull(tableIndex, "tableIndex is null");
        checkArgument(this.enabledSchemes.contains(tableIndex.scheme), "single point index scheme '" +
                tableIndex.scheme.toString() + "' is not enabled.");

        this.indexIdToTableIndex.putIfAbsent(tableIndex.indexId, tableIndex);

        SinglePointIndex singlePointIndex = this.singlePointIndexImpls.get(tableIndex);
        if (singlePointIndex == null)
        {
            this.lock.lock();
            try
            {
                // double check to avoid redundant creation of singlePointIndex
                singlePointIndex = this.singlePointIndexImpls.get(tableIndex);
                if (singlePointIndex == null)
                {
                    singlePointIndex = this.singlePointIndexProviders.get(tableIndex.getScheme()).createInstance(
                            tableIndex.tableId, tableIndex.indexId, tableIndex.scheme, tableIndex.isUnique());
                    this.singlePointIndexImpls.put(tableIndex, singlePointIndex);
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
            for (TableIndex tableIndex : this.singlePointIndexImpls.keySet())
            {
                try
                {
                    SinglePointIndex removing = this.singlePointIndexImpls.get(tableIndex);
                    if (removing != null)
                    {
                        removing.close();
                    }
                } catch (IOException e)
                {
                    throw new SinglePointIndexException(
                            "failed to close single point index with id " + tableIndex.indexId, e);
                }
            }
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
    public void closeIndex(long tableId, long indexId, boolean closeAndRemove) throws SinglePointIndexException
    {
        this.lock.lock();
        try
        {
            TableIndex tableIndex = this.indexIdToTableIndex.remove(indexId);
            if (tableIndex != null)
            {
                SinglePointIndex removed = this.singlePointIndexImpls.remove(tableIndex);
                if (removed != null)
                {
                    try
                    {
                        if (closeAndRemove)
                        {
                            removed.closeAndRemove();
                        } else
                        {
                            removed.close();
                        }
                    } catch (IOException e)
                    {
                        throw new SinglePointIndexException(
                                "failed to close single point index with id " + tableIndex.indexId, e);
                    }
                } else
                {
                    logger.warn("index with id {} once opened but not found", indexId);
                }
            } else
            {
                TableIndex tableIndex1 = new TableIndex(tableId, indexId, null, false);
                SinglePointIndex removed = this.singlePointIndexImpls.remove(tableIndex1);
                if (removed != null)
                {
                    try
                    {
                        if (closeAndRemove)
                        {
                            removed.closeAndRemove();
                        } else
                        {
                            removed.close();
                        }
                    } catch (IOException e)
                    {
                        throw new SinglePointIndexException(
                                "failed to close single point index with id " + tableIndex.indexId, e);
                    }
                    logger.warn("index with id {} is found but not opened properly", indexId);
                }
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

    public synchronized void printCacheStats()
    {
        for (Map.Entry<TableIndex, SinglePointIndex> entry : singlePointIndexImpls.entrySet())
        {
            SinglePointIndex spi = entry.getValue();
            if (spi instanceof CachingSinglePointIndex)
            {
                CachingSinglePointIndex cachingSPI = (CachingSinglePointIndex) spi;
                System.out.println(cachingSPI.getCacheStats());
            }
        }
    }
}
