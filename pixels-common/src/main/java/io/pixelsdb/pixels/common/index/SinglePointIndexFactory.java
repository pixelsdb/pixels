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
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2025-02-08
 */
public class SinglePointIndexFactory
{
    private static final Logger logger = LogManager.getLogger(SinglePointIndexFactory.class);
    private final Map<TableIndex, SinglePointIndex> singlePointIndexImpls = new HashMap<>();
    private final Set<SinglePointIndex.Scheme> enabledSchemes = new TreeSet<>();
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

    private static SinglePointIndexFactory instance = null;

    public static SinglePointIndexFactory Instance()
    {
        if (instance == null)
        {
            instance = new SinglePointIndexFactory();
            Runtime.getRuntime().addShutdownHook(new Thread(()->
            {
                try
                {
                    instance.closeAll();
                } catch (IOException e)
                {
                    logger.error("Failed to close all single point index instances.", e);
                    e.printStackTrace();
                }
            }));
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
     * @param scheme the scheme of the index
     * @return the single point index instance
     * @throws SinglePointIndexException
     */
    public synchronized SinglePointIndex getSinglePointIndex(long tableId, long indexId, SinglePointIndex.Scheme scheme) throws SinglePointIndexException
    {
        // 'synchronized' in Java is reentrant,  it is fine to call the other getSinglePointIndex() from here.
        return getSinglePointIndex(new TableIndex(tableId, indexId, scheme));
    }

    /**
     * Get the single point index instance.
     * @param tableIndex the object contains the table id, index id, and index scheme
     * @return the single point index instance
     * @throws SinglePointIndexException
     */
    public synchronized SinglePointIndex getSinglePointIndex(TableIndex tableIndex) throws SinglePointIndexException
    {
        requireNonNull(tableIndex, "tableIndex is null");
        checkArgument(this.enabledSchemes.contains(tableIndex.scheme), "single point index scheme '" +
                tableIndex.scheme.toString() + "' is not enabled.");
        if (singlePointIndexImpls.containsKey(tableIndex))
        {
            return singlePointIndexImpls.get(tableIndex);
        }

        SinglePointIndex singlePointIndex = this.singlePointIndexProviders.get(tableIndex.getScheme())
                .createInstance(tableIndex.tableId, tableIndex.indexId, tableIndex.scheme);
        singlePointIndexImpls.put(tableIndex, singlePointIndex);

        return singlePointIndex;
    }

    /**
     * Close all the opened single point index instances.
     * @throws IOException
     */
    public synchronized void closeAll() throws IOException
    {
        for (TableIndex tableIndex : singlePointIndexImpls.keySet())
        {
            singlePointIndexImpls.get(tableIndex).close();
        }
    }

    public static class TableIndex
    {
        private final long tableId;
        private final long indexId;
        private final SinglePointIndex.Scheme scheme;

        public TableIndex(long tableId, long indexId, SinglePointIndex.Scheme scheme)
        {
            this.tableId = tableId;
            this.indexId = indexId;
            this.scheme = scheme;
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

        @Override
        public int hashCode()
        {
            return Objects.hash(tableId, indexId, scheme);
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
            return this.indexId == that.indexId &&
                    this.tableId == that.tableId &&
                    this.scheme == that.scheme;
        }
    }
}
