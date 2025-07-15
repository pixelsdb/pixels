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
    private final Map<SinglePointIndex.Scheme, SinglePointIndex> singlePointIndexImpls = new HashMap<>();
    private final Set<SinglePointIndex.Scheme> enabledSchemes = new TreeSet<>();
    private final Set<Long> tableIds = new TreeSet<>();
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

    public boolean isEnabled(SinglePointIndex.Scheme scheme)
    {
        return this.enabledSchemes.contains(scheme);
    }

    /**
     * Recreate all the enabled {@link SinglePointIndex} instances.
     * <b>Be careful:</b> all the Storage enabled Storage must be configured well before
     * calling this method. It is better to call {@link #reload(SinglePointIndex.Scheme,long tableId)} to reload
     * the Storage that you are sure it is configured or does not need any dynamic configuration.
     *
     * @throws IOException
     */
    @Deprecated
    public synchronized void reloadAll() throws IOException
    {
        for (SinglePointIndex.Scheme scheme : enabledSchemes)
        {
            for(long tableId : tableIds) {
                reload(scheme,tableId);
            }
        }
    }

    /**
     * Recreate the {@link SinglePointIndex} instance for the given storage scheme.
     *
     * @param scheme the given storage scheme
     * @throws IOException
     */
    public synchronized void reload(SinglePointIndex.Scheme scheme, long tableId) throws IOException
    {
        SinglePointIndex singlePointIndex = this.singlePointIndexImpls.remove(scheme);
        if (singlePointIndex != null)
        {
            singlePointIndex.close();
        }
        singlePointIndex = this.getSinglePointIndex(scheme, tableId);
        requireNonNull(scheme, "failed to create the single point index instance");
        this.singlePointIndexImpls.put(scheme, singlePointIndex);
    }

    /**
     * Get the single point index instance from a scheme name.
     * @param scheme
     * @return
     * @throws IOException
     */
    public synchronized SinglePointIndex getSinglePointIndex(String scheme, long tableId) throws IOException
    {
        try
        {
            // 'synchronized' in Java is reentrant,
            // it is fine to call the other getSinglePointIndex() from here.
            return getSinglePointIndex(SinglePointIndex.Scheme.from(scheme), tableId);
        }
        catch (RuntimeException re)
        {
            throw new IOException("Invalid single point index scheme: " + scheme, re);
        }
    }

    public synchronized SinglePointIndex getSinglePointIndex(SinglePointIndex.Scheme scheme, long tableId) throws IOException
    {
        checkArgument(this.enabledSchemes.contains(scheme), "single point index scheme '" +
                scheme.toString() + "' is not enabled.");
        if (singlePointIndexImpls.containsKey(scheme))
        {
            return singlePointIndexImpls.get(scheme);
        }

        SinglePointIndex singlePointIndex = this.singlePointIndexProviders.get(scheme).createInstance(scheme, tableId);
        singlePointIndexImpls.put(scheme, singlePointIndex);
        tableIds.add(tableId);

        return singlePointIndex;
    }

    public ImmutableMap<SinglePointIndex.Scheme, SinglePointIndexProvider> getStorageProviders()
    {
        return singlePointIndexProviders;
    }

    public synchronized void closeAll() throws IOException
    {
        for (SinglePointIndex.Scheme scheme : singlePointIndexImpls.keySet())
        {
            singlePointIndexImpls.get(scheme).close();
        }
    }
}
