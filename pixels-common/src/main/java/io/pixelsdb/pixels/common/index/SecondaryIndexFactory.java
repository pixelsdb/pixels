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
public class SecondaryIndexFactory
{
    private static final Logger logger = LogManager.getLogger(SecondaryIndexFactory.class);
    private final Map<SecondaryIndex.Scheme, SecondaryIndex> secondaryIndexImpls = new HashMap<>();
    private final Set<SecondaryIndex.Scheme> enabledSchemes = new TreeSet<>();
    /**
     * The providers of the enabled secondary index schemes.
     */
    private final ImmutableMap<SecondaryIndex.Scheme, SecondaryIndexProvider> secondaryIndexProviders;

    private SecondaryIndexFactory()
    {
        String value = ConfigFactory.Instance().getProperty("enabled.secondary.index.schemes");
        requireNonNull(value, "enabled.secondary.index.schemes is not configured");
        String[] schemeNames = value.trim().split(",");
        checkArgument(schemeNames.length > 0,
                "at lease one secondary index scheme must be enabled");
        ImmutableMap.Builder<SecondaryIndex.Scheme, SecondaryIndexProvider> providersBuilder = ImmutableMap.builder();
        ServiceLoader<SecondaryIndexProvider> providerLoader = ServiceLoader.load(SecondaryIndexProvider.class);
        for (String name : schemeNames)
        {
            SecondaryIndex.Scheme scheme = SecondaryIndex.Scheme.from(name);
            this.enabledSchemes.add(scheme);
            boolean providerExists = false;
            for (SecondaryIndexProvider secondaryIndexProvider : providerLoader)
            {
                if (secondaryIndexProvider.compatibleWith(scheme))
                {
                    providersBuilder.put(scheme, secondaryIndexProvider);
                    providerExists = true;
                    break;
                }
            }
            if (!providerExists)
            {
                // only log a warning, do not throw exception.
                logger.warn(String.format(
                        "no secondary index provider exists for scheme: %s", scheme.name()));
            }
        }
        this.secondaryIndexProviders = providersBuilder.build();
    }

    private static SecondaryIndexFactory instance = null;

    public static SecondaryIndexFactory Instance()
    {
        if (instance == null)
        {
            instance = new SecondaryIndexFactory();
            Runtime.getRuntime().addShutdownHook(new Thread(()->
            {
                try
                {
                    instance.closeAll();
                } catch (IOException e)
                {
                    logger.error("Failed to close all secondary index instances.", e);
                    e.printStackTrace();
                }
            }));
        }
        return instance;
    }

    public List<SecondaryIndex.Scheme> getEnabledSchemes()
    {
        return ImmutableList.copyOf(this.enabledSchemes);
    }

    public boolean isEnabled(SecondaryIndex.Scheme scheme)
    {
        return this.enabledSchemes.contains(scheme);
    }

    /**
     * Recreate all the enabled {@link SecondaryIndex} instances.
     * <b>Be careful:</b> all the Storage enabled Storage must be configured well before
     * calling this method. It is better to call {@link #reload(SecondaryIndex.Scheme)} to reload
     * the Storage that you are sure it is configured or does not need any dynamic configuration.
     *
     * @throws IOException
     */
    @Deprecated
    public synchronized void reloadAll() throws IOException
    {
        for (SecondaryIndex.Scheme scheme : enabledSchemes)
        {
            reload(scheme);
        }
    }

    /**
     * Recreate the {@link SecondaryIndex} instance for the given storage scheme.
     *
     * @param scheme the given storage scheme
     * @throws IOException
     */
    public synchronized void reload(SecondaryIndex.Scheme scheme) throws IOException
    {
        SecondaryIndex secondaryIndex = this.secondaryIndexImpls.remove(scheme);
        if (secondaryIndex != null)
        {
            secondaryIndex.close();
        }
        secondaryIndex = this.getSecondaryIndex(scheme);
        requireNonNull(scheme, "failed to create secondary index instance");
        this.secondaryIndexImpls.put(scheme, secondaryIndex);
    }

    /**
     * Get the secondary index instance from a scheme name.
     * @param scheme
     * @return
     * @throws IOException
     */
    public synchronized SecondaryIndex getSecondaryIndex(String scheme) throws IOException
    {
        try
        {
            // 'synchronized' in Java is reentrant,
            // it is fine to call the other getSecondaryIndex() from here.
            return getSecondaryIndex(SecondaryIndex.Scheme.from(scheme));
        }
        catch (RuntimeException re)
        {
            throw new IOException("Invalid secondary index scheme: " + scheme, re);
        }
    }

    public synchronized SecondaryIndex getSecondaryIndex(SecondaryIndex.Scheme scheme) throws IOException
    {
        checkArgument(this.enabledSchemes.contains(scheme), "secondary index scheme '" +
                scheme.toString() + "' is not enabled.");
        if (secondaryIndexImpls.containsKey(scheme))
        {
            return secondaryIndexImpls.get(scheme);
        }

        SecondaryIndex secondaryIndex = this.secondaryIndexProviders.get(scheme).createInstance(scheme);
        secondaryIndexImpls.put(scheme, secondaryIndex);

        return secondaryIndex;
    }

    public ImmutableMap<SecondaryIndex.Scheme, SecondaryIndexProvider> getStorageProviders()
    {
        return secondaryIndexProviders;
    }

    public synchronized void closeAll() throws IOException
    {
        for (SecondaryIndex.Scheme scheme : secondaryIndexImpls.keySet())
        {
            secondaryIndexImpls.get(scheme).close();
        }
    }
}
