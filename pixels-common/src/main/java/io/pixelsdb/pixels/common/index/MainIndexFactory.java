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
import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author Rolland1944, hank
 * @create 2025-06-24
 * @update 2025-07-20 rename from MainIndexManager to MainIndexFactory
 */
public class MainIndexFactory
{
    private static final Logger logger = LogManager.getLogger(MainIndexFactory.class);
    private final Map<Long, MainIndex> mainIndexImpls = new HashMap<>();
    private final Set<MainIndex.Scheme> enabledSchemes = new TreeSet<>();
    /**
     * The providers of the enabled main index schemes.
     */
    private final ImmutableMap<MainIndex.Scheme, MainIndexProvider> mainIndexProviders;

    private MainIndexFactory()
    {
        String value = ConfigFactory.Instance().getProperty("enabled.main.index.schemes");
        requireNonNull(value, "enabled.main.index.schemes is not configured");
        String[] schemeNames = value.trim().split(",");
        checkArgument(schemeNames.length > 0,
                "at lease one main index scheme must be enabled");

        ImmutableMap.Builder<MainIndex.Scheme, MainIndexProvider> providersBuilder = ImmutableMap.builder();
        ServiceLoader<MainIndexProvider> providerLoader = ServiceLoader.load(MainIndexProvider.class);
        for (String name : schemeNames)
        {
            MainIndex.Scheme scheme = MainIndex.Scheme.from(name);
            this.enabledSchemes.add(scheme);
            boolean providerExists = false;
            for (MainIndexProvider mainIndexProvider : providerLoader)
            {
                if (mainIndexProvider.compatibleWith(scheme))
                {
                    providersBuilder.put(scheme, mainIndexProvider);
                    providerExists = true;
                    break;
                }
            }
            if (!providerExists)
            {
                // only log a warning, do not throw exception.
                logger.warn("no main index provider exists for scheme: {}", scheme.name());
            }
        }
        this.mainIndexProviders = providersBuilder.build();
    }

    private static MainIndexFactory instance = null;

    public static MainIndexFactory Instance()
    {
        if (instance == null)
        {
            instance = new MainIndexFactory();
            Runtime.getRuntime().addShutdownHook(new Thread(()->
            {
                try
                {
                    instance.closeAll();
                } catch (IOException e)
                {
                    logger.error("Failed to close all main index instances.", e);
                    e.printStackTrace();
                }
            }));
        }
        return instance;
    }

    public List<MainIndex.Scheme> getEnabledSchemes()
    {
        return ImmutableList.copyOf(this.enabledSchemes);
    }

    public boolean isSchemeEnabled(MainIndex.Scheme scheme)
    {
        return this.enabledSchemes.contains(scheme);
    }

    /**
     * Get the main index instance.
     * @param tableId the table id of the index
     * @param scheme the scheme of the index
     * @return the main index instance
     * @throws SinglePointIndexException
     */
    public synchronized MainIndex getMainIndex(long tableId, @Nonnull MainIndex.Scheme scheme) throws MainIndexException
    {
        checkArgument(this.enabledSchemes.contains(scheme), "main index scheme '" +
                scheme + "' is not enabled.");
        if (mainIndexImpls.containsKey(tableId))
        {
            return mainIndexImpls.get(tableId);
        }

        MainIndex mainIndex = this.mainIndexProviders.get(scheme).createInstance(tableId, scheme);
        mainIndexImpls.put(tableId, mainIndex);

        return mainIndex;
    }

    /**
     * Close all the opened main index instances.
     * @throws IOException
     */
    public synchronized void closeAll() throws IOException
    {
        for (long tableId : mainIndexImpls.keySet())
        {
            mainIndexImpls.get(tableId).close();
        }
    }
}
