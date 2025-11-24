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

import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.ShutdownHookManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * @author Rolland1944, hank
 * @create 2025-06-24
 * @update 2025-07-20 rename from MainIndexManager to MainIndexFactory
 */
public class MainIndexFactory
{
    private static final Logger logger = LogManager.getLogger(MainIndexFactory.class);
    private final Map<Long, MainIndex> mainIndexImpls = new ConcurrentHashMap<>();
    private final MainIndex.Scheme enabledScheme;
    /**
     * The providers of the enabled main index schemes.
     */
    private MainIndexProvider mainIndexProvider;

    private MainIndexFactory() throws MainIndexException
    {
        String name = ConfigFactory.Instance().getProperty("enabled.main.index.scheme");
        requireNonNull(name, "enabled.main.index.scheme is not configured");

        ServiceLoader<MainIndexProvider> providerLoader = ServiceLoader.load(MainIndexProvider.class);
        this.enabledScheme = MainIndex.Scheme.from(name);
        boolean providerExists = false;
        for (MainIndexProvider mainIndexProvider : providerLoader)
        {
            if (mainIndexProvider.compatibleWith(enabledScheme))
            {
                this.mainIndexProvider = mainIndexProvider;
                providerExists = true;
                break;
            }
        }
        if (!providerExists)
        {
            throw new MainIndexException("no main index provider exists for enabled scheme " + enabledScheme.name());
        }
    }

    private static volatile MainIndexFactory instance = null;

    public static MainIndexFactory Instance() throws MainIndexException
    {
        if (instance == null)
        {
            synchronized (MainIndexFactory.class)
            {
                if (instance == null)
                {
                    instance = new MainIndexFactory();
                    ShutdownHookManager.Instance().registerShutdownHook(MainIndexFactory.class, false, () ->
                    {
                        try
                        {
                            instance.closeAll();
                        } catch (MainIndexException e)
                        {
                            logger.error("Failed to close all main index instances.", e);
                            e.printStackTrace();
                        }
                    });
                }
            }
        }
        return instance;
    }

    public MainIndex.Scheme getEnabledScheme()
    {
        return this.enabledScheme;
    }

    public boolean isSchemeEnabled(MainIndex.Scheme scheme)
    {
        return this.enabledScheme == scheme;
    }

    /**
     * Get the main index instance.
     * @param tableId the table id of the index
     * @return the main index instance
     * @throws SinglePointIndexException
     */
    public MainIndex getMainIndex(long tableId) throws MainIndexException
    {
        MainIndex mainIndex = this.mainIndexImpls.get(tableId);
        if (mainIndex == null)
        {
            synchronized (this)
            {
                // double check to avoid redundant creation of mainIndex
                mainIndex = this.mainIndexImpls.get(tableId);
                if (mainIndex == null)
                {
                    mainIndex = this.mainIndexProvider.createInstance(tableId, this.enabledScheme);
                    this.mainIndexImpls.put(tableId, mainIndex);
                }
            }
        }
        return mainIndex;
    }

    /**
     * Close all the opened main index instances.
     * @throws IOException
     */
    public synchronized void closeAll() throws MainIndexException
    {
        for (long tableId : mainIndexImpls.keySet())
        {
            try
            {
                MainIndex removing = mainIndexImpls.get(tableId);
                if (removing != null)
                {
                    removing.close();
                }
            }
            catch (IOException e)
            {
                throw new MainIndexException(
                        "failed to close main index of table " + tableId, e);
            }
        }
        mainIndexImpls.clear();
    }

    /**
     * Close the main index.
     * @param tableId the  id of the main index
     * @param closeAndRemove remove the index storage after closing if true
     * @throws MainIndexException
     */
    public synchronized void closeIndex(long tableId, boolean closeAndRemove) throws MainIndexException
    {
        MainIndex removed = mainIndexImpls.remove(tableId);
        if (removed != null)
        {
            try
            {
                if (closeAndRemove)
                {
                    removed.closeAndRemove();
                }
                else
                {
                    removed.close();
                }
            }
            catch (IOException e)
            {
                throw new MainIndexException(
                        "failed to close main index of table " + tableId, e);
            }
        }
    }
}
