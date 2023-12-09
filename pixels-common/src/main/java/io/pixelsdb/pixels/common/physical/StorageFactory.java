/*
 * Copyright 2021 PixelsDB.
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
package io.pixelsdb.pixels.common.physical;

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
 * @create 2021-08-20
 * @author hank
 */
public class StorageFactory
{
    private static final Logger logger = LogManager.getLogger(StorageFactory.class);
    private final Map<Storage.Scheme, Storage> storageImpls = new HashMap<>();
    private final Set<Storage.Scheme> enabledSchemes = new TreeSet<>();
    /**
     * The providers of the enabled storage schemes.
     */
    private final ImmutableMap<Storage.Scheme, StorageProvider> storageProviders;

    private StorageFactory()
    {
        String value = ConfigFactory.Instance().getProperty("enabled.storage.schemes");
        requireNonNull(value, "enabled.storage.schemes is not configured");
        String[] schemeNames = value.trim().split(",");
        checkArgument(schemeNames.length > 0,
                "at lease one storage scheme must be enabled");
        ImmutableMap.Builder<Storage.Scheme, StorageProvider> providersBuilder = ImmutableMap.builder();
        ServiceLoader<StorageProvider> providerLoader = ServiceLoader.load(StorageProvider.class);
        for (String name : schemeNames)
        {
            Storage.Scheme scheme = Storage.Scheme.from(name);
            this.enabledSchemes.add(scheme);
            boolean providerExists = false;
            for (StorageProvider storageProvider : providerLoader)
            {
                if (storageProvider.compatibleWith(scheme))
                {
                    providersBuilder.put(scheme, storageProvider);
                    providerExists = true;
                    break;
                }
            }
            if (!providerExists)
            {
                // only log a warning, do not throw exception.
                logger.warn(String.format(
                        "no storage provider exists for scheme: %s", scheme.name()));
            }
        }
        this.storageProviders = providersBuilder.build();
    }

    private static StorageFactory instance = null;

    public static StorageFactory Instance()
    {
        if (instance == null)
        {
            instance = new StorageFactory();
            Runtime.getRuntime().addShutdownHook(new Thread(()->
            {
                try
                {
                    instance.closeAll();
                } catch (IOException e)
                {
                    logger.error("Failed to close all storage instances.", e);
                    e.printStackTrace();
                }
            }));
        }
        return instance;
    }

    public List<Storage.Scheme> getEnabledSchemes()
    {
        return ImmutableList.copyOf(this.enabledSchemes);
    }

    public boolean isEnabled(Storage.Scheme scheme)
    {
        return this.enabledSchemes.contains(scheme);
    }

    /**
     * Recreate all the enabled Storage instances.
     * <b>Be careful:</b> all the Storage enabled Storage must be configured well before
     * calling this method. It is better to call {@link #reload(Storage.Scheme)} to reload
     * the Storage that you are sure it is configured or does not need any dynamic configuration.
     *
     * @throws IOException
     */
    @Deprecated
    public synchronized void reloadAll() throws IOException
    {
        for (Storage.Scheme scheme : enabledSchemes)
        {
            reload(scheme);
        }
    }

    /**
     * Recreate the Storage instance for the given storage scheme.
     *
     * @param scheme the given storage scheme
     * @throws IOException
     */
    public synchronized void reload(Storage.Scheme scheme) throws IOException
    {
        Storage storage = this.storageImpls.remove(scheme);
        if (storage != null)
        {
            storage.close();
        }
        storage = this.getStorage(scheme);
        requireNonNull(storage, "failed to create Storage instance");
        this.storageImpls.put(scheme, storage);
    }

    /**
     * Get the storage instance from a scheme name or a scheme prefixed path.
     * @param schemeOrPath
     * @return
     * @throws IOException
     */
    public synchronized Storage getStorage(String schemeOrPath) throws IOException
    {
        try
        {
            // 'synchronized' in Java is reentrant,
            // it is fine to call the other getStorage() from here.
            if (schemeOrPath.contains("://"))
            {
                return getStorage(Storage.Scheme.fromPath(schemeOrPath));
            }
            else
            {
                return getStorage(Storage.Scheme.from(schemeOrPath));
            }
        }
        catch (RuntimeException re)
        {
            throw new IOException("Invalid s" +
                    "torage scheme or path: " + schemeOrPath, re);
        }
    }

    public synchronized Storage getStorage(Storage.Scheme scheme) throws IOException
    {
        checkArgument(this.enabledSchemes.contains(scheme), "storage scheme '" +
                scheme.toString() + "' is not enabled.");
        if (storageImpls.containsKey(scheme))
        {
            return storageImpls.get(scheme);
        }

        Storage storage = this.storageProviders.get(scheme).createStorage(scheme);
        storageImpls.put(scheme, storage);

        return storage;
    }

    public ImmutableMap<Storage.Scheme, StorageProvider> getStorageProviders()
    {
        return storageProviders;
    }

    public void closeAll() throws IOException
    {
        for (Storage.Scheme scheme : storageImpls.keySet())
        {
            storageImpls.get(scheme).close();
        }
    }
}
