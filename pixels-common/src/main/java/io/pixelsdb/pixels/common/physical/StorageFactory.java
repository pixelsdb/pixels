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

import io.pixelsdb.pixels.common.physical.impl.HDFS;
import io.pixelsdb.pixels.common.physical.impl.LocalFS;
import io.pixelsdb.pixels.common.physical.impl.S3;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created at: 20/08/2021
 * Author: hank
 */
public class StorageFactory
{
    private Logger logger = LogManager.getLogger(StorageFactory.class);
    private Map<Storage.Scheme, Storage> storageImpls = new HashMap<>();

    private StorageFactory() { }

    private static StorageFactory instance = null;

    public static StorageFactory Instance()
    {
        if (instance == null)
        {
            instance = new StorageFactory();
        }
        return instance;
    }

    public synchronized void reload() throws IOException
    {
        this.storageImpls.clear();
        storageImpls.put(Storage.Scheme.hdfs, new HDFS());
        storageImpls.put(Storage.Scheme.file, new LocalFS());
        storageImpls.put(Storage.Scheme.s3, new S3());
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
            // 'synchronized' in Java is reentrant, it is fine the call the other getStorage().
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
            throw new IOException("Invalid storage scheme or path: " + schemeOrPath, re);
        }
    }

    public synchronized Storage getStorage(Storage.Scheme scheme) throws IOException
    {
        if (storageImpls.containsKey(scheme))
        {
            return storageImpls.get(scheme);
        }

        Storage storage = null;
        if (scheme == Storage.Scheme.hdfs)
        {
            storage = new HDFS();
        }
        else if (scheme == Storage.Scheme.s3)
        {
            storage = new S3();
        }
        else if (scheme == Storage.Scheme.file)
        {
            storage = new LocalFS();
        }
        if (storage != null)
        {
            storageImpls.put(scheme, storage);
        }

        return storage;
    }

    public void closeAll() throws IOException
    {
        for (Storage.Scheme scheme : storageImpls.keySet())
        {
            storageImpls.get(scheme).close();
        }
    }
}
