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
    private Map<String, Storage> storageImpls = new HashMap<>();

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
        storageImpls.put("hdfs", new HDFS());
        storageImpls.put("local", new LocalFS());
        storageImpls.put("s3", new S3());
    }

    public synchronized Storage getStorage(String scheme) throws IOException
    {
        if (storageImpls.containsKey(scheme))
        {
            return storageImpls.get(scheme);
        }

        Storage storage = null;
        if (scheme.equalsIgnoreCase("hdfs"))
        {
            storage = new HDFS();
        }
        else if (scheme.equalsIgnoreCase("s3"))
        {
            storage = new S3();
        }
        else if (scheme.equalsIgnoreCase("file"))
        {
            storage = new LocalFS();
        }
        if (storage != null)
        {
            storageImpls.put(scheme, storage);
        }

        return storage;
    }
}
