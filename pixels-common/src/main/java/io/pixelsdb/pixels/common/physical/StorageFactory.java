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

    private StorageFactory() {}

    private static StorageFactory instance = null;

    public static StorageFactory Instance()
    {
        if (instance == null)
        {
            instance = new StorageFactory();
        }
        return instance;
    }

    public synchronized void reload()
    {
        this.storageImpls.clear();
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
