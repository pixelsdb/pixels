package io.pixelsdb.pixels.common.physical;

import io.pixelsdb.pixels.common.exception.FSException;
import io.pixelsdb.pixels.common.physical.impl.HDFS;
import io.pixelsdb.pixels.common.physical.impl.LocalFS;
import io.pixelsdb.pixels.common.physical.impl.S3;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
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

    public Storage getStorage(URI uri) throws FSException
    {
        if (storageImpls.containsKey(uri.getScheme()))
        {
            return storageImpls.get(uri.getScheme());
        }

        Storage storage = null;
        if (uri.getScheme().equalsIgnoreCase("hdfs"))
        {
            storage = new HDFS(uri);
        }
        else if (uri.getScheme().equalsIgnoreCase("s3"))
        {
            storage = new S3(uri);
        }
        else if (uri.getScheme().equalsIgnoreCase("file"))
        {
            storage = new LocalFS(uri);
        }
        storageImpls.put(uri.getScheme(), storage);

        return storage;
    }
}
