package io.pixelsdb.pixels.index.rockset;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.rockset.jni.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class RocksetFactory
{
    private static final String dbPath = ConfigFactory.Instance().getProperty("index.rockset.data.path");
    private static final boolean multiCF = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("index.rocksdb.multicf"));
    private static long dbHandle;

//    private static Cache blockCache;
    private static final long blockCacheCapacity = Long.parseLong(ConfigFactory.Instance().getProperty("index.rocksdb.block.cache.capacity"));
    private static final int blockCacheShardBits = Integer.parseInt(ConfigFactory.Instance().getProperty("index.rocksdb.block.cache.shard.bits"));
    /**
     * The reference counter.
     */
    private static final AtomicInteger reference = new AtomicInteger(0);
    private static final Map<String, RocksetColumnFamilyHandle> cfHandles = new ConcurrentHashMap<>();
    private static final String defaultColumnFamily = "default"; // Change for Rockset if needed

    private RocksetFactory() { }

    private static long createRocksetDB() throws Exception {
        // Simulated implementation, replace with actual logic when implemented
        // This placeholder returns an instance of your native database
        return initializeNativeDatabase();
    }

    // Placeholder for the native method to initialize the Rockset database
    private static native long initializeNativeDatabase() throws Exception;

    public static synchronized RocksetColumnFamilyHandle getOrCreateColumnFamily(long tableId, long indexId) throws Exception {
        String cfName = getCFName(tableId, indexId);

        // Return cached handle if exists
        if (cfHandles.containsKey(cfName)) {
            return cfHandles.get(cfName);
        }

        long db = getRocksetDB();
        RocksetColumnFamilyHandle handle = createColumnFamily(db, cfName.getBytes(StandardCharsets.UTF_8));
        cfHandles.put(cfName, handle);
        return handle;
    }

    private static native RocksetColumnFamilyHandle createColumnFamily(long db, byte[] columnFamilyName) throws Exception;

    private static String getCFName(long tableId, long indexId) {
        return defaultColumnFamily; // This may change based on Rockset's design
    }

    public static synchronized long getRocksetDB() throws Exception {
        if (dbHandle == 0) {
            dbHandle = createRocksetDB();
        }
        reference.incrementAndGet();
        return dbHandle;
    }

    public static synchronized void close() {
        if (dbHandle != 0 && reference.decrementAndGet() == 0) {
            for (RocksetColumnFamilyHandle handle : cfHandles.values()) {
                handle.close(); // Ensure that native handles are properly closed
            }
            cfHandles.clear();
            // Add closing logic for your native database
            closeNativeDatabase(dbHandle);
            dbHandle = 0;
        }
    }

    // Placeholder for native method to close the Rockset database
    private static native void closeNativeDatabase(long db);

    public static synchronized String getDbPath() {
        return dbPath;
    }
}

