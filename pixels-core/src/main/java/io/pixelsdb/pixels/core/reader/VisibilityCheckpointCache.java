/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.core.reader;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Client-side cache for visibility checkpoints.
 * This cache is used by PixelsReader to avoid repeated loading of checkpoint files from storage.
 */
public class VisibilityCheckpointCache
{
    private static final Logger logger = LogManager.getLogger(VisibilityCheckpointCache.class);
    private static final VisibilityCheckpointCache instance = new VisibilityCheckpointCache();

    private final Cache<Long, Map<String, long[]>> cache;

    private VisibilityCheckpointCache()
    {
        long leaseDuration = Long.parseLong(ConfigFactory.Instance().getProperty("retina.offload.cache.lease.duration"));
        
        this.cache = Caffeine.newBuilder()
                .expireAfterAccess(leaseDuration, TimeUnit.SECONDS)
                .removalListener((key, value, cause) -> {
                    logger.info("Client-side visibility cache for timestamp {} evicted due to {}", key, cause);
                })
                .build();
    }

    public static VisibilityCheckpointCache getInstance()
    {
        return instance;
    }

    public long[] getVisibilityBitmap(long timestamp, String checkpointPath, long targetFileId, int targetRgId) throws IOException
    {
        Map<String, long[]> timestampCache = cache.getIfPresent(timestamp);
        
        if (timestampCache == null)
        {
            synchronized (this)
            {
                timestampCache = cache.getIfPresent(timestamp);
                if (timestampCache == null)
                {
                    timestampCache = loadCheckpointFile(checkpointPath);
                    cache.put(timestamp, timestampCache);
                }
            }
        }

        String rgKey = targetFileId + "_" + targetRgId;
        return timestampCache.getOrDefault(rgKey, null);
    }

    private Map<String, long[]> loadCheckpointFile(String path) throws IOException
    {
        long start = System.currentTimeMillis();
        Storage storage = StorageFactory.Instance().getStorage(path);
        long fileLength = storage.getStatus(path).getLength();

        byte[] content;
        try (PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(storage, path))
        {
            ByteBuffer buffer = reader.readFully((int) fileLength);
            if (buffer.hasArray())
            {
                content = buffer.array();
            }
            else
            {
                content = new byte[(int) fileLength];
                buffer.get(content);
            }
        }

        Map<String, long[]> timestampCache;
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(content)))
        {
            int rgCount = in.readInt();
            // Initial capacity: count / 0.75 + 1 to avoid rehash
            timestampCache = new ConcurrentHashMap<>((int) (rgCount / 0.75) + 1);
            
            for (int i = 0; i < rgCount; i++)
            {
                long fileId = in.readLong();
                int rgId = in.readInt();
                int len = in.readInt();
                long[] bitmap = new long[len];
                for (int j = 0; j < len; j++)
                {
                    bitmap[j] = in.readLong();
                }
                timestampCache.put(fileId + "_" + rgId, bitmap);
            }
        }
        long end = System.currentTimeMillis();
        logger.info("Loaded visibility checkpoint from {} in {} ms, RG count: {}", path, (end - start), timestampCache.size());
        return timestampCache;
    }
}
