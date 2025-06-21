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
package io.pixelsdb.pixels.index.rocksdb;

import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.MainIndexImpl;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.common.index.SinglePointIndexProvider;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * @author hank, Rolland1944
 * @create 2025-02-19
 */
public class RocksetIndexProvider implements SinglePointIndexProvider
{
    private static final Logger logger = LogManager.getLogger(RocksetIndexProvider.class);
    private final MainIndex mainIndex = new MainIndexImpl();
    private final String bucketName = ConfigFactory.Instance().getProperty("rockset.s3.bucket");
    private final String s3Prefix = ConfigFactory.Instance().getProperty("rockset.s3.prefix");
    private final String localDbPath = ConfigFactory.Instance().getProperty("rockset.local.data.path");
    private final String persistentCachePath = ConfigFactory.Instance().getProperty("rockset.persistent.cache.path");
    private final long persistentCacheSizeGB = Long.parseLong(ConfigFactory.Instance().getProperty("rockset.persistent.cache.size.gb"));
    private final boolean readOnly = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("rockset.read.only"));

    @Override
    public SinglePointIndex createInstance(@Nonnull SinglePointIndex.Scheme scheme) throws IOException
    {
        if (scheme == SinglePointIndex.Scheme.rockset)
        {
             try
             {
                 return new RocksetIndex(mainIndex, bucketName, s3Prefix, localDbPath, persistentCachePath, persistentCacheSizeGB, readOnly);
             }
             catch (Exception e) {
                 logger.error("Failed to create RocksDB instance", e);
                 return null;
             }
        }
        throw new IllegalArgumentException("Unsupported scheme: " + scheme);
    }

    @Override
    public boolean compatibleWith(@Nonnull SinglePointIndex.Scheme scheme)
    {
        return scheme == SinglePointIndex.Scheme.rockset;
    }
}