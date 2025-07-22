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
package io.pixelsdb.pixels.index.rockset;

import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.common.index.SinglePointIndexProvider;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import javax.annotation.Nonnull;

/**
 * @author hank, Rolland1944
 * @create 2025-02-19
 */
public class RocksetIndexProvider implements SinglePointIndexProvider
{
    private static final String bucketName = ConfigFactory.Instance().getProperty("index.rockset.s3.bucket");
    private static final String s3Prefix = ConfigFactory.Instance().getProperty("index.rockset.s3.prefix");
    private static final String localDbPath = ConfigFactory.Instance().getProperty("index.rockset.local.data.path");
    private static final String persistentCachePath = ConfigFactory.Instance().getProperty("index.rockset.persistent.cache.path");
    private static final long persistentCacheSizeGB = Long.parseLong(ConfigFactory.Instance().getProperty("index.rockset.persistent.cache.size.gb"));
    private static final boolean readOnly = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("index.rockset.read.only"));

    @Override
    public SinglePointIndex createInstance(long tableId, long indexId, @Nonnull SinglePointIndex.Scheme scheme,
                                           boolean unique) throws SinglePointIndexException
    {
        if (scheme == SinglePointIndex.Scheme.rockset)
        {
             try
             {
                 CloudDBOptions dbOptions = new CloudDBOptions().setBucketName(bucketName).setS3Prefix(s3Prefix)
                         .setLocalDbPath(localDbPath).setPersistentCachePath(persistentCachePath)
                         .setPersistentCacheSizeGB(persistentCacheSizeGB).setReadOnly(readOnly);
                 return new RocksetIndex(tableId, indexId, dbOptions, unique);
             }
             catch (Exception e)
             {
                 throw new SinglePointIndexException("failed to create Rockset instance", e);
             }
        }
        throw new SinglePointIndexException("unsupported scheme: " + scheme);
    }

    @Override
    public boolean compatibleWith(@Nonnull SinglePointIndex.Scheme scheme)
    {
        return scheme == SinglePointIndex.Scheme.rockset;
    }
}