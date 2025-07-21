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

import javax.annotation.Nonnull;

/**
 * @author hank
 * @create 2025-07-21
 */
public class CloudDBOptions
{
    private String bucketName;
    private String s3Prefix;
    private String localDbPath;
    private String persistentCachePath;
    private long persistentCacheSizeGB;
    private boolean readOnly;

    public CloudDBOptions() { }

    public String getBucketName()
    {
        return bucketName;
    }

    public CloudDBOptions setBucketName(@Nonnull String bucketName)
    {
        this.bucketName = bucketName;
        return this;
    }

    public String getS3Prefix()
    {
        return s3Prefix;
    }

    public CloudDBOptions setS3Prefix(@Nonnull String s3Prefix)
    {
        this.s3Prefix = s3Prefix;
        return this;
    }

    public String getLocalDbPath()
    {
        return localDbPath;
    }

    public CloudDBOptions setLocalDbPath(@Nonnull String localDbPath)
    {
        this.localDbPath = localDbPath;
        return this;
    }

    public String getPersistentCachePath()
    {
        return persistentCachePath;
    }

    public CloudDBOptions setPersistentCachePath(@Nonnull String persistentCachePath)
    {
        this.persistentCachePath = persistentCachePath;
        return this;
    }

    public long getPersistentCacheSizeGB()
    {
        return persistentCacheSizeGB;
    }

    public CloudDBOptions setPersistentCacheSizeGB(long persistentCacheSizeGB)
    {
        this.persistentCacheSizeGB = persistentCacheSizeGB;
        return this;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    public CloudDBOptions setReadOnly(boolean readOnly)
    {
        this.readOnly = readOnly;
        return this;
    }
}
