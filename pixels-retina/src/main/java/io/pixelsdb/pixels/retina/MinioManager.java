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
package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.pixelsdb.pixels.storage.s3.Minio.ConfigMinio;

/**
 * Manage data written to minio via writeBuffer.
 * The key is tableId + "/" + entryId.
 */
public class MinioManager
{
    private final Storage minio;
    private final String minioPathPrefix;
    public MinioManager()
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        try
        {
            ConfigMinio(configFactory.getProperty("minio.region"),
                    configFactory.getProperty("minio.endpoint"),
                    configFactory.getProperty("minio.access.key"),
                    configFactory.getProperty("minio.secret.key"));
            this.minio = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        this.minioPathPrefix = configFactory.getProperty("minio.path.prefix");
    }

    private static MinioManager instance = null;

    public static MinioManager Instance()
    {
        if (instance == null)
        {
            instance = new MinioManager();
        }
        return instance;
    }

    private String buildKey(long tableId, long entryId)
    {
        return this.minioPathPrefix + String.format("%d/%d", tableId, entryId);
    }

    public void write(long tableId, long entryId, byte[] data) throws IOException
    {
        String key = buildKey(tableId, entryId);
        PhysicalWriter writer = PhysicalWriterUtil.newPhysicalWriter(this.minio, key, true);
        writer.append(data, 0, data.length);
        writer.close();
    }

    public ByteBuffer read(long tableId, long entryId) throws IOException
    {
        String key = buildKey(tableId, entryId);
        PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(this.minio, key);
        int length = (int) reader.getFileLength();
        return reader.readFully(length);
    }

    public boolean exist(long tableId, long entryId) throws IOException
    {
        String key = buildKey(tableId, entryId);
        return this.minio.exists(key);
    }

    public void delete(long tableId, long entryId) throws IOException
    {
        String key = buildKey(tableId, entryId);
        this.minio.delete(key,false);
    }
}
