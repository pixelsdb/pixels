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

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ObjectStorageManager
{
    private static volatile ObjectStorageManager instance;
    private final Storage storage;
    private final String path;

    private ObjectStorageManager() throws RetinaException
    {
        ConfigFactory config = ConfigFactory.Instance();
        this.path = config.getProperty("retina.buffer.object.storage.folder");
        String storageSchema = config.getProperty("retina.buffer.object.storage.schema");

        try
        {
            switch(storageSchema)
            {
                case "s3":
                    this.storage = StorageFactory.Instance().getStorage(Storage.Scheme.s3);
                    break;
                case "minio":
                    this.storage = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
                    break;
                default:
                    throw new RetinaException("Unsupported storage schema: " + storageSchema);
            }
        } catch (IOException e)
        {
            throw new RetinaException("Failed to initialize object storage manager.", e);
        }
    }

    public static ObjectStorageManager Instance() throws RetinaException
    {
        if (instance == null)
        {
            synchronized (ObjectStorageManager.class)
            {
                if (instance == null)
                {
                    instance = new ObjectStorageManager();
                }
            }
        }
        return instance;
    }

    private String buildKey(long tableId, long entryId)
    {
        return this.path + String.format("%d/%d", tableId, entryId);
    }

    public void write(long tableId, long entryId, byte[] data) throws IOException
    {
        String key = buildKey(tableId, entryId);
        PhysicalWriter writer = PhysicalWriterUtil.newPhysicalWriter(this.storage, key, true);
        writer.append(data, 0, data.length);
        writer.close();
    }

    public ByteBuffer read(long tableId, long entryId) throws IOException
    {
        String key = buildKey(tableId, entryId);
        PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(this.storage, key);
        int length = (int) reader.getFileLength();
        return reader.readFully(length);
    }

    public boolean exist(long tableId, long entryId) throws IOException
    {
        String key = buildKey(tableId, entryId);
        return this.storage.exists(key);
    }

    public void delete(long tableId, long entryId) throws IOException
    {
        String key = buildKey(tableId, entryId);
        this.storage.delete(key,false);
    }
}
