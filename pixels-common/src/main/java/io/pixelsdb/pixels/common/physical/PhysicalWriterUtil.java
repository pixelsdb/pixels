/*
 * Copyright 2018 PixelsDB.
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

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.utils.Constants.DEFAULT_HDFS_BLOCK_SIZE;

/**
 * @author guodong
 * @author hank
 */
public class PhysicalWriterUtil
{
    private PhysicalWriterUtil()
    {
    }

    /**
     * Get a physical writer, using the given writer option.
     * @param storage the storage to use
     * @param path the path of the file to write
     * @param option the writer option
     * @return
     * @throws IOException
     */
    public static PhysicalWriter newPhysicalWriter(
            Storage storage, String path, PhysicalWriterOption option) throws IOException
    {
        checkArgument(storage != null, "storage should not be null");
        checkArgument(path != null, "path should not be null");

        if (!StorageFactory.Instance().getStorageProviders().containsKey(storage.getScheme()))
        {
            throw new IOException(String.format("Storage scheme '%s' is not enabled or supported.",
                    storage.getScheme().name()));
        }
        return StorageFactory.Instance().getStorageProviders().get(storage.getScheme())
                .createWriter(storage, path, option);
    }

    public static PhysicalWriter newPhysicalWriter(Storage storage, String path) throws IOException
    {
        return newPhysicalWriter(storage, path, null);
    }

    /**
     * Get a physical file system writer.
     *
     * @param storage the storage to use
     * @param path the path of the file to write
     * @param blockSize the block size of the file
     * @param replication the replication of the file
     * @param addBlockPadding add block padding or not
     * @param overwrite overwrite the existing file with the same path if true
     * @return
     * @throws IOException
     */
    public static PhysicalWriter newPhysicalWriter(
            Storage storage, String path, long blockSize, short replication,
            boolean addBlockPadding, boolean overwrite) throws IOException
    {
        PhysicalWriterOption option = new PhysicalWriterOption()
                .setBlockSize(blockSize).setReplication(replication)
                .setAddBlockPadding(addBlockPadding).setOverwrite(overwrite);
        return newPhysicalWriter(storage, path, option);
    }

    /**
     * Get a physical file system writer. If the file with the same path
     * already exists, this method should throw an IOException.
     *
     * @param storage the storage to use
     * @param path the path of the file to write
     * @param blockSize the block size of the file
     * @param replication the replication of the file
     * @param addBlockPadding add block padding or not
     * @return
     * @throws IOException
     */
    public static PhysicalWriter newPhysicalWriter(
            Storage storage, String path, long blockSize,
            short replication, boolean addBlockPadding) throws IOException
    {
        return newPhysicalWriter(storage, path, blockSize,
                replication, addBlockPadding, false);
    }

    /**
     * Get a physical file system writer. If the storage is HDFS, default blocks,
     * one replication, and addBlockPadding=true are used by default.
     *
     * @param storage the storage to use
     * @param path the path of the file to write
     * @param overwrite overwrite the existing file with the same path if true
     * @return the physical writer
     * @throws IOException
     */
    public static PhysicalWriter newPhysicalWriter(
            Storage storage, String path, boolean overwrite) throws IOException
    {
        return newPhysicalWriter(storage, path, DEFAULT_HDFS_BLOCK_SIZE,
                (short) 1, true, overwrite);
    }

    /**
     * Get a physical file system writer.
     *
     * @param scheme          name of the scheme
     * @param path            write file path
     * @param blockSize       hdfs block size
     * @param replication     hdfs block replication num
     * @param addBlockPadding add block padding or not
     * @param overwrite       overwrite the existing file with the same path if true
     * @return the physical writer
     */
    public static PhysicalWriter newPhysicalWriter(
            Storage.Scheme scheme, String path, long blockSize, short replication,
            boolean addBlockPadding, boolean overwrite) throws IOException
    {
        checkArgument(scheme != null, "scheme should not be null");
        checkArgument(path != null, "path should not be null");
        return newPhysicalWriter(StorageFactory.Instance().getStorage(scheme), path,
                blockSize, replication, addBlockPadding, overwrite);
    }

    /**
     * Get a physical file system writer. If the file with the same path already exists,
     * this method should throw an IOException.
     *
     * @param scheme          name of the scheme
     * @param path            write file path
     * @param blockSize       hdfs block size
     * @param replication     hdfs block replication num
     * @param addBlockPadding add block padding or not
     * @return the physical writer
     */
    public static PhysicalWriter newPhysicalWriter(
            Storage.Scheme scheme, String path, long blockSize, short replication,
            boolean addBlockPadding) throws IOException
    {
        checkArgument(scheme != null, "scheme should not be null");
        checkArgument(path != null, "path should not be null");
        return newPhysicalWriter(StorageFactory.Instance().getStorage(scheme), path,
                blockSize, replication, addBlockPadding, false);
    }

    /**
     * Get a physical file system writer. If the storage is HDFS, default blocks, one
     * replication, and addBlockPadding=true are used by default.
     *
     * @param scheme          name of the scheme
     * @param path            write file path
     * @param overwrite       overwrite the existing file with the same path if true
     * @return the physical writer
     */
    public static PhysicalWriter newPhysicalWriter(
            Storage.Scheme scheme, String path, boolean overwrite) throws IOException
    {
        checkArgument(scheme != null, "scheme should not be null");
        checkArgument(path != null, "path should not be null");
        return newPhysicalWriter(StorageFactory.Instance().getStorage(scheme), path,
                DEFAULT_HDFS_BLOCK_SIZE, (short) 1, true, overwrite);
    }
}
