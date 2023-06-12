/*
 * Copyright 2021 PixelsDB.
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

import com.google.common.collect.ImmutableList;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @create 2021-08-20
 * @author  hank
 */
public interface Storage
{
    /**
     * If we want to add more storage schemes here, modify this enum.
     */
    enum Scheme
    {
        hdfs,  // HDFS
        file,  // local fs
        s3,    // Amazon S3
        minio, // Minio
        redis, // Redis
        gcs,   // google cloud storage
        mock; // mock

        /**
         * Case-insensitive parsing from String name to enum value.
         * @param value the name of storage scheme.
         * @return
         */
        public static Scheme from(String value)
        {
            return valueOf(value.toLowerCase());
        }

        /**
         * Parse the scheme from the path which is prefixed with the storage scheme.
         * @param schemedPath the path started with storage scheme, e.g., s3://
         * @return the storage scheme of the path, or null if the path is not started with a valid scheme
         */
        public static Scheme fromPath(String schemedPath)
        {
            if (!schemedPath.contains("://"))
            {
                return null;
            }
            String scheme = schemedPath.substring(0, schemedPath.indexOf("://"));
            if (!Scheme.isValid(scheme))
            {
                return null;
            }
            return Scheme.from(scheme);
        }

        /**
         * Whether the value is a valid storage scheme.
         * @param value
         * @return
         */
        public static boolean isValid(String value)
        {
            for (Scheme scheme : values())
            {
                if (scheme.equals(value))
                {
                    return true;
                }
            }
            return false;
        }

        public boolean equals(String other)
        {
            return this.toString().equalsIgnoreCase(other);
        }

        public boolean equals(Scheme other)
        {
            // enums in Java can be compared using '=='.
            return this == other;
        }
    }

    Scheme getScheme();

    String ensureSchemePrefix(String path) throws IOException;

    /**
     * Get the statuses of the files or subdirectories in the given path of one or more directories.
     * The path in the returned status does not start with the scheme name.
     * For local fs, path is considered as local.
     * @param path the given path, may contain multiple directories.
     * @return the statuses of the files or subdirectories.
     * @throws IOException
     */
    List<Status> listStatus(String... path) throws IOException;

    /**
     * Get the paths of the files or subdirectories in the given path of one or more directories.
     * The returned path does not start with the scheme name.
     * For local fs, path is considered as local.
     * @param path the given path, may contain multiple directories.
     * @return the paths of the files or subdirectories.
     * @throws IOException
     */
    List<String> listPaths(String... path) throws IOException;

    /**
     * Get the status of this path if it is a file.
     * For local fs, path is considered as local.
     * @param path
     * @return
     * @throws IOException
     */
    Status getStatus(String path) throws IOException;

    /**
     * We assume there is a unique id for each file or object
     * in the storage system.
     * For HDFS, we assume each file only has one block, and the
     * file id is the id of this block.
     * @param path
     * @return
     * @throws IOException if HDFS file has more than one block.
     */
    long getFileId(String path) throws IOException;

    /**
     * Whether the storage system provides data locality. For example,
     * on-premise HDFS provides the locality for each file, however,
     * S3 and local file systems can not provide meaningful locality
     * information for data-locality scheduling of computation tasks.
     * By default, this method returns false.
     *
     * <p><b>Note:</b> if this method returns true, then getLocations
     * and getHosts must be override to return the physical locality
     * information of the given path.</p>
     * @return
     */
    default boolean hasLocality()
    {
        return false;
    }

    /**
     * Get the network locations of the given file. Each file may
     * have multiple locations if it is replicated or partitioned
     * in the storage system.
     *
     * <p>The default implementation simply parses the location
     * from the URI of the path.</p>
     * @param path the path of the file.
     * @return
     * @throws IOException
     */
    default List<Location> getLocations(String path) throws IOException
    {
        requireNonNull(path, "path is null");
        return ImmutableList.of(new Location(URI.create(ensureSchemePrefix(path))));
    }

    /**
     * Get the hostnames of the storage node where the file replicas
     * or partitions are located in. It is similar to getLocations()
     * however only returns the hostname information.
     *
     * <p>The default implementation simply parses the host from the
     * URI of the path.</p>
     * @param path the path of the file.
     * @return
     * @throws IOException
     */
    default String[] getHosts(String path) throws IOException
    {
        requireNonNull(path, "path is null");
        return new String[]{URI.create(ensureSchemePrefix(path)).getHost()};
    }

    /**
     * Create the directory named by this abstract pathname,
     * including any necessary but nonexistent parent directories.
     * @param path
     * @return
     * @throws IOException
     */
    boolean mkdirs(String path) throws IOException;

    /**
     * For local fs, path is considered as local.
     * @param path
     * @return
     * @throws IOException
     */
    DataInputStream open(String path) throws IOException;

    /**
     * For local fs, path is considered as local.
     * @param path
     * @param overwrite
     * @param bufferSize
     * @return
     * @throws IOException if path is a directory.
     */
    DataOutputStream create(String path, boolean overwrite,
                            int bufferSize) throws IOException;

    /**
     * For local fs, path is considered as local.
     * @param path
     * @param overwrite
     * @param bufferSize
     * @param replication is ignored by default, if the storage does not have explicit replication.
     * @return
     * @throws IOException if path is a directory.
     */
    default DataOutputStream create(String path, boolean overwrite,
                            int bufferSize, short replication) throws IOException
    {
        return create(path, overwrite, bufferSize);
    }

    /**
     * This method is for the compatability of block-based storage like HDFS.
     * For local fs, path is considered as local.
     * @param path
     * @param overwrite
     * @param bufferSize
     * @param replication
     * @param blockSize is ignored by default, except in HDFS.
     * @return
     * @throws IOException if path is a directory.
     */
    default DataOutputStream create(String path, boolean overwrite,
                            int bufferSize, short replication, long blockSize) throws IOException
    {
        return create(path, overwrite, bufferSize, replication);
    }

    /**
     * For local fs, path is considered as local.
     * @param path the path to delete
     * @param recursive whether delete recursively
     * @return true if the path is deleted successfully, false if the path does not exist
     * @throws IOException
     */
    boolean delete(String path, boolean recursive) throws IOException;

    /**
     * Whether this storage supports direct (short circuit) copying.
     * @return true if copy is supported.
     */
    boolean supportDirectCopy();

    /**
     * Copy from the source to the destination without going through this client.
     * @param src
     * @param dest
     * @return
     * @throws IOException
     */
    boolean directCopy(String src, String dest) throws IOException;

    /**
     * Close the storage.
     * @throws IOException
     */
    void close() throws IOException;

    /**
     * For local fs, path is considered as local.
     * @param path
     * @return
     * @throws IOException
     */
    boolean exists(String path) throws IOException;

    /**
     * For local fs, path is considered as local.
     * @param path
     * @return
     * @throws IOException
     */
    boolean isFile(String path) throws IOException;

    /**
     * For local fs, path is considered as local.
     * @param path
     * @return
     * @throws IOException
     */
    boolean isDirectory(String path) throws IOException;
}
