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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Created at: 20/08/2021
 * Author: hank
 */
public interface Storage
{
    /**
     * If we want to add more storage schemes here, modify this enum.
     */
    enum Scheme
    {
        hdfs, // HDFS
        file, // local fs
        s3; // Amazon S3

        /**
         * Case insensitive parsing from String to enum value.
         * @param value
         * @return
         */
        public static Scheme from(String value)
        {
            return valueOf(value.toLowerCase());
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
            return this == other;
        }
    }

    Scheme getScheme();

    /**
     * Get the statuses of the contents in this path if it is a directory.
     * For local fs, path is considered as local.
     * @param path
     * @return
     * @throws IOException
     */
    List<Status> listStatus(String path) throws IOException;

    /**
     * Get the paths of the contents in this path if it is a directory.
     * For local fs, path is considered as local.
     * @param path
     * @return
     * @throws IOException
     */
    List<String> listPaths(String path) throws IOException;

    /**
     * Get the status of this path if it is a file.
     * For local fs, path is considered as local.
     * @param path
     * @return
     * @throws IOException
     */
    Status getStatus(String path) throws IOException;

    /**
     * We assume there is an unique id for each file or object
     * in the storage system.
     * For HDFS, we assume each file only has one block, and the
     * file id is the id of this block.
     * @param path
     * @return
     * @throws IOException if HDFS file has more than one blocks.
     */
    long getFileId(String path) throws IOException;

    List<Location> getLocations(String path) throws IOException;

    String[] getHosts(String path) throws IOException;

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
     * @param replication
     * @return
     * @throws IOException if path is a directory.
     */
    DataOutputStream create(String path, boolean overwrite,
                            int bufferSize, short replication) throws IOException;

    /**
     * For local fs, path is considered as local.
     * @param path
     * @param recursive
     * @return
     * @throws IOException
     */
    boolean delete(String path, boolean recursive) throws IOException;

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
