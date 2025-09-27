/*
 * Copyright 2022-2023 PixelsDB.
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

import static java.util.Objects.requireNonNull;

/**
 * The path of an object in object storage systems such as AWS S3, MinIO, and Google GCS.
 *
 * @author hank
 * @create 2022-09-04 23:55 (AbstractS3.Path)
 * @update 2023-04-15 (moved to ObjectPath)
 */
public class ObjectPath
{
    public String bucket = null;
    public String key = null;
    public boolean valid = false;
    /**
     * True if this path is folder.
     * In S3, 'folder' is an empty object with its name ends with '/'.
     * Besides, we also consider the path that only contains a
     * bucket name as a folder.
     */
    public boolean isFolder = false;

    public ObjectPath(String path)
    {
        requireNonNull(path);
        // remove the scheme header.
        if (path.contains("://"))
        {
            path = path.substring(path.indexOf("://") + 3);
        }
        else if (path.startsWith("/"))
        {
            path = path.substring(1);
        }
        // the substring before the first '/' is the bucket name.
        int slash = path.indexOf("/");
        if (slash > 0)
        {
            this.bucket = path.substring(0, slash);
            if (slash < path.length()-1)
            {
                this.key = path.substring(slash + 1);
                this.isFolder = this.key.endsWith("/");
            }
            else
            {
                // this is a bucket.
                this.isFolder = true;
            }
            this.valid = true;
        }
        else if (!path.isEmpty())
        {
            // this is a bucket.
            this.bucket = path;
            this.isFolder = true;
            this.valid = true;
        }
    }

    public ObjectPath(String bucket, String key)
    {
        this.bucket = requireNonNull(bucket, "bucket is null");
        this.key = key;
        this.valid = true;
        if (key != null)
        {
            this.isFolder = key.endsWith("/");
        }
        else
        {
            this.isFolder = true;
        }
    }

    @Override
    public String toString()
    {
        if (!this.valid)
        {
            return null;
        }
        if (this.key == null)
        {
            return this.bucket;
        }
        return this.bucket + "/" + this.key;
    }

    /**
     * Convert this path to a String with the scheme prefix of the storage.
     * @param storage the storage.
     * @return the String form of the path.
     * @throws IOException
     */
    public String toStringWithPrefix(Storage storage) throws IOException
    {
        if (!this.valid)
        {
            return null;
        }
        if (this.key == null)
        {
            return this.bucket;
        }
        return storage.ensureSchemePrefix(this.bucket + "/" + this.key);
    }
}
