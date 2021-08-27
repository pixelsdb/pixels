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

import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;

/**
 * Created at: 20/08/2021
 * Author: hank
 */
public class Status implements Comparable
{
    private String path;
    private long length;
    private boolean isdir;
    private short replication;

    public Status()
    {
        this(null, 0, false, 0);
    }

    //We should deprecate this soon?
    public Status(String path, long length, boolean isdir, int replication)
    {
        this.path = path;
        this.length = length;
        this.isdir = isdir;
        this.replication = (short) replication;
    }

    public Status (FileStatus hdfs)
    {
        this.path = hdfs.getPath().toString();
        this.length = hdfs.getLen();
        this.isdir = hdfs.isDirectory();
        this.replication = hdfs.getReplication();
    }

    /**
     * Copy constructor.
     *
     * @param other FileStatus to copy
     */
    public Status(Status other) throws IOException
    {
        this(other.getPath(), other.getLength(), other.isDirectory(), other.getReplication());
    }

    /**
     * Get the length of this file, in bytes.
     *
     * @return the length of this file, in bytes.
     */
    public long getLength()
    {
        return length;
    }

    /**
     * Is this a file?
     *
     * @return true if this is a file
     */
    public boolean isFile()
    {
        return !isdir;
    }

    /**
     * Is this a directory?
     *
     * @return true if this is a directory
     */
    public boolean isDirectory()
    {
        return isdir;
    }

    /**
     * Get the replication factor of a file.
     *
     * @return the replication factor of a file.
     */
    public short getReplication()
    {
        return replication;
    }

    public String getPath()
    {
        return path;
    }

    public String getName()
    {
        int slash = this.path.lastIndexOf("/");
        return this.path.substring(slash+1);
    }

    public void setPath(final String path)
    {
        this.path = path;
    }

    /**
     * Compare this object to another object
     *
     * @param o the object to be compared.
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     * @throws ClassCastException if the specified object's is not of
     *                            type FileStatus
     */
    @Override
    public int compareTo(Object o)
    {
        Status other = (Status) o;
        return this.getPath().compareTo(other.getPath());
    }

    /**
     * Compare if this object is equal to another object
     *
     * @param o the object to be compared.
     * @return true if two file status has the same path name; false if not.
     */
    @Override
    public boolean equals(Object o)
    {
        if (o == null)
        {
            return false;
        }
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof Status))
        {
            return false;
        }
        Status other = (Status) o;
        return this.getPath().equals(other.getPath());
    }

    /**
     * Returns a hash code value for the object, which is defined as
     * the hash code of the path name.
     *
     * @return a hash code value for the path name.
     */
    @Override
    public int hashCode()
    {
        return getPath().hashCode();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("{");
        sb.append("path=" + path);
        sb.append("; isDirectory=" + isdir);
        if (!isDirectory())
        {
            sb.append("; length=" + length);
        }
        sb.append("}");
        return sb.toString();
    }
}
