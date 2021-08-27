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

import org.apache.hadoop.fs.BlockLocation;

import java.io.IOException;

/**
 * In Pixels, we assume that each file or object has its locations.
 *
 * <p>
 * In some storage systems, like HDFS, each file may have a set of
 * blocks and each block has its own location. For these storage systems,
 * the file's locations are the locations of the blocks.
 * <p/>
 * Created at: 20/08/2021
 * Author: hank
 */
public class Location
{
    private String[] hosts; // Datanode hostnames
    private String[] names; // Datanode IP:Port for accessing the block
    private boolean corrupt;

    private static final String[] EMPTY_STR_ARRAY = new String[0];

    /**
     * Default Constructor
     */
    public Location()
    {
        this(EMPTY_STR_ARRAY, EMPTY_STR_ARRAY);
    }

    /**
     * Copy constructor
     */
    public Location(Location that)
    {
        this.hosts = that.hosts;
        this.names = that.names;
        this.corrupt = that.corrupt;
    }

    public Location(BlockLocation blockLocation) throws IOException
    {
        this.hosts = blockLocation.getHosts();
        this.names = blockLocation.getNames();
        this.corrupt = blockLocation.isCorrupt();
    }

    /**
     * Constructor with host, name, offset and length
     */
    public Location(String[] names, String[] hosts)
    {
        this(names, hosts, false);
    }

    public Location(String[] names, String[] hosts, boolean corrupt)
    {
        if (names == null)
        {
            this.names = EMPTY_STR_ARRAY;
        } else
        {
            this.names = names;
        }
        if (hosts == null)
        {
            this.hosts = EMPTY_STR_ARRAY;
        } else
        {
            this.hosts = hosts;
        }
        this.corrupt = corrupt;
    }

    /**
     * Get the list of hosts (hostname) hosting this block
     */
    public String[] getHosts()
    {
        return hosts;
    }

    /**
     * Get the list of names (IP:xferPort) hosting this block
     */
    public String[] getNames()
    {
        return names;
    }

    /**
     * Get the corrupt flag.
     */
    public boolean isCorrupt()
    {
        return corrupt;
    }

    /**
     * Set the corrupt flag.
     */
    public void setCorrupt(boolean corrupt)
    {
        this.corrupt = corrupt;
    }

    /**
     * Set the hosts hosting this block
     */
    public void setHosts(String[] hosts)
    {
        if (hosts == null)
        {
            this.hosts = EMPTY_STR_ARRAY;
        } else
        {
            this.hosts = hosts;
        }
    }

    /**
     * Set the names (host:port) hosting this block
     */
    public void setNames(String[] names)
    {
        if (names == null)
        {
            this.names = EMPTY_STR_ARRAY;
        } else
        {
            this.names = names;
        }
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder();
        if (corrupt)
        {
            result.append("(corrupt)");
        }
        for (String h : hosts)
        {
            result.append(h);
            result.append(',');
        }
        return result.toString();
    }
}
