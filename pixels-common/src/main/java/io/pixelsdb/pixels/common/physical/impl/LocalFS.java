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
package io.pixelsdb.pixels.common.physical.impl;

import io.pixelsdb.pixels.common.physical.Location;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Created at: 20/08/2021
 * Author: hank
 */
public class LocalFS implements Storage
{
    public LocalFS()
    {

    }

    @Override
    public Scheme getScheme()
    {
        return Scheme.file;
    }

    @Override
    public List<Status> listStatus(String path)
    {
        return null;
    }

    @Override
    public Status getStatus(String path)
    {
        return null;
    }

    @Override
    public List<String> listPaths(String path) throws IOException
    {
        return null;
    }

    @Override
    public long getId(String path) throws IOException
    {
        return 0;
    }

    @Override
    public List<Location> getLocations(String path)
    {
        return null;
    }

    @Override
    public String[] getHosts(String path) throws IOException
    {
        return new String[0];
    }

    @Override
    public DataInputStream open(String path)
    {
        return null;
    }

    @Override
    public DataOutputStream create(String path, boolean overwrite, int bufferSize, short replication) throws IOException
    {
        return null;
    }

    @Override
    public boolean delete(String path, boolean recursive) throws IOException
    {
        return false;
    }

    @Override
    public boolean exists(String path)
    {
        return false;
    }

    @Override
    public boolean isFile(String path)
    {
        return false;
    }

    @Override
    public boolean isDirectory(String path)
    {
        return false;
    }
}
