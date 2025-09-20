/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.storage.http;

import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.storage.http.io.HttpInputStream;
import io.pixelsdb.pixels.storage.http.io.HttpOutputStream;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public final class HttpStream implements Storage
{
    private static final String SchemePrefix = Scheme.httpstream.name() + "://";

    public HttpStream() { }

    @Override
    public Scheme getScheme() { return Scheme.httpstream; }

    @Override
    public String ensureSchemePrefix(String path) throws IOException
    {
        if (path.startsWith(SchemePrefix))
        {
            return path;
        }
        if (path.contains("://"))
        {
            throw new IOException("Path '" + path +
                    "' already has a different scheme prefix than '" + SchemePrefix + "'.");
        }
        return SchemePrefix + path;
    }

    /**
     * This method is used for read content from http.
     * @param path
     * @return
     */
    @Override
    public DataInputStream open(String path) throws IOException
    {
        HttpStreamPath httpStreamPath = new HttpStreamPath(path);
        if (!httpStreamPath.isValid())
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }

        HttpInputStream inputStream;
        try
        {
            inputStream = new HttpInputStream(httpStreamPath.getHost(), httpStreamPath.getPort());
        } catch (Exception e)
        {
            throw new IOException("Failed to open HttpInputStream.", e);
        }
        return new DataInputStream(inputStream);
    }

    @Override
    public List<Status> listStatus(String... path)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listPaths(String... path)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Status getStatus(String path)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getFileId(String path)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean mkdirs(String path)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * This method is used for write content to http stream.
     */
    @Override
    public DataOutputStream create(String path, boolean overwrite, int bufferSize) throws IOException
    {
        HttpStreamPath httpStreamPath = new HttpStreamPath(path);
        if (!httpStreamPath.isValid())
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        return new DataOutputStream(new HttpOutputStream(httpStreamPath.getHost(), httpStreamPath.getPort(), bufferSize));
    }

    @Override
    public boolean delete(String path, boolean recursive)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportDirectCopy() { return false; }

    @Override
    public boolean directCopy(String src, String dest)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException { }

    @Override
    public boolean exists(String path)
    {
        throw new UnsupportedOperationException();
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