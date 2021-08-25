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
public class S3 implements Storage
{
    public S3()
    {

    }

    @Override
    public String getScheme()
    {
        return "s3";
    }

    @Override
    public List<Status> listStatus(String path)
    {
        return null;
    }

    @Override
    public List<String> listPaths(String path) throws IOException
    {
        return null;
    }

    @Override
    public Status getStatus(String path)
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
