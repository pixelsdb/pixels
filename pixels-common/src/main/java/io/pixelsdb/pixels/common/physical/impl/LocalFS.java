package io.pixelsdb.pixels.common.physical.impl;

import io.pixelsdb.pixels.common.physical.Location;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.URI;
import java.util.List;

/**
 * Created at: 20/08/2021
 * Author: hank
 */
public class LocalFS implements Storage
{
    public LocalFS(URI uri)
    {

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
    public Location getLocation(String path)
    {
        return null;
    }

    @Override
    public DataInputStream open(String path)
    {
        return null;
    }

    @Override
    public DataOutputStream create(String path)
    {
        return null;
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
