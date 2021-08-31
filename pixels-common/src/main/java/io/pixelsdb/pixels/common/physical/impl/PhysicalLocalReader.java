package io.pixelsdb.pixels.common.physical.impl;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Storage;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created at: 30/08/2021
 * Author: hank
 */
public class PhysicalLocalReader implements PhysicalReader
{
    private LocalFS local;
    private String path;
    private long id;
    private RandomAccessFile raf;

    public PhysicalLocalReader(Storage storage, String path) throws IOException
    {
        if (storage instanceof LocalFS)
        {
            this.local = (LocalFS) storage;
        }
        else
        {
            throw new IOException("Storage is not LocalFS.");
        }
        this.path = path;
        this.raf = this.local.openRaf(path);
        this.id = this.local.getFileId(path);
    }

    @Override
    public long getFileLength() throws IOException
    {
        return raf.length();
    }

    @Override
    public void seek(long desired) throws IOException
    {
        raf.seek(desired);
    }

    @Override
    public int read(byte[] buffer) throws IOException
    {
        return raf.read(buffer);
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException
    {
        return raf.read(buffer, offset, length);
    }

    @Override
    public void readFully(byte[] buffer) throws IOException
    {
        raf.readFully(buffer);
    }

    @Override
    public void readFully(byte[] buffer, int offset, int length) throws IOException
    {
        raf.readFully(buffer, offset, length);
    }

    @Override
    public long readLong() throws IOException
    {
        return raf.readLong();
    }

    @Override
    public int readInt() throws IOException
    {
        return raf.readInt();
    }

    @Override
    public void close() throws IOException
    {
        this.raf.close();
    }

    @Override
    public String getPath()
    {
        return path;
    }

    /**
     * Get the last domain in path.
     *
     * @return
     */
    @Override
    public String getName()
    {
        if (path == null)
        {
            return null;
        }
        int slash = path.lastIndexOf("/");
        return path.substring(slash + 1);
    }

    /**
     * For local file, block id is also the file id.
     * @return
     * @throws IOException
     */
    @Override
    public long getBlockId() throws IOException
    {
        return id;
    }
}
