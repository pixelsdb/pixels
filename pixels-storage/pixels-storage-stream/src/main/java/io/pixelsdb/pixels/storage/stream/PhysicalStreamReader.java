package io.pixelsdb.pixels.storage.stream;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Storage;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;

public class PhysicalStreamReader implements PhysicalReader
{
    private final Storage stream;
    private final String path;
    private final DataInputStream dataInputStream;

    public PhysicalStreamReader(Storage storage, String path) throws IOException
    {
        if (storage instanceof Stream)
        {
            this.stream = (Stream) storage;
        }
        else
        {
            throw new IOException("Storage is not LocalFS.");
        }
        this.path = path;
        this.dataInputStream = storage.open(path);
    }

    @Override
    public long getFileLength() throws IOException
    {
        throw new UnsupportedOperationException("Can't get file length in PhysicalStreamReader");
    }

    @Override
    public void seek(long desired) throws IOException
    {
        throw new UnsupportedOperationException("Can't get file length in PhysicalStreamReader");
    }

    @Override
    public ByteBuffer readFully(int length) throws IOException
    {
        byte[] buffer = new byte[length];
        dataInputStream.readFully(buffer);
        return ByteBuffer.wrap(buffer);
    }

    @Override
    public void readFully(byte[] buffer) throws IOException
    {
        dataInputStream.readFully(buffer);
    }

    @Override
    public void readFully(byte[] buffer, int offset, int length) throws IOException
    {
        dataInputStream.readFully(buffer, offset, length);
    }

    @Override
    public long readLong(ByteOrder byteOrder) throws IOException
    {
        if (requireNonNull(byteOrder).equals(ByteOrder.BIG_ENDIAN))
        {
            return dataInputStream.readLong();
        }
        else
        {
            return Long.reverseBytes(dataInputStream.readLong());
        }
    }

    @Override
    public int readInt(ByteOrder byteOrder) throws IOException
    {
        if (requireNonNull(byteOrder).equals(ByteOrder.BIG_ENDIAN))
        {
            return dataInputStream.readInt();
        }
        else
        {
            return Integer.reverseBytes(dataInputStream.readInt());
        }
    }

    @Override
    public boolean supportsAsync() { return false; }

    @Override
    public CompletableFuture<ByteBuffer> readAsync(long offset, int len) throws IOException
    {
        throw new UnsupportedOperationException("Can't get file length in PhysicalStreamReader");
    }

    @Override
    public void close() throws IOException
    {
        this.dataInputStream.close();
    }

    @Override
    public String getPath() { return path; }

    /**
     * Get the port in path.
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
        int slash = path.lastIndexOf(":");
        return path.substring(slash + 1);
    }

    @Override
    public long getBlockId() throws IOException
    {
        throw new IOException("Can't get block id in PhysicalStreamReader");
    }

    @Override
    public Storage.Scheme getStorageScheme() { return stream.getScheme(); }

    @Override
    public int getNumReadRequests() { return 0; }
}
