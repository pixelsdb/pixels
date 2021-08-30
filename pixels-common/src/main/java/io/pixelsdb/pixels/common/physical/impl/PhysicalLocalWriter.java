package io.pixelsdb.pixels.common.physical.impl;

import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.Constants;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created at: 30/08/2021
 * Author: hank
 */
public class PhysicalLocalWriter implements PhysicalWriter
{
    private LocalFS local;
    private String path;
    private long position;
    private DataOutputStream rawWriter;

    public PhysicalLocalWriter(Storage storage, String path) throws IOException
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
        this.position = 0;
        this.rawWriter = this.local.create(path, false, Constants.LOCAL_BUFFER_SIZE, (short) 1);
    }

    /**
     * Prepare the writer to ensure the length can fit into current block.
     *
     * @param length length of content
     * @return starting offset after preparing. If -1, means prepare has failed,
     * due to the specified length cannot fit into current block.
     */
    @Override
    public long prepare(int length) throws IOException
    {
        return position;
    }

    /**
     * Append content to the file.
     *
     * @param buffer content buffer
     * @return start offset of content in the file.
     */
    @Override
    public long append(ByteBuffer buffer) throws IOException
    {
        buffer.flip();
        int length = buffer.remaining();
        return append(buffer.array(), buffer.arrayOffset() + buffer.position(), length);
    }

    /**
     * Append content to the file.
     *
     * @param buffer content buffer container
     * @param offset start offset of actual content buffer
     * @param length length of actual content buffer
     * @return start offset of content in the file.
     */
    @Override
    public long append(byte[] buffer, int offset, int length) throws IOException
    {
        long start = position;
        rawWriter.write(buffer, offset, length);
        position += length;
        return start;
    }

    /**
     * Close writer.
     */
    @Override
    public void close() throws IOException
    {
        rawWriter.close();
    }

    /**
     * Flush writer.
     */
    @Override
    public void flush() throws IOException
    {
        rawWriter.flush();
    }
}
