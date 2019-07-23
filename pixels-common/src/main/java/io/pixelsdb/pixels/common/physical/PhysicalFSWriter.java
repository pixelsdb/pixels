package io.pixelsdb.pixels.common.physical;

import io.pixelsdb.pixels.common.utils.Constants;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class PhysicalFSWriter
        implements PhysicalWriter
{
    private static final Logger LOGGER = LogManager.getLogger(PhysicalFSWriter.class);

    private final FileSystem fs;
    private final Path path;
    private final long blockSize;
    private final short replication;
    private final boolean addBlockPadding;
    private final FSDataOutputStream rawWriter;

    public PhysicalFSWriter(FileSystem fs, Path path, long blockSize,
                            short replication, boolean addBlockPadding,
                            FSDataOutputStream rawWriter)
    {
        this.fs = fs;
        this.path = path;
        this.blockSize = blockSize;
        this.replication = replication;
        this.addBlockPadding = addBlockPadding;
        this.rawWriter = rawWriter;
    }

    @Override
    public long prepare(int length) throws IOException
    {
        if (length > blockSize)
        {
            return -1L;
        }
        // see if row group can fit in the current hdfs block, else pad the remaining space in the block
        long start = rawWriter.getPos();
        long availBlockSpace = blockSize - (start % blockSize);
        if (length < blockSize && length > availBlockSpace && addBlockPadding)
        {
            byte[] pad = new byte[(int) Math.min(Constants.HDFS_BUFFER_SIZE, availBlockSpace)];
            LOGGER.info(String.format("Padding Pixels by %d bytes while appending row group buffer...", availBlockSpace));
            start += availBlockSpace;
            while (availBlockSpace > 0)
            {
                int writeLen = (int) Math.min(availBlockSpace, pad.length);
                rawWriter.write(pad, 0, writeLen);
                availBlockSpace -= writeLen;
            }
        }
        return start;
    }

    @Override
    public long append(ByteBuffer buffer) throws IOException
    {
        buffer.flip();
        int length = buffer.remaining();
        return append(buffer.array(), buffer.arrayOffset() + buffer.position(), length);
    }

    @Override
    public long append(byte[] buffer, int offset, int length) throws IOException
    {
        long start = rawWriter.getPos();
        rawWriter.write(buffer, offset, length);
        return start;
    }

    @Override
    public void close() throws IOException
    {
        rawWriter.close();
    }

    @Override
    public void flush() throws IOException
    {
        rawWriter.flush();
    }

    public FileSystem getFs()
    {
        return fs;
    }

    public Path getPath()
    {
        return path;
    }

    public long getBlockSize()
    {
        return blockSize;
    }

    public short getReplication()
    {
        return replication;
    }

    public boolean isAddBlockPadding()
    {
        return addBlockPadding;
    }
}
