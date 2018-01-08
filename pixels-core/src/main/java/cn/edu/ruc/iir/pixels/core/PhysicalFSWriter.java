package cn.edu.ruc.iir.pixels.core;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class PhysicalFSWriter implements PhysicalWriter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalFSWriter.class);

    private final long blockSize;
    private final boolean addBlockPadding;
    private final FSDataOutputStream rawWriter;

    public PhysicalFSWriter(FileSystem fs, Path path, long blockSize, short replication, boolean addBlockPadding) throws IOException
    {
        this.blockSize = blockSize;
        this.addBlockPadding = addBlockPadding;

        rawWriter = fs.create(path, false, Constants.HDFS_BUFFER_SIZE, replication, blockSize);
    }

    @Override
    public long prepare(int length) throws IOException
    {
        if (length > blockSize) {
            return -1L;
        }
        // see if row group can fit in the current hdfs block, else pad the remaining space in the block
        long start = rawWriter.getPos();
        long availBlockSpace = blockSize - (start % blockSize);
        if (length < blockSize && length > availBlockSpace && addBlockPadding) {
            byte[] pad = new byte[(int) Math.min(Constants.HDFS_BUFFER_SIZE, availBlockSpace)];
            LOGGER.info(String.format("Padding Pixels by %d bytes while appending row group buffer...", availBlockSpace));
            start += availBlockSpace;
            while (availBlockSpace > 0) {
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
}
