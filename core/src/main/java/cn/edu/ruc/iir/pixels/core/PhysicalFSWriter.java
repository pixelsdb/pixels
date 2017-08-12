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
    private static final int HDFS_BUFFER_SIZE = 256 * 1024;

    private final Path path;
    private final long blockSize;
    private final short replication;
    private final boolean addBlockPadding;
    private final FSDataOutputStream rawWriter;

    public PhysicalFSWriter(FileSystem fs, Path path, long blockSize, short replication, boolean addBlockPadding) throws IOException
    {
        this.path = path;
        this.blockSize = blockSize;
        this.replication = replication;
        this.addBlockPadding = addBlockPadding;

        rawWriter = fs.create(path, false, HDFS_BUFFER_SIZE, replication, blockSize);
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

    @Override
    public long appendRowGroupBuffer(ByteBuffer buffer) throws IOException
    {
        long start = rawWriter.getPos();
        buffer.flip();
        int length = buffer.remaining();
        long availBlockSpace = blockSize - (start % blockSize);

        // see if row group can fit in the current hdfs block, else pad the remaining space in the block
        if (length < blockSize && length > availBlockSpace && addBlockPadding) {
            byte[] pad = new byte[(int) Math.min(HDFS_BUFFER_SIZE, availBlockSpace)];
            LOGGER.info(String.format("Padding Pixels by %d bytes while appending row group buffer...", availBlockSpace));
            start += availBlockSpace;
            while (availBlockSpace > 0) {
                int writeLen = (int) Math.min(availBlockSpace, pad.length);
                rawWriter.write(pad, 0, writeLen);
                availBlockSpace -= writeLen;
            }
        }

        rawWriter.write(buffer.array(), buffer.arrayOffset() + buffer.position(), length);
        return start;
    }

    @Override
    public void writeFileTail(PixelsProto.FileTail fileTail) throws IOException
    {
        rawWriter.write(fileTail.toByteArray());
        int tailLen = fileTail.getSerializedSize();
        rawWriter.writeInt(tailLen);
        long pos = rawWriter.getPos();
        rawWriter.writeLong(pos);
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
