package cn.edu.ruc.iir.pixels.common.physical;

import cn.edu.ruc.iir.pixels.common.exception.FSException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public class PhysicalFSReader
        implements PhysicalReader
{
    private final FileSystem fs;
    private final Path path;
    private final FSDataInputStream rawReader;

    public PhysicalFSReader(FileSystem fs, Path path, FSDataInputStream rawReader)
    {
        this.fs = fs;
        this.path = path;
        this.rawReader = rawReader;
    }

    @Override
    public long getFileLength() throws IOException
    {
        return fs.getFileStatus(path).getLen();
    }

    public void seek(long desired) throws IOException
    {
        rawReader.seek(desired);
    }

    @Override
    public long readLong() throws IOException
    {
        return rawReader.readLong();
    }

    @Override
    public int readInt() throws IOException
    {
        return rawReader.readInt();
    }

    @Override
    public int read(byte[] buffer) throws IOException
    {
        return rawReader.read(buffer);
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException
    {
        return rawReader.read(buffer, offset, length);
    }

    @Override
    public void readFully(byte[] buffer) throws IOException
    {
        rawReader.readFully(buffer);
    }

    @Override
    public void readFully(byte[] buffer, int offset, int length) throws IOException
    {
        rawReader.readFully(buffer, offset, length);
    }

    @Override
    public void close() throws IOException
    {
        rawReader.close();
    }

    public FileSystem getFs()
    {
        return fs;
    }

    public Path getPath()
    {
        return path;
    }

    @Override
    public long getCurrentBlockId() throws FSException
    {
        HdfsDataInputStream hdis = null;
        if (rawReader instanceof HdfsDataInputStream)
        {
            hdis = (HdfsDataInputStream) rawReader;
            return hdis.getCurrentBlock().getBlockId();
        }
        else
        {
            throw new FSException("can not get block id from non-hdfs data input stream.");
        }
    }
}
