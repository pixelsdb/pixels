package cn.edu.ruc.iir.pixels.core;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
}
