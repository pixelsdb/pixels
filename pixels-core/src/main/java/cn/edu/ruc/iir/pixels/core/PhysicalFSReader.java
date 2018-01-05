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
public class PhysicalFSReader implements PhysicalReader
{
    private final FileSystem fs;
    private final Path path;
    private final FSDataInputStream rawReader;

    public PhysicalFSReader(FileSystem fs, Path path) throws IOException
    {
        this.fs = fs;
        this.path = path;

        rawReader = fs.open(path);
    }

    @Override
    public long getFileLength() throws IOException
    {
        return fs.getFileStatus(path).getLen();
    }

    @Override
    public FSDataInputStream getRawReader()
    {
        return rawReader;
    }

    @Override
    public void close() throws IOException
    {
        rawReader.close();
    }
}
