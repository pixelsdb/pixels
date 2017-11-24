package cn.edu.ruc.iir.pixels.core;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public interface PhysicalReader
{
    long getFileLength() throws IOException;

    FSDataInputStream getRawReader();

    void close() throws IOException;
}
