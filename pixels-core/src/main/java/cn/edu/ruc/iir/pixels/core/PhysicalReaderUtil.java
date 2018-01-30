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
public class PhysicalReaderUtil
{
    private PhysicalReaderUtil()
    {}

    public static PhysicalFSReader newPhysicalFSReader(FileSystem fs, Path path)
    {
        FSDataInputStream rawReader = null;
        try {
            rawReader = fs.open(path);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        if (rawReader == null) {
            return null;
        }
        return new PhysicalFSReader(fs, path, rawReader);
    }
}
