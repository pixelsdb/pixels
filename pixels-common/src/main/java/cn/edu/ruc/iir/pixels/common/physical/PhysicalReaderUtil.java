package cn.edu.ruc.iir.pixels.common.physical;

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
    {
    }

    public static PhysicalFSReader newPhysicalFSReader(FileSystem fs, Path path)
    {
        FSDataInputStream rawReader = null;
        try
        {
            rawReader = fs.open(path);
            if (rawReader != null)
            {
                return new PhysicalFSReader(fs, path, rawReader);
            }
        }
        catch (IOException e)
        {
            if (rawReader != null)
            {
                try
                {
                    rawReader.close();
                }
                catch (IOException e1)
                {
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
        }

        return null;
    }
}
