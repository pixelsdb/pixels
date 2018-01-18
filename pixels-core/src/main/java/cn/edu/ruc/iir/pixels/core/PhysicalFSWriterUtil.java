package cn.edu.ruc.iir.pixels.core;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public class PhysicalFSWriterUtil
{
    private PhysicalFSWriterUtil()
    {}

    /**
     * Get a physical file system writer.
     * @param fs file system
     * @param path write file path
     * @param blockSize hdfs block size
     * @param replication hdfs block replication num
     * @param addBlockPadding add block padding or not
     * @return physical writer
     * */
    public static PhysicalWriter newPhysicalFSWriter(
            FileSystem fs, Path path, long blockSize, short replication, boolean addBlockPadding)
            throws IOException
    {
        return new PhysicalFSWriter(fs, path, blockSize, replication, addBlockPadding);
    }
}
