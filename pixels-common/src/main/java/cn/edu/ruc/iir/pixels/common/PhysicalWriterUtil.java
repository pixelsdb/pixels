package cn.edu.ruc.iir.pixels.common;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public class PhysicalWriterUtil
{
    private PhysicalWriterUtil()
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
    public static PhysicalFSWriter newPhysicalFSWriter(
            FileSystem fs, Path path, long blockSize, short replication, boolean addBlockPadding)
    {
        FSDataOutputStream rawWriter = null;
        try {
            rawWriter = fs.create(path, false, Constants.HDFS_BUFFER_SIZE, replication, blockSize);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        if (rawWriter == null) {
            return null;
        }
        return new PhysicalFSWriter(fs, path, blockSize, replication, addBlockPadding, rawWriter);
    }
}
