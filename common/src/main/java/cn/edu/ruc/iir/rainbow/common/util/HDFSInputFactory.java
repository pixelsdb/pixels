package cn.edu.ruc.iir.rainbow.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Created by hank on 16-12-24.
 */
public class HDFSInputFactory
{
    private static HDFSInputFactory instance = null;

    public static HDFSInputFactory Instance ()
    {
        if (instance == null)
        {
            instance = new HDFSInputFactory();
        }
        return instance;
    }

    private HDFSInputFactory () {}

    public FileStatus[] getFileStatuses (String dirPath, Configuration conf) throws IOException
    {
        FileSystem fs = FileSystem.get(URI.create(dirPath), conf);
        return fs.listStatus(new Path(dirPath));
    }
}
