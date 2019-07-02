package cn.edu.ruc.iir.pixels.hive.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Created at: 19-7-1
 * Author: hank
 */
public class HDFSLog
{
    public static BufferedWriter getLogWriter(Configuration conf, String path) throws IOException
    {
        FileSystem fs = FileSystem.get(conf);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path))));
        return writer;
    }

    public static BufferedWriter getLogWriter(FileSystem fs, String path) throws IOException
    {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path))));
        return writer;
    }
}
