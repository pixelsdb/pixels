package cn.edu.ruc.iir.pixels.test;

import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

/**
 * pixels
 *
 * java -jar xxx.jar srcPath dstPath
 *
 * @author guodong
 */
public class CopyFiles
{
    public static void main(String[] args)
    {
        if (args.length != 2)
        {
            System.out.println("Copy files between two HDFS dirs. USAGE: java -jar copy-tool.jar <src> <dst>");
            System.exit(-1);
        }

        String srcPath = args[0];
        String dstPath = args[1];

        if (!dstPath.endsWith("/"))
        {
            dstPath += "/";
        }

        ConfigFactory configFactory = ConfigFactory.Instance();
        long blockSize = Long.parseLong(configFactory.getProperty("block.size")) * 1024L * 1024L;
        short replication = Short.parseShort(configFactory.getProperty("block.replication"));
        int hdfsBufferSize = 256 * 1024;

        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try {
            FileSystem sourceFS = FileSystem.get(URI.create(srcPath), configuration);
            FileSystem destFS = FileSystem.get(URI.create(dstPath), configuration);

            FileStatus[] srcFiles = sourceFS.listStatus(new Path(srcPath));
            for (FileStatus srcFile : srcFiles)
            {
                String name = srcFile.getPath().getName();
                Path dst = new Path(dstPath + name);
                FSDataInputStream inputStream = sourceFS.open(srcFile.getPath(), hdfsBufferSize);
                FSDataOutputStream outputStream = destFS.create(dst, false, hdfsBufferSize, replication, blockSize);
                IOUtils.copyBytes(inputStream, outputStream, hdfsBufferSize, true);
                inputStream.close();
                outputStream.close();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
