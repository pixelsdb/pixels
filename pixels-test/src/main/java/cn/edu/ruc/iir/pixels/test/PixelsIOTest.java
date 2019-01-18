package cn.edu.ruc.iir.pixels.test;

import cn.edu.ruc.iir.pixels.common.physical.PhysicalFSReader;
import cn.edu.ruc.iir.pixels.common.physical.PhysicalReaderUtil;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsIOTest
{
    private static Logger logger = LogManager.getLogger(PixelsIOTest.class);
    public static void main(String[] args)
    {
//        String dir = "hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact/";
        String dir = args[0];
        ConfigFactory configFactory = ConfigFactory.Instance();
        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        byte[] oneBite = new byte[10240]; // 10K each time
        try {
            FileSystem fs = FileSystem.get(URI.create(configFactory.getProperty("pixels.warehouse.path")), configuration);
            FileStatus[] fileStatuses = fs.listStatus(new Path(dir));
            for (FileStatus fileStatus : fileStatuses)
            {
                Path path = fileStatus.getPath();
                PhysicalFSReader physicalFSReader = PhysicalReaderUtil.newPhysicalFSReader(fs, path);
                assert physicalFSReader != null;
                long fileLen = physicalFSReader.getFileLength();
                int offset = 0;
                long readFileStart = System.currentTimeMillis();
                while (offset + 10240 < fileLen) {
                    physicalFSReader.readFully(oneBite);
                    offset += 10240;
                }
                long readFileEnd = System.currentTimeMillis();
                logger.info("Read file: " + path.toString() + ", cost: " + (readFileEnd - readFileStart) + "ms");
                System.out.println("Read file: " + path.toString() + ", cost: " + (readFileEnd - readFileStart) + "ms");
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
