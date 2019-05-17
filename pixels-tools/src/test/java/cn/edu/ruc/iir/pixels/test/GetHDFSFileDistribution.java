package cn.edu.ruc.iir.pixels.test;

import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * pixels
 *
 * @author guodong
 */
public class GetHDFSFileDistribution
{
    @Test
    public void test()
    {
        String dir = "hdfs://dbiir10:9000/pixels/pixels/test_1187/v_1_compact/";
        ConfigFactory configFactory = ConfigFactory.Instance();
        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try
        {
            FileSystem fs = FileSystem.get(URI.create(configFactory.getProperty("pixels.warehouse.path")), configuration);
            FileStatus[] fileStatuses = fs.listStatus(new Path(dir));
            Map<String, Integer> hostBlockMap = new HashMap<>();
            for (FileStatus fileStatus : fileStatuses)
            {
                BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, Long.MAX_VALUE);
                for (BlockLocation blockLocation : blockLocations)
                {
                    String[] hosts = blockLocation.getHosts();
                    for (String host : hosts)
                    {
                        if (!hostBlockMap.containsKey(host))
                        {
                            hostBlockMap.put(host, 1);
                        }
                        else
                        {
                            int originV = hostBlockMap.get(host);
                            hostBlockMap.put(host, originV + 1);
                        }
                    }
                }
            }
            for (String host : hostBlockMap.keySet())
            {
                System.out.println("" + host + ":" + hostBlockMap.get(host));
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
