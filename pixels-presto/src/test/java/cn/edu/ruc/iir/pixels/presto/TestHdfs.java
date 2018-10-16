package cn.edu.ruc.iir.pixels.presto;

import cn.edu.ruc.iir.pixels.presto.impl.FSFactory;
import cn.edu.ruc.iir.pixels.presto.impl.PixelsPrestoConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto
 * @ClassName: TestHdfs
 * @Description: read info from hdfs
 * @author: tao
 * @date: Create in 2018-02-23 11:21
 **/
public class TestHdfs {

    /**
     * @ClassName: TestHdfs
     * @Title: reference: [Java访问HDFS中的数据块](https://www.cnblogs.com/zhangyinhua/p/7695700.html#_lab2_1_0)
     * @Description: reading block info from namenode
     * @param:
     * @author: tao
     * @date: 下午2:45 18-2-23
     */
    @Test
    public void testReadBlock() {
        String filePath = "hdfs://presto00:9000/pixels/v2/point2000w-10.pxl";

        PixelsPrestoConfig config = new PixelsPrestoConfig().setPixelsHome("");
        FSFactory fsFactory = new FSFactory(config);
        List<LocatedBlock> allBlocks = fsFactory.listLocatedBlocks(filePath);
        for (LocatedBlock block : allBlocks) {
            ExtendedBlock eBlock = block.getBlock();
            System.out.println("BlockId: " + eBlock.getBlockId());
            System.out.println("BlockName: " + eBlock.getBlockName());
            System.out.println("BlockSize: " + block.getBlockSize());
            System.out.println(
                    block.getStartOffset());
            System.out.println(eBlock.getGenerationStamp());
            // 获取当前的数据块所在的DataNode的信息
            DatanodeInfo[] locations =
                    block.getLocations();
            for (DatanodeInfo info : locations) {
                System.out.println("IpAddr: " + info.getIpAddr());
                System.out.println("HostName: " + info.getHostName());
                System.out.println(info.getName());
            }
        }
    }

    @Test
    public void testDistribute() {
        PixelsPrestoConfig config = new PixelsPrestoConfig().setPixelsHome("");
        String hdfsDir = "hdfs://dbiir01:9000/pixels/pixels/test_105/v_0_order";
        FSFactory fsFactory = new FSFactory(config);
        List<Path> hdfsList = fsFactory.listFiles(hdfsDir);
        Map<String, Integer> hostMap = new HashMap<>();
        for(Path hdfsPath : hdfsList){
            String filePath = hdfsPath.toString();
            List<LocatedBlock> allBlocks = fsFactory.listLocatedBlocks(filePath);
            System.out.println(hdfsPath);
            for (LocatedBlock block : allBlocks) {
                DatanodeInfo[] locations =
                        block.getLocations();
                for (DatanodeInfo info : locations) {
                    String hostname = info.getHostName();
                    if(hostMap.get(hostname) == null){
                        hostMap.put(hostname, 1);
                    }else {
                        int count = hostMap.get(hostname);
                        count++;
                        hostMap.put(hostname, count);
                    }
                    System.out.println(hostMap.keySet().toString());
                    System.out.println(hostMap.values());
                }
            }
        }
    }

    @Test
    public void testListFiles() throws IOException
    {
        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(configuration);
        FileStatus[] statuses = fs.listStatus(new Path("hdfs://presto00:9000/"));
        System.out.println(statuses.length);
    }
}
