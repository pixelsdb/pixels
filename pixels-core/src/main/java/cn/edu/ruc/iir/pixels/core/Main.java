package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Compact;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.utils.DateUtil;
import cn.edu.ruc.iir.pixels.core.compactor.CompactLayout;
import cn.edu.ruc.iir.pixels.core.compactor.PixelsCompactor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class Main
{
    @SuppressWarnings("Duplicates")
    public static void main(String[] args) throws IOException, MetadataException
    {
        // get compact layout
        MetadataService metadataService = new MetadataService("dbiir01", 18888);
        List<Layout> layouts = metadataService.getLayouts("pixels", "test_105");
        System.out.println("existing number of layouts: " + layouts.size());
        Layout layout = null;
        int layoutId = Integer.parseInt(args[0]);
        for (Layout layout1 : layouts)
        {
            if (layout1.getId() == layoutId)
            {
                layout = layout1;
                break;
            }
        }

        Compact compact = layout.getCompactObject();
        int numRowGroupInBlock = compact.getNumRowGroupInBlock();
        CompactLayout compactLayout = CompactLayout.fromCompact(compact);

        // get input file paths
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        FileSystem fs = FileSystem.get(URI.create("hdfs://dbiir01:9000/"), conf);
        FileStatus[] statuses = fs.listStatus(
                new Path("hdfs://dbiir01:9000/pixels/pixels/test_105/v_" + layout.getVersion() + "_order"));

        // compact
        int NO = 0;
        for (int i = 0; i + numRowGroupInBlock < statuses.length; i+=numRowGroupInBlock)
        {
            List<Path> sourcePaths = new ArrayList<>();
            for (int j = 0; j < numRowGroupInBlock; ++j)
            {
                //System.out.println(statuses[i+j].getPath().toString());
                sourcePaths.add(statuses[i+j].getPath());
            }

            long start = System.currentTimeMillis();


            String filePath = "hdfs://dbiir01:9000/pixels/pixels/test_105/v_" + layout.getVersion() + "_compact/" +
                    NO + "_" +
                    DateUtil.getCurTime() +
                    ".compact.pxl";
            PixelsCompactor pixelsCompactor =
                    PixelsCompactor.newBuilder()
                            .setSourcePaths(sourcePaths)
                            .setCompactLayout(compactLayout)
                            .setFS(fs)
                            .setFilePath(new Path(filePath))
                            .setBlockSize(2L *1024*1024*1024)
                            .setReplication((short) 2)
                            .setBlockPadding(false)
                            .build();
            pixelsCompactor.compact();
            pixelsCompactor.close();


            NO++;

            System.out.println(((System.currentTimeMillis() - start) / 1000.0) + " s for [" + filePath + "]");
        }
    }
}
