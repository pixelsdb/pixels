package cn.edu.ruc.iir.pixels.core.compactor;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Compact;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.utils.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsCompactorCli
{
    public static void main(String[] args)
    {
        PixelsCompactorCli compactor = new PixelsCompactorCli();
        String method = args[0];
        if (method.equalsIgnoreCase("order"))
        {
            try
            {
                compactor.orderCompact();
            }
            catch (MetadataException | IOException e)
            {
                e.printStackTrace();
            }
        }
        else if (method.equalsIgnoreCase("naive"))
        {
            try
            {
                compactor.basicCompact();
            }
            catch (MetadataException | IOException e)
            {
                e.printStackTrace();
            }
        }
        else {
            System.out.println("java -jar pixels-compactor.jar naive/order");
            System.exit(-1);
        }
    }

    private void basicCompact() throws MetadataException, IOException
    {
        // get compact layout
        MetadataService metadataService = new MetadataService("presto00", 18888);
        List<Layout> layouts = metadataService.getLayouts("pixels", "testnull_pixels");
        System.out.println("existing number of layouts: " + layouts.size());
        Layout layout = layouts.get(0);
        Compact compact = layout.getCompactObject();
        int rowGroupNum = compact.getNumRowGroupInBlock();
        int colNum = compact.getNumColumn();
        CompactLayout compactLayout = new CompactLayout(rowGroupNum, colNum);
        for (int i = 0; i < rowGroupNum; i++)
        {
            for (int j = 0; j < colNum; j++)
            {
                compactLayout.append(i, j);
            }
        }

        compact(layout.getOrderPath(), layout.getCompactPath(), compactLayout);
    }

    private void orderCompact() throws MetadataException, IOException
    {
        // get compact layout
        MetadataService metadataService = new MetadataService("presto00", 18888);
        List<Layout> layouts = metadataService.getLayouts("pixels", "testnull_pixels");
        System.out.println("existing number of layouts: " + layouts.size());
        Layout layout = layouts.get(0);
        Compact compact = layout.getCompactObject();
        CompactLayout compactLayout = CompactLayout.fromCompact(compact);

        compact(layout.getOrderPath(), layout.getCompactPath(), compactLayout);
    }

    private void compact(String inputPath, String outputPath, CompactLayout compactLayout)
            throws IOException
    {
        // get input file paths
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(URI.create("hdfs://presto00:9000/"), conf);
        FileStatus[] statuses = fs.listStatus(
                new Path(inputPath));

        // compact
        int NO = 0;
        for (int i = 0; i + 16 < statuses.length; i+=16)
        {
            List<Path> sourcePaths = new ArrayList<>();
            for (int j = 0; j < 16; ++j)
            {
                //System.out.println(statuses[i+j].getPath().toString());
                sourcePaths.add(statuses[i+j].getPath());
            }

            long start = System.currentTimeMillis();

            if (!outputPath.endsWith("/"))
            {
                outputPath = outputPath + "/";
            }
            String filePath = outputPath +
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
                            .setReplication((short) 1)
                            .setBlockPadding(false)
                            .build();
            pixelsCompactor.compact();
            pixelsCompactor.close();

            NO++;

            System.out.println(((System.currentTimeMillis() - start) / 1000.0) + " s for [" + filePath + "]");
        }
    }
}
