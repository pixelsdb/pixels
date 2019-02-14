package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.cache.PixelsCacheReader;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.physical.PhysicalReader;
import cn.edu.ruc.iir.pixels.common.physical.PhysicalReaderUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.List;

/**
 * pixels
 *
 * @author guodong
 */
public class TestCacheContent
{
    public static void main(String[] args)
    {
        String assignedFiles = "hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact/20190102095407_4.compact.pxl;hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact/20190102095216_3.compact_copy_20190103032605_83.pxl;hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact/20190102101758_17.compact_copy_20190103033701_117.pxl;hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact/20190102100317_9.compact.pxl;hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact/20190102094644_0.compact_copy_20190103033759_120.pxl;hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact/20190102095748_6.compact_copy_20190103033951_126.pxl;hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact/20190102095939_7.compact_copy_20190103032103_67.pxl;hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact/20190102095939_7.compact_copy_20190103033352_107.pxl;hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact/20190102095939_7.compact_copy_20190103032720_87.pxl;hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact/20190102100129_8.compact_copy_20190103034029_128.pxl;hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact/20190102101037_13.compact_copy_20190103031631_53.pxl;hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact/20190102095025_2.compact_copy_20190103031257_42.pxl;hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact/20190102100317_9.compact_copy_20190103032141_69.pxl;hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact/20190102100508_10.compact_copy_20190103032817_90.pxl;hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact/20190102101418_15.compact_copy_20190103031709_55.pxl;hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact/20190102102138_19.compact_copy_20190103031202_39.pxl";
        String[] files = assignedFiles.split(";");
        Path[] paths = new Path[files.length];
        for (int i = 0; i < files.length; i++)
        {
            paths[i] = new Path(files[i]);
        }

        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

            FileSystem fs = FileSystem.get(URI.create("hdfs://dbiir27:9000/pixels/pixels"), configuration);
            MetadataService metadataService = new MetadataService("dbiir27", 18888);
            Layout layout = metadataService.getLayout("pixels", "test_1187", 1).get(0);
            int cacheBorder = layout.getCompactObject().getCacheBorder();
            List<String> cacheOrder = layout.getCompactObject().getColumnletOrder().subList(0, cacheBorder);
            PixelsCacheReader cacheReader = PixelsCacheReader
                    .newBuilder()
                    .setIndexLocation("/Users/Jelly/Desktop/test_1187.index")
                    .setIndexSize(8589934592L)
                    .setCacheLocation("/Users/Jelly/Desktop/test_1187.cache")
                    .setCacheSize(1073741824L)
                    .build();

            for (int p = 0; p < paths.length; p++) {
                PixelsReader pixelsReader = PixelsReaderImpl
                        .newBuilder()
                        .setPath(paths[p])
                        .setFS(fs)
                        .setCacheOrder(cacheOrder)
                        .setEnableCache(true)
                        .setPixelsCacheReader(cacheReader)
                        .build();
                PhysicalReader physicalReader = PhysicalReaderUtil.newPhysicalFSReader(fs, paths[p]);
                assert physicalReader != null;
                for (short i = 0; i < 32; i++) {
                    PixelsProto.RowGroupFooter rowGroupFooter = pixelsReader.getRowGroupFooter(i);
                    for (short j = 0; j < 3; j++) {
                        PixelsProto.ColumnChunkIndex chunkIndex =
                                rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(j);
                        long chunkOffset = chunkIndex.getChunkOffset();
                        int chunkLen = (int) chunkIndex.getChunkLength();
                        byte[] fileColumnlet = new byte[chunkLen];
                        physicalReader.seek(chunkOffset);
                        physicalReader.readFully(fileColumnlet);
                        byte[] cachedColumnlet = cacheReader.get(files[p], i, j);

                        for (int k = 0; k < cachedColumnlet.length; k++) {
                            if (fileColumnlet[k] != cachedColumnlet[k]) {
                                System.out.println("Not match");
                                break;
                            }
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
