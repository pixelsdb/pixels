package cn.edu.ruc.iir.pixels.test;

import cn.edu.ruc.iir.pixels.cache.MemoryMappedFile;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheReader;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Compact;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.physical.FSFactory;
import cn.edu.ruc.iir.pixels.common.physical.PhysicalReader;
import cn.edu.ruc.iir.pixels.common.physical.PhysicalReaderUtil;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.core.PixelsReaderImpl;
import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * pixels
 *
 * @author guodong
 */
public class CheckCacheContent
{
    public static void main(String[] args)
            throws Exception
    {
        String path = "hdfs://dbiir01:9000/pixels/pixels/test_1187/v_1_compact/20190223144340_13.compact_copy_20190223153853_93.pxl";

        MemoryMappedFile cacheFile;
        MemoryMappedFile indexFile;
        ConfigFactory config = ConfigFactory.Instance();
        cacheFile = new MemoryMappedFile(config.getProperty("cache.location"), Long.parseLong(config.getProperty("cache.size")));
        indexFile = new MemoryMappedFile(config.getProperty("index.location"), Long.parseLong(config.getProperty("index.size")));
        FSFactory fsFactory = FSFactory.Instance(config.getProperty("hdfs.config.dir"));

        MetadataService metadataService = new MetadataService("dbiir01", 18888);
        Layout layout = metadataService.getLayout("pixels", "test_1187", 2).get(0);
        Compact compact = layout.getCompactObject();
        int cacheBorder = compact.getCacheBorder();
        List<String> columnletOrder = compact.getColumnletOrder();
        List<String> cachedColumnlets = columnletOrder.subList(0, cacheBorder);

        PixelsCacheReader cacheReader = PixelsCacheReader
                .newBuilder()
                .setCacheFile(cacheFile)
                .setIndexFile(indexFile)
                .build();
        byte[] cacheContent = cacheReader.get(path, (short) 8, (short) 337);
        System.out.println("Cache content length " + cacheContent.length);

        PixelsReader pixelsReader = PixelsReaderImpl
                .newBuilder()
                .setPath(new Path(path))
                .setFS(fsFactory.getFileSystem().get())
                .setEnableCache(false)
                .setCacheOrder(cachedColumnlets)
                .setPixelsCacheReader(cacheReader)
                .build();
        PixelsProto.RowGroupFooter rowGroupFooter = pixelsReader.getRowGroupFooter(8);
        PhysicalReader physicalReader =
                PhysicalReaderUtil.newPhysicalFSReader(fsFactory.getFileSystem().get(), new Path(path));
        PixelsProto.ColumnChunkIndex chunkIndex = rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(337);
        physicalReader.seek(chunkIndex.getChunkOffset());
        byte[] diskContent = new byte[(int) chunkIndex.getChunkLength()];
        physicalReader.readFully(diskContent);

        System.out.println("Disk content length " + chunkIndex.getChunkLength());
        if (cacheContent.length != diskContent.length)
        {
            System.out.println("Length not match");
            return;
        }
        for (int i = 0; i < cacheContent.length; i++)
        {
            if (cacheContent[i] != diskContent[i])
            {
                System.out.println("byte not match " + cacheContent[i] + ":" + diskContent[i]);
            }
        }
    }
}
