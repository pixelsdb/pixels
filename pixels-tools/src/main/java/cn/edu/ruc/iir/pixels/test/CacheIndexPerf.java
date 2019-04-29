package cn.edu.ruc.iir.pixels.test;

import cn.edu.ruc.iir.pixels.cache.MemoryMappedFile;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheIdx;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheKey;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheReader;
import cn.edu.ruc.iir.pixels.common.exception.FSException;
import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.physical.FSFactory;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * pixels
 * java -jar xxx.jar hostname metahost layout_version thread_num
 * java -jar pixels-tools-0.1.0-SNAPSHOT-full.jar dbiir24 dbiir27 3 1
 *
 * @author guodong
 */
public class CacheIndexPerf
{
    private static ConfigFactory config = ConfigFactory.Instance();
    private static PixelsCacheKey[] pixelsCacheKeys;
    private static List<String> cachedColumnlets;
    private static List<Path> cachedPaths = new ArrayList<>();

    public static void main(String[] args)
    {
        try
        {
            long prepareStart = System.currentTimeMillis();
            CacheIndexPerf cacheIndexPerf = new CacheIndexPerf();
            cacheIndexPerf.prepare(args[0], args[1], Integer.parseInt(args[2]));
            int threadNum = Integer.parseInt(args[3]);

            Thread[] threads = new Thread[threadNum];
            int readCount = cachedColumnlets.size() * cachedPaths.size();
            pixelsCacheKeys = new PixelsCacheKey[readCount];

            MemoryMappedFile indexFile = new MemoryMappedFile(config.getProperty("index.location"),
                                                              Long.parseLong(config.getProperty("index.size")));

            int idx = 0;
            for (Path path : cachedPaths)
            {
                for (int i = 0; i < cachedColumnlets.size(); i++)
                {
                    String[] columnletIdSplits = cachedColumnlets.get(i).split(":");
                    PixelsCacheKey cacheKey = new PixelsCacheKey(path.toString(),
                                                                 Short.parseShort(columnletIdSplits[0]),
                                                                 Short.parseShort(columnletIdSplits[1]));
                    pixelsCacheKeys[idx++] = cacheKey;
                }
            }

            for (int i = 0; i < threadNum; i++)
            {
                int[] accesses = new int[readCount];
                Random random = new Random(System.nanoTime());
                for (int k = 0; k < readCount; k++)
                {
                    accesses[k] = random.nextInt(readCount);
                }
                threads[i] = new Thread(new CacheSearcher(accesses, indexFile));
            }
            long prepareEnd = System.currentTimeMillis();
            System.out.println("[prepare]: " + (prepareEnd - prepareStart));

            long searchStart = System.nanoTime();
            for (int i = 0; i < threadNum; i++)
            {
                threads[i].start();
            }
            for (int i = 0; i < threadNum; i++)
            {
                threads[i].join();
            }
            long searchEnd = System.nanoTime();
            System.out.println("[search]: " + (searchEnd - searchStart));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    // prepare correct answers
    private void prepare(String hostName, String metaHost, int layoutVersion)
            throws MetadataException, FSException
    {
        MetadataService metadataService = new MetadataService(metaHost, 18888);
        Layout layout = metadataService.getLayout("pixels", "test_1187", layoutVersion).get(0);
        cachedColumnlets =
                layout.getCompactObject().getColumnletOrder().subList(0, layout.getCompactObject().getCacheBorder());
        FSFactory fsFactory = FSFactory.Instance(config.getProperty("hdfs.config.dir"));
        List<Path> paths = fsFactory.listFiles(layout.getCompactPath());
        for (Path path : paths)
        {
            if (fsFactory.getBlockLocations(path, 0, Long.MAX_VALUE).get(0).getHostText().equalsIgnoreCase(hostName))
            {
                cachedPaths.add(path);
            }
        }
    }

    static class CacheSearcher implements Runnable
    {
        private final int[] idxes;
        private final MemoryMappedFile indexFile;

        CacheSearcher(int[] idxes, MemoryMappedFile indexFile)
        {
            this.idxes = idxes;
            this.indexFile = indexFile;
        }

        @Override
        public void run()
        {
            PixelsCacheReader cacheReader = PixelsCacheReader.newBuilder()
                                                             .setCacheFile(null)
                                                             .setIndexFile(indexFile)
                                                             .build();
            int totalAcNum = 0;
            long searchStart = System.nanoTime();
            for (int i = 0; i < idxes.length; i++)
            {
                PixelsCacheKey cacheKey = pixelsCacheKeys[idxes[i]];
                PixelsCacheIdx idx = cacheReader.search(cacheKey.getBlockId(),
                                                        (short) cacheKey.getRowGroupId(),
                                                        (short) cacheKey.getColumnId());
                if (idx == null)
                {
                    System.out.println("[error] cannot find " + cacheKey.getBlockId()
                                               + "-" + cacheKey.getRowGroupId()
                                               + "-" + cacheKey.getColumnId());
                }
                else
                {
//                    totalAcNum += idx.getAccNum();
                }
            }
            long searchEnd = System.nanoTime();
            System.out.println("[search]: " + totalAcNum + "," + (searchEnd - searchStart));
        }
    }
}

