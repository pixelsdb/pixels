package cn.edu.ruc.iir.pixels.test;

import cn.edu.ruc.iir.pixels.cache.MemoryMappedFile;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheIdx;
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
public class CacheReadPerf
{
    private static ConfigFactory config = ConfigFactory.Instance();
    private static List<String> cachedColumnlets;
    private static List<Path> cachedPaths = new ArrayList<>();

    public CacheReadPerf()
    {
    }

    // args: hostname layout_version log_path
    public static void main(String[] args)
    {
        try
        {
            long prepareStart = System.currentTimeMillis();
            CacheReadPerf checkCacheConcurrentReader = new CacheReadPerf();
            checkCacheConcurrentReader.prepare(args[0], args[1], Integer.parseInt(args[2]));
            int threadNum = Integer.parseInt(args[3]);

            CacheReader[] readers = new CacheReader[threadNum];
            Thread[] threads = new Thread[threadNum];
            int readCount = cachedColumnlets.size() * cachedPaths.size();

            MemoryMappedFile indexFile = new MemoryMappedFile(config.getProperty("index.location"),
                                                              Long.parseLong(config.getProperty("index.size")));
            MemoryMappedFile cacheFile = new MemoryMappedFile(config.getProperty("cache.location"),
                                                              Long.parseLong(config.getProperty("cache.size")));
            PixelsCacheReader cacheReader = PixelsCacheReader
                    .newBuilder()
                    .setIndexFile(indexFile)
                    .setCacheFile(cacheFile)
                    .build();
            PixelsCacheIdx[] cacheIdxes = new PixelsCacheIdx[readCount];
            int idx = 0;
            for (Path path : cachedPaths)
            {
                for (int i = 0; i < cachedColumnlets.size(); i++)
                {
                    String[] columnletIdSplits = cachedColumnlets.get(i).split(":");
                    PixelsCacheIdx pixelsCacheIdx = cacheReader
                            .search(path.toString(), Short.parseShort(columnletIdSplits[0]),
                                    Short.parseShort(columnletIdSplits[1]));
                    cacheIdxes[idx] = pixelsCacheIdx;
                    idx++;
                }
            }

            for (int i = 0; i < threadNum; i++)
            {
                Random random = new Random(System.nanoTime());
                long[] offsets = new long[readCount];
                int[] lengths = new int[readCount];
                for (int k = 0; k < readCount; k++)
                {
                    int id = random.nextInt(readCount);
                    offsets[k] = cacheIdxes[id].offset;
                    lengths[k] = cacheIdxes[id].length;
                }
                CacheReader reader = new CacheReader(cacheFile, offsets, lengths);
                readers[i] = reader;
            }
            long prepareEnd = System.currentTimeMillis();
            System.out.println("[prepare]: " + (prepareEnd - prepareStart) + "ms");

            long readStart = System.currentTimeMillis();
            for (int i = 0; i < threadNum; i++)
            {
                Thread t = new Thread(readers[i]);
                threads[i] = t;
                t.start();
            }
            for (Thread t : threads)
            {
                t.join();
            }
            long readEnd = System.currentTimeMillis();
            System.out.println("[read]: " + (readEnd - readStart) + "ms");
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
        Layout layout = metadataService.getLayout("pixels", "test_1187", layoutVersion);
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

    static class CacheReader implements Runnable
    {
        private final long[] offsets;
        private final int[] lengths;
        private final MemoryMappedFile cacheFile;

        public CacheReader(MemoryMappedFile cacheFile,
                           long[] offsets, int[] lengths)
        {
            this.cacheFile = cacheFile;
            this.offsets = offsets;
            this.lengths = lengths;
        }

        @Override
        public void run()
        {
            long readStart = System.nanoTime();
            long readSize = 0;
            for (int i = 0; i < offsets.length; i++)
            {
                byte[] content = new byte[lengths[i]];
                cacheFile.getBytes(offsets[i], content, 0, lengths[i]);
                readSize += content.length;
            }
            long readEnd = System.nanoTime();
            System.out.println(
                    "[read]: " + readSize + "," + (readEnd - readStart) + "," + (readSize * 1.0 / (readEnd - readStart)));
        }
    }
}

