package cn.edu.ruc.iir.pixels.test;

import cn.edu.ruc.iir.pixels.cache.MemoryMappedFile;
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
public class CacheGetPerf
{
    private static ConfigFactory config = ConfigFactory.Instance();
    private static PixelsCacheKey[] cacheKeys;

    // args: hostname layout_version log_path
    public static void main(String[] args)
    {
        try
        {
            long prepareStart = System.currentTimeMillis();
            CacheGetPerf checkCacheConcurrentReader = new CacheGetPerf();
            int readCount = checkCacheConcurrentReader.prepare(args[0], args[1], Integer.parseInt(args[2]));
            int threadNum = Integer.parseInt(args[3]);

            CacheReader[] readers = new CacheReader[threadNum];
            Thread[] threads = new Thread[threadNum];

            MemoryMappedFile indexFile = new MemoryMappedFile(config.getProperty("index.location"),
                                                              Long.parseLong(config.getProperty("index.size")));
            MemoryMappedFile cacheFile = new MemoryMappedFile(config.getProperty("cache.location"),
                                                              Long.parseLong(config.getProperty("cache.size")));

            for (int i = 0; i < threadNum; i++)
            {
                Random random = new Random(System.nanoTime());
                int[] idxes = new int[readCount];
                for (int k = 0; k < readCount; k++)
                {
                    idxes[k] = random.nextInt(readCount);
                }
                CacheReader reader = new CacheReader(indexFile, cacheFile, idxes);
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
            System.out.println("[get total]: " + (readEnd - readStart) + "ms");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    // prepare correct answers
    private int prepare(String hostName, String metaHost, int layoutVersion)
            throws MetadataException, FSException
    {
        MetadataService metadataService = new MetadataService(metaHost, 18888);
        Layout layout = metadataService.getLayout("pixels", "test_1187", layoutVersion);
        List<String> cachedColumnlets =
                layout.getCompactObject().getColumnletOrder().subList(0, layout.getCompactObject().getCacheBorder());
        FSFactory fsFactory = FSFactory.Instance(config.getProperty("hdfs.config.dir"));
        List<Path> paths = fsFactory.listFiles(layout.getCompactPath());
        List<Path> cachedPaths = new ArrayList<>();
        for (Path path : paths)
        {
            if (fsFactory.getBlockLocations(path, 0, Long.MAX_VALUE).get(0).getHostText().equalsIgnoreCase(hostName))
            {
                cachedPaths.add(path);
            }
        }
        int idx = 0;
        int size = cachedColumnlets.size() * cachedPaths.size();
        cacheKeys = new PixelsCacheKey[size];
        for (Path path : cachedPaths)
        {
            for (String columnlet : cachedColumnlets)
            {
                String[] parts = columnlet.split(":");
                PixelsCacheKey cacheKey = new PixelsCacheKey(path.toString(), Short.parseShort(parts[0]),
                                                             Short.parseShort(parts[1]));
                cacheKeys[idx++] = cacheKey;
            }
        }
        return size;
    }

    static class CacheReader implements Runnable
    {
        private final int[] idxes;
        private final PixelsCacheReader cacheReader;

        CacheReader(MemoryMappedFile indexFile, MemoryMappedFile cacheFile,
                    int[] idxes)
        {
            this.idxes = idxes;
            this.cacheReader = PixelsCacheReader.newBuilder()
                                                .setCacheFile(cacheFile)
                                                .setIndexFile(indexFile)
                                                .build();
        }

        @Override
        public void run()
        {
            long readStart = System.nanoTime();
            long readSize = 0;
            for (int i : idxes)
            {
                PixelsCacheKey key = cacheKeys[i];
                byte[] content = cacheReader
                        .get(key.blockId, key.rowGroupId, key.columnId);
                readSize += content.length;
            }
            long readEnd = System.nanoTime();
            System.out.println(
                    "[get]: " + readSize + "," + (readEnd - readStart) + "," + (readSize * 1.0 / (readEnd - readStart)));
        }
    }
}

