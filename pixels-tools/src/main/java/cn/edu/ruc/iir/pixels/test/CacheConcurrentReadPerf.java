package cn.edu.ruc.iir.pixels.test;

import cn.edu.ruc.iir.pixels.cache.MemoryMappedFile;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheReader;
import cn.edu.ruc.iir.pixels.common.exception.FSException;
import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.physical.FSFactory;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * pixels
 * java -jar xxx.jar hostname layout_version logs_dir thread_num
 * java -jar xxx.jar dbiir02 2 /home/iir/opt/pixels/logs/ 1
 *
 * @author guodong
 */
public class CacheConcurrentReadPerf
{
    private ConfigFactory config = ConfigFactory.Instance();
    private List<String> cachedColumnlets;
    private List<Path> cachedPaths;
    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;

    public CacheConcurrentReadPerf()
            throws Exception
    {
        cacheFile = new MemoryMappedFile(config.getProperty("cache.location"), Long.parseLong(config.getProperty("cache.size")));
        indexFile = new MemoryMappedFile(config.getProperty("index.location"), Long.parseLong(config.getProperty("index.size")));
    }

    // args: hostname layout_version log_path
    public static void main(String[] args)
    {
        try {
            CacheConcurrentReadPerf checkCacheConcurrentReader = new CacheConcurrentReadPerf();
            checkCacheConcurrentReader.prepare(args[0], Integer.parseInt(args[1]));
            int threadNum = Integer.parseInt(args[3]);

            Thread[] threads = new Thread[threadNum];
            for (int i = 0; i < threadNum; i++)
            {
                CacheReader cacheReader = new CacheReader(i, checkCacheConcurrentReader.cacheFile,
                                                          checkCacheConcurrentReader.indexFile,
                                                          checkCacheConcurrentReader.cachedColumnlets,
                                                          checkCacheConcurrentReader.cachedPaths,
                                                          args[2]);
                Thread t = new Thread(cacheReader);
                threads[i] = t;
                t.start();
            }

            for (Thread t : threads)
            {
                t.join();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    // prepare correct answers
    private void prepare(String hostName, int layoutVersion)
            throws MetadataException, FSException
    {
        MetadataService metadataService = new MetadataService("dbiir01", 18888);
        Layout layout = metadataService.getLayout("pixels", "test_1187", layoutVersion);
        this.cachedColumnlets = layout.getCompactObject().getColumnletOrder().subList(0, layout.getCompactObject().getCacheBorder());
        FSFactory fsFactory = FSFactory.Instance(config.getProperty("hdfs.config.dir"));
        List<Path> paths = fsFactory.listFiles(layout.getCompactPath());
        this.cachedPaths = new ArrayList<>(30);
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
        private final int id;
        private final List<String> cachedColumnlets;
        private final List<Path> cachedPaths;
        private final PixelsCacheReader cacheReader;
        private final int readLimit;
        private Map<Long, CacheStat> stats = new HashMap<>();
        private String logDir;

        public CacheReader(int id, MemoryMappedFile cacheFile, MemoryMappedFile indexFile, List<String> cachedColumnlets,
                           List<Path> cachedPaths, String logDir)
        {
            this.id = id;
            this.cachedColumnlets = cachedColumnlets;
            this.cachedPaths = cachedPaths;
            this.logDir = logDir;
            this.readLimit = cachedColumnlets.size();
            this.cacheReader = PixelsCacheReader
                    .newBuilder()
                    .setIndexFile(indexFile)
                    .setCacheFile(cacheFile)
                    .build();
        }

        @Override
        public void run()
        {
            ConfigFactory config = ConfigFactory.Instance();
            FSFactory fsFactory = null;
            try
            {
                fsFactory = FSFactory.Instance(config.getProperty("hdfs.config.fir"));
            } catch (FSException e)
            {
                e.printStackTrace();
            }
            try {
                if (!logDir.endsWith("/"))
                {
                    logDir = logDir + "/";
                }
                BufferedWriter writer = new BufferedWriter(new FileWriter(logDir + id + ".csv"));
                Random random = new Random(System.nanoTime());
                long id = 0;
                for (Path path : cachedPaths)
                {
                    int counter = 0;
                    long blockId = fsFactory.listLocatedBlocks(path).get(0).getBlock().getBlockId();
                    while (counter < readLimit)
                    {
                        int readIndex = random.nextInt(readLimit);
                        if (readIndex < 0)
                        {
                            continue;
                        }
                        String columnletId = cachedColumnlets.get(readIndex);
                        String[] columnletIdSplits = columnletId.split(":");
                        long startNano = System.nanoTime();
                        byte[] columnlet = cacheReader.get(blockId, Short.parseShort(columnletIdSplits[0]),
                                                           Short.parseShort(columnletIdSplits[1]));
                        long cost = System.nanoTime() - startNano;
                        CacheStat stat = new CacheStat(id, columnlet.length, cost);
                        stats.put(id++, stat);
                        counter++;
                    }
                }
                for (CacheStat cacheStat : stats.values())
                {
                    writer.write(cacheStat.size + "," + cacheStat.cost);
                    writer.newLine();
                }
                writer.close();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            } catch (FSException e)
            {
                e.printStackTrace();
            }
        }
    }

    static class CacheStat
    {
        private final long id;
        private final long size;
        private final long cost;

        public CacheStat(long id, long size, long cost)
        {
            this.id = id;
            this.size = size;
            this.cost = cost;
        }
    }
}
