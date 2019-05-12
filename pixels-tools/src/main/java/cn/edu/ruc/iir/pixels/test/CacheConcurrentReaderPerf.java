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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * pixels
 *
 * java -jar xxx.jar dbiir02 3 /home/iir/opt/pixels/logs/
 *
 * @author guodong
 */
public class CacheConcurrentReaderPerf
{
    private ConcurrentHashMap<String, PixelsCacheIdx> cacheContainer = new ConcurrentHashMap<>();
    private List<String> readColumnetIds = new ArrayList<>();
    private ConfigFactory config = ConfigFactory.Instance();
    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;

    public CacheConcurrentReaderPerf()
            throws Exception
    {
        cacheFile = new MemoryMappedFile(config.getProperty("cache.location"), Long.parseLong(config.getProperty("cache.size")));
        indexFile = new MemoryMappedFile(config.getProperty("index.location"), Long.parseLong(config.getProperty("index.size")));
    }

    // args: hostname layout_version log_path
    public static void main(String[] args)
    {
        try {
            CacheConcurrentReaderPerf cacheConcurrentReaderPerf = new CacheConcurrentReaderPerf();
            cacheConcurrentReaderPerf.prepare(args[0], Integer.parseInt(args[1]));

            Thread[] threads = new Thread[10];
            for (int i = 0; i < 10; i++)
            {
                CacheSearcher cacheReader = new CacheSearcher(i, cacheConcurrentReaderPerf.cacheFile,
                                                              cacheConcurrentReaderPerf.indexFile,
                                                              cacheConcurrentReaderPerf.readColumnetIds,
                                                              cacheConcurrentReaderPerf.cacheContainer,
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
        List<String> cachedColumnletIds = layout.getCompactObject().getColumnletOrder().subList(0, 100);
        FSFactory fsFactory = FSFactory.Instance(config.getProperty("hdfs.config.dir"));
        List<Path> paths = fsFactory.listFiles(layout.getCompactPath());
        List<Path> localFiles = new ArrayList<>(30);
        for (Path path : paths)
        {
            if (fsFactory.getBlockLocations(path, 0, Long.MAX_VALUE).get(0).getHostText().equalsIgnoreCase(hostName))
            {
                localFiles.add(path);
            }
        }

        PixelsCacheReader cacheReader = PixelsCacheReader
                .newBuilder()
                .setIndexFile(indexFile)
                .setCacheFile(cacheFile)
                .build();
        for (Path path : localFiles)
        {
            String fileName = path.toString();
            for (String columnletId : cachedColumnletIds)
            {
                String[] columnletIdSplits = columnletId.split(":");
                short rgId = Short.parseShort(columnletIdSplits[0]);
                short colId = Short.parseShort(columnletIdSplits[1]);
                long blockId = fsFactory.listLocatedBlocks(fileName).get(0).getBlock().getBlockId();
                String id = blockId + "-" + rgId + "-" + colId;
                PixelsCacheIdx cacheIdx = cacheReader.search(blockId, rgId, colId);
                if (cacheIdx == null)
                {
                    System.out.println("Find null for " + id);
                }
                else {
                    readColumnetIds.add(id);
                    cacheContainer.put(id, cacheIdx);
                }
            }
        }
        readColumnetIds = Collections.unmodifiableList(readColumnetIds);
    }

    static class CacheSearcher implements Runnable
    {
        private final int id;
        private final List<String> readColumnletIds;
        private final int readLimit;
        private final PixelsCacheReader cacheReader;
        private final ConcurrentHashMap<String, PixelsCacheIdx> cacheContainer;
        private String logDir;

        public CacheSearcher(int id, MemoryMappedFile cacheFile, MemoryMappedFile indexFile, List<String> readColumnletIds,
                             ConcurrentHashMap<String, PixelsCacheIdx> cacheContainer, String logDir)
        {
            this.id = id;
            this.readColumnletIds = readColumnletIds;
            this.readLimit = readColumnletIds.size();
            this.cacheContainer = cacheContainer;
            this.logDir = logDir;
            this.cacheReader = PixelsCacheReader
                    .newBuilder()
                    .setIndexFile(indexFile)
                    .setCacheFile(cacheFile)
                    .build();
        }

        @Override
        public void run()
        {
            try {
                if (!logDir.endsWith("/"))
                {
                    logDir = logDir + "/";
                }
                BufferedWriter writer = new BufferedWriter(new FileWriter(logDir + id));
                Random random = new Random(System.nanoTime());
                int counter = 0;
                while (counter < 5000)
                {
                    int readIndex = random.nextInt(readLimit);
                    if (readIndex < 0)
                    {
                        continue;
                    }
                    String columnletId = readColumnletIds.get(readIndex);
                    String[] columnletIdSplits = columnletId.split("-");
                    PixelsCacheIdx cacheIdx = cacheReader.search(Long.parseLong(columnletIdSplits[0]), Short.parseShort(columnletIdSplits[1]),
                                                       Short.parseShort(columnletIdSplits[2]));
                    PixelsCacheIdx expected = cacheContainer.get(columnletId);
                    if (cacheIdx == null)
                    {
                        if (expected == null)
                        {
                            writer.write("[ok]both null. " + id + ". " + columnletId);
                            writer.newLine();
                        }
                        else
                        {
                            writer.write("[error]" + id + ". " + columnletId + ", null:" + expected.toString());
                            writer.newLine();
                        }
                        continue;
                    }
                    if (cacheIdx.equals(expected))
                    {
                        writer.write("[ok]" + id + ". " + columnletId);
                        writer.newLine();
                    }
                    else
                    {
                        writer.write("[error]" + id + ". " + columnletId + ", " + cacheIdx.toString() + ":" + expected.toString());
                        writer.newLine();
                    }
                    counter++;
                    writer.flush();
                }
                writer.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    static class CacheReader implements Runnable
    {
        private final int id;
        private final List<String> readColumnletIds;
        private final int readLimit;
        private final PixelsCacheReader cacheReader;
        private final ConcurrentHashMap<String, byte[]> cacheContainer;
        private String logDir;

        public CacheReader(int id, MemoryMappedFile cacheFile, MemoryMappedFile indexFile, List<String> readColumnletIds,
                           ConcurrentHashMap<String, byte[]> cacheContainer, String logDir)
        {
            this.id = id;
            this.readColumnletIds = readColumnletIds;
            this.readLimit = readColumnletIds.size();
            this.cacheContainer = cacheContainer;
            this.logDir = logDir;
            this.cacheReader = PixelsCacheReader
                    .newBuilder()
                    .setIndexFile(indexFile)
                    .setCacheFile(cacheFile)
                    .build();
        }

        @Override
        public void run()
        {
            try {
                if (!logDir.endsWith("/"))
                {
                    logDir = logDir + "/";
                }
                BufferedWriter writer = new BufferedWriter(new FileWriter(logDir + id));
                Random random = new Random(System.nanoTime());
                int counter = 0;
                while (counter < 5000)
                {
                    int readIndex = random.nextInt(readLimit);
                    if (readIndex < 0)
                    {
                        continue;
                    }
                    String columnletId = readColumnletIds.get(readIndex);
                    String[] columnletIdSplits = columnletId.split("-");
                    byte[] columnlet = cacheReader.get(Long.parseLong(columnletIdSplits[0]), Short.parseShort(columnletIdSplits[1]),
                                                       Short.parseShort(columnletIdSplits[2]));
                    byte[] expected = cacheContainer.get(columnletId);
                    if (Arrays.equals(columnlet, expected))
                    {
                        writer.write("[ok]" + id + ". " + columnletId);
                        writer.newLine();
                    }
                    else
                    {
                        writer.write("[error]" + id + ". " + columnletId + ", " + columnlet.length + ":" + expected.length);
                        writer.newLine();
                    }
                    counter++;
                    writer.flush();
                }
                writer.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
