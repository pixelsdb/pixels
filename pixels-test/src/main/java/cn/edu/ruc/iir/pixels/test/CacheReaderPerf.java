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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * pixels
 *
 * java -jar pixels-test-0.1.0-SNAPSHOT-full.jar /home/iir/sbin/drop_caches.sh /home/iir/opt/pixels/logs/cache_perf.csv /home/iir/opt/pixels/logs/cache_workload.txt
 * @author guodong
 */
public class CacheReaderPerf
{
    private static MemoryMappedFile cacheFile;
    private static MemoryMappedFile indexFile;
    private static String cacheDropScript;
    private static FSFactory fsFactory;
    private static Map<Path, List<PixelsProto.RowGroupFooter>> rowGroupMetas = new HashMap<>();

    // 1. get table layout and cache order
    // 2. generate read workloads
    // 2.1 sequentially read: read columnlets sequentially
    // 2.2 randomly read: read columnlets randomly
    // 3. prepare global environment: file footer/header, row group footer, metadata, etc.
    // 4. run read workloads and generate performance report.

    public static void main(String[] args)
    {
        cacheDropScript = args[0];
        String logFile = args[1];
        String workloadFile = args[2];
        CacheReaderPerf cacheReaderPerf = new CacheReaderPerf();

        List<String> cachedColumnlets;
        List<Path> localFiles;
        ConfigFactory config = ConfigFactory.Instance();

        String hostName = System.getenv("HOSTNAME");
        if (hostName == null)
        {
            try
            {
                hostName = InetAddress.getLocalHost().getHostName();
            }
            catch (UnknownHostException e)
            {
                e.printStackTrace();
            }
        }
        System.out.println("Hostname: " + hostName);

        try {
            long mapFileStartNano = System.nanoTime();
            cacheFile = new MemoryMappedFile(config.getProperty("cache.location"), Long.parseLong(config.getProperty("cache.size")));
            long mapFileEndNano = System.nanoTime();
            long cacheMemInitCost = mapFileEndNano - mapFileStartNano;
            mapFileStartNano = System.nanoTime();
            indexFile = new MemoryMappedFile(config.getProperty("index.location"), Long.parseLong(config.getProperty("index.size")));
            mapFileEndNano = System.nanoTime();
            long indexMemInitCost = mapFileEndNano - mapFileStartNano;
            fsFactory = FSFactory.Instance(config.getProperty("hdfs.config.dir"));
            System.out.println("cache file init: " + cacheMemInitCost + ", index file init: " + indexMemInitCost);

            // get cached columnlets
            MetadataService metadataService = new MetadataService("dbiir27", 18888);
            Layout layout = metadataService.getLayout("pixels", "test_1187", 2).get(0);
            Compact compact = layout.getCompactObject();
            int cacheBorder = compact.getCacheBorder();
            List<String> columnletOrder = compact.getColumnletOrder();
            cachedColumnlets = columnletOrder.subList(0, cacheBorder);
            System.out.println("Get cached columnlets");

            // get local read files
            List<Path> paths = fsFactory.listFiles(layout.getCompactPath());
            localFiles = new ArrayList<>(30);
            for (Path path : paths)
            {
                if (fsFactory.getBlockLocations(path, 0, Long.MAX_VALUE).get(0).getHostText().equalsIgnoreCase(hostName))
                {
                    localFiles.add(path);
                }
            }
            System.out.println("Get local files: " + localFiles.size());

            // prepare metadata
            PixelsCacheReader cacheReader = PixelsCacheReader
                    .newBuilder()
                    .setCacheFile(cacheFile)
                    .setIndexFile(indexFile)
                    .build();
            for (Path path : localFiles)
            {
                PixelsReader pixelsReader = PixelsReaderImpl
                        .newBuilder()
                        .setPath(path)
                        .setFS(fsFactory.getFileSystem().get())
                        .setEnableCache(false)
                        .setCacheOrder(cachedColumnlets)
                        .setPixelsCacheReader(cacheReader)
                        .build();
                List<PixelsProto.RowGroupFooter> rowGroupFooters = new ArrayList<>();
                for (int i = 0; i < 32; i++)
                {
                    rowGroupFooters.add(pixelsReader.getRowGroupFooter(i));
                }
                rowGroupMetas.put(path, rowGroupFooters);
            }
            System.out.println("Prepared metadata");

            // generate read workloads
            String[] seqReadWorkload0 = new String[160];
            cachedColumnlets.subList(0, 160).toArray(seqReadWorkload0);
            String[] seqReadWorkload1 = new String[320];
            cachedColumnlets.subList(0, 320).toArray(seqReadWorkload1);
            String[] seqReadWorkload2 = new String[640];
            cachedColumnlets.subList(0, 640).toArray(seqReadWorkload2);
            String[] rndReadWorkload0 = new String[160];
            Random random = new Random(System.nanoTime());
            for (int i = 0; i < 160; i++)
            {
                int rnd = random.nextInt(cacheBorder);
                rndReadWorkload0[i] = cachedColumnlets.get(rnd);
            }
            String[] rndReadWorkload1 = new String[320];
            random = new Random(System.nanoTime());
            for (int i = 0; i < 320; i++)
            {
                int rnd = random.nextInt(cacheBorder);
                rndReadWorkload1[i] = cachedColumnlets.get(rnd);
            }
            String[] rndReadWorkload2 = new String[640];
            for (int i = 0; i < 640; i++)
            {
                int rnd = random.nextInt(cacheBorder);
                rndReadWorkload2[i] = cachedColumnlets.get(rnd);
            }
            System.out.println("Generated workloads");
            BufferedWriter workloadWriter = new BufferedWriter(new FileWriter(new File(workloadFile)));
            workloadWriter.write("s0");
            workloadWriter.newLine();
            for (String c : seqReadWorkload0)
            {
                workloadWriter.write(c + ",");
            }
            workloadWriter.newLine();
            workloadWriter.write("s1");
            workloadWriter.newLine();
            for (String c : seqReadWorkload1)
            {
                workloadWriter.write(c + ",");
            }
            workloadWriter.newLine();
            workloadWriter.write("s2");
            workloadWriter.newLine();
            for (String c : seqReadWorkload2)
            {
                workloadWriter.write(c + ",");
            }
            workloadWriter.newLine();
            workloadWriter.write("r0");
            workloadWriter.newLine();
            for (String c : rndReadWorkload0)
            {
                workloadWriter.write(c + ",");
            }
            workloadWriter.newLine();
            workloadWriter.write("r1");
            workloadWriter.newLine();
            for (String c : rndReadWorkload1)
            {
                workloadWriter.write(c + ",");
            }
            workloadWriter.newLine();
            workloadWriter.write("r2");
            workloadWriter.newLine();
            for (String c : rndReadWorkload2)
            {
                workloadWriter.write(c + ",");
            }
            workloadWriter.newLine();
            workloadWriter.close();

            // run seq read workloads
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(logFile)));
            String header = "id,workload,file,cost,size";
            writer.write(header);
            writer.newLine();

            List<StatisticMetric> cs0 = cacheReaderPerf.cacheRead("cs0", seqReadWorkload0, localFiles);
            long id = 0;
            for (StatisticMetric metric : cs0)
            {
                writer.write("" + (id++) + ",cs0," + metric.id + "," + metric.cost + "," + metric.size);
                writer.newLine();
            }
            writer.flush();

            List<StatisticMetric> ds0 = cacheReaderPerf.diskRead("ds0", seqReadWorkload0, localFiles);
            for (StatisticMetric metric : ds0)
            {
                writer.write("" + (id++) + ",ds0," + metric.id + "," + metric.cost + "," + metric.size);
                writer.newLine();
            }
            writer.flush();

            List<StatisticMetric> cs1 = cacheReaderPerf.cacheRead("cs1", seqReadWorkload1, localFiles);
            for (StatisticMetric metric : cs1)
            {
                writer.write("" + (id++) + ",cs1," + metric.id + "," + metric.cost + "," + metric.size);
                writer.newLine();
            }
            writer.flush();

            List<StatisticMetric> ds1 = cacheReaderPerf.diskRead("ds1", seqReadWorkload1, localFiles);
            for (StatisticMetric metric : ds1)
            {
                writer.write("" + (id++) + ",ds1," + metric.id + "," + metric.cost + "," + metric.size);
                writer.newLine();
            }
            writer.flush();

            List<StatisticMetric> cs2 = cacheReaderPerf.cacheRead("cs2", seqReadWorkload2, localFiles);
            for (StatisticMetric metric : cs2)
            {
                writer.write("" + (id++) + ",cs2," + metric.id + "," + metric.cost + "," + metric.size);
                writer.newLine();
            }
            writer.flush();

            List<StatisticMetric> ds2 = cacheReaderPerf.diskRead("ds2", seqReadWorkload2, localFiles);
            for (StatisticMetric metric : ds2)
            {
                writer.write("" + (id++) + ",ds2," + metric.id + "," + metric.cost + "," + metric.size);
                writer.newLine();
            }
            writer.flush();

            // run random read workloads
            List<StatisticMetric> cr0 = cacheReaderPerf.cacheRead("cr0", rndReadWorkload0, localFiles);
            for (StatisticMetric metric : cr0)
            {
                writer.write("" + (id++) + ",cr0," + metric.id + "," + metric.cost + "," + metric.size);
                writer.newLine();
            }
            writer.flush();

            List<StatisticMetric> dr0 = cacheReaderPerf.diskRead("dr0", rndReadWorkload0, localFiles);
            for (StatisticMetric metric : dr0)
            {
                writer.write("" + (id++) + ",dr0," + metric.id + "," + metric.cost + "," + metric.size);
                writer.newLine();
            }
            writer.flush();

            List<StatisticMetric> cr1 = cacheReaderPerf.cacheRead("cr1", rndReadWorkload1, localFiles);
            for (StatisticMetric metric : cr1)
            {
                writer.write("" + (id++) + ",cr1," + metric.id + "," + metric.cost + "," + metric.size);
                writer.newLine();
            }
            writer.flush();

            List<StatisticMetric> dr1 = cacheReaderPerf.diskRead("dr1", rndReadWorkload1, localFiles);
            for (StatisticMetric metric : dr1)
            {
                writer.write("" + (id++) + ",dr1," + metric.id + "," + metric.cost + "," + metric.size);
                writer.newLine();
            }
            writer.flush();

            List<StatisticMetric> cr2 = cacheReaderPerf.cacheRead("cr2", rndReadWorkload2, localFiles);
            for (StatisticMetric metric : cr2)
            {
                writer.write("" + (id++) + ",cr2," + metric.id + "," + metric.cost + "," + metric.size);
                writer.newLine();
            }
            writer.flush();

            List<StatisticMetric> dr2 = cacheReaderPerf.diskRead("dr2", rndReadWorkload2, localFiles);
            for (StatisticMetric metric : dr2)
            {
                writer.write("" + (id++) + ",dr2," + metric.id + "," + metric.cost + "," + metric.size);
                writer.newLine();
            }

            writer.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<StatisticMetric> diskRead(String id, String[] columnlets, List<Path> files)
            throws IOException, InterruptedException
    {
        System.out.println("Disk reading workload " + id);
        // clear cache
        System.out.println("Clearing cache...");
        ProcessBuilder processBuilder = new ProcessBuilder(cacheDropScript);
        Process process = processBuilder.start();
        process.waitFor();
        System.out.println("Done clearing cache...");

        ColumnletId[] columnletIds = new ColumnletId[columnlets.length];
        for (int i = 0; i < columnlets.length; i++)
        {
            short rgId = Short.parseShort(columnlets[i].split(":")[0]);
            short colId = Short.parseShort(columnlets[i].split(":")[1]);
            columnletIds[i] = new ColumnletId(rgId, colId);
        }

        List<StatisticMetric> metrics = new ArrayList<>();
        for (Path path : files)
        {
            System.out.println("Disk reading file " + path.toString());
            PhysicalReader physicalReader =
                    PhysicalReaderUtil.newPhysicalFSReader(fsFactory.getFileSystem().get(), path);
            List<PixelsProto.RowGroupFooter> rowGroupFooters = rowGroupMetas.get(path);
            List<PixelsProto.ColumnChunkIndex> chunkIndices = new ArrayList<>();
            for (ColumnletId columnletId : columnletIds)
            {
                PixelsProto.ColumnChunkIndex chunkIndex =
                        rowGroupFooters.get(columnletId.rgId).getRowGroupIndexEntry()
                                       .getColumnChunkIndexEntries(columnletId.colId);
                chunkIndices.add(chunkIndex);
            }
            chunkIndices.sort(Comparator.comparingLong(PixelsProto.ColumnChunkIndex::getChunkOffset));
            long currentOffset = chunkIndices.get(0).getChunkOffset();
            List<Chunk> chunks = new ArrayList<>();
            Chunk chunk = new Chunk();
            chunk.offset = currentOffset;
            chunk.length = 0;
            for (PixelsProto.ColumnChunkIndex chunkIndex : chunkIndices)
            {
                long chunkOffset = chunkIndex.getChunkOffset();
                int chunkLen = (int) chunkIndex.getChunkLength();
                if (chunk.merge(chunkOffset, chunkLen))
                {
                    continue;
                }
                chunks.add(chunk);
                chunk = new Chunk();
                chunk.offset = chunkOffset;
                chunk.length = chunkLen;
            }
            chunks.add(chunk);
            System.out.println("Disk reading chunks " + chunks.size());

            long readStartNano = System.nanoTime();
            long size = 0;
            for (Chunk ck : chunks)
            {
                physicalReader.seek(ck.offset);
                byte[] content = new byte[ck.length];
                physicalReader.readFully(content);
                size += content.length;
            }
            long readEndNano = System.nanoTime();
            long cost = readEndNano - readStartNano;
            StatisticMetric metric = new StatisticMetric(path.getName(), cost, size);
            metrics.add(metric);
            physicalReader.close();
        }
        return metrics;
    }

    private List<StatisticMetric> cacheRead(String id, String[] columnlets, List<Path> files)
            throws IOException, InterruptedException
    {
        System.out.println("Cache reading workload " + id);
        // clear cache
        System.out.println("Clearing cache...");
        ProcessBuilder processBuilder = new ProcessBuilder(cacheDropScript);
        Process process = processBuilder.start();
        process.waitFor();
        System.out.println("Done clearing cache...");

        PixelsCacheReader cacheReader = PixelsCacheReader
                .newBuilder()
                .setIndexFile(indexFile)
                .setCacheFile(cacheFile)
                .build();
        ColumnletId[] columnletIds = new ColumnletId[columnlets.length];
        for (int i = 0; i < columnlets.length; i++)
        {
            short rgId = Short.parseShort(columnlets[i].split(":")[0]);
            short colId = Short.parseShort(columnlets[i].split(":")[1]);
            columnletIds[i] = new ColumnletId(rgId, colId);
        }

        // read
        List<StatisticMetric> metrics = new ArrayList<>();
        long size = 0;
        for (Path path : files)
        {
            System.out.println("Cache reading file " + path.toString());
            long readStartNano = System.nanoTime();
            for (ColumnletId columnletId : columnletIds)
            {
                byte[] content = cacheReader.get(path.toString(), columnletId.rgId, columnletId.colId);
                size += content.length;
            }
            long readEndNano = System.nanoTime();
            long cost = readEndNano - readStartNano;
            StatisticMetric metric = new StatisticMetric(path.getName(), cost, size);
            metrics.add(metric);
            size = 0;
        }
        return metrics;
    }

    class StatisticMetric
    {
        private final String id;
        private final long cost;
        private final long size;
//        private final String columnlet;

        public StatisticMetric(String id, long cost, long size)
        {
            this.id = id;
            this.cost = cost;
            this.size = size;
        }
    }

    class ColumnletId
    {
        private final short rgId;
        private final short colId;

        public ColumnletId(short rgId, short colId)
        {
            this.rgId = rgId;
            this.colId = colId;
        }
    }

    class Chunk
    {
        private long offset;
        private int length;

        public boolean merge(long off, int len)
        {
            if (offset + length == off)
            {
                length += len;
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
