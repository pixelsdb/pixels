package cn.edu.ruc.iir.pixels.test;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * pixels
 *
 * @author guodong
 */
public class CacheLogAnalyzer
{
    private String logPath = "/Users/Jelly/Desktop/pixels/cache/logs/";
    private String csvPath = "/Users/Jelly/Desktop/pixels/cache/csv/";

    @Test
    public void analyze()
            throws IOException
    {
        Files.list(Paths.get(logPath)).forEach(this::processLogFile);
        Files.list(Paths.get(csvPath)).forEach(this::summarize);
    }

    @Test
    public void cleanLogFile()
    {
        String input = "/Users/Jelly/Desktop/pixels/cache/Mar17/pixels-cache.csv";
        String output = "/Users/Jelly/Desktop/pixels/cache/Mar17/pixels-cache-clean.csv";
        try
        {
            BufferedReader reader = new BufferedReader(new FileReader(input));
            BufferedWriter writer = new BufferedWriter(new FileWriter(output));
            String line;
            while ((line = reader.readLine()) != null)
            {
                if (line.contains("[cache stat"))
                {
                    writer.write(line);
                    writer.newLine();
                }
            }
            reader.close();
            writer.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private void processLogFile(Path path)
    {
        List<CacheAccessMetric> cacheAccessMetrics = new ArrayList<>();
        List<CacheAccessMetric> windowAccessBuffer = new ArrayList<>(100);
        List<CacheReadItem> windowReadBuffer = new ArrayList<>(100);
        List<CacheSearchItem> windowSearchBuffer = new ArrayList<>(100);
        List<CacheAccessMetric> removeList = new ArrayList<>();

        String fileName = path.getFileName().toString().split("\\.")[0];
        int lineWindow = 100;

        try
        {
            BufferedReader reader = new BufferedReader(new FileReader(path.toFile()));
            String line;
            long lineNum = 0L;
            while ((line = reader.readLine()) != null)
            {
                lineNum++;
                if (line.contains("[DEBUG]"))
                {
                    continue;
                }
                Timestamp timestamp = Timestamp.valueOf(line.substring(0, 19));
                int index = line.indexOf("[cache access]");
                if (index != -1)
                {
                    line = line.substring(index + 14);
                    String[] lineSplits = line.split(",");
                    String cacheId = lineSplits[0];
                    boolean cacheHit = lineSplits[1].equalsIgnoreCase("true");
                    long cacheSize = Long.parseLong(lineSplits[2]);
                    int acTime = Integer.parseInt(lineSplits[3]);
                    CacheAccessMetric accessMetric = new CacheAccessMetric(cacheId, timestamp, lineNum, acTime, cacheHit, cacheSize);
                    CacheReadItem readFound = null;
                    CacheSearchItem searchFound = null;
                    for (CacheReadItem cacheReadItem : windowReadBuffer)
                    {
                        if (cacheReadItem.cacheId.equalsIgnoreCase(cacheId) && Math.abs(lineNum - cacheReadItem.lineNum) <= lineWindow)
                        {
                            accessMetric.readTime = cacheReadItem.readTime;
                            accessMetric.readLineNum = cacheReadItem.lineNum;
                            readFound = cacheReadItem;
                            break;
                        }
                    }
                    if (readFound != null)
                    {
                        windowReadBuffer.remove(readFound);
                    }
                    for (CacheSearchItem cacheSearchItem : windowSearchBuffer)
                    {
                        if (cacheSearchItem.cacheId.equalsIgnoreCase(cacheId) && Math.abs(lineNum - cacheSearchItem.lineNum) <= lineWindow)
                        {
                            accessMetric.searchTime = cacheSearchItem.searchTime;
                            accessMetric.searchLineNum = cacheSearchItem.lineNum;
                            searchFound = cacheSearchItem;
                            break;
                        }
                    }
                    if (searchFound != null)
                    {
                        windowSearchBuffer.remove(searchFound);
                    }

                    windowAccessBuffer.add(accessMetric);
                    continue;
                }

                index = line.indexOf("[cache search]");
                if (index != -1)
                {
                    line = line.substring(index + 14);
                    String[] lineSplits = line.split(",");
                    String cacheId = lineSplits[0];
                    int searchTime = Integer.parseInt(lineSplits[1]);
                    boolean found = false;
                    for (CacheAccessMetric metric : windowAccessBuffer)
                    {
                        if (metric.readTime != -1 && metric.searchTime != -1)
                        {
                            cacheAccessMetrics.add(metric);
                            removeList.add(metric);
                        }
                        else if (metric.cacheId.equalsIgnoreCase(cacheId) && Math.abs(lineNum - metric.getAcLineNum()) <= lineWindow)
                        {
                            metric.setSearchTime(searchTime);
                            metric.setSearchLineNum(lineNum);
                            found = true;
                        }
                    }
                    for (CacheAccessMetric metric : removeList)
                    {
                        windowAccessBuffer.remove(metric);
                    }
                    removeList.clear();
                    if (false == found)
                    {
                        windowSearchBuffer.add(new CacheSearchItem(cacheId, timestamp, searchTime, lineNum));
                    }
                    continue;
                }

                index = line.indexOf("[cache read]");
                if (index != -1)
                {
                    line = line.substring(index + 12);
                    String[] lineSplits = line.split(",");
                    String cacheId = lineSplits[0];
                    int readTime = Integer.parseInt(lineSplits[1]);
                    boolean found = false;
                    for (CacheAccessMetric metric : windowAccessBuffer)
                    {
                        if (metric.readTime != -1 && metric.searchTime != -1)
                        {
                            cacheAccessMetrics.add(metric);
                            removeList.add(metric);
                        }
                        else if (metric.cacheId.equalsIgnoreCase(cacheId) && Math.abs(lineNum - metric.getAcLineNum()) <= lineWindow)
                        {
                            metric.setReadTime(readTime);
                            metric.setReadLineNum(lineNum);
                            found = true;
                        }
                    }
                    for (CacheAccessMetric metric : removeList)
                    {
                        windowAccessBuffer.remove(metric);
                    }
                    removeList.clear();
                    if (false == found)
                    {
                        windowReadBuffer.add(new CacheReadItem(cacheId, timestamp, readTime, lineNum));
                    }
                }
            }

            cacheAccessMetrics.addAll(windowAccessBuffer);
            reader.close();
            System.out.println("Done reading file " + fileName);

            String filePath = csvPath;
            if (!csvPath.endsWith("/"))
            {
                filePath += "/";
            }
            filePath = filePath + fileName + ".csv";
            BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
            String header = "id,cid,act,srt,rt,hit,size,ts,acl,srl,rl";
            writer.write(header);
            writer.newLine();
            int id = 0;
            for (CacheAccessMetric cacheAccessMetric : cacheAccessMetrics)
            {
                writer.write(id + ",");
                id++;
                writer.write(cacheAccessMetric.toString());
                writer.newLine();
            }
            writer.close();
            System.out.println("Done processing file " + fileName);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private void summarize(Path path)
    {
        if (false == path.getFileName().toString().endsWith("csv"))
        {
            return;
        }
        List<Integer> acTimes = new ArrayList<>();
        List<Integer> srTimes = new ArrayList<>();
        List<Integer> rTimes = new ArrayList<>();

        String line;
        try
        {
            BufferedReader reader = new BufferedReader(new FileReader(path.toFile()));
            reader.readLine();
            while ((line = reader.readLine()) != null)
            {
                String[] lineSplits = line.split(",");
                acTimes.add(Integer.parseInt(lineSplits[2]));
                srTimes.add(Integer.parseInt(lineSplits[3]));
                rTimes.add(Integer.parseInt(lineSplits[4]));
            }
            acTimes.sort(Integer::compareTo);
            srTimes.sort(Integer::compareTo);
            rTimes.sort(Integer::compareTo);

            System.out.println(path.getFileName().toString().split("\\.")[0]);
            int size = acTimes.size();
            System.out.println("ac min: " + acTimes.get(0));
            System.out.println("ac max: " + acTimes.get(size - 1));
            if (size / 2 == 0)
            {
                System.out.println("ac med: " + acTimes.get(size / 2) + acTimes.get(size / 2 + 1) / 2);
            }
            else
            {
                System.out.println("ac med: " + acTimes.get(size / 2));
            }
            long acSum = 0;
            for (int t : acTimes)
            {
                acSum += t;
            }
            System.out.println("ac avg: " + acSum / size);
            System.out.println("ac 99p: " + acTimes.get(Math.round(size * 0.99f)));
            System.out.println("ac 95p: " + acTimes.get(Math.round(size * 0.95f)));
            System.out.println("ac 90p: " + acTimes.get(Math.round(size * 0.90f)));

            System.out.println("sr min: " + srTimes.get(0));
            System.out.println("sr max: " + srTimes.get(size - 1));
            if (size / 2 == 0)
            {
                System.out.println("sr med: " + srTimes.get(size / 2) + srTimes.get(size / 2 + 1) / 2);
            }
            else
            {
                System.out.println("sr med: " + srTimes.get(size / 2));
            }
            long srSum = 0;
            for (int t : srTimes)
            {
                srSum += t;
            }
            System.out.println("sr avg: " + srSum / size);
            System.out.println("sr 99p: " + srTimes.get(Math.round(size * 0.99f)));
            System.out.println("sr 95p: " + srTimes.get(Math.round(size * 0.95f)));
            System.out.println("sr 90p: " + srTimes.get(Math.round(size * 0.90f)));

            System.out.println("r min: " + rTimes.get(0));
            System.out.println("r max: " + rTimes.get(size - 1));
            if (size / 2 == 0)
            {
                System.out.println("r med: " + rTimes.get(size / 2) + rTimes.get(size / 2 + 1) / 2);
            }
            else
            {
                System.out.println("r med: " + rTimes.get(size / 2));
            }
            long rSum = 0;
            for (int t : rTimes)
            {
                rSum += t;
            }
            System.out.println("r avg: " + rSum / size);
            System.out.println("r 99p: " + rTimes.get(Math.round(size * 0.99f)));
            System.out.println("r 95p: " + rTimes.get(Math.round(size * 0.95f)));
            System.out.println("r 90p: " + rTimes.get(Math.round(size * 0.90f)));
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    class CacheAccessMetric
    {
        private final String cacheId;
        private final Timestamp timestamp;
        private final long acLineNum;
        private final int acTime;
        private final boolean acHit;
        private final long acSize;

        private int searchTime = -1;
        private long searchLineNum = 0;
        private int readTime = -1;
        private long readLineNum = 0;

        public CacheAccessMetric(String cacheId, Timestamp timestamp, long lineNum, int acTime, boolean acHit, long acSize)
        {
            this.cacheId = cacheId;
            this.timestamp = timestamp;
            this.acLineNum = lineNum;
            this.acTime = acTime;
            this.acHit = acHit;
            this.acSize = acSize;
        }

        public void setSearchTime(int searchTime)
        {
            this.searchTime = searchTime;
        }

        public void setReadTime(int readTime)
        {
            this.readTime = readTime;
        }

        public void setSearchLineNum(long searchLineNum)
        {
            this.searchLineNum = searchLineNum;
        }

        public void setReadLineNum(long readLineNum)
        {
            this.readLineNum = readLineNum;
        }

        public String getCacheId()
        {
            return cacheId;
        }

        public Timestamp getTimestamp()
        {
            return timestamp;
        }

        public long getAcLineNum()
        {
            return acLineNum;
        }

        public int getAcTime()
        {
            return acTime;
        }

        public boolean isAcHit()
        {
            return acHit;
        }

        public long getAcSize()
        {
            return acSize;
        }

        public int getSearchTime()
        {
            return searchTime;
        }

        public int getReadTime()
        {
            return readTime;
        }

        public long getSearchLineNum()
        {
            return searchLineNum;
        }

        public long getReadLineNum()
        {
            return readLineNum;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(cacheId).append(",")
                    .append(acTime).append(",")
                    .append(searchTime).append(",")
                    .append(readTime).append(",")
                    .append(acHit).append(",")
                    .append(acSize).append(",")
                    .append(timestamp.toString()).append(",")
                    .append(acLineNum).append(",")
                    .append(searchLineNum).append(",")
                    .append(readLineNum);
            return sb.toString();
        }
    }

    class CacheSearchItem
    {
        private final String cacheId;
        private final Timestamp timestamp;
        private final int searchTime;
        private final long lineNum;

        CacheSearchItem(String cacheId, Timestamp timestamp, int searchTime, long lineNum)
        {
            this.cacheId = cacheId;
            this.timestamp = timestamp;
            this.searchTime = searchTime;
            this.lineNum = lineNum;
        }
    }

    class CacheReadItem
    {
        private final String cacheId;
        private final Timestamp timestamp;
        private final int readTime;
        private final long lineNum;

        CacheReadItem(String cacheId, Timestamp timestamp, int readTime, long lineNum)
        {
            this.cacheId = cacheId;
            this.timestamp = timestamp;
            this.readTime = readTime;
            this.lineNum = lineNum;
        }
    }
}
