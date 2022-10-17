package io.pixelsdb.pixels.cache;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.PixelsProto;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

// TODO: how it used? I remember that I didn't do a clear refactor
public class MockPixelsPhysicalReader {
    private static List<String> cacheIdxs = new ArrayList<>();
    private static List<String> cacheKeys = new ArrayList<>();
    private static Map<String, String> keyToIdxs = new HashMap<>();
    private static Map<Long, Long> blkSizes = new HashMap<>();

    private static Map<Long, String> blkToKeys = new HashMap<>();
    private static Map<Long, String> blkToIdxs = new HashMap<>();

    static {
        try {
            // read the mock file
            BufferedReader br = new BufferedReader(new FileReader("dumpedCache.txt"));
            String line = br.readLine();
            String idxString = "";
            String keyString = "";
            while (line != null) {
                keyString = line.split(";")[1];
                String[] keyTokens = keyString.split("-");
                long blkId = Long.parseLong(keyTokens[0]);
                short rgId = Short.parseShort(keyTokens[1]);
                short colId = Short.parseShort(keyTokens[2]);
                idxString = line.split(";")[2];
                String[] idxTokens = idxString.split("-");
                long offset = Long.parseLong(idxTokens[0]);
                int length = Integer.parseInt(idxTokens[1]);

                if (blkSizes.containsKey(blkId)) {
                    blkSizes.put(blkId, blkSizes.get(blkId) + length);
                } else {
                    blkSizes.put(blkId, (long) length);
                }
                cacheIdxs.add(idxString);
                cacheKeys.add(keyString);
                keyToIdxs.put(keyString, idxString);
                line = br.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private final long blkId;
    public MockPixelsPhysicalReader(Storage storage, String path) throws IOException
    {
        assert (storage.getScheme() == Storage.Scheme.mock);
        blkId = Long.parseLong(path);
    }

    public int read(short rgId, short colId, byte[] buf) {
        String cacheIdx = keyToIdxs.get(blkId + "-" + rgId + "-" + colId);
        String[] idxTokens = cacheIdx.split("-");
        long offset = Long.parseLong(idxTokens[0]);
        int length = Integer.parseInt(idxTokens[1]);
        if (buf.length >= length) {
            Arrays.fill(buf, 0, length, (byte) ('A' + (cacheIdx.hashCode() % 26)));
        }
        return length;
    }

    public byte[] read(short rgId, short colId)
            throws IOException
    {   // TODO: this is a bad interface
        String cacheIdx = keyToIdxs.get(blkId + "-" + rgId + "-" + colId);
        String[] idxTokens = cacheIdx.split("-");
        long offset = Long.parseLong(idxTokens[0]);
        int length = Integer.parseInt(idxTokens[1]);
        byte[] content = new byte[length];
        Arrays.fill(content, (byte) ('A' + (cacheIdx.hashCode() % 26)));
        return content;
    }

    public long getCurrentBlockId() throws IOException
    {
        return blkId;
    }
}
