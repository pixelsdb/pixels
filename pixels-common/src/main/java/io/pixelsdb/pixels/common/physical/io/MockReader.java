package io.pixelsdb.pixels.common.physical.io;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Storage;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class MockReader implements PhysicalReader {
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

    private long blockId;
    private long length;


    public MockReader(Storage storage, String path) {
        // path is a block id
        assert (storage.getScheme() == Storage.Scheme.mock);
        blockId = Long.parseLong(path);
        length = blkSizes.get(blockId);
    }

    @Override
    public long getFileLength() throws IOException {
        return length;
    }

    @Override
    public void seek(long desired) throws IOException {

    }

    @Override
    public ByteBuffer readFully(int length) throws IOException {
        return null;
    }

    @Override
    public void readFully(byte[] buffer) throws IOException {

    }

    @Override
    public void readFully(byte[] buffer, int offset, int length) throws IOException {

    }

    @Override
    public boolean supportsAsync() {
        return false;
    }

    @Override
    public CompletableFuture<ByteBuffer> readAsync(long offset, int length) throws IOException {
        return null;
    }

    @Override
    public long readLong() throws IOException {
        return 0;
    }

    @Override
    public int readInt() throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public String getPath() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public long getBlockId() throws IOException {
        return 0;
    }

    @Override
    public Storage.Scheme getStorageScheme() {
        return null;
    }
}
