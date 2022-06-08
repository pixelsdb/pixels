package io.pixelsdb.pixels.cache;

import javax.naming.OperationNotSupportedException;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileCacheContentReader implements CacheContentReader {
    private final String baseDir;
    private Map<PixelsCacheIdx, RandomAccessFile> map = new HashMap<>(512000);

    FileCacheContentReader(String baseDir)  {
        this.baseDir = baseDir;
        try {
            BufferedReader br = new BufferedReader(new FileReader("tmp.txt"));
            String line = br.readLine();
            String idxString = "";
            while (line != null) {
                idxString = line.split(";")[2];

                String[] idxTokens = idxString.split("-");
                long offset = Long.parseLong(idxTokens[0]);
                int length = Integer.parseInt(idxTokens[1]);
                Path full = Paths.get(baseDir, offset + "-" + length);
                map.put(new PixelsCacheIdx(offset, length), new RandomAccessFile(full.toString(), "r"));
                line = br.readLine();
            }
            System.out.println("open done");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @Override
    public void read(PixelsCacheIdx idx, byte[] buf) throws IOException {
//        Path full = Paths.get(baseDir, idx.offset + "-" + idx.length);
//        RandomAccessFile file = new RandomAccessFile(full.toString(), "r");
        RandomAccessFile file = map.get(idx);
        file.read(buf, 0, idx.length);
//        file.close();
    }

    @Override
    public void batchRead(PixelsCacheIdx[] idxs, ByteBuffer buf) throws IOException {
        assert(false); // not implemented yet
    }
}
