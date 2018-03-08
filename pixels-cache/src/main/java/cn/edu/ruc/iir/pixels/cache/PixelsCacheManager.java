package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.cache.mq.MappedBusReader;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * pixels cache manager.
 * This is a scheduled thread.
 *
 * @author guodong
 */
public class PixelsCacheManager
        extends Thread
{
    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;
    private final Map<ColumnletId, ColumnletIdx> index;
    private final MappedBusReader mqReader;
    private final Map<ColumnletId, Integer> missingCounter;

    public PixelsCacheManager(MemoryMappedFile cacheFile, MemoryMappedFile indexFile, MappedBusReader mqReader)
    {
        this.cacheFile = cacheFile;
        this.indexFile = indexFile;
        this.index = new HashMap<>();
        this.mqReader = mqReader;
        this.missingCounter = new HashMap<>();
    }

    @Override
    public void run()
    {
        try {
            while (mqReader.next()) {
                byte[] columnlet = new byte[16];
                mqReader.readBuffer(columnlet, 0);
                ByteBuffer columnletBuf = ByteBuffer.wrap(columnlet);
                ColumnletId columnletId = new ColumnletId(columnletBuf.getLong(),
                        columnletBuf.getInt(), columnletBuf.getInt());
                if (missingCounter.containsKey(columnletId)) {
                    missingCounter.put(columnletId, missingCounter.get(columnletId) + 1);
                }
                else {
                    missingCounter.put(columnletId, 0);
                }
            }
        }
        catch (EOFException e) {
            e.printStackTrace();
        }
    }

    private void compact()
    {}
}
