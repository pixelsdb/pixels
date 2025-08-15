/*
 * Copyright 2019 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.cache;

import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Compact;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsProto;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author guodong
 */
public class TestPixelsCacheWriter
{
    @Test
    public void testSimpleCacheWriter()
    {
        try
        {
            PixelsCacheWriter cacheWriter = PixelsCacheWriter.newBuilder()
                    .setCacheBaseLocation("/home/hank/Desktop/pixels.cache")
                    .setCacheSize(1024 * 1024 * 64L)
                    .setIndexBaseLocation("/home/hank/Desktop/pixels.index")
                    .setIndexSize(1024 * 1024 * 64L)
                    .setOverwrite(true)
                    .setCachedStorage(StorageFactory.Instance().getStorage(Storage.Scheme.file))
                    .build();
            int index = 0;
            for (short i = 0; i < 1000; i++)
            {
                for (short j = 0; j < 64; j++)
                {
                    PixelsCacheKey cacheKey = new PixelsCacheKey(-1, i, j);
                    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
                    byteBuffer.putInt(index++);
                    byte[] value = byteBuffer.array();
                    cacheWriter.write(cacheKey, value);
                }
            }
            cacheWriter.flush();
            for (PixelsZoneWriter zone : cacheWriter.getZones()) {
                PixelsRadix radix = zone.getRadix();
                radix.printStats();
            }
            
            for (PixelsZoneWriter zone : cacheWriter.getZones()) {
                PixelsRadix radix1 = PixelsCacheUtil.loadRadixIndex(zone.getIndexFile());
                radix1.printStats();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void prepareCacheData()
    {
        try
        {
            // get fs
            Storage storage = StorageFactory.Instance().getStorage("hdfs");
            PixelsCacheWriter cacheWriter = PixelsCacheWriter.newBuilder()
                    .setCacheBaseLocation("/Users/Jelly/Desktop/pixels.cache")
                    .setCacheSize(1024 * 1024 * 1024L)
                    .setIndexBaseLocation("/Users/Jelly/Desktop/pixels.index")
                    .setIndexSize(1024 * 1024 * 1024L)
                    .setOverwrite(true)
                    .setCachedStorage(StorageFactory.Instance().getStorage(Storage.Scheme.file))
                    .build();
            String directory = "hdfs://node01:9000/pixels/pixels/test_105/v_1_compact";
            long cacheLength = 0L;
            List<Status> fileStatuses = storage.listStatus(directory);
            MetadataService metadataService = MetadataService.CreateInstance("node10", 18888);
            Layout layout = metadataService.getLayout("pixels", "test_105", 0);
            Compact compact = layout.getCompact();
            int cacheBorder = compact.getCacheBorder();
            List<String> cacheOrders = compact.getColumnChunkOrder().subList(0, cacheBorder);
            long startNano = System.nanoTime();
            // write cache
            for (Status fileStatus : fileStatuses)
            {
                String file = fileStatus.getPath();
                try (PixelsPhysicalReader pixelsPhysicalReader = new PixelsPhysicalReader(storage, file))
                {
                    for (int i = 0; i < cacheBorder; i++)
                    {
                        String[] cacheColumnChunkIdParts = cacheOrders.get(i).split(":");
                        short cacheRGId = Short.parseShort(cacheColumnChunkIdParts[0]);
                        short cacheColId = Short.parseShort(cacheColumnChunkIdParts[1]);
                        PixelsProto.RowGroupFooter rowGroupFooter = pixelsPhysicalReader.readRowGroupFooter(cacheRGId);
                        PixelsProto.ColumnChunkIndex chunkIndex =
                                rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(cacheColId);
                        int chunkLen = chunkIndex.getChunkLength();
                        long chunkOffset = chunkIndex.getChunkOffset();
                        cacheLength += chunkLen;
                        byte[] columnChunk = pixelsPhysicalReader.read(chunkOffset, chunkLen);
                        PixelsCacheKey cacheKey = new PixelsCacheKey(pixelsPhysicalReader.getCurrentBlockId(), cacheRGId, cacheColId);
                        cacheWriter.write(cacheKey, columnChunk);
                    }
                }
            }
            long endNano = System.nanoTime();
            System.out.println("Time cost: " + (endNano - startNano) + "ns");
            System.out.println("Total length: " + cacheLength);
            long flushStartNano = System.nanoTime();
            // flush index
            cacheWriter.flush();
            long flushEndNano = System.nanoTime();
            System.out.println("Flush time cost: " + (flushEndNano - flushStartNano) + "ns");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
