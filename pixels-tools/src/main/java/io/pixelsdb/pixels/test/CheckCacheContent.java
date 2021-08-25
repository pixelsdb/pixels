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
package io.pixelsdb.pixels.test;

import io.pixelsdb.pixels.cache.MemoryMappedFile;
import io.pixelsdb.pixels.cache.PixelsCacheReader;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Compact;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * pixels
 * <p>
 * String path = "hdfs://dbiir01:9000/pixels/pixels/test_1187/v_1_compact/20190223144340_13.compact_copy_20190223153853_93.pxl";
 * <p>
 * java -jar xxx.jar path rowgroup_id col_id layout_version
 *
 * @author guodong
 */
public class CheckCacheContent
{
    public static void main(String[] args)
            throws Exception
    {
        String path = args[0];
        int rgId = Integer.parseInt(args[1]);
        int colId = Integer.parseInt(args[2]);
        int layoutVersion = Integer.parseInt(args[3]);

        MemoryMappedFile cacheFile;
        MemoryMappedFile indexFile;
        ConfigFactory config = ConfigFactory.Instance();
        cacheFile = new MemoryMappedFile(config.getProperty("cache.location"),
                Long.parseLong(config.getProperty("cache.size")));
        indexFile = new MemoryMappedFile(config.getProperty("index.location"),
                Long.parseLong(config.getProperty("index.size")));
        //FSFactory fsFactory = FSFactory.Instance(config.getProperty("hdfs.config.dir"));
        Storage storage = StorageFactory.Instance().getStorage(config.getProperty("storage.scheme"));

        MetadataService metadataService = new MetadataService("dbiir01", 18888);
        Layout layout = metadataService.getLayout("pixels", "test_1187", layoutVersion);
        Compact compact = layout.getCompactObject();
        int cacheBorder = compact.getCacheBorder();
        List<String> columnletOrder = compact.getColumnletOrder();
        List<String> cachedColumnlets = columnletOrder.subList(0, cacheBorder);

        PixelsCacheReader cacheReader = PixelsCacheReader
                .newBuilder()
                .setCacheFile(cacheFile)
                .setIndexFile(indexFile)
                .build();
        long blockId = storage.getId(path);
        ByteBuffer cacheContent = cacheReader.get(blockId, (short) rgId, (short) colId);
        System.out.println("Cache content length " + cacheContent.capacity());

        PixelsReader pixelsReader = PixelsReaderImpl
                .newBuilder()
                .setPath(path)
                .setStorage(storage)
                .setEnableCache(false)
                .setCacheOrder(cachedColumnlets)
                .setPixelsCacheReader(cacheReader)
                .build();
        PixelsProto.RowGroupFooter rowGroupFooter = pixelsReader.getRowGroupFooter(rgId);
        PhysicalReader physicalReader =
                PhysicalReaderUtil.newPhysicalReader(storage, path);
        PixelsProto.ColumnChunkIndex chunkIndex = rowGroupFooter.getRowGroupIndexEntry()
                .getColumnChunkIndexEntries(colId);
        physicalReader.seek(chunkIndex.getChunkOffset());
        byte[] diskContent = new byte[(int) chunkIndex.getChunkLength()];
        physicalReader.readFully(diskContent);

        System.out.println("Disk content length " + chunkIndex.getChunkLength());
        if (cacheContent.capacity() != diskContent.length)
        {
            System.out.println("Length not match");
            return;
        }
        for (int i = 0; i < cacheContent.capacity(); i++)
        {
            if (cacheContent.get(i) != diskContent[i])
            {
                System.out.println("byte not match " + cacheContent.get(i) + ":" + diskContent[i]);
            }
        }
    }
}
