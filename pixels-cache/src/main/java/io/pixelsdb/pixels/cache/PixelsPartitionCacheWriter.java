/*
 * Copyright 2022 PixelsDB.
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


import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.metadata.domain.Compact;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;

public class PixelsPartitionCacheWriter {

    private final static Logger logger = LogManager.getLogger(PixelsPartitionCacheWriter.class);
    private final MemoryMappedFile cacheBackFile;
    private final MemoryMappedFile indexBackFile;
    private final MemoryMappedFile indexDiskBackFile;
    private final Storage storage;
    private final EtcdUtil etcdUtil;
    private final boolean writeContent;
    private final int bandwidthLimit = 1000;


    /**
     * The host name of the node where this cache writer is running.
     */
    private final String host;
    private final int partitions; // should be a power of 2
    private final MemoryMappedFile[] cachePartitions; // length=partitions + 1
    private final MemoryMappedFile[] indexPartitions; // length=partitions + 1
    // permanent disk copy of the index file
    private final MemoryMappedFile[] indexDiskPartitions; // length = partition
    private final Function<MemoryMappedFile, @Nullable CacheIndexWriter> indexWriterFactory;

    private PixelsPartitionCacheWriter(MemoryMappedFile cacheFile,
                                       MemoryMappedFile indexFile,
                                       MemoryMappedFile indexDiskFile,
                                       MemoryMappedFile[] cachePartitions,
                                       MemoryMappedFile[] indexPartitions,
                                       MemoryMappedFile[] indexDiskPartitions,
                                       Function<MemoryMappedFile, @Nullable CacheIndexWriter> indexWriterFactory,
                                       Storage storage,
                                       int partitions,
                                       EtcdUtil etcdUtil,
                                       String host,
                                       boolean writeContent)
    {
        this.cacheBackFile = cacheFile;
        this.indexBackFile = indexFile;
        this.indexDiskBackFile = indexDiskFile;

        this.cachePartitions = cachePartitions;
        this.indexPartitions = indexPartitions;
        this.indexDiskPartitions = indexDiskPartitions;

        this.indexWriterFactory = indexWriterFactory;

        this.storage = storage;
        this.partitions = partitions;

        checkArgument(this.cachePartitions.length == this.partitions + 1);
        checkArgument(this.indexPartitions.length == this.partitions + 1);
        checkArgument(this.indexDiskPartitions.length == this.partitions);


        this.etcdUtil = etcdUtil;
        this.host = host;
        this.writeContent = writeContent;
    }

    public static class Builder
    {
        private String builderCacheLocation = "";
        private long builderCacheSize;
        private String builderIndexLocation = "";
        private String builderIndexDiskLocation = "";

        private long builderIndexSize;
        private boolean builderOverwrite = true;
        private String builderHostName = null;
        private PixelsCacheConfig cacheConfig = null;
        private int partitions = 16;
        // TODO: configure it with pixels.properties
        private Function<MemoryMappedFile, CacheIndexWriter> indexWriterFactory = RadixIndexWriter::new;
        private boolean writeContent = false;

        private Builder()
        {
        }

        public Builder setIndexType(String indexType)
        {
            checkArgument(Objects.equals(indexType, "hash") || Objects.equals(indexType, "radix"),
                    "unknown index type " + indexType);
            if (indexType.equals("hash")) {
                indexWriterFactory = HashIndexWriter::new;
            } else {
                indexWriterFactory = RadixIndexWriter::new;
            }
            return this;
        }

        public Builder setWriteContent(boolean writeContent)
        {
            this.writeContent = writeContent;
            return this;
        }

        public Builder setPartitions(int partitions)
        {
            this.partitions = partitions;
            return this;
        }

        public Builder setCacheLocation(String cacheLocation)
        {
            checkArgument(cacheLocation != null && !cacheLocation.isEmpty(),
                    "cache location should bot be empty");
            this.builderCacheLocation = cacheLocation;

            return this;
        }

        public Builder setCacheSize(long cacheSize)
        {
            checkArgument(cacheSize > 0, "cache size should be positive");
            this.builderCacheSize = MemoryMappedFile.roundTo4096(cacheSize);

            return this;
        }

        public Builder setIndexLocation(String indexLocation)
        {
            checkArgument(indexLocation != null && !indexLocation.isEmpty(),
                    "index location should not be empty");
            this.builderIndexLocation = indexLocation;

            return this;
        }

        public Builder setIndexDiskLocation(String indexDiskLocation) {
            checkArgument(indexDiskLocation != null && !indexDiskLocation.isEmpty(),
                    "index location should not be empty");
            this.builderIndexDiskLocation = indexDiskLocation;

            return this;
        }

        public Builder setIndexSize(long size)
        {
            checkArgument(size > 0, "index size should be positive");
            // TODO: dont do this rounding, enforce a full division with index.size / partitions
            this.builderIndexSize = MemoryMappedFile.roundTo4096(size);

            return this;
        }

        public Builder setOverwrite(boolean overwrite)
        {
            this.builderOverwrite = overwrite;
            return this;
        }

        public Builder setHostName(String hostName)
        {
            checkArgument(hostName != null, "hostname should not be null");
            this.builderHostName = hostName;
            return this;
        }

        public Builder setCacheConfig(PixelsCacheConfig cacheConfig)
        {
            checkArgument(cacheConfig != null, "cache config should not be null");
            this.cacheConfig = cacheConfig;
            return this;
        }

        // TODO: what is build2 used for?
        public PixelsPartitionCacheWriter build2() throws Exception {
            long cachePartitionSize = builderCacheSize / partitions;
            long indexPartitionSize = builderIndexSize / partitions;
            checkArgument (cachePartitionSize * partitions == builderCacheSize);
            checkArgument (indexPartitionSize * partitions == builderIndexSize);
            MemoryMappedFile cacheHeader = new MemoryMappedFile(
                    Paths.get(builderCacheLocation, "header").toString(), PixelsCacheUtil.CACHE_DATA_OFFSET);
            MemoryMappedFile indexHeader = new MemoryMappedFile(
                    Paths.get(builderIndexLocation, "header").toString(), PixelsCacheUtil.PARTITION_INDEX_META_SIZE);
            MemoryMappedFile indexDiskHeader = new MemoryMappedFile(
                    Paths.get(builderIndexDiskLocation, "header").toString(), PixelsCacheUtil.PARTITION_INDEX_META_SIZE);
            MemoryMappedFile[] cachePartitions = new MemoryMappedFile[partitions + 1];
            MemoryMappedFile[] indexPartitions = new MemoryMappedFile[partitions + 1];
            MemoryMappedFile[] indexDiskPartitions = new MemoryMappedFile[partitions];

            for (int partition = 0; partition < partitions; ++partition) {
                indexPartitions[partition] = new MemoryMappedFile(
                        Paths.get(builderIndexLocation, "physical-" + partition).toString(),
                        indexPartitionSize);
                indexDiskPartitions[partition] = new MemoryMappedFile(
                        Paths.get(builderIndexDiskLocation, "physical-" + partition).toString(),
                        indexPartitionSize);
                cachePartitions[partition] = new MemoryMappedFile(
                        Paths.get(builderCacheLocation, "physical-" + partition).toString(),
                        cachePartitionSize);
            }
            indexPartitions[partitions] = new MemoryMappedFile(
                    Paths.get(builderIndexLocation, "physical-" + partitions).toString(),
                    indexPartitionSize);
            cachePartitions[partitions] = new MemoryMappedFile(
                    Paths.get(builderCacheLocation, "physical-" + partitions).toString(),
                    cachePartitionSize);

            if (!builderOverwrite)
            {
                checkArgument(PixelsCacheUtil.checkMagic(indexHeader) && PixelsCacheUtil.checkMagic(cacheHeader),
                        "overwrite=false, but cacheFile and indexFile is polluted");
            }
            //   else, create a new radix tree, and initialize the index and cache file.
            else
            {
                // set the header of the file and each partition
                PixelsCacheUtil.initializePartitionMeta(indexDiskHeader, (short) partitions, indexPartitionSize);
                PixelsCacheUtil.initializePartitionMeta(indexHeader, (short) partitions, indexPartitionSize);
                PixelsCacheUtil.initializeCacheFile(cacheHeader);
                for (int i = 0; i < partitions; ++i) {
                    PixelsCacheUtil.initializeIndexFile(indexPartitions[i]);
                    PixelsCacheUtil.initializeIndexFile(indexDiskPartitions[i]);
                    PixelsCacheUtil.initializeCacheFile(cachePartitions[i]);
                }

            }
            EtcdUtil etcdUtil = EtcdUtil.Instance();

            Storage storage = StorageFactory.Instance().getStorage(cacheConfig.getStorageScheme());

            return new PixelsPartitionCacheWriter(cacheHeader, indexHeader, indexDiskHeader,
                    cachePartitions, indexPartitions, indexDiskPartitions, indexWriterFactory, storage,
                    partitions, etcdUtil, builderHostName, writeContent);
        }

        public PixelsPartitionCacheWriter build()
                throws Exception
        {
            // calculate a decent size of the builderCacheSize and BuilderIndexSize based on the partition
            long cachePartitionSize = builderCacheSize / partitions;
            long indexPartitionSize = builderIndexSize / partitions;
            checkArgument (cachePartitionSize * partitions == builderCacheSize);
            checkArgument (indexPartitionSize * partitions == builderIndexSize);
            // with an additional buffer area, totally (partitions + 1) * cachePartitionSize size
            MemoryMappedFile cacheFile = new MemoryMappedFile(builderCacheLocation, (partitions + 1) * cachePartitionSize + PixelsCacheUtil.CACHE_DATA_OFFSET);
            MemoryMappedFile indexFile = new MemoryMappedFile(builderIndexLocation, (partitions + 1) * indexPartitionSize + PixelsCacheUtil.PARTITION_INDEX_META_SIZE);
            MemoryMappedFile indexDiskFile = new MemoryMappedFile(builderIndexDiskLocation, (partitions + 1) * indexPartitionSize + PixelsCacheUtil.PARTITION_INDEX_META_SIZE);
            checkArgument (cachePartitionSize * (partitions + 1) + PixelsCacheUtil.CACHE_DATA_OFFSET < cacheFile.getSize());
            checkArgument (indexPartitionSize * (partitions + 1) + PixelsCacheUtil.PARTITION_INDEX_META_SIZE < indexFile.getSize());
            checkArgument (indexPartitionSize * (partitions + 1) + PixelsCacheUtil.PARTITION_INDEX_META_SIZE < indexDiskFile.getSize());

            // split the cacheFile and indexFile into partitions
            // the last partition serves as the buffer partition
            MemoryMappedFile[] cachePartitions = new MemoryMappedFile[partitions + 1];
            MemoryMappedFile[] indexPartitions = new MemoryMappedFile[partitions + 1];
            MemoryMappedFile[] indexDiskPartitions = new MemoryMappedFile[partitions];

            for (int partition = 0; partition < partitions; ++partition) {
                long indexOffset = PixelsCacheUtil.PARTITION_INDEX_META_SIZE + partition * indexPartitionSize;
                long cacheOffset = PixelsCacheUtil.CACHE_DATA_OFFSET + partition * cachePartitionSize;
                logger.debug(String.format("partition=%d, indexOffset=%fMiB, cacheOffset=%fGiB", partition, indexOffset / 1024.0 / 1024.0, cacheOffset / 1024.0 / 1024 / 1024));
                indexPartitions[partition] = indexFile.regionView(indexOffset, indexPartitionSize);
                indexDiskPartitions[partition] = indexDiskFile.regionView(indexOffset, indexPartitionSize);
                cachePartitions[partition] = cacheFile.regionView(cacheOffset, cachePartitionSize);
            }
            indexPartitions[partitions] = indexFile.regionView(PixelsCacheUtil.PARTITION_INDEX_META_SIZE +
                    partitions * indexPartitionSize, indexPartitionSize);
            cachePartitions[partitions] = cacheFile.regionView(PixelsCacheUtil.CACHE_DATA_OFFSET +
                    partitions * indexPartitionSize, cachePartitionSize);

            // check if cache and index exists.
            // if overwrite is not true, and cache and index file already exists, reconstruct radix from existing index.
            if (!builderOverwrite && PixelsCacheUtil.checkMagic(indexFile) && PixelsCacheUtil.checkMagic(cacheFile))
            {
                checkArgument(PixelsCacheUtil.checkMagic(indexFile) && PixelsCacheUtil.checkMagic(cacheFile),
                        "overwrite=false, but cacheFile and indexFile is polluted");
            }
            //   else initialize the index and cache file.
            else
            {
                // set the header of the file and each partition
                PixelsCacheUtil.initializePartitionMeta(indexDiskFile, (short) partitions, indexPartitionSize);
                PixelsCacheUtil.initializePartitionMeta(indexFile, (short) partitions, indexPartitionSize);
                PixelsCacheUtil.initializeCacheFile(cacheFile);
                for (int i = 0; i < partitions; ++i) {
                    PixelsCacheUtil.initializeIndexFile(indexPartitions[i]);
                    PixelsCacheUtil.initializeIndexFile(indexDiskPartitions[i]);
                    PixelsCacheUtil.initializeCacheFile(cachePartitions[i]);
                }

            }
            EtcdUtil etcdUtil = EtcdUtil.Instance();

            Storage storage = StorageFactory.Instance().getStorage(cacheConfig.getStorageScheme());

            return new PixelsPartitionCacheWriter(cacheFile, indexFile, indexDiskFile,
                    cachePartitions, indexPartitions, indexDiskPartitions, indexWriterFactory, storage,
                    partitions, etcdUtil, builderHostName, writeContent);
        }
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public MemoryMappedFile getIndexFile()
    {
        return indexBackFile;
    }

    /**
     * <p>
     * This function is only used to bulk load all the cache content at one time.
     * Readers will be blocked until this function is finished.
     * </p>
     * Return code:
     * -1: update failed.
     * 0: no updates are needed or update successfully.
     * 2: update size exceeds the limit.
     * bulk load method
     */
    public int updateAll(int version, Layout layout)
    {
        try
        {
            // get the caching file list
            String key = Constants.CACHE_LOCATION_LITERAL + version + "_" + host;
            KeyValue keyValue = etcdUtil.getKeyValue(key);
            if (keyValue == null)
            {
                logger.debug("Found no allocated files. No updates are needed. " + key);
                return 0;
            }
            String fileStr = keyValue.getValue().toString(StandardCharsets.UTF_8);
            String[] files = fileStr.split(";");
            Compact compact = layout.getCompact();
            int cacheBorder = compact.getCacheBorder();
            List<String> cacheColumnletOrders = compact.getColumnletOrder().subList(0, cacheBorder);
            return internalUpdateAll(version, cacheColumnletOrders, files);
        }
        catch (IOException | InterruptedException e)
        {
            e.printStackTrace();
            return -1;
        }
    }

    public int updateIncremental(int version, Layout layout)
    {
        try
        {
            // get the caching file list
            String key = Constants.CACHE_LOCATION_LITERAL + version + "_" + host;
            KeyValue keyValue = etcdUtil.getKeyValue(key);
            if (keyValue == null)
            {
                logger.debug("Found no allocated files. No updates are needed. " + key);
                return 0;
            }
            String fileStr = keyValue.getValue().toString(StandardCharsets.UTF_8);
            String[] files = fileStr.split(";");
            Compact compact = layout.getCompact();
            int cacheBorder = compact.getCacheBorder();
            List<String> cacheColumnletOrders = compact.getColumnletOrder().subList(0, cacheBorder);
            return internalUpdateIncremental(version, cacheColumnletOrders, files);
        }
        catch (IOException | InterruptedException e)
        {
            e.printStackTrace();
            return -1;
        }
    }

    // let the files be a dependency, better for test
    // bulkload will set free and start to the init state
    public int bulkLoad(int version, List<String> cacheColumnletOrders, String[] files) {
        try
        {
            return internalUpdateAll(version, cacheColumnletOrders, files);
        }
        catch (IOException | InterruptedException e)
        {
            e.printStackTrace();
            return -1;
        }
    }
    // incremental load will update the cache based on last free+start version
    public int incrementalLoad(int version, List<String> cacheColumnletOrders, String[] files) {
        try
        {
            return internalUpdateIncremental(version, cacheColumnletOrders, files);
        }
        catch (IOException | InterruptedException e)
        {
            e.printStackTrace();
            return -1;
        }
    }

    public int internalUpdateIncremental(int version, List<String> cacheColumnletOrders, String[] files) throws IOException, InterruptedException {
        // now we need to consider the protocol
        int status = 0;
        List<List<Short>> partitionRgIds = new ArrayList<>(partitions);
        for (int i = 0; i < partitions; ++i) {
            partitionRgIds.add(new ArrayList<>());
        }
        List<List<Short>> partitionColIds = new ArrayList<>(partitions);
        for (int i = 0; i < partitions; ++i) {
            partitionColIds.add(new ArrayList<>());
        }
        // do a partition on layout+cacheColumnOrders by the hashcode
        constructPartitionRgAndCols(cacheColumnletOrders, files, partitionRgIds, partitionColIds);
        logger.debug("partition counts = " + Arrays.toString(partitionRgIds.stream().map(List::size).toArray()));

        // fetch the current free and start
        int freeAndStartDisk = PixelsCacheUtil.retrieveFirstAndFree(indexDiskBackFile);
        int freeAndStart = PixelsCacheUtil.retrieveFirstAndFree(indexBackFile);
        checkArgument(freeAndStart == freeAndStartDisk,
                String.format("tmpfs freeAndStart(%d) != disk freeAndStart(%d)", freeAndStart, freeAndStartDisk));
        int free = freeAndStart >>> 16;
        int start = freeAndStart & 0x0000ffff;
        int writeLogicalPartition = 0;


        while (writeLogicalPartition < partitions) {
            logger.debug(String.format("free=%d, start=%d, writeLogicalPartition=%d", free, start, writeLogicalPartition));

            MemoryMappedFile freeIndexPartition = indexPartitions[free];
            MemoryMappedFile freeIndexDiskPartition = indexDiskPartitions[writeLogicalPartition];
            MemoryMappedFile freeCachePartition = cachePartitions[free];

            // start update, write the update on `writeLogicalPartition` to `free`
            status = partitionUpdateAll(version, writeLogicalPartition, indexWriterFactory,
                    freeIndexPartition, freeIndexDiskPartition, freeCachePartition,
                    files, partitionRgIds.get(writeLogicalPartition), partitionColIds.get(writeLogicalPartition));
            if (status != 0) {
                return status; // TODO: now a single partition fail will cause a full failure
            }

            // update free, start and writeLogicalPartition
            if (writeLogicalPartition == 0) start = free;
            free = PixelsCacheUtil.retrievePhysicalPartition(indexBackFile, writeLogicalPartition, partitions);
            writeLogicalPartition = writeLogicalPartition + 1;

            // write the free and start in the meta header
            PixelsCacheUtil.setFirstAndFree(indexDiskBackFile, (short) free, (short) start);
            PixelsCacheUtil.setFirstAndFree(indexBackFile, (short) free, (short) start);
        }

        // after all the partition has been udpated, update the cache metadata header
        PixelsCacheUtil.setPartitionedIndexFileVersion(indexDiskBackFile, version);
        PixelsCacheUtil.setPartitionedIndexFileVersion(indexBackFile, version);

        return status;
    }

    private int hashcode(byte[] bytes) {
        int var1 = 1;

        for(int var3 = 0; var3 < bytes.length; ++var3) {
            var1 = 31 * var1 + bytes[var3];
        }

        return var1;
    }


    // Note: the xxxPartition are guaranteed by the caller that they are safe to write anything
    private int partitionUpdateAll(int version, int partition, java.util.function.Function<MemoryMappedFile, @Nullable CacheIndexWriter> indexWriterFactory,
                                   MemoryMappedFile indexPartition, MemoryMappedFile indexDiskPartition, MemoryMappedFile cachePartition,
                                   String[] files, List<Short> rgIds, List<Short> colIds) throws IOException, InterruptedException {
        try {
            PixelsCacheUtil.beginIndexWriteNoReaderCount(indexDiskPartition);
            PixelsCacheUtil.beginIndexWriteNoReaderCount(indexPartition);
        } catch (InterruptedException e) {
            logger.error("Failed to get write permission on index disk partition " + partition, e);
            return -1;
        }
        // write the data
        CacheIndexWriter indexWriter = indexWriterFactory.apply(indexDiskPartition);

        long currCacheOffset = PixelsCacheUtil.CACHE_DATA_OFFSET;

        logger.debug("number of files=" + files.length);
        logger.debug("rgId.size=" + rgIds.size() + " colId.size=" + colIds.size());

//        int bandwidthLimit = 50; // 50 mib/s
        long startTime = System.currentTimeMillis();
        long windowWriteBytes = 0;
        byte[] columnletBuf = new byte[1024 * 1024];
        for (String file : files)
        {
//            PixelsPhysicalReader pixelsPhysicalReader = new PixelsPhysicalReader(storage, file);
            // FIXME: I tried to make Mock compatible with PixelsPhysicalReader at MockReader level, but it
            //        seems too hard and the refactor of PixelsPhysicalReader. So now we have to uncomment to
            //        use MockPixelsPhysicalReader
            MockPixelsPhysicalReader pixelsPhysicalReader = new MockPixelsPhysicalReader(storage, file);

            int physicalLen;
            long physicalOffset;
            // update radix and cache content
            for (int i = 0; i < rgIds.size(); i++)
            {
                short rowGroupId = rgIds.get(i);
                short columnId = colIds.get(i);
                long blockId = pixelsPhysicalReader.getCurrentBlockId();
                int columnletLen = pixelsPhysicalReader.read(rowGroupId, columnId, columnletBuf);

                while (columnletBuf.length < columnletLen) {
                    columnletBuf = new byte[columnletBuf.length * 2];
                    columnletLen = pixelsPhysicalReader.read(rowGroupId, columnId, columnletBuf);
                    logger.debug("increase columnletBuf size");
                }
                physicalLen = columnletLen;
                if (currCacheOffset + physicalLen >= cachePartition.getSize())
                {
                    logger.warn("Cache writes have exceeded cache size. Break. Current size: " + currCacheOffset);
                    return 2;
                }
                indexWriter.put(new PixelsCacheKey(blockId, rowGroupId, columnId),
                        new PixelsCacheIdx(currCacheOffset, physicalLen));
                // TODO: uncomment it! we now test the index write first
                if (writeContent) {
                    cachePartition.setBytes(currCacheOffset, columnletBuf, 0, columnletLen); // sequential write pattern
                    windowWriteBytes += columnletLen;
                    if (windowWriteBytes / 1024.0 / 1024 / ((System.currentTimeMillis() - startTime) / 1000.0) > bandwidthLimit) {
                        double writeMib = windowWriteBytes / 1024.0 / 1024;
                        double expectTimeMili = writeMib / bandwidthLimit * 1000;
//                        checkArgument(expectTimeMili > System.currentTimeMillis() - startTime);
//                        logger.debug("trigger sleep " + windowWriteBytes / 1024.0 / 1024 / ((System.currentTimeMillis() - startTime) / 1000.0));
//                        Thread.sleep((long) (expectTimeMili - System.currentTimeMillis() + startTime) * 2);
                        Thread.sleep(100);

                    }
                }
                logger.trace(
                        "Cache write: " + file + "-" + rowGroupId + "-" + columnId + ", offset: " + currCacheOffset + ", length: " + columnletLen);
                currCacheOffset += physicalLen;
            }
        }
        logger.debug("Cache writer ends at offset: " + currCacheOffset / 1024.0 / 1024.0 / 1024.0 + "GiB");

        // first write to the indexDiskPartition
        // write the cache version
        long serializeOffset = indexWriter.flush();
        if (serializeOffset < 0) {
            return 2; // exceed the size
        }
        logger.debug("index writer ends at offset: " + serializeOffset / 1024.0 / 1024.0 + "MiB");

        PixelsCacheUtil.setIndexVersion(indexDiskPartition, version);
        PixelsCacheUtil.setCacheStatus(cachePartition, PixelsCacheUtil.CacheStatus.OK.getId());
        PixelsCacheUtil.setCacheSize(cachePartition, currCacheOffset);

        PixelsCacheUtil.endIndexWrite(indexDiskPartition);


        // TODO: ensure that the indexDiskPartition is flushed to the disk, so the index in memory is safe
        // then copy the indexDiskPartition to indexPartition in the tmpfs
        // Note: we should not copy serializeOffset bytes, it is not precise and some items might not be copied
        indexPartition.copyMemory(indexDiskPartition.getAddress(), indexPartition.getAddress(), indexDiskPartition.getSize());
        PixelsCacheUtil.setIndexVersion(indexPartition, version);
        PixelsCacheUtil.endIndexWrite(indexPartition);
        return 0;

    }

    private void constructPartitionRgAndCols(List<String> cacheColumnletOrders, String[] files,
                                             List<List<Short>> partitionRgIds, List<List<Short>> partitionColIds) {
        ByteBuffer hashKeyBuf = ByteBuffer.allocate(2 + 2);
        for (int i = 0; i < cacheColumnletOrders.size(); i++) {
            String[] columnletIdStr = cacheColumnletOrders.get(i).split(":");
            short rowGroupId = Short.parseShort(columnletIdStr[0]);
            short columnId = Short.parseShort(columnletIdStr[1]);
            hashKeyBuf.putShort(0, rowGroupId);
            hashKeyBuf.putShort(2, columnId);
            int hash = hashcode(hashKeyBuf.array()) & 0x7fffffff;
            int partition = hash % partitions;
            partitionRgIds.get(partition).add(rowGroupId);
            partitionColIds.get(partition).add(columnId);
        }
    }

    // bulk load method, it will write all the partitions at once.
    private int internalUpdateAll(int version, List<String> cacheColumnletOrders, String[] files)
            throws IOException, InterruptedException {
        int status = 0;
        List<List<Short>> partitionRgIds = new ArrayList<>(partitions);
        for (int i = 0; i < partitions; ++i) {
            partitionRgIds.add(new ArrayList<>());
        }
        List<List<Short>> partitionColIds = new ArrayList<>(partitions);
        for (int i = 0; i < partitions; ++i) {
            partitionColIds.add(new ArrayList<>());
        }
        // do a partition on layout+cacheColumnOrders by the hashcode
        constructPartitionRgAndCols(cacheColumnletOrders, files, partitionRgIds, partitionColIds);
        logger.debug("partition counts = " + Arrays.toString(partitionRgIds.stream().map(List::size).toArray()));

        // update region by region
        for (int partition = 0; partition < partitions; ++partition) {
            status = partitionUpdateAll(version, partition, indexWriterFactory,
                    indexPartitions[partition], indexDiskPartitions[partition], cachePartitions[partition],
                    files, partitionRgIds.get(partition), partitionColIds.get(partition));
            if (status != 0) {
                return status; // TODO: now a single partition fail will cause a full failure
            }
        }

        return status;
    }

    public void close()
            throws Exception
    {
        indexBackFile.unmap();
        indexDiskBackFile.unmap();
        cacheBackFile.unmap();
    }



}
