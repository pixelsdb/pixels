package io.pixelsdb.pixels.cache;


import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Compact;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.core.PixelsProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class PixelsPartitionCacheWriter {

    /* same as PixelsCacheWriter */
    private final static Logger logger = LogManager.getLogger(PixelsCacheWriter.class);
//    private final RandomAccessFile cacheFile; // TODO: can we also memory mapped? it might consume a lot of spaces in address space
    private final MemoryMappedFile cacheBackFile; // TODO: can we also memory mapped? it might consume a lot of spaces in address space
    private final MemoryMappedFile indexBackFile;
    private final MemoryMappedFile indexDiskBackFile;
    private final Storage storage;
    private final EtcdUtil etcdUtil;


    /**
     * The host name of the node where this cache writer is running.
     */
    private final String host;
    /**
     * Call beginIndexWrite() before changing radix, which is shared by all threads.
     */
//    private PixelsRadix radix;

    // TODO: the cache index algorithm shall be abstracted out, rather than fixed as either radix tree or not
    //       radix tree
    // TODO: how to partition the cache region? by columnId or blk+rg+column?
    /* something special for us */
    private final int partitions; // should be a power of 2 // TODO: init from the properties
    private final PixelsRadix[] radixs; // length = partition
    private final MemoryMappedFile[] cachePartitions; // length=partitions + 1
    private final MemoryMappedFile[] indexPartitions; // length=partitions + 1
    // permanent disk copy of the index file
    private final MemoryMappedFile[] indexDiskPartitions; // length = partition


    private PixelsPartitionCacheWriter(MemoryMappedFile cacheFile,
                              MemoryMappedFile indexFile,
                              MemoryMappedFile indexDiskFile,
                              MemoryMappedFile[] cachePartitions,
                              MemoryMappedFile[] indexPartitions,
                              MemoryMappedFile[] indexDiskPartitions,
                              Storage storage,
                              PixelsRadix[] radixs,
                              int partitions,
                              EtcdUtil etcdUtil,
                              String host)
    {
        this.cacheBackFile = cacheFile;
        this.indexBackFile = indexFile;
        this.indexDiskBackFile = indexDiskFile;

        this.cachePartitions = cachePartitions;
        this.indexPartitions = indexPartitions;
        this.indexDiskPartitions = indexDiskPartitions;

        this.storage = storage;
        this.radixs = radixs;
        this.partitions = partitions;

        checkArgument(this.cachePartitions.length == this.partitions + 1);
        checkArgument(this.indexPartitions.length == this.partitions + 1);
        checkArgument(this.indexDiskPartitions.length == this.partitions);
        checkArgument(this.radixs.length == this.partitions);


        this.etcdUtil = etcdUtil;
        this.host = host;
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

        private Builder()
        {
        }

        public PixelsPartitionCacheWriter.Builder setPartitions(int partitions)
        {
            this.partitions = partitions;
            return this;
        }

        public PixelsPartitionCacheWriter.Builder setCacheLocation(String cacheLocation)
        {
            checkArgument(cacheLocation != null && !cacheLocation.isEmpty(),
                    "cache location should bot be empty");
            this.builderCacheLocation = cacheLocation;

            return this;
        }

        public PixelsPartitionCacheWriter.Builder setCacheSize(long cacheSize)
        {
            checkArgument(cacheSize > 0, "cache size should be positive");
            this.builderCacheSize = MemoryMappedFile.roundTo4096(cacheSize);

            return this;
        }

        public PixelsPartitionCacheWriter.Builder setIndexLocation(String indexLocation)
        {
            checkArgument(indexLocation != null && !indexLocation.isEmpty(),
                    "index location should not be empty");
            this.builderIndexLocation = indexLocation;

            return this;
        }

        public PixelsPartitionCacheWriter.Builder setIndexDiskLocation(String indexDiskLocation) {
            checkArgument(indexDiskLocation != null && !indexDiskLocation.isEmpty(),
                    "index location should not be empty");
            this.builderIndexDiskLocation = indexDiskLocation;

            return this;
        }

        public PixelsPartitionCacheWriter.Builder setIndexSize(long size)
        {
            checkArgument(size > 0, "index size should be positive");
            // TODO: dont do this rounding, enforce a full division with index.size / partitions
            this.builderIndexSize = MemoryMappedFile.roundTo4096(size);

            return this;
        }

        public PixelsPartitionCacheWriter.Builder setOverwrite(boolean overwrite)
        {
            this.builderOverwrite = overwrite;
            return this;
        }

        public PixelsPartitionCacheWriter.Builder setHostName(String hostName)
        {
            checkArgument(hostName != null, "hostname should not be null");
            this.builderHostName = hostName;
            return this;
        }

        public PixelsPartitionCacheWriter.Builder setCacheConfig(PixelsCacheConfig cacheConfig)
        {
            checkArgument(cacheConfig != null, "cache config should not be null");
            this.cacheConfig = cacheConfig;
            return this;
        }

        public PixelsPartitionCacheWriter build()
                throws Exception
        {
            // TODO: add unit test on it
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

            PixelsRadix[] radixs = new PixelsRadix[partitions];
            // check if cache and index exists.
            // if overwrite is not true, and cache and index file already exists, reconstruct radix from existing index.
            if (!builderOverwrite && PixelsCacheUtil.checkMagic(indexFile) && PixelsCacheUtil.checkMagic(cacheFile))
            {
                checkArgument(PixelsCacheUtil.checkMagic(indexFile) && PixelsCacheUtil.checkMagic(cacheFile),
                        "overwrite=false, but cacheFile and indexFile is polluted");
                for (int i = 0; i < partitions; ++i) {
                    radixs[i] = new PixelsRadix();
                }
            }
            //   else, create a new radix tree, and initialize the index and cache file.
            else
            {
                // set the header of the file and each partition
                PixelsCacheUtil.initializePartitionMeta(indexDiskFile, (short) partitions, indexPartitionSize);
                PixelsCacheUtil.initializePartitionMeta(indexFile, (short) partitions, indexPartitionSize);
                PixelsCacheUtil.initializeCacheFile(cacheFile);
                for (int i = 0; i < partitions; ++i) {
                    radixs[i] = new PixelsRadix();
                    PixelsCacheUtil.initializeIndexFile(indexPartitions[i]);
                    PixelsCacheUtil.initializeIndexFile(indexDiskPartitions[i]);
                    PixelsCacheUtil.initializeCacheFile(cachePartitions[i]);
                }

            }
            EtcdUtil etcdUtil = EtcdUtil.Instance();

            Storage storage = StorageFactory.Instance().getStorage(cacheConfig.getStorageScheme());

            return new PixelsPartitionCacheWriter(cacheFile, indexFile, indexDiskFile,
                    cachePartitions, indexPartitions, indexDiskPartitions, storage, radixs,
                    partitions, etcdUtil, builderHostName);
        }
    }

    public static PixelsPartitionCacheWriter.Builder newBuilder()
    {
        return new PixelsPartitionCacheWriter.Builder();
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
            Compact compact = layout.getCompactObject();
            int cacheBorder = compact.getCacheBorder();
            List<String> cacheColumnletOrders = compact.getColumnletOrder().subList(0, cacheBorder);
            return internalUpdateAll(version, cacheColumnletOrders, files);
        }
        catch (IOException e)
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
            Compact compact = layout.getCompactObject();
            int cacheBorder = compact.getCacheBorder();
            List<String> cacheColumnletOrders = compact.getColumnletOrder().subList(0, cacheBorder);
            return internalUpdateIncremental(version, cacheColumnletOrders, files);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return -1;
        }
    }

    // let the files be a dependency, better for test
    public int bulkLoad(int version, List<String> cacheColumnletOrders, String[] files) {
        try
        {
            return internalUpdateAll(version, cacheColumnletOrders, files);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return -1;
        }
    }
    // better for test purpose
    public int incrementalLoad(int version, List<String> cacheColumnletOrders, String[] files) {
        try
        {
            return internalUpdateIncremental(version, cacheColumnletOrders, files);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return -1;
        }
    }

    public int internalUpdateIncremental(int version, List<String> cacheColumnletOrders, String[] files) throws IOException {
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
            status = partitionUpdateAll(version, writeLogicalPartition, radixs[writeLogicalPartition],
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

        // TODO: after all the partition has been udpated, update the cache metadata header
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


    // the xxxPartition are guranteed by the caller that they are safe to write anything
    private int partitionUpdateAll(int version, int partition, PixelsRadix radix,
                                   MemoryMappedFile indexPartition, MemoryMappedFile indexDiskPartition, MemoryMappedFile cachePartition,
                                   String[] files, List<Short> rgIds, List<Short> colIds) throws IOException {
        try {
            // TODO: for us, this should be done immediately, since there is no contention between reader and writer
            PixelsCacheUtil.beginIndexWrite(indexDiskPartition);
            PixelsCacheUtil.beginIndexWrite(indexPartition);
        } catch (InterruptedException e) {
            logger.error("Failed to get write permission on index disk partition " + partition, e);
            return -1;
        }
        // write the data
        radix.removeAll();
        long currCacheOffset = PixelsCacheUtil.CACHE_DATA_OFFSET;

        logger.debug("number of files=" + files.length);
        logger.debug("rgId.size=" + rgIds.size() + " colId.size=" + colIds.size());

        for (String file : files)
        {
//            PixelsPhysicalReader pixelsPhysicalReader = new PixelsPhysicalReader(storage, file);
            MockPixelsPhysicalReader pixelsPhysicalReader = new MockPixelsPhysicalReader(storage, file);

            int physicalLen;
            long physicalOffset;
            // update radix and cache content
            for (int i = 0; i < rgIds.size(); i++)
            {
                short rowGroupId = rgIds.get(i);
                short columnId = colIds.get(i);
//                PixelsProto.RowGroupFooter rowGroupFooter = pixelsPhysicalReader.readRowGroupFooter(rowGroupId);
//                PixelsProto.ColumnChunkIndex chunkIndex =
//                        rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(columnId);
//                long blockId = pixelsPhysicalReader.getCurrentBlockId();
//                physicalLen = (int) chunkIndex.getChunkLength();
//                physicalOffset = chunkIndex.getChunkOffset();
//                if (currCacheOffset + physicalLen >= cachePartition.getSize())
//                {
//                    logger.debug("Cache writes have exceeded cache size. Break. Current size: " + currCacheOffset);
//                    return 2;
//                }
//                radix.put(new PixelsCacheKey(blockId, rowGroupId, columnId),
//                        new PixelsCacheIdx(currCacheOffset, physicalLen));
//                // TODO: use another read api
//                byte[] columnlet = pixelsPhysicalReader.read(physicalOffset, physicalLen);

                long blockId = pixelsPhysicalReader.getCurrentBlockId();
                byte[] columnlet = pixelsPhysicalReader.read(rowGroupId, columnId);
                physicalLen = columnlet.length;
                if (currCacheOffset + physicalLen >= cachePartition.getSize())
                {
                    logger.warn("Cache writes have exceeded cache size. Break. Current size: " + currCacheOffset);
                    return 2;
                }
                radix.put(new PixelsCacheKey(blockId, rowGroupId, columnId),
                        new PixelsCacheIdx(currCacheOffset, physicalLen));
                // TODO: uncomment it! we now test the index write first
            //    cachePartition.setBytes(currCacheOffset, columnlet); // sequential write pattern
                logger.trace(
                        "Cache write: " + file + "-" + rowGroupId + "-" + columnId + ", offset: " + currCacheOffset + ", length: " + columnlet.length);
                currCacheOffset += physicalLen;
            }
        }
        logger.debug("Cache writer ends at offset: " + currCacheOffset / 1024.0 / 1024.0 / 1024.0 + "GiB");

        // first write to the indexDiskPartition
        RadixSerializer serializer = new RadixSerializer(radix, indexDiskPartition);
        // write the cache version
        long serializeOffset = serializer.serialize();
        if (serializeOffset < 0) {
            return 2; // exceed the size
        }
        logger.debug("index writer ends at offset: " + serializeOffset / 1024.0 / 1024.0 + "MiB");

        PixelsCacheUtil.setIndexVersion(indexDiskPartition, version);
        PixelsCacheUtil.setCacheStatus(cachePartition, PixelsCacheUtil.CacheStatus.OK.getId());
        PixelsCacheUtil.setCacheSize(cachePartition, currCacheOffset);

        PixelsCacheUtil.endIndexWrite(indexDiskPartition);


        // TODO: ensure that the indexDiskPartition is flushed to the disk
        // then copy the indexDiskPartition to indexPartition in the tmpfs
        // TODO: check what should be write in the header
        // TODO: it might overflow the integer
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
            throws IOException
    {
        int status = 0;
        List<List<Short>> partitionRgIds = new ArrayList<>(partitions);
        for (int i = 0; i < partitions; ++i) {
            partitionRgIds.add(new ArrayList<>());
        }
        List<List<Short>> partitionColIds = new ArrayList<>(partitions);
        for (int i = 0; i < partitions; ++i) {
            partitionColIds.add(new ArrayList<>());
        }
        // TODO: what if we partition only on rgId and colId? now it pose a lot of memory cost
        // do a partition on layout+cacheColumnOrders by the hashcode
        constructPartitionRgAndCols(cacheColumnletOrders, files, partitionRgIds, partitionColIds);
        logger.debug("partition counts = " + Arrays.toString(partitionRgIds.stream().map(List::size).toArray()));

        // update region by region
        for (int partition = 0; partition < partitions; ++partition) {
            //
            // write to indexDisk part
            status = partitionUpdateAll(version, partition, radixs[partition],
                    indexPartitions[partition], indexDiskPartitions[partition], cachePartitions[partition],
                    files, partitionRgIds.get(partition), partitionColIds.get(partition));
            if (status != 0) {
                return status; // TODO: now a single partition fail will cause a full failure
            }
        }
        // TODO: after all the partition has been udpated, update the cache metadata header
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
