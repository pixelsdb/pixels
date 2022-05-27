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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class PixelsPartitionCacheWriter {

    /* same as PixelsCacheWriter */
    private final static Logger logger = LogManager.getLogger(PixelsCacheWriter.class);
//    private final RandomAccessFile cacheFile; // TODO: can we also memory mapped? it might consume a lot of spaces in address space
    private final MemoryMappedFile cacheFile; // TODO: can we also memory mapped? it might consume a lot of spaces in address space

    private final MemoryMappedFile indexFile;
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
    private long currentIndexOffset;
    private long allocatedIndexOffset = PixelsCacheUtil.INDEX_RADIX_OFFSET;
    private long cacheOffset = PixelsCacheUtil.CACHE_DATA_OFFSET; // this is only used in the write() method.
    private ByteBuffer nodeBuffer = ByteBuffer.allocate(8 * 256);
    private ByteBuffer cacheIdxBuffer = ByteBuffer.allocate(PixelsCacheIdx.SIZE);
    private Set<String> cachedColumnlets = new HashSet<>();

    // TODO: the cache index algorithm shall be abstracted out, rather than fixed as either radix tree or not
    //       radix tree
    // TODO: how to partition the cache region? by columnId or blk+rg+column?
    /* something special for us */
    private final int partitions; // should be a power of 2 // TODO: init from the properties
    private final PixelsRadix[] radixs;
    private final MemoryMappedFile[] cachePartitions; // length=partitions + 1
    private final MemoryMappedFile[] indexPartitions;
    private int free;


    private PixelsPartitionCacheWriter(MemoryMappedFile cacheFile,
                              MemoryMappedFile indexFile,
                              MemoryMappedFile[] cachePartitions,
                              MemoryMappedFile[] indexPartitions,
                              Storage storage,
                              PixelsRadix[] radixs,
                              int partitions,
                              Set<String> cachedColumnlets,
                              EtcdUtil etcdUtil,
                              String host)
    {
        this.cacheFile = cacheFile;
        this.indexFile = indexFile;
        this.cachePartitions = cachePartitions;
        this.indexPartitions = indexPartitions;

        this.storage = storage;
        this.radixs = radixs;
        this.partitions = partitions;
        this.free = this.partitions;

        assert (this.cachePartitions.length == this.partitions + 1);
        assert (this.indexPartitions.length == this.partitions + 1);

        this.etcdUtil = etcdUtil;
        this.host = host;
        this.nodeBuffer.order(ByteOrder.BIG_ENDIAN);
        if (cachedColumnlets != null && cachedColumnlets.isEmpty() == false)
        {
            cachedColumnlets.addAll(cachedColumnlets);
        }
    }

    public static class Builder
    {
        private String builderCacheLocation = "";
        private long builderCacheSize;
        private String builderIndexLocation = "";
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

        public PixelsPartitionCacheWriter.Builder setIndexSize(long size)
        {
            checkArgument(size > 0, "index size should be positive");
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
            // TODO: calculate a decent size of the builderCacheSize and BuilderIndexSize based on the partition
            // TODO: add unit test on it
            long cachePartitionSize = builderCacheSize / partitions;
            long indexPartitionSize = builderIndexSize / partitions;
            assert (cachePartitionSize * partitions == builderCacheSize);
            assert (indexPartitionSize * partitions == builderIndexSize);
            // with an additional buffer area, totally (partitions + 1) * cachePartitionSize size
            MemoryMappedFile cacheFile = new MemoryMappedFile(builderCacheLocation, builderCacheSize + cachePartitionSize);
            MemoryMappedFile indexFile = new MemoryMappedFile(builderIndexLocation, builderIndexSize + indexPartitionSize);
            assert (cachePartitionSize * (partitions + 1) == cacheFile.getSize());
            assert (indexPartitionSize * (partitions + 1) == indexFile.getSize());

            // TODO: split the cacheFile and indexFile into partitions
            // the last partition serves as the buffer partition
            MemoryMappedFile[] cachePartitions = new MemoryMappedFile[partitions + 1];
            MemoryMappedFile[] indexPartitions = new MemoryMappedFile[partitions + 1];
            for (int partition = 0; partition < partitions + 1; ++partition) {
                long indexOffset = partition * indexPartitionSize;
                long cacheOffset = partition * cachePartitionSize;
                indexPartitions[partition] = indexFile.regionView(indexOffset);
                cachePartitions[partition] = cacheFile.regionView(cacheOffset);
            }

            PixelsRadix[] radixs = new PixelsRadix[partitions];
            // check if cache and index exists.
            Set<String> cachedColumnlets = new HashSet<>();
            // if overwrite is not true, and cache and index file already exists, reconstruct radix from existing index.
            if (!builderOverwrite && PixelsCacheUtil.checkMagic(indexFile) && PixelsCacheUtil.checkMagic(cacheFile))
            {
                // TODO: load the radix region by region
                throw new OperationNotSupportedException();
                // cache exists in local cache file and index, reload the index.
//                radix = PixelsCacheUtil.loadRadixIndex(indexFile);
//                // build cachedColumnlets for PixelsCacheWriter.
//                int cachedVersion = PixelsCacheUtil.getIndexVersion(indexFile);
//                MetadataService metadataService = new MetadataService(
//                        cacheConfig.getMetaHost(), cacheConfig.getMetaPort());
//                Layout cachedLayout = metadataService.getLayout(
//                        cacheConfig.getSchema(), cacheConfig.getTable(), cachedVersion);
//                Compact compact = cachedLayout.getCompactObject();
//                int cacheBorder = compact.getCacheBorder();
//                cachedColumnlets.addAll(compact.getColumnletOrder().subList(0, cacheBorder));
//                metadataService.shutdown();
            }
            //   else, create a new radix tree, and initialize the index and cache file.
            else
            {
                for (int i = 0; i < partitions; ++i) {
                    radixs[i] = new PixelsRadix();
                }
                PixelsCacheUtil.initialize(indexFile, cacheFile);
            }
            EtcdUtil etcdUtil = EtcdUtil.Instance();

            Storage storage = StorageFactory.Instance().getStorage(cacheConfig.getStorageScheme());

            return new PixelsPartitionCacheWriter(cacheFile, indexFile, cachePartitions, indexPartitions, storage, radixs,
                    partitions, cachedColumnlets, etcdUtil, builderHostName);
        }
    }

    public static PixelsPartitionCacheWriter.Builder newBuilder()
    {
        return new PixelsPartitionCacheWriter.Builder();
    }

    public MemoryMappedFile getIndexFile()
    {
        return indexFile;
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
            return internalUpdateAll(version, layout, files);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return -1;
        }
    }

    private int hashcode(byte[] bytes) {
        int var1 = 1;

        for(int var3 = 0; var3 < bytes.length; ++var3) {
            var1 = 31 * var1 + bytes[var3];
        }

        return var1;
    }

    // bulk load method, it will write all the partitions at once.
    private int internalUpdateAll(int version, Layout layout, String[] files)
            throws IOException
    {
        int status = 0;
        // get the new caching layout
        Compact compact = layout.getCompactObject();
        int cacheBorder = compact.getCacheBorder();
        List<String> cacheColumnletOrders = compact.getColumnletOrder().subList(0, cacheBorder);
        ByteBuffer hashKeyBuf = ByteBuffer.allocate(PixelsCacheKey.SIZE);
        // do a partition on layout+cacheColumnOrders by the hashcode
        for (long fakeBlkId = 0; fakeBlkId < files.length; ++fakeBlkId) {
            hashKeyBuf.putLong(0, fakeBlkId);

            for (int i = 0; i < cacheColumnletOrders.size(); i++) {
                String[] columnletIdStr = cacheColumnletOrders.get(i).split(":");
                short rowGroupId = Short.parseShort(columnletIdStr[0]);
                short columnId = Short.parseShort(columnletIdStr[1]);
                hashKeyBuf.putShort(8, rowGroupId);
                hashKeyBuf.putShort(10, columnId);
                int hash = hashcode(hashKeyBuf.array()) & 0x7fffffff;
                int partition = hash % partitions;

            }
        }
        // set rwFlag as write
        logger.debug("Set index rwFlag as write");
        try
        {
            /**
             * Before updating the cache content, in beginIndexWrite:
             * 1. Set rwFlag to block subsequent readers.
             * 2. Wait for the existing readers to finish, i.e.
             *    wait for the readCount to be cleared (become zero).
             */
            PixelsCacheUtil.beginIndexWrite(indexFile);
        } catch (InterruptedException e)
        {
            status = -1;
            logger.error("Failed to get write permission on index.", e);
            return status;
        }

        // update cache content
        if (cachedColumnlets == null || cachedColumnlets.isEmpty())
        {
            cachedColumnlets = new HashSet<>(cacheColumnletOrders.size());
        }
        else
        {
            cachedColumnlets.clear();
        }
        radix.removeAll();
        long currCacheOffset = PixelsCacheUtil.CACHE_DATA_OFFSET;
        boolean enableAbsoluteBalancer = Boolean.parseBoolean(
                ConfigFactory.Instance().getProperty("enable.absolute.balancer"));
        outer_loop:
        for (String file : files)
        {
            if (enableAbsoluteBalancer && storage.hasLocality())
            {
                // TODO: this is used for experimental purpose only.
                // may be removed later.
                file = ensureLocality(file);
            }
            PixelsPhysicalReader pixelsPhysicalReader = new PixelsPhysicalReader(storage, file);
            int physicalLen;
            long physicalOffset;
            // update radix and cache content
            for (int i = 0; i < cacheColumnletOrders.size(); i++)
            {
                String[] columnletIdStr = cacheColumnletOrders.get(i).split(":");
                short rowGroupId = Short.parseShort(columnletIdStr[0]);
                short columnId = Short.parseShort(columnletIdStr[1]);
                PixelsProto.RowGroupFooter rowGroupFooter = pixelsPhysicalReader.readRowGroupFooter(rowGroupId);
                PixelsProto.ColumnChunkIndex chunkIndex =
                        rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(columnId);
                physicalLen = (int) chunkIndex.getChunkLength();
                physicalOffset = chunkIndex.getChunkOffset();
                if (currCacheOffset + physicalLen >= cacheFile.getSize())
                {
                    logger.debug("Cache writes have exceeded cache size. Break. Current size: " + currCacheOffset);
                    status = 2;
                    break outer_loop;
                }
                else
                {
                    radix.put(new PixelsCacheKey(pixelsPhysicalReader.getCurrentBlockId(), rowGroupId, columnId),
                            new PixelsCacheIdx(currCacheOffset, physicalLen));
                    byte[] columnlet = pixelsPhysicalReader.read(physicalOffset, physicalLen);
                    cacheFile.setBytes(currCacheOffset, columnlet);
                    logger.debug(
                            "Cache write: " + file + "-" + rowGroupId + "-" + columnId + ", offset: " + currCacheOffset + ", length: " + columnlet.length);
                    currCacheOffset += physicalLen;
                }
            }
        }
        for (String cachedColumnlet : cacheColumnletOrders)
        {
            cachedColumnlets.add(cachedColumnlet);
        }
        logger.debug("Cache writer ends at offset: " + currCacheOffset);
        // flush index
        flushIndex();
        // update cache version
        PixelsCacheUtil.setIndexVersion(indexFile, version);
        PixelsCacheUtil.setCacheStatus(cacheFile, PixelsCacheUtil.CacheStatus.OK.getId());
        PixelsCacheUtil.setCacheSize(cacheFile, currCacheOffset);
        // set rwFlag as readable
        PixelsCacheUtil.endIndexWrite(indexFile);
        logger.debug("Cache index ends at offset: " + currentIndexOffset);

        return status;
    }



}
