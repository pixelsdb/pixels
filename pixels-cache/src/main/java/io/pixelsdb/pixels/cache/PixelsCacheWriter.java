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

import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Compact;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.core.PixelsProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * pixels cache writer
 *
 * @author guodong, hank, alph00
 */
public class PixelsCacheWriter
{
    private final static Logger logger = LogManager.getLogger(PixelsCacheWriter.class);

    private final PixelsLocator locator;
    private final List<PixelsZoneWriter> zones;
    private final PixelsBucketTypeInfo bucketTypeInfo;
    private final PixelsBucketToZoneMap bucketToZoneMap;
    private final MemoryMappedFile globalIndexFile;
    private final Storage storage;
    private final EtcdUtil etcdUtil;
    private int zoneNum;
    private List<String> files;
    /**
     * The host name of the node where this cache writer is running.
     */
    private final String host;
    private final Set<String> cachedColumnChunks = new HashSet<>();

    private PixelsCacheWriter(PixelsLocator locator,
                              List<PixelsZoneWriter> zones,
                              PixelsBucketTypeInfo bucketTypeInfo,
                              PixelsBucketToZoneMap bucketToZoneMap,
                              MemoryMappedFile globalIndexFile,
                              Storage storage,
                              Set<String> cachedColumnChunks,
                              EtcdUtil etcdUtil,
                              String host,
                              int zoneNum)
    {
        this.locator = locator;
        this.zones = zones;
        this.storage = storage;
        this.etcdUtil = etcdUtil;
        this.host = host;
        this.globalIndexFile = globalIndexFile;
        this.bucketTypeInfo = bucketTypeInfo;
        this.bucketToZoneMap = bucketToZoneMap;
        this.zoneNum = zoneNum;
        if (cachedColumnChunks != null && cachedColumnChunks.isEmpty() == false)
        {
            this.cachedColumnChunks.addAll(cachedColumnChunks);
        }
        this.bucketToZoneMap.setHashNodeNum(this.locator.getNodeNum());// it is used when scaling cache size.
    }

    public static class Builder
    {
        private String builderCacheLocation = "";
        private long builderZoneSize;
        private String builderIndexLocation = "";
        private long builderZoneIndexSize;
        private boolean builderOverwrite = true;
        private String builderHostName = null;
        private PixelsCacheConfig cacheConfig = null;
        private int zoneNum = 3;// default zone num: 2 lazy + 1 swap
        private int swapZoneNum = 1;
        private PixelsBucketTypeInfo bucketTypeInfo = null;

        private Builder()
        {
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
            this.builderZoneSize = cacheSize;

            return this;
        }

        public Builder setIndexLocation(String indexLocation)
        {
            checkArgument(indexLocation != null && !indexLocation.isEmpty(),
                    "index location should not be empty");
            this.builderIndexLocation = indexLocation;

            return this;
        }

        public Builder setIndexSize(long indexSize)
        {
            checkArgument(indexSize > 0, "index size should be positive");
            this.builderZoneIndexSize = indexSize;

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

        public Builder setZoneNum(int zoneNum)
        {
            checkArgument(zoneNum > 1, "zone number should be positive and greater than 1");
            this.zoneNum=zoneNum;
            return this;
        }

        public Builder setSwapZoneNum(int swapZoneNum)
        {
            checkArgument(swapZoneNum > 0, "swap zone number should be positive");
            this.swapZoneNum = swapZoneNum;
            return this;
        }

        public PixelsCacheWriter build() throws Exception
        {
            this.builderZoneSize = this.builderZoneSize / (zoneNum - swapZoneNum);
            this.builderZoneIndexSize = this.builderZoneIndexSize / (zoneNum - swapZoneNum);

            bucketTypeInfo = PixelsBucketTypeInfo.newBuilder(zoneNum).build();

            List<PixelsZoneWriter> zones = new ArrayList<>(zoneNum);
            for(int i=0; i<zoneNum; i++)
            {
                zones.add(new PixelsZoneWriter(builderCacheLocation+"."+String.valueOf(i),builderIndexLocation+"."+String.valueOf(i),builderZoneSize,builderZoneIndexSize,i));
            }

            MemoryMappedFile globalIndexFile = new MemoryMappedFile(builderIndexLocation, builderZoneIndexSize);

            Set<String> cachedColumnChunks = new HashSet<>();

            PixelsBucketToZoneMap bucketToZoneMap = new PixelsBucketToZoneMap(globalIndexFile, zoneNum);

            // check if cache and index exists.
            // if overwrite is not true, and cache and index file already exists, reconstruct radix from existing index.
            if (!builderOverwrite && PixelsZoneUtil.checkMagic(globalIndexFile) && PixelsZoneUtil.checkMagic(globalIndexFile))
            {
                // // reload bucketToZoneMap from index.
                // PixelsZoneUtil.getBucketZoneMap(globalIndexFile, bucketToZoneMap);
                // cache exists in local cache file and index, reload the index.
                for(int i=0; i<zoneNum; i++)
                {
                    int zoneId = bucketToZoneMap.getBucketToZone(i);
                    zones.get(zoneId).loadIndex();
                    switch (zones.get(zoneId).getZoneType())
                    {
                        case LAZY:
                            bucketTypeInfo.incrementLazyBucketNum();
                            bucketTypeInfo.getLazyBucketIds().add(i);
                            break;
                        case SWAP:
                            bucketTypeInfo.incrementSwapBucketNum();
                            bucketTypeInfo.getSwapBucketIds().add(i);
                            break;
                        case EAGER:
                            bucketTypeInfo.incrementEagerBucketNum();
                            bucketTypeInfo.getEagerBucketIds().add(i);
                            break;
                        default:
                            logger.warn("zone type error");
                    }
                }
                // build cachedColumnChunks for PixelsCacheWriter.
                int cachedVersion = PixelsZoneUtil.getIndexVersion(globalIndexFile);
                KeyValue keyValue = EtcdUtil.Instance().getKeyValue(Constants.LAYOUT_VERSION_LITERAL);
                if (keyValue != null)
                {
                    String value = keyValue.getValue().toString(StandardCharsets.UTF_8);
                    // PIXELS-636: get schema and table name from etcd instead of config file.
                    String[] splits = value.split(":");
                    checkArgument(splits.length == 2, "invalid value for key '" +
                            Constants.LAYOUT_VERSION_LITERAL + "' in etcd: " + value);
                    SchemaTableName schemaTableName = new SchemaTableName(splits[0]);
                    MetadataService metadataService = MetadataService.Instance();
                    Layout cachedLayout = metadataService.getLayout(
                            schemaTableName.getSchemaName(), schemaTableName.getSchemaName(), cachedVersion);
                    Compact compact = cachedLayout.getCompact();
                    int cacheBorder = compact.getCacheBorder();
                    cachedColumnChunks.addAll(compact.getColumnChunkOrder());//.subList(0, cacheBorder));
                }
            }
            else
            {
                PixelsZoneUtil.initializeGlobalIndex(globalIndexFile, bucketToZoneMap);
                bucketTypeInfo.setLazyBucketNum(zoneNum - swapZoneNum);
                bucketTypeInfo.getLazyBucketIds().addAll(new ArrayList<Integer>(){{for(int i=0;i<zoneNum-swapZoneNum;i++)add(i);}});
                for (int i = 0; i < zoneNum - swapZoneNum; i++)
                {
                    zones.get(i).buildLazy(cacheConfig);
                }
                // initialize swap zone
                bucketTypeInfo.setSwapBucketNum(swapZoneNum);
                bucketTypeInfo.getSwapBucketIds().addAll(new ArrayList<Integer>(){{for(int i=zoneNum-swapZoneNum;i<zoneNum;i++)add(i);}});
                for (int i = zoneNum - swapZoneNum; i < zoneNum; i++)
                {
                    zones.get(i).buildSwap(cacheConfig);
                }
            }

            // initialize the hashFunction with the number of zones.
            PixelsLocator locator = new PixelsLocator(zoneNum - swapZoneNum);

            EtcdUtil etcdUtil = EtcdUtil.Instance();

            Storage storage = StorageFactory.Instance().getStorage(cacheConfig.getStorageScheme());

            return new PixelsCacheWriter(locator, zones, bucketTypeInfo, bucketToZoneMap, globalIndexFile, storage,
                    cachedColumnChunks, etcdUtil, builderHostName, zoneNum);
        }
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public MemoryMappedFile getIndexFile()
    {
        return globalIndexFile;
    }

    public MemoryMappedFile getIndexFile(int zoneId)
    {
        return zones.get(zoneId).getIndexFile();
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
                logger.warn("Found no allocated files, no updates are performed, key=" + key);
                return 0;
            }
            String fileStr = keyValue.getValue().toString(StandardCharsets.UTF_8);
            String[] files = fileStr.split(";");
            this.files = Arrays.asList(files);
            return internalUpdateAll(version, layout, files);
        }
        catch (IOException e)
        {
            logger.error("failed to update local cache fully", e);
            return -1;
        }
    }

    /**
     * return true if all the zones are empty
     */
    public boolean isCacheEmpty ()
    {
        /**
         * There are no concurrent updates on the cache,
         * thus we don't have to synchronize the access to cachedColumnChunks.
         */
        for(PixelsZoneWriter zone:zones)
        {
            if(zone.getZoneType() != PixelsZoneUtil.ZoneType.SWAP && !zone.isZoneEmpty())
            {
                return false;
            }
        }
        return true;
    }

    public boolean isExistZoneEmpty ()
    {
        for(PixelsZoneWriter zone:zones)
        {
            if(zone.getZoneType() != PixelsZoneUtil.ZoneType.SWAP && zone.isZoneEmpty())
            {
                return true;
            }
        }
        return false;
    }

    /**
     * <p>
     * This function is used to update the cache content incrementally,
     * using the three-phase update protocol. Which means readers are only
     * blocked during the first (compaction) and the third (accomplishing)
     * phase. While in the second (loading) also most expensive phase, readers
     * are not blocked.
     * </p>
     * Return code:
     * -1: update failed.
     * 0: no updates are needed or update successfully.
     * 2: update size exceeds the limit.
     */
    public int updateIncremental (int version, Layout layout)
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
            this.files = Arrays.asList(files);
            return internalUpdateIncremental(version, layout, files);
        }
        catch (IOException | InterruptedException e)
        {
            logger.error("failed to update local cache incrementally", e);
            return -1;
        }
    }


    /**
     * Currently, this is an interface for unit tests.
     * This method only updates index content and cache content (without touching headers)
     */
    public void write(PixelsCacheKey key, byte[] value) throws IOException 
    {
       int zoneId = bucketToZoneMap.getBucketToZone(locator.getLocation(key));
       PixelsZoneWriter zone = zones.get(zoneId);
       zone.write(key, value);
    }


    /**
     * Currently, this is an interface for unit tests.
     */
    public void flush()
    {
        for (PixelsZoneWriter zone:zones)
        {
            zone.flushIndex();
        }
    }

    private int internalUpdateAll(int version, Layout layout, String[] files) throws IOException
    {
        int status = 0;
        // get the new caching layout
        Compact compact = layout.getCompact();
        int cacheBorder = compact.getCacheBorder();
        List<String> cacheColumnChunkOrders = compact.getColumnChunkOrder();//.subList(0, cacheBorder);


        // update cache content
        cachedColumnChunks.clear();
        for(int j=0; j<bucketTypeInfo.getLazyBucketNum(); ++j)
        {
            // get origin lazy zone
            int bucketId = bucketTypeInfo.getLazyBucketIds().get(j);
            int zoneId = bucketToZoneMap.getBucketToZone(bucketId);
            PixelsZoneWriter zone = zones.get(zoneId);
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
                PixelsZoneUtil.beginIndexWrite(zone.getIndexFile());
            } 
            catch (InterruptedException e)
            {
                status = -1;
                logger.error("Failed to get write permission on index.", e);
                return status;
            }
            zone.getRadix().removeAll();
            long currCacheOffset = PixelsZoneUtil.ZONE_DATA_OFFSET;
            boolean enableAbsoluteBalancer = Boolean.parseBoolean(
                    ConfigFactory.Instance().getProperty("cache.absolute.balancer.enabled"));
            int rowGroupNumInLayout = compact.getNumRowGroupInFile();
            outer_loop:
            for (String file : files)
            {
                if (enableAbsoluteBalancer && storage.hasLocality())
                {
                    // TODO: this is used for experimental purpose only.
                    // may be removed later.
                    file = ensureLocality(file);
                }
                int physicalLen;
                long physicalOffset;
                try (PixelsPhysicalReader pixelsPhysicalReader = new PixelsPhysicalReader(storage, file))
                {
                    if (pixelsPhysicalReader.getRowGroupNum() < rowGroupNumInLayout)
                    {
                        // TODO: Now the strategy for handling incomplete files is discarding them directly.
                        //  This may lead to certain column chunks not being read into the cache as expected.
                        logger.warn(rowGroupNumInLayout + " row groups are required for cache, but only " +
                                pixelsPhysicalReader.getRowGroupNum() + " row groups are found in " +
                                file + ". This file will be ignored.");
                        continue;
                    }
                    // update radix and cache content
                    for (String cacheColumnChunkOrder : cacheColumnChunkOrders)
                    {
                        String[] columnChunkIdStr = cacheColumnChunkOrder.split(":");
                        short rowGroupId = Short.parseShort(columnChunkIdStr[0]);
                        short columnId = Short.parseShort(columnChunkIdStr[1]);
                        PixelsCacheKey key = new PixelsCacheKey(pixelsPhysicalReader.getCurrentBlockId(), rowGroupId, columnId);
                        if(locator.getLocation(key) != bucketId)
                        {
                            continue;
                        }

                        PixelsProto.RowGroupFooter rowGroupFooter = pixelsPhysicalReader.readRowGroupFooter(rowGroupId);
                        PixelsProto.ColumnChunkIndex chunkIndex =
                                rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(columnId);
                        physicalLen = (int) chunkIndex.getChunkLength();
                        physicalOffset = chunkIndex.getChunkOffset();
                        if (currCacheOffset + physicalLen >= zone.getZoneFile().getSize())
                        {
                            logger.debug("Cache writes have exceeded bucket size. Break. Current size: " + currCacheOffset + "current bucketId: " + bucketId);
                            break outer_loop;
                        } 
                        else
                        {
                            zone.getRadix().put(new PixelsCacheKey(pixelsPhysicalReader.getCurrentBlockId(), rowGroupId, columnId),
                                    new PixelsCacheIdx(currCacheOffset, physicalLen));
                            byte[] columnChunk = pixelsPhysicalReader.read(physicalOffset, physicalLen);
                            zone.getZoneFile().setBytes(currCacheOffset, columnChunk);
                            logger.debug(
                                    "Cache write: " + file + "-" + rowGroupId + "-" + columnId + ", offset: " + currCacheOffset + ", length: " + columnChunk.length);
                            currCacheOffset += physicalLen;
                        }
                    }
                }
            }
            // flush index
            zone.flushIndex();
            // update cache version
            PixelsZoneUtil.setIndexVersion(zone.getIndexFile(), version);
            PixelsZoneUtil.setStatus(zone.getZoneFile(), PixelsZoneUtil.ZoneStatus.OK.getId());
            PixelsZoneUtil.setSize(zone.getZoneFile(), currCacheOffset);
            // set rwFlag as readable
            PixelsZoneUtil.endIndexWrite(zone.getIndexFile());
            logger.debug("Writer ends at offset: " + currCacheOffset);
        }

        cachedColumnChunks.addAll(cacheColumnChunkOrders);
        // update cache version
        PixelsZoneUtil.setIndexVersion(globalIndexFile, version);
        logger.info("finish internalUpdateAll");
        return status;
    }

    /**
     * This method needs further tests.
     * @param version
     * @param layout
     * @param files
     * @return
     * @throws IOException
     */
    private int internalUpdateIncremental(int version, Layout layout, String[] files)
            throws IOException, InterruptedException, NullPointerException 
    {
        int status = 0;
        /**
         * Get the new caching layout.
         */
        Compact compact = layout.getCompact();
        int cacheBorder = compact.getCacheBorder();
        List<String> nextVersionCached = compact.getColumnChunkOrder();//.subList(0, cacheBorder);
        /**
         * Prepare structures for the survived and new coming cache elements.
         */
        List<String> survivedColumnChunks = new ArrayList<>();
        List<String> newCachedColumnChunks = new ArrayList<>();
        for (String columnChunk : nextVersionCached)
        {
            if (this.cachedColumnChunks.contains(columnChunk))
            {
                survivedColumnChunks.add(columnChunk);
            }
            else
            {
                newCachedColumnChunks.add(columnChunk);
            }
        }
        List<PixelsCacheEntry> survivedIdxes = new ArrayList<>(survivedColumnChunks.size()*files.length);
        Map<Long, List<PixelsCacheKey>> missSurvivedIdxes = new HashMap<>();// missing survivedIdxes in reason of zone capacity limit
        int rowGroupNumInLayout = compact.getNumRowGroupInFile();
        for (String file : files)
        {
            try (PixelsPhysicalReader physicalReader = new PixelsPhysicalReader(storage, file))
            {
                if(physicalReader.getRowGroupNum() < rowGroupNumInLayout)
                {
                    // TODO: Now the strategy for handling incomplete files is discarding them directly.
                    //  This may lead to certain column chunks not being read into the cache as expected.
                    logger.warn(rowGroupNumInLayout + " row groups are required for cache, but only " +
                            physicalReader.getRowGroupNum() + " row groups are found in " +
                            file + ". This file was ignored before.");
                    continue;
                }
                // TODO: in case of block id was changed, the survived column chunks in this block can not survive in the cache update.
                // This problem only affects the efficiency, but it is better to resolve it.
                long blockId = physicalReader.getCurrentBlockId();
                for (String survivedColumnChunk : survivedColumnChunks)
                {
                    String[] columnChunkIdStr = survivedColumnChunk.split(":");
                    short rowGroupId = Short.parseShort(columnChunkIdStr[0]);
                    short columnId = Short.parseShort(columnChunkIdStr[1]);
                    long bucketId = locator.getLocation(new PixelsCacheKey(blockId, rowGroupId, columnId));
                    int zoneId = bucketToZoneMap.getBucketToZone(bucketId);
                    PixelsZoneWriter zone = zones.get(zoneId);
                    PixelsCacheIdx curCacheIdx = zone.getRadix().get(blockId, rowGroupId, columnId);
                    if(curCacheIdx == null)
                    {
                        if(missSurvivedIdxes.containsKey(blockId))
                        {
                            missSurvivedIdxes.get(blockId).add(new PixelsCacheKey(blockId, rowGroupId, columnId));
                        }
                        else
                        {
                            missSurvivedIdxes.put(blockId, new ArrayList<>());
                            missSurvivedIdxes.get(blockId).add(new PixelsCacheKey(blockId, rowGroupId, columnId));
                        }
                    }
                    else
                    {
                        survivedIdxes.add(new PixelsCacheEntry(new PixelsCacheKey(blockId, rowGroupId, columnId), curCacheIdx));
                    }
                }
            }
        }
        // ascending order according to the offset in cache file.
        Collections.sort(survivedIdxes);

        for(int j=0; j<bucketTypeInfo.getLazyBucketNum(); ++j)
        {
            /**
             * Start phase 1: compact the survived cache elements.
             */
            logger.debug("Start cache compaction...");
            long newCacheOffset = PixelsZoneUtil.ZONE_DATA_OFFSET;
            // get origin lazy zone
            int bucketId = bucketTypeInfo.getLazyBucketIds().get(j);
            int zoneId = bucketToZoneMap.getBucketToZone(bucketId);
            PixelsZoneWriter zone = zones.get(zoneId);
            // get swap zone
            int swapBucketId = bucketTypeInfo.getSwapBucketIds().get(0);
            int swapZoneId = bucketToZoneMap.getBucketToZone(swapBucketId);
            PixelsZoneWriter swapZone = zones.get(swapZoneId);
            // copy the survived cache elements from this lazy zone to the swap zone.
            for (PixelsCacheEntry survivedIdx : survivedIdxes) 
            {
                if (locator.getLocation(survivedIdx.key) != bucketId) 
                {
                    continue;
                }
                swapZone.getZoneFile().copyMemory(zone.getZoneFile().getAddress() + survivedIdx.idx.offset, swapZone.getZoneFile().getAddress() + newCacheOffset, survivedIdx.idx.length);
                swapZone.getRadix().put(survivedIdx.key, new PixelsCacheIdx(newCacheOffset, survivedIdx.idx.length));
                newCacheOffset += survivedIdx.idx.length;
            }
            swapZone.flushIndex();
            PixelsZoneUtil.setStatus(swapZone.getZoneFile(), PixelsZoneUtil.ZoneStatus.OK.getId());
            PixelsZoneUtil.setSize(swapZone.getZoneFile(), newCacheOffset);
            swapZone.changeZoneTypeS2L();
            logger.debug("Cache compaction to bucket " + bucketId + " finished");

            /**
             * Start phase 2: load and append new cache elements to the cache file.
             */
            logger.debug("Start cache append...");
            List<PixelsCacheEntry> newIdxes = new ArrayList<>();
            boolean enableAbsoluteBalancer = Boolean.parseBoolean(
                    ConfigFactory.Instance().getProperty("cache.absolute.balancer.enabled"));
            outer_loop:
            for (String file : files)
            {
                if (enableAbsoluteBalancer && storage.hasLocality())
                {
                    // TODO: this is used for experimental purpose only.
                    // may be removed later.
                    file = ensureLocality(file);
                }
                try (PixelsPhysicalReader pixelsPhysicalReader = new PixelsPhysicalReader(storage, file))
                {
                    if(pixelsPhysicalReader.getRowGroupNum() < rowGroupNumInLayout)
                    {
                        // TODO: Now the strategy for handling incomplete files is discarding them directly.
                        //  This may lead to certain column chunks not being read into the cache as expected.
                        logger.warn(rowGroupNumInLayout + " row groups are required for cache, but only " +
                                pixelsPhysicalReader.getRowGroupNum() + " row groups are found in " +
                                file + ". This file was ignored before.");
                        continue;
                    }
                    int physicalLen;
                    long physicalOffset;
                    // update radix and cache content
                    for (int i = 0; i < newCachedColumnChunks.size(); i++)
                    {
                        String[] columnChunkIdStr = newCachedColumnChunks.get(i).split(":");
                        short rowGroupId = Short.parseShort(columnChunkIdStr[0]);
                        short columnId = Short.parseShort(columnChunkIdStr[1]);
                        if(locator.getLocation(new PixelsCacheKey(pixelsPhysicalReader.getCurrentBlockId(), rowGroupId, columnId)) != bucketId) 
                        {
                            continue;
                        }
                        PixelsProto.RowGroupFooter rowGroupFooter = pixelsPhysicalReader.readRowGroupFooter(rowGroupId);
                        PixelsProto.ColumnChunkIndex chunkIndex =
                                rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(columnId);
                        physicalLen = chunkIndex.getChunkLength();
                        physicalOffset = chunkIndex.getChunkOffset();
                        if (newCacheOffset + physicalLen >= swapZone.getZoneFile().getSize())
                        {
                            logger.debug("Cache writes have exceeded bucket size. Break. Current size: " + newCacheOffset + "current bucketId: " + bucketId);
                            break outer_loop;
                        }
                        else
                        {
                            newIdxes.add(new PixelsCacheEntry(
                                    new PixelsCacheKey(pixelsPhysicalReader.getCurrentBlockId(), rowGroupId, columnId),
                                    new PixelsCacheIdx(newCacheOffset, physicalLen)));
                            byte[] columnChunk = pixelsPhysicalReader.read(physicalOffset, physicalLen);
                            swapZone.getZoneFile().setBytes(newCacheOffset, columnChunk);
                            logger.debug(
                                    "Cache write: " + file + "-" + rowGroupId + "-" + columnId + ", offset: " + newCacheOffset + ", length: " + columnChunk.length);
                            newCacheOffset += physicalLen;
                        }
                    }
                    // load missSurvivedIdxes from bottom storage
                    if(missSurvivedIdxes.containsKey(pixelsPhysicalReader.getCurrentBlockId()))
                    {
                        for(PixelsCacheKey key : missSurvivedIdxes.get(pixelsPhysicalReader.getCurrentBlockId()))
                        {
                            if(locator.getLocation(key) != bucketId)
                            {
                                continue;
                            }
                            PixelsProto.RowGroupFooter rowGroupFooter = pixelsPhysicalReader.readRowGroupFooter(key.rowGroupId);
                            PixelsProto.ColumnChunkIndex chunkIndex = rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(key.columnId);
                            physicalLen = chunkIndex.getChunkLength();
                            physicalOffset = chunkIndex.getChunkOffset();
                            if(newCacheOffset + physicalLen >= swapZone.getZoneFile().getSize())
                            {
                                logger.debug("Cache writes have exceeded bucket size. Break. Current size: " + newCacheOffset + "current bucketId: " + bucketId);
                                break outer_loop;
                            }
                            else
                            {
                                byte[] columnChunk = pixelsPhysicalReader.read(physicalOffset, physicalLen);
                                swapZone.getZoneFile().setBytes(newCacheOffset, columnChunk);
                                swapZone.getRadix().put(key, new PixelsCacheIdx(newCacheOffset, physicalLen));
                                newCacheOffset += physicalLen;
                            }
                        }
                    }
                }
            }
            logger.debug("Bucket " + bucketId + " append finished, writer ends at offset: " + newCacheOffset);

            /**
             * Start phase 3: activate new cache elements.
             */
            logger.debug("Start activating new cache elements...");
            for (PixelsCacheEntry newIdx : newIdxes)
            {
                swapZone.getRadix().put(newIdx.key, newIdx.idx);
            }
            // flush index
            swapZone.flushIndex();
            // update cache version
            PixelsZoneUtil.setIndexVersion(swapZone.getIndexFile(), version);
            PixelsZoneUtil.setStatus(swapZone.getZoneFile(), PixelsZoneUtil.ZoneStatus.OK.getId());
            PixelsZoneUtil.setSize(swapZone.getZoneFile(), newCacheOffset);

            // update bucketToZoneMap to let this bucketId point to the old swap zone.
            bucketToZoneMap.updateBucketZoneMap(bucketId,swapZoneId);
            // change zone type from lazy to swap
            long start = System.currentTimeMillis();
            while(PixelsZoneUtil.getReaderCount(zone.getIndexFile()) > 0)
            {
                Thread.sleep(10);
                if(System.currentTimeMillis() - start > 2*PixelsZoneUtil.ZONE_READ_LEASE_MS)
                {
                    PixelsZoneUtil.eliminateReaderCount(zone.getIndexFile());
                    break;
                }
            }
            zone.changeZoneTypeL2S();
            PixelsZoneUtil.setIndexVersion(zone.getIndexFile(), version);
            
            bucketToZoneMap.updateBucketZoneMap(swapBucketId,zoneId);
        }
        this.cachedColumnChunks.clear();
        this.cachedColumnChunks.addAll(survivedColumnChunks);
        // save the new cached column chunks into cachedColumnChunks.
        this.cachedColumnChunks.addAll(newCachedColumnChunks);
        // update cache version
        PixelsZoneUtil.setIndexVersion(globalIndexFile, version);
        logger.info("finish internalUpdateIncremental");
        return status;
    }

    /**
     * NOTE:
     * 1. The expand operation does not change the cache size or zone num in the config file, once the PixelsWorker restarts, the expansion is no longer in effect.
     * 2. Now Cache can automatically build the new physical zone file, but it needs to be pinned manually.
     */
    public void expand() throws Exception
    {
        if(bucketTypeInfo.getSwapBucketNum() == 0)
        {
            logger.warn("No swap zone, can not expand!");
            return;
        }
        if(bucketToZoneMap.isMapFull(zoneNum + 1))
        {
            logger.warn("routing table is full, can not expand!");
            return;
        }
        // build new physical zone, insert it into the end of the physical zone list
        int newZoneId = zoneNum;
        String originZoneName  = zones.get(0).getZoneFile().getName();
        String baseZoneName    = originZoneName.substring(0, originZoneName.lastIndexOf('.'));
        String newZoneLocation = baseZoneName + "." + newZoneId;
        String originIndexName  = zones.get(0).getIndexFile().getName();
        String baseIndexName    = originIndexName.substring(0, originIndexName.lastIndexOf('.'));
        String newIndexLocation = baseIndexName + "." + newZoneId;
        long zoneSize = zones.get(0).getZoneFile().getSize();
        long indexSize = zones.get(0).getIndexFile().getSize();
        PixelsZoneWriter newZone = new PixelsZoneWriter(newZoneLocation, newIndexLocation, zoneSize, indexSize, newZoneId);
        newZone.buildLazy();
        newZone.getRadix().removeAll();
        zones.add(newZone);
        zoneNum++;
        // update logical bucket info and bucketToZoneMap, insert the new bucket into the first swap bucket position and move the other swap buckets backward
        int firstSwapZoneId = bucketToZoneMap.getBucketToZone(bucketTypeInfo.getSwapBucketIds().get(0));
        int newBucketId = bucketTypeInfo.getSwapBucketIds().get(0);
        bucketTypeInfo.getLazyBucketIds().add(newBucketId);
        bucketTypeInfo.incrementLazyBucketNum();
        for(int i = 0; i < bucketTypeInfo.getSwapBucketNum() - 1; i++)
        {
            bucketTypeInfo.getSwapBucketIds().set(i, bucketTypeInfo.getSwapBucketIds().get(i+1));
        }
        bucketTypeInfo.getSwapBucketIds().set(bucketTypeInfo.getSwapBucketNum() - 1, newZoneId);
        for(int i = bucketTypeInfo.getSwapBucketNum() - 1; i > 0; i--)
        {
            bucketToZoneMap.updateBucketZoneMap(bucketTypeInfo.getSwapBucketIds().get(i), bucketToZoneMap.getBucketToZone(bucketTypeInfo.getSwapBucketIds().get(i-1)));
        }
        bucketToZoneMap.updateBucketZoneMap(bucketTypeInfo.getSwapBucketIds().get(0), firstSwapZoneId);
        // there is no data in the cache, just build a empty zone
        if(this.files == null || this.files.size() == 0)
        {
            PixelsZoneUtil.setIndexVersion(newZone.getIndexFile(), PixelsZoneUtil.getIndexVersion(zones.get(bucketTypeInfo.getLazyBucketIds().get(0)).getIndexFile()));
            PixelsZoneUtil.setStatus(newZone.getZoneFile(), PixelsZoneUtil.ZoneStatus.OK.getId());
            bucketToZoneMap.updateBucketZoneMap(newBucketId, newZoneId);
            locator.addNode();
            bucketToZoneMap.setHashNodeNum(locator.getNodeNum());
            return;
        }

        long newZoneOffset = PixelsZoneUtil.ZONE_DATA_OFFSET;
        PixelsLocator pseudoLocator = new PixelsLocator(locator.getNodeNum());
        pseudoLocator.addNode();
        for(int i = 0; i < pseudoLocator.getReplicaNum(); i++)
        {
            // Phase 1: find the next bucket by hashcycle
            long nextBucketId = pseudoLocator.findNextBucket(newBucketId, i);
            int nextZoneId = bucketToZoneMap.getBucketToZone(nextBucketId);
            PixelsZoneWriter nextZone = zones.get(nextZoneId);
            // Phase 2: copy data
            for (String file : this.files)
            {
                try (PixelsPhysicalReader pixelsPhysicalReader = new PixelsPhysicalReader(storage, file))
                {
                    for (String cacheColumnChunkOrder : this.cachedColumnChunks)
                    {
                        String[] columnChunkIdStr = cacheColumnChunkOrder.split(":");
                        short rowGroupId = Short.parseShort(columnChunkIdStr[0]);
                        short columnId = Short.parseShort(columnChunkIdStr[1]);
                        long blockId = pixelsPhysicalReader.getCurrentBlockId();
                        PixelsCacheKey key = new PixelsCacheKey(blockId, rowGroupId, columnId);
                        if(!pseudoLocator.isDataInReplica(key, newBucketId, i))
                        {
                            continue;
                        }
                        PixelsCacheIdx curIdx = nextZone.getRadix().get(blockId, rowGroupId, columnId);
                        if(curIdx != null)
                        {
                            if(newZoneOffset + curIdx.length < newZone.getZoneFile().getSize())
                            {
                                long srcAddr = nextZone.getZoneFile().getAddress() + curIdx.offset;
                                long destAddr = newZone.getZoneFile().getAddress() + newZoneOffset;
                                // direct memory copy, avoid intermediate byte array
                                newZone.getZoneFile().copyMemory(srcAddr, destAddr, curIdx.length);
                                newZone.getRadix().put(key, new PixelsCacheIdx(newZoneOffset, curIdx.length));
                                newZoneOffset += curIdx.length;
                            }
                            else
                            {
                                logger.debug("Cache writes have exceeded bucket size. Break. Current size: " + newZoneOffset + "current bucketId: " + newBucketId);
                            }
                        }
                        else
                        {
                            // Phase 3: load new hot data from the bottom storage, now just read the uncached hot data according to the last cache version.
                            PixelsProto.RowGroupFooter rowGroupFooter = pixelsPhysicalReader.readRowGroupFooter(rowGroupId);
                            PixelsProto.ColumnChunkIndex chunkIndex = rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(columnId);
                            int physicalLen = (int) chunkIndex.getChunkLength();
                            long physicalOffset = chunkIndex.getChunkOffset();
                            if(newZoneOffset + physicalLen < newZone.getZoneFile().getSize())
                            {
                                byte[] columnChunk = pixelsPhysicalReader.read(physicalOffset, physicalLen);
                                newZone.getZoneFile().setBytes(newZoneOffset, columnChunk);
                                newZone.getRadix().put(key, new PixelsCacheIdx(newZoneOffset, physicalLen));
                                newZoneOffset += physicalLen;
                            }
                            else
                            {
                                logger.debug("Cache writes have exceeded bucket size. Break. Current size: " + newZoneOffset + "current bucketId: " + newBucketId);
                            }
                        }
                    }
                }
            }
        }
        newZone.flushIndex();
        PixelsZoneUtil.setIndexVersion(newZone.getIndexFile(), PixelsZoneUtil.getIndexVersion(zones.get(bucketTypeInfo.getLazyBucketIds().get(0)).getIndexFile()));
        PixelsZoneUtil.setStatus(newZone.getZoneFile(), PixelsZoneUtil.ZoneStatus.OK.getId());
        PixelsZoneUtil.setSize(newZone.getZoneFile(), newZoneOffset);
        // Phase 4: update routing table (bucketToZoneMap)
        bucketToZoneMap.updateBucketZoneMap(newBucketId, newZoneId);
        // Phase 5: update hashcycle in locator
        locator.addNode();
        bucketToZoneMap.setHashNodeNum(locator.getNodeNum());// trigger the readers to update their locator, the new zone will be served after the locator is updated
        // Phase 6: compact the next zone of the new zone, now it will be completed in the next updateIncremental.
        logger.info("finish expand operation");
    }

    /**
     * NOTE:
     * 1. Shrink can not be used together with expand in the same PixelsWorker life cycle, for 2 reasons:
     *   1.1 PixelsCacheReader cannot accurately determine the precise number of expand and shrink operations when both expand and shrink are used together.
     *   1.2 if expand is called after shrink, PixelsCacheReader cannot know the correct last lazy zone id.
     * 2. The shrink operation does not change the cache size or zone num in the config file, once the PixelsWorker restarts, the shrink is no longer in effect.
     * 3. Now the deleted physical zone file needs to be unpinned and removed manually.
     */
    public void shrink() throws Exception
    {
        if (bucketTypeInfo.getLazyBucketNum() <= 0) 
        {
            logger.warn("No zone to shrink");
            return;
        }
        // Phase 1: find the next bucket by hashcycle
        // Phase 2: merge the data in the deleting zone to the next zone
        // Now just uncache the data in the deleting zone, the data belong to the next zone will be merged in the next updateIncremental.
        // Phase 3: update hashcycle in locator
        locator.removeNode();
        bucketToZoneMap.setHashNodeNum(locator.getNodeNum());
        // Phase 4: update routing table (bucketToZoneMap)
        Thread.sleep(5);// Wait for readers that are still using the old locator to finish their routing-table lookups. This is only a temporary solution and needs to be improved later.
        int lastLazyBucketId = bucketTypeInfo.getLazyBucketIds().get(bucketTypeInfo.getLazyBucketNum() - 1);
        int lastLazyZoneId = bucketToZoneMap.getBucketToZone(lastLazyBucketId);
        bucketToZoneMap.updateBucketZoneMap(lastLazyBucketId, -1);
        
        
        // delete the physical zoneWriter
        zones.get(lastLazyZoneId).close();
        zones.remove(lastLazyZoneId);
        zoneNum--;
        // update logical bucket info and bucketToZoneMap, delete the last lazy bucket and move the other swap buckets forward
        bucketTypeInfo.getLazyBucketIds().remove(bucketTypeInfo.getLazyBucketNum() - 1);
        bucketTypeInfo.decrementLazyBucketNum();
        int lastSwapZoneId = bucketToZoneMap.getBucketToZone(bucketTypeInfo.getSwapBucketIds().get(bucketTypeInfo.getSwapBucketNum() - 1));
        for(int i = bucketTypeInfo.getSwapBucketNum() - 1; i > 0; i--)
        {
            bucketTypeInfo.getSwapBucketIds().set(i, bucketTypeInfo.getSwapBucketIds().get(i-1));
        }
        bucketTypeInfo.getSwapBucketIds().set(0, lastLazyBucketId);
        for(int i = 0; i < bucketTypeInfo.getSwapBucketNum()-1; i++)
        {
            bucketToZoneMap.updateBucketZoneMap(bucketTypeInfo.getSwapBucketIds().get(i), bucketToZoneMap.getBucketToZone(bucketTypeInfo.getSwapBucketIds().get(i+1)-1));
        }
        bucketToZoneMap.updateBucketZoneMap(bucketTypeInfo.getSwapBucketIds().get(bucketTypeInfo.getSwapBucketNum() - 1), lastSwapZoneId-1);
        logger.info("finish shrink operation");
    }

    /**
     * This method is currently used for experimental purpose.
     * @param path
     * @return
     */
    private String ensureLocality (String path)
    {
        String newPath = path.substring(0, path.indexOf(".pxl")) + "_" + host + ".pxl";
        try
        {
            String[] dataNodes = storage.getHosts(path);
            boolean isLocal = false;
            for (String dataNode : dataNodes)
            {
                if (dataNode.equals(host))
                {
                    isLocal = true;
                    break;
                }
            }
            if (isLocal == false)
            {
                // file is not local, move it to local.
                PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(storage, path);
                PhysicalWriter writer = PhysicalWriterUtil.newPhysicalWriter(storage, newPath,
                        2048l*1024l*1024l, (short) 1, true);
                byte[] buffer = new byte[1024*1024*32]; // 32MB buffer for copy.
                long copiedBytes = 0l, fileLength = reader.getFileLength();
                boolean success = true;
                try
                {
                    while (copiedBytes < fileLength)
                    {
                        int bytesToCopy = 0;
                        if (copiedBytes + buffer.length <= fileLength)
                        {
                            bytesToCopy = buffer.length;
                        }
                        else
                        {
                            bytesToCopy = (int) (fileLength - copiedBytes);
                        }
                        reader.readFully(buffer, 0, bytesToCopy);
                        writer.prepare(bytesToCopy);
                        writer.append(buffer, 0, bytesToCopy);
                        copiedBytes += bytesToCopy;
                    }
                    reader.close();
                    writer.flush();
                    writer.close();
                }
                catch (IOException e)
                {
                    logger.error("failed to copy file", e);
                    success = false;
                }
                if (success)
                {
                    storage.delete(path, false);
                    return newPath;
                }
                else
                {
                    storage.delete(newPath, false);
                    return path;
                }

            }
            else
            {
                return path;
            }
        }
        catch (IOException e)
        {
            logger.error("failed to ensure the locality of a file/data object.", e);
        }

        return null;
    }

    /**
     * Currently, this is an interface for unit tests.
     */
    public List<PixelsZoneWriter> getZones() 
    {
       return zones;
    }

    public void close() throws Exception
    {
        for(PixelsZoneWriter zone:zones)
        {
            zone.close();
        }
        globalIndexFile.unmap();
    }
}
