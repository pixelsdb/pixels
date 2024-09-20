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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * pixels cache writer
 *
 * @author guodong
 * @author hank
 */
public class PixelsCacheWriter
{
    private final static Logger logger = LogManager.getLogger(PixelsCacheWriter.class);

    private final MemoryMappedFile cacheFile;
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
    private PixelsRadix radix;
    private long currentIndexOffset;
    private long allocatedIndexOffset = PixelsCacheUtil.INDEX_RADIX_OFFSET;
    private long cacheOffset = PixelsCacheUtil.CACHE_DATA_OFFSET; // this is only used in the write() method.
    private ByteBuffer nodeBuffer = ByteBuffer.allocate(8 * 256);
    private ByteBuffer cacheIdxBuffer = ByteBuffer.allocate(PixelsCacheIdx.SIZE);
    private Set<String> cachedColumnChunks = new HashSet<>();

    private PixelsCacheWriter(MemoryMappedFile cacheFile,
                              MemoryMappedFile indexFile,
                              Storage storage,
                              PixelsRadix radix,
                              Set<String> cachedColumnChunks,
                              EtcdUtil etcdUtil,
                              String host)
    {
        this.cacheFile = cacheFile;
        this.indexFile = indexFile;
        this.storage = storage;
        this.radix = radix;
        this.etcdUtil = etcdUtil;
        this.host = host;
        this.nodeBuffer.order(ByteOrder.BIG_ENDIAN);
        if (cachedColumnChunks != null && cachedColumnChunks.isEmpty() == false)
        {
            cachedColumnChunks.addAll(cachedColumnChunks);
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

        private Builder()
        {
        }

        public PixelsCacheWriter.Builder setCacheLocation(String cacheLocation)
        {
            checkArgument(cacheLocation != null && !cacheLocation.isEmpty(),
                    "cache location should bot be empty");
            this.builderCacheLocation = cacheLocation;

            return this;
        }

        public PixelsCacheWriter.Builder setCacheSize(long cacheSize)
        {
            checkArgument(cacheSize > 0, "cache size should be positive");
            this.builderCacheSize = cacheSize;

            return this;
        }

        public PixelsCacheWriter.Builder setIndexLocation(String indexLocation)
        {
            checkArgument(indexLocation != null && !indexLocation.isEmpty(),
                    "index location should not be empty");
            this.builderIndexLocation = indexLocation;

            return this;
        }

        public PixelsCacheWriter.Builder setIndexSize(long size)
        {
            checkArgument(size > 0, "index size should be positive");
            this.builderIndexSize = size;

            return this;
        }

        public PixelsCacheWriter.Builder setOverwrite(boolean overwrite)
        {
            this.builderOverwrite = overwrite;
            return this;
        }

        public PixelsCacheWriter.Builder setHostName(String hostName)
        {
            checkArgument(hostName != null, "hostname should not be null");
            this.builderHostName = hostName;
            return this;
        }

        public PixelsCacheWriter.Builder setCacheConfig(PixelsCacheConfig cacheConfig)
        {
            checkArgument(cacheConfig != null, "cache config should not be null");
            this.cacheConfig = cacheConfig;
            return this;
        }

        public PixelsCacheWriter build()
                throws Exception
        {
            MemoryMappedFile cacheFile = new MemoryMappedFile(builderCacheLocation, builderCacheSize);
            MemoryMappedFile indexFile = new MemoryMappedFile(builderIndexLocation, builderIndexSize);
            PixelsRadix radix;
            // check if cache and index exists.
            Set<String> cachedColumnChunks = new HashSet<>();
            // if overwrite is not true, and cache and index file already exists, reconstruct radix from existing index.
            if (!builderOverwrite && PixelsCacheUtil.checkMagic(indexFile) && PixelsCacheUtil.checkMagic(cacheFile))
            {
                // cache exists in local cache file and index, reload the index.
                radix = PixelsCacheUtil.loadRadixIndex(indexFile);
                // build cachedColumnChunks for PixelsCacheWriter.
                int cachedVersion = PixelsCacheUtil.getIndexVersion(indexFile);
                MetadataService metadataService = MetadataService.Instance();
                Layout cachedLayout = metadataService.getLayout(
                        cacheConfig.getSchema(), cacheConfig.getTable(), cachedVersion);
                Compact compact = cachedLayout.getCompact();
                int cacheBorder = compact.getCacheBorder();
                cachedColumnChunks.addAll(compact.getColumnChunkOrder().subList(0, cacheBorder));
            }
            //   else, create a new radix tree, and initialize the index and cache file.
            else
            {
                radix = new PixelsRadix();
                PixelsCacheUtil.initialize(indexFile, cacheFile);
            }
            EtcdUtil etcdUtil = EtcdUtil.Instance();

            Storage storage = StorageFactory.Instance().getStorage(cacheConfig.getStorageScheme());

            return new PixelsCacheWriter(cacheFile, indexFile, storage, radix,
                    cachedColumnChunks, etcdUtil, builderHostName);
        }
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public MemoryMappedFile getIndexFile()
    {
        return indexFile;
    }

    public MemoryMappedFile getCacheFile()
    {
        return cacheFile;
    }

    /**
     * DO NOT USE THIS METHOD. Only for unit test.
     * @return
     */
    public PixelsRadix getRadix()
    {
        return this.radix;
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
            logger.info("pixels cache writer get cache plan, key=" + key);
            KeyValue keyValue = etcdUtil.getKeyValue(key);
            if (keyValue == null)
            {
                logger.info("Found no allocated files. No updates are needed. " + key);
                return 0;
            }
            String fileStr = keyValue.getValue().toString(StandardCharsets.UTF_8);
            String[] files = fileStr.split(";");
            logger.info("files to cache: " + fileStr);
            return internalUpdateAll(version, layout, files);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return -1;
        }
    }

    public boolean isCacheEmpty ()
    {
        return PixelsCacheUtil.isCacheFileEmpty(this.cacheFile);
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
            return internalUpdateIncremental(version, layout, files);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * Currently, this is an interface for unit tests.
     * This method only updates index content and cache content (without touching headers)
     */
    public void write(PixelsCacheKey key, byte[] value)
    {
        PixelsCacheIdx cacheIdx = new PixelsCacheIdx(cacheOffset, value.length);
        cacheFile.setBytes(cacheOffset, value);
        cacheOffset += value.length;
        radix.put(key, cacheIdx);
    }

    /**
     * Currently, this is an interface for unit tests.
     */
    public void flush()
    {
        flushIndex();
    }

    private int internalUpdateAll(int version, Layout layout, String[] files) throws IOException
    {
        int status = 0;
        // get the new caching layout
        Compact compact = layout.getCompact();
        int cacheBorder = compact.getCacheBorder();
        List<String> cacheColumnChunkOrders = compact.getColumnChunkOrder().subList(0, cacheBorder);
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
        }
        catch (InterruptedException e)
        {
            status = -1;
            logger.error("Failed to get write permission on index.", e);
            return status;
        }

        // update cache content
        if (cachedColumnChunks == null || cachedColumnChunks.isEmpty())
        {
            cachedColumnChunks = new HashSet<>(cacheColumnChunkOrders.size());
        }
        else
        {
            cachedColumnChunks.clear();
        }
        radix.removeAll();
        long currCacheOffset = PixelsCacheUtil.CACHE_DATA_OFFSET;
        boolean enableAbsoluteBalancer = Boolean.parseBoolean(
                ConfigFactory.Instance().getProperty("enable.absolute.balancer"));
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
            PixelsPhysicalReader pixelsPhysicalReader = new PixelsPhysicalReader(storage, file);
            if(pixelsPhysicalReader.getRowGroupNum() < rowGroupNumInLayout)
            {
                // TODO: Now the strategy for handling incomplete files is discarding them directly. This may lead to certain column chunks not being read into the cache as expected.
                logger.warn(rowGroupNumInLayout + " row groups are required for cache, but only " + pixelsPhysicalReader.getRowGroupNum() + " row groups are found in " + file + ". This file will be ignored.");
                continue;
            }
            int physicalLen;
            long physicalOffset;
            // update radix and cache content
            for (int i = 0; i < cacheColumnChunkOrders.size(); i++)
            {
                String[] columnChunkIdStr = cacheColumnChunkOrders.get(i).split(":");
                short rowGroupId = Short.parseShort(columnChunkIdStr[0]);
                short columnId = Short.parseShort(columnChunkIdStr[1]);
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
                    byte[] columnChunk = pixelsPhysicalReader.read(physicalOffset, physicalLen);
                    cacheFile.setBytes(currCacheOffset, columnChunk);
                    logger.debug(
                            "Cache write: " + file + "-" + rowGroupId + "-" + columnId + ", offset: " + currCacheOffset + ", length: " + columnChunk.length);
                    currCacheOffset += physicalLen;
                }
            }
        }
        for (String cachedColumnChunk : cacheColumnChunkOrders)
        {
            cachedColumnChunks.add(cachedColumnChunk);
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

    /**
     * This method needs further tests.
     * @param version
     * @param layout
     * @param files
     * @return
     * @throws IOException
     */
    private int internalUpdateIncremental(int version, Layout layout, String[] files)
            throws IOException
    {
        int status = 0;
        /**
         * Get the new caching layout.
         */
        Compact compact = layout.getCompact();
        int cacheBorder = compact.getCacheBorder();
        List<String> nextVersionCached = compact.getColumnChunkOrder().subList(0, cacheBorder);
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
        this.cachedColumnChunks.clear();
        PixelsRadix oldRadix = radix;
        List<PixelsCacheEntry> survivedIdxes = new ArrayList<>(survivedColumnChunks.size()*files.length);
        int rowGroupNumInLayout = compact.getNumRowGroupInFile();
        for (String file : files)
        {
            PixelsPhysicalReader physicalReader = new PixelsPhysicalReader(storage, file);
            if(physicalReader.getRowGroupNum() < rowGroupNumInLayout)
            {
                // TODO: Now the strategy for handling incomplete files is discarding them directly. This may lead to certain column chunks not being read into the cache as expected.
                logger.warn(rowGroupNumInLayout + " row groups are required for cache, but only " + physicalReader.getRowGroupNum() + " row groups are found in " + file + ". This file was ignored before.");
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
                PixelsCacheIdx curCacheIdx = oldRadix.get(blockId, rowGroupId, columnId);
                survivedIdxes.add(
                        new PixelsCacheEntry(new PixelsCacheKey(
                                physicalReader.getCurrentBlockId(), rowGroupId, columnId), curCacheIdx));
            }
        }
        // ascending order according to the offset in cache file.
        Collections.sort(survivedIdxes);

        /**
         * Start phase 1: compact the survived cache elements.
         */
        logger.debug("Start cache compaction...");
        long newCacheOffset = PixelsCacheUtil.CACHE_DATA_OFFSET;
        PixelsRadix newRadix = new PixelsRadix();
        // set rwFlag as write
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
        for (PixelsCacheEntry survivedIdx : survivedIdxes)
        {
            cacheFile.copyMemory(survivedIdx.idx.offset, newCacheOffset, survivedIdx.idx.length);
            newRadix.put(survivedIdx.key, new PixelsCacheIdx(newCacheOffset, survivedIdx.idx.length));
            newCacheOffset += survivedIdx.idx.length;
        }
        this.radix = newRadix;
        // flush index
        flushIndex();
        // set rwFlag as readable
        PixelsCacheUtil.endIndexWrite(indexFile);
        oldRadix.removeAll();
        PixelsCacheUtil.setCacheStatus(cacheFile, PixelsCacheUtil.CacheStatus.OK.getId());
        PixelsCacheUtil.setCacheSize(cacheFile, newCacheOffset);
        // save the survived column chunks into cachedColumnChunks.
        this.cachedColumnChunks.addAll(survivedColumnChunks);
        logger.debug("Cache compaction finished, index ends at offset: " + currentIndexOffset);

        /**
         * Start phase 2: load and append new cache elements to the cache file.
         */
        logger.debug("Start cache append...");
        List<PixelsCacheEntry> newIdxes = new ArrayList<>();
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
            if(pixelsPhysicalReader.getRowGroupNum() < rowGroupNumInLayout)
            {
                // TODO: Now the strategy for handling incomplete files is discarding them directly. This may lead to certain column chunks not being read into the cache as expected.
                logger.warn(rowGroupNumInLayout + " row groups are required for cache, but only " + pixelsPhysicalReader.getRowGroupNum() + " row groups are found in " + file + ". This file will be ignored.");
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
                PixelsProto.RowGroupFooter rowGroupFooter = pixelsPhysicalReader.readRowGroupFooter(rowGroupId);
                PixelsProto.ColumnChunkIndex chunkIndex =
                        rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(columnId);
                physicalLen = chunkIndex.getChunkLength();
                physicalOffset = chunkIndex.getChunkOffset();
                if (newCacheOffset + physicalLen >= cacheFile.getSize())
                {
                    logger.debug("Cache writes have exceeded cache size. Break. Current size: " + newCacheOffset);
                    status = 2;
                    break outer_loop;
                }
                else
                {
                    newIdxes.add(new PixelsCacheEntry(
                            new PixelsCacheKey(pixelsPhysicalReader.getCurrentBlockId(), rowGroupId, columnId),
                            new PixelsCacheIdx(newCacheOffset, physicalLen)));
                    byte[] columnChunk = pixelsPhysicalReader.read(physicalOffset, physicalLen);
                    cacheFile.setBytes(newCacheOffset, columnChunk);
                    logger.debug(
                            "Cache write: " + file + "-" + rowGroupId + "-" + columnId + ", offset: " + newCacheOffset + ", length: " + columnChunk.length);
                    newCacheOffset += physicalLen;
                }
            }
        }
        logger.debug("Cache append finished, cache writer ends at offset: " + newCacheOffset);

        /**
         * Start phase 3: activate new cache elements.
         */
        logger.debug("Start activating new cache elements...");
        // set rwFlag as write
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
            // TODO: recovery needed here.
            return status;
        }
        for (PixelsCacheEntry newIdx : newIdxes)
        {
            radix.put(newIdx.key, newIdx.idx);
        }
        // flush index
        flushIndex();
        // save the new cached column chunks into cachedColumnChunks.
        for (String newColumnChunk : newCachedColumnChunks)
        {
            this.cachedColumnChunks.add(newColumnChunk);
        }
        // update cache version
        PixelsCacheUtil.setIndexVersion(indexFile, version);
        PixelsCacheUtil.setCacheStatus(cacheFile, PixelsCacheUtil.CacheStatus.OK.getId());
        PixelsCacheUtil.setCacheSize(cacheFile, newCacheOffset);
        // set rwFlag as readable
        PixelsCacheUtil.endIndexWrite(indexFile);
        logger.debug("Cache index ends at offset: " + currentIndexOffset);

        return status;
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
                        } else
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
                } catch (IOException e)
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

            } else
            {
                return path;
            }
        } catch (IOException e)
        {
            logger.error("failed to ensure the locality of a file/data object.", e);
        }

        return null;
    }

    /**
     * Traverse radix to get all cached values, and put them into cachedColumnChunks list.
     */
    private void traverseRadix(List<PixelsCacheIdx> cacheIdxes)
    {
        RadixNode root = radix.getRoot();
        if (root.getSize() == 0)
        {
            return;
        }
        visitRadix(cacheIdxes, root);
    }

    /**
     * Visit radix recursively in depth first way.
     * Maybe considering using a stack to store edge values along the visitation path.
     * Push edges in as going deeper, and pop out as going shallower.
     */
    private void visitRadix(List<PixelsCacheIdx> cacheIdxes, RadixNode node)
    {
        if (node.isKey())
        {
            PixelsCacheIdx value = node.getValue();
            PixelsCacheIdx idx = new PixelsCacheIdx(value.offset, value.length);
            cacheIdxes.add(idx);
        }
        for (RadixNode n : node.getChildren().values())
        {
            visitRadix(cacheIdxes, n);
        }
    }

    /**
     * Write radix tree node.
     */
    private void writeRadix(RadixNode node)
    {
        if (flushNode(node))
        {
            for (RadixNode n : node.getChildren().values())
            {
                writeRadix(n);
            }
        }
    }

    /**
     * Flush node content to the index file based on {@code currentIndexOffset}.
     * Header(4 bytes) + [Child(8 bytes)]{n} + edge(variable size) + value(optional).
     * Header: isKey(1 bit) + edgeSize(22 bits) + childrenSize(9 bits)
     * Child: leader(1 byte) + child_offset(7 bytes)
     */
    private boolean flushNode(RadixNode node)
    {
        nodeBuffer.clear();
        if (currentIndexOffset >= indexFile.getSize())
        {
            logger.debug("Offset exceeds index size. Break. Current size: " + currentIndexOffset);
            return false;
        }
        if (node.offset == 0)
        {
            node.offset = currentIndexOffset;
        }
        else
        {
            currentIndexOffset = node.offset;
        }
        allocatedIndexOffset += node.getLengthInBytes();
        int header = 0;
        int edgeSize = node.getEdge().length;
        header = header | (edgeSize << 9);
        int isKeyMask = 1 << 31;
        if (node.isKey())
        {
            header = header | isKeyMask;
        }
        header = header | node.getChildren().size();
        indexFile.setInt(currentIndexOffset, header);  // header
        currentIndexOffset += 4;
        for (Byte key : node.getChildren().keySet())
        {   // children
            RadixNode n = node.getChild(key);
            int len = n.getLengthInBytes();
            n.offset = allocatedIndexOffset;
            allocatedIndexOffset += len;
            long childId = 0L;
            childId = childId | ((long) key << 56);  // leader
            childId = childId | n.offset;  // offset
            nodeBuffer.putLong(childId);
//            indexFile.putLong(currentIndexOffset, childId);
//            currentIndexOffset += 8;
        }
        byte[] nodeBytes = new byte[node.getChildren().size() * 8];
        nodeBuffer.flip();
        nodeBuffer.get(nodeBytes);
        indexFile.setBytes(currentIndexOffset, nodeBytes); // children
        currentIndexOffset += nodeBytes.length;
        indexFile.setBytes(currentIndexOffset, node.getEdge()); // edge
        currentIndexOffset += node.getEdge().length;
        if (node.isKey())
        {  // value
            node.getValue().getBytes(cacheIdxBuffer);
            indexFile.setBytes(currentIndexOffset, cacheIdxBuffer.array());
            currentIndexOffset += 12;
        }
        return true;
    }

    /**
     * Flush out index to index file from start.
     */
    private void flushIndex()
    {
        // set index content offset, skip the index header.
        currentIndexOffset = PixelsCacheUtil.INDEX_RADIX_OFFSET;
        allocatedIndexOffset = PixelsCacheUtil.INDEX_RADIX_OFFSET;
        // if root contains nodes, which means the tree is not empty,then write nodes.
        if (radix.getRoot().getSize() != 0)
        {
            writeRadix(radix.getRoot());
        }
    }

    public void close()
            throws Exception
    {
        indexFile.unmap();
        cacheFile.unmap();
    }
}
