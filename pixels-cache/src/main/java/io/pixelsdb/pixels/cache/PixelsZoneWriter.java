/*
 * Copyright 2024 PixelsDB.
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

import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

/**
 * @create 2024-01-17
 * @author alph00
 */
public class PixelsZoneWriter 
{
    private final static Logger logger = LogManager.getLogger(PixelsZoneWriter.class);
    private final MemoryMappedFile zoneFile;
    private final int zoneId;
    private final MemoryMappedFile indexFile;
    private PixelsRadix radix;
    private long currentIndexOffset;
    private long allocatedIndexOffset = PixelsZoneUtil.INDEX_RADIX_OFFSET;
    private long cacheOffset = PixelsZoneUtil.ZONE_DATA_OFFSET; // this is only used in the write() method.
    private ByteBuffer nodeBuffer = ByteBuffer.allocate(8 * 256);
    private ByteBuffer cacheIdxBuffer = ByteBuffer.allocate(PixelsCacheIdx.SIZE);

    public PixelsZoneWriter(String builderZoneLocation, String builderIndexLocation, long builderZoneSize, long builderIndexSize, int zoneId) throws Exception 
    {
        this.nodeBuffer.order(ByteOrder.BIG_ENDIAN);
        this.zoneFile = new MemoryMappedFile(builderZoneLocation, builderZoneSize);
        this.indexFile = new MemoryMappedFile(builderIndexLocation, builderIndexSize);
        this.zoneId = zoneId;
    }

    public void buildLazy(PixelsCacheConfig cacheConfig) throws Exception 
    {
        radix = new PixelsRadix();
        PixelsZoneUtil.initializeLazy(indexFile, zoneFile);
    }

    public void buildLazy() throws Exception 
    {
        radix = new PixelsRadix();
        PixelsZoneUtil.initializeLazy(indexFile, zoneFile);
    }

    public void buildSwap(PixelsCacheConfig cacheConfig) throws Exception 
    {
        radix = new PixelsRadix();
        PixelsZoneUtil.initializeSwap(indexFile, zoneFile);
    }

    public void loadIndex() throws Exception 
    {
        radix = PixelsZoneUtil.loadRadixIndex(indexFile);
    }

    public MemoryMappedFile getIndexFile() 
    {
        return indexFile;
    }

    public PixelsRadix getRadix() 
    {
        return radix;
    }

    public MemoryMappedFile getZoneFile() 
    {
        return zoneFile;
    }

    public PixelsZoneUtil.ZoneType getZoneType() 
    {
        if (PixelsZoneUtil.getType(this.zoneFile) == PixelsZoneUtil.ZoneType.LAZY.getId()) 
        {
            return PixelsZoneUtil.ZoneType.LAZY;
        } 
        else if (PixelsZoneUtil.getType(this.zoneFile) == PixelsZoneUtil.ZoneType.SWAP.getId()) 
        {
            return PixelsZoneUtil.ZoneType.SWAP;
        } 
        else 
        {
            return PixelsZoneUtil.ZoneType.EAGER;
        }
    }

    public boolean isZoneEmpty() 
    {
        return PixelsZoneUtil.getStatus(this.zoneFile) == PixelsZoneUtil.ZoneStatus.EMPTY.getId() &&
                PixelsZoneUtil.getSize(this.zoneFile) == 0;
    }

    /**
     * Flush out index to index file from start.
     */
    public void flushIndex() 
    {
        // set index content offset, skip the index header.
        currentIndexOffset = PixelsZoneUtil.INDEX_RADIX_OFFSET;
        allocatedIndexOffset = PixelsZoneUtil.INDEX_RADIX_OFFSET;
        // if root contains nodes, which means the tree is not empty,then write nodes.
        if (radix.getRoot().getSize() != 0) 
        {
            writeRadix(radix.getRoot());
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

    public void close() throws Exception 
    {
        indexFile.unmap();
        zoneFile.unmap();
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

    public int getZoneId() 
    {
        return zoneId;
    }

    /**
     * Change zone type from lazy to swap.
     */
    public void changeZoneTypeL2S() 
    {
        if (getZoneType() != PixelsZoneUtil.ZoneType.LAZY) 
        {
            logger.info("L2S conversion requires LAZY zone, but the zone is not LAZY");
        }
        PixelsZoneUtil.setType(zoneFile, PixelsZoneUtil.ZoneType.SWAP.getId());
        PixelsZoneUtil.setSize(zoneFile, 0);
        PixelsZoneUtil.setStatus(zoneFile, PixelsZoneUtil.ZoneStatus.EMPTY.getId());
        radix.removeAll();
        radix = new PixelsRadix();
    }

    /**
     * Change zone type from swap to lazy.
     */
    public void changeZoneTypeS2L() 
    {
        if (getZoneType() != PixelsZoneUtil.ZoneType.SWAP) 
        {
            logger.info("S2L conversion requires SWAP zone, but the zone is not SWAP");
        }
        PixelsZoneUtil.setType(zoneFile, PixelsZoneUtil.ZoneType.LAZY.getId());
    }


    /**
     * Currently, this is an interface for unit tests.
     * This method only updates index content and cache content (without touching headers)
     */
    public void write(PixelsCacheKey key, byte[] value)
    {
        PixelsCacheIdx cacheIdx = new PixelsCacheIdx(cacheOffset, value.length);
        zoneFile.setBytes(cacheOffset, value);
        cacheOffset += value.length;
        radix.put(key, cacheIdx);
    }
}
