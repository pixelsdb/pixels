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
package io.pixelsdb.pixels.cache.legacy;

import io.pixelsdb.pixels.cache.PixelsCacheIdx;
import io.pixelsdb.pixels.cache.PixelsCacheKey;
import io.pixelsdb.pixels.cache.PixelsCacheUtil;
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class RadixIndexReader implements AutoCloseable, CacheIndexReader {

    private static final Logger logger = LogManager.getLogger(RadixIndexReader.class);
    private final MemoryMappedFile indexFile;

    /**
     * <p>
     *     A node can have at most 256 children, plus an edge (a segment of
     *     lookup key). Each child is 8 bytes.
     * </p>
     * <p>
     *     In pixels, where will be one PixelsCacheReader on each split. And each
     *     split is supposed to be processed by one single thread. There are
     *     typically 10 - 200 concurrent threads (splits) on a machine. So that it
     *     is not a problem to allocate nodeData in each PixelsCacheReader instance.
     *     By this, we can avoid frequent memory allocation in the search() method.
     * </p>
     * <p>
     *     For the edge, although PixelsRadix can support 1MB edge length, we only
     *     use 12 byte path (PixelsCacheKey). That means no edges can exceeds 12 bytes.
     *     So that 16 bytes is more than enough.
     * </p>
     */
    private byte[] nodeData = new byte[256 * 8 + 16];
    private ByteBuffer childrenBuffer = ByteBuffer.wrap(nodeData);

    private ByteBuffer keyBuffer = ByteBuffer.allocate(PixelsCacheKey.SIZE).order(ByteOrder.BIG_ENDIAN);

    public RadixIndexReader(MemoryMappedFile indexFile) {
        this.indexFile = indexFile;
    }

    @Override
    public PixelsCacheIdx read(PixelsCacheKey key) {
        return search(key.blockId, key.rowGroupId, key.columnId);
    }

    private PixelsCacheIdx search(long blockId, short rowGroupId, short columnId)
    {
        PixelsCacheKey.getBytes(keyBuffer, blockId, rowGroupId, columnId);

        return search(keyBuffer);
    }

    /**
     * Search key from radix tree.
     * If found, update counter in cache idx.
     * Else, return null
     */
    private PixelsCacheIdx search(ByteBuffer keyBuffer)
    {
        int dramAccessCounter = 0;
        int radixLevel = 0;
        final int keyLen = keyBuffer.position();
        long currentNodeOffset = PixelsCacheUtil.INDEX_RADIX_OFFSET;
        int bytesMatched = 0;
        int bytesMatchedInNodeFound = 0;

        // get root
        // TODO: root currently does not have edge, which is not efficient in some cases.
        int currentNodeHeader = indexFile.getInt(currentNodeOffset);
        dramAccessCounter++;
        int currentNodeChildrenNum = currentNodeHeader & 0x000001FF;
        int currentNodeEdgeSize = (currentNodeHeader & 0x7FFFFE00) >>> 9;
        if (currentNodeChildrenNum == 0 && currentNodeEdgeSize == 0)
        {
            return null;
        }
        indexFile.getBytes(currentNodeOffset + 4, this.nodeData, 0, currentNodeChildrenNum * 8);
        dramAccessCounter++;
        radixLevel++;

        // search
        // it can be more clear with a do while function
        outer_loop:
        while (bytesMatched < keyLen)
        {
            // search each child for the matching node
            long matchingChildOffset = 0L;
            childrenBuffer.position(0);
            childrenBuffer.limit(currentNodeChildrenNum * 8);
            // linearly scan all the children
            for (int i = 0; i < currentNodeChildrenNum; i++)
            {
                // long is 8 byte, which is 64 bit
                long child = childrenBuffer.getLong();
                // first byte is matching byte
                byte leader = (byte) ((child >>> 56) & 0xFF);
                if (leader == keyBuffer.get(bytesMatched))
                {
                    // child last 7 bytes is grandson's offset, that is, the address is 7-bytes long
                    matchingChildOffset = child & 0x00FFFFFFFFFFFFFFL;
                    break;
                }
            }
            if (matchingChildOffset == 0)
            {
                break;
            }

            currentNodeOffset = matchingChildOffset;
            bytesMatchedInNodeFound = 0;
            currentNodeHeader = indexFile.getInt(currentNodeOffset);
            dramAccessCounter++;
            currentNodeChildrenNum = currentNodeHeader & 0x000001FF;
            currentNodeEdgeSize = (currentNodeHeader & 0x7FFFFE00) >>> 9;
            // read the children and edge in one memory access.
            indexFile.getBytes(currentNodeOffset + 4,
                    this.nodeData, 0, currentNodeChildrenNum * 8 + currentNodeEdgeSize);
            dramAccessCounter++;
            int edgeEndOffset = currentNodeChildrenNum * 8 + currentNodeEdgeSize;
            /**
             * The first byte is matched in the child leader of the parent node,
             * therefore we start the matching from the second byte in edge.
             * this is useful for below `bytesMatchedInNodeFound == currentNodeEdgeSize`
             */
            bytesMatched++;
            bytesMatchedInNodeFound++;
            // now we are visiting the edge! rather than children data
            // it seems between children and edge, there is a one byte gap?
            // or the first byte of the edge does not matter here anyway
            for (int i = currentNodeChildrenNum * 8 + 1; i < edgeEndOffset && bytesMatched < keyLen; i++)
            {
                // the edge is shared across this node, so the edge should be fully matched
                if (this.nodeData[i] != keyBuffer.get(bytesMatched))
                {
                    break outer_loop;
                }
                bytesMatched++;
                bytesMatchedInNodeFound++;
            }

            // only increase level when a child is really matched.
            radixLevel++;
        }

        // if matches, node found.
        if (bytesMatched == keyLen && bytesMatchedInNodeFound == currentNodeEdgeSize)
        {
            // if the current node is leaf node.
            if (((currentNodeHeader >>> 31) & 1) > 0)
            {
                byte[] idx = new byte[12];
                indexFile.getBytes(currentNodeOffset + 4 + (currentNodeChildrenNum * 8) + currentNodeEdgeSize,
                        idx, 0, 12);
                dramAccessCounter++;
                PixelsCacheIdx cacheIdx = new PixelsCacheIdx(idx);
                cacheIdx.dramAccessCount = dramAccessCounter;
                cacheIdx.radixLevel = radixLevel;
                return cacheIdx;
            }
        }
        return null;
    }

    @Override
    public void batchRead(PixelsCacheKey[] keys, PixelsCacheIdx[] results) {
        throw new RuntimeException("not implemented yet");
    }

    @Override
    public void close() throws Exception {
        indexFile.unmap();
    }
}
