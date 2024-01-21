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

import io.pixelsdb.pixels.common.exception.CacheException;
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import io.pixelsdb.pixels.common.utils.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Created at: 2024/1/20
 *
 * @author alph00
 */
class PixelsZoneUtil {
    private final static Logger logger = LogManager.getLogger(PixelsZoneUtil.class);

    /**
     * Issue #88:
     * Do not use 2 ^ n - 1, it is not latex :)
     * <p>
     * Issue #91:
     * Use three bytes, instead of two bytes, for reader count.
     * The following masks and const numbers are also changed accordingly.
     */
    public static final int MAX_READER_COUNT = 0x007FFFFF;

    /**
     * The masks and const numbers are initialized according to the native endianness.
     * The zone index is also read and write using native endianness.
     */
    public static final int RW_MASK;
    public static final int READER_COUNT_MASK;
    public static final int READER_COUNT_INC;
    public static final int ZERO_READER_COUNT_WITH_RW_FLAG;
    public static final int READER_COUNT_RIGHT_SHIFT_BITS;
    /**
     * We only use the first 14 bytes in the index {magic(6)+rw_flag(1)+reader_count(3)+version(4)}
     * for metadata header, but we start radix tree from offset 16 for word alignment.
     */
    public static final int INDEX_RADIX_OFFSET = 16;
    /**
     * We use the first 18 bytes in the zone file {magic(6)+status(2)+size(8)+type(2)} for
     * metadata header, but we start radix tree from offset 24 for word alignment.
     */
    public static final int ZONE_DATA_OFFSET = 24;

    /**
     * The length of zone read lease in millis.
     */
    public static final int ZONE_READ_LEASE_MS = 100;

    static {
        if (MemoryMappedFile.getOrder() == ByteOrder.LITTLE_ENDIAN) {
            /**
             * If the index file is in little-endian, rw flag is in the lowest
             * byte of v, while reader count is in the highest three bytes.
             */
            RW_MASK = 0x000000FF;
            READER_COUNT_MASK = 0xFFFFFF00;
            READER_COUNT_INC = 0x00000100;
            ZERO_READER_COUNT_WITH_RW_FLAG = 0x00000001;
            READER_COUNT_RIGHT_SHIFT_BITS = 8;
        } else {
            /**
             * If the index file is in big-endian, rw flag is in the highest
             * bytes of v, while reader count is in the lowest three bytes.
             */
            RW_MASK = 0xFF000000;
            READER_COUNT_MASK = 0x00FFFFFF;
            READER_COUNT_INC = 0x00000001;
            ZERO_READER_COUNT_WITH_RW_FLAG = 0x01000000;
            READER_COUNT_RIGHT_SHIFT_BITS = 0;
        }
    }

    public enum ZoneStatus {
        INCONSISTENT((short) -1), EMPTY((short) 0), OK((short) 1);

        private final short id;

        ZoneStatus(short id) {
            this.id = id;
        }

        public short getId() {
            return id;
        }
    }

    public enum ZoneType {
        LAZY((short) 0), SWAP((short) 1), EAGER((short) 2);

        private final short id;

        ZoneType(short id) {
            this.id = id;
        }

        public short getId() {
            return id;
        }
    }

    public static void initializeGlobalIndex(MemoryMappedFile indexFile, PixelsBucketToZoneMap bucketToZoneMap) {
        // init index
        setMagic(indexFile);
        clearIndexRWAndCount(indexFile);
        setIndexVersion(indexFile, 0);
        bucketToZoneMap.initialize();
    }

    public static void initializeLazy(MemoryMappedFile indexFile, MemoryMappedFile zoneFile) {
        // init index
        setMagic(indexFile);
        clearIndexRWAndCount(indexFile);
        // setIndexVersion(indexFile, 0);
        // init zone
        setMagic(zoneFile);
        setStatus(zoneFile, PixelsZoneUtil.ZoneStatus.EMPTY.getId());
        setSize(zoneFile, 0);
        setType(zoneFile, ZoneType.LAZY.getId());
    }

    public static void initializeSwap(MemoryMappedFile indexFile, MemoryMappedFile zoneFile) {
        // init index
        setMagic(indexFile);
        clearIndexRWAndCount(indexFile);
        // setIndexVersion(indexFile, 0);
        // init zone
        setMagic(zoneFile);
        setStatus(zoneFile, PixelsZoneUtil.ZoneStatus.EMPTY.getId());
        setSize(zoneFile, 0);
        setType(zoneFile, ZoneType.SWAP.getId());
    }

    public static void setIndexVersion(MemoryMappedFile indexFile, int version) {
        indexFile.setIntVolatile(10, version);
    }

    public static int getIndexVersion(MemoryMappedFile indexFile) {
        return indexFile.getIntVolatile(10);
    }

    private static void setMagic(MemoryMappedFile file) {
        file.setBytes(0, Constants.FILE_MAGIC.getBytes(StandardCharsets.UTF_8));
    }

    public static String getMagic(MemoryMappedFile file) {
        byte[] magic = new byte[6];
        file.getBytes(0, magic, 0, 6);
        return new String(magic, StandardCharsets.UTF_8);
    }

    public static String getMagic(RandomAccessFile file) throws IOException {
        byte[] magic = new byte[6];
        file.seek(0);
        file.readFully(magic, 0, 6);
        return new String(magic, StandardCharsets.UTF_8);
    }

    public static boolean checkMagic(MemoryMappedFile file) {
        String magic = getMagic(file);
        return magic.equalsIgnoreCase(Constants.FILE_MAGIC);
    }

    private static void clearIndexRWAndCount(MemoryMappedFile indexFile) {
        indexFile.setIntVolatile(6, 0);
    }

    /**
     * Read radix from index file.
     *
     * @param indexFile the index file to be read.
     * @return the radix tree read from index file.
     */
    public static PixelsRadix loadRadixIndex(MemoryMappedFile indexFile) throws CacheException {
        PixelsRadix radix = new PixelsRadix();
        readRadix(indexFile, PixelsZoneUtil.INDEX_RADIX_OFFSET, radix.getRoot(), 1);
        return radix;
    }

    /**
     * Read and construct the index from index file.
     *
     * @param indexFile  the index file to be read.
     * @param nodeOffset the offset of the current root node of the free (or sub-tree).
     * @param node       the current root node to be read from index file.
     * @param level      the current level of the node, starts from 1 for root of the tree.
     */
    private static void readRadix(MemoryMappedFile indexFile, long nodeOffset,
                                  RadixNode node, int level) throws CacheException {
        long[] children = readNode(indexFile, nodeOffset, node, level);

        if (node.isKey()) {
            return;
        }

        if (children == null) {
            throw new CacheException("Can not read node normally.");
        }

        for (long childId : children) {
            // offset is in the lowest 56 bits, the highest 8 bits leader is discarded.
            long childOffset = childId & 0x00FFFFFFFFFFFFFFL;
            RadixNode childNode = new RadixNode();
            readRadix(indexFile, childOffset, childNode, level + 1);
            node.addChild(childNode, true);
        }
    }

    /**
     * Read the index node from index file.
     *
     * @param indexFile  the index file to be read.
     * @param nodeOffset the offset of this node in index file.
     * @param node       the node to be read from index file.
     * @param level      the current level of this node.
     * @return the children ids (1 byte leader + 7 bytes offset) of the node.
     */
    private static long[] readNode(MemoryMappedFile indexFile, long nodeOffset,
                                   RadixNode node, int level) {
        if (nodeOffset >= indexFile.getSize()) {
            logger.debug("Offset exceeds index size. Break. Current size: " + nodeOffset);
            return null;
        }
        int dramAccessCounter = 0;
        node.offset = nodeOffset;
        int nodeHeader = indexFile.getInt(nodeOffset);
        dramAccessCounter++;
        int nodeChildrenNum = nodeHeader & 0x000001FF;
        int nodeEdgeSize = (nodeHeader & 0x7FFFFE00) >>> 9;

        byte[] childrenData = new byte[nodeChildrenNum * 8];
        indexFile.getBytes(nodeOffset + 4, childrenData, 0, nodeChildrenNum * 8);
        /**
         * To ensure the consistent endian (big-endian) in Java,
         * we use ByteBuffer to wrap the bytes instead of directly getLong() from indexFile.
         */
        ByteBuffer childrenBuffer = ByteBuffer.wrap(childrenData);
        long[] children = new long[nodeChildrenNum];
        for (int i = 0; i < nodeChildrenNum; ++i) {
            children[i] = childrenBuffer.getLong();
        }
        dramAccessCounter++;
        byte[] edge = new byte[nodeEdgeSize];
        indexFile.getBytes(nodeOffset + 4 + nodeChildrenNum * 8, edge, 0, nodeEdgeSize);
        dramAccessCounter++;
        node.setEdge(edge);

        if (((nodeHeader >>> 31) & 1) > 0) {
            node.setKey(true);
            // read value
            byte[] idx = new byte[12];
            indexFile.getBytes(nodeOffset + 4 + (nodeChildrenNum * 8) + nodeEdgeSize,
                    idx, 0, 12);
            dramAccessCounter++;
            PixelsCacheIdx cacheIdx = new PixelsCacheIdx(idx);
            cacheIdx.dramAccessCount = dramAccessCounter;
            cacheIdx.radixLevel = level;
            node.setValue(cacheIdx);
        } else {
            node.setKey(false);
        }

        return children;
    }

    public static void setStatus(MemoryMappedFile file, short status) {
        file.setShortVolatile(6, status);
    }

    public static short getStatus(MemoryMappedFile file) {
        return file.getShortVolatile(6);
    }

    public static void setSize(MemoryMappedFile file, long size) {
        file.setLongVolatile(8, size);
    }

    public static long getSize(MemoryMappedFile file) {
        return file.getLongVolatile(8);
    }

    public static void setType(MemoryMappedFile file, short type) {
        file.setShortVolatile(16, type);
    }

    public static short getType(MemoryMappedFile file) {
        return file.getShortVolatile(16);
    }

/*
    public static void setZoneTypeLazy(MemoryMappedFile file) {
        setType(file, ZoneType.LAZY.getId());
    }
*/

/*
    public static void setZoneTypeSwap(MemoryMappedFile file) {
        setType(file, ZoneType.SWAP.getId());
    }
*/

    public static void setZoneTypeEager(MemoryMappedFile file) {
        setType(file, ZoneType.EAGER.getId());
    }

    public static void beginIndexWrite(MemoryMappedFile indexFile) throws InterruptedException {
        // Set the rw flag.
        indexFile.setByteVolatile(6, (byte) 1);
        final int sleepMs = 10;
        int waitMs = 0;
        while ((indexFile.getIntVolatile(6) & READER_COUNT_MASK) > 0) // polling to see if something is finished
        {
            /**
             * Wait for the existing readers to finish.
             * As rw flag has been set, there will be no new readers,
             * the existing readers should finished zone reading in
             * 10s (10000ms). If the reader can not finish zone reading
             * in 10s, it is considered as failed.
             */
            Thread.sleep(sleepMs);
            waitMs += sleepMs;
            if (waitMs > ZONE_READ_LEASE_MS) {
                // clear reader count to continue writing.
                indexFile.setIntVolatile(6, ZERO_READER_COUNT_WITH_RW_FLAG);
                break;
            }
        }
    }

    // eliminate reader count, so writer only sleep for LEASE*2 time
    public static void beginIndexWriteNoReaderCount(MemoryMappedFile indexFile) throws InterruptedException {
        Thread.sleep(ZONE_READ_LEASE_MS * 2);
    }

    public static void endIndexWrite(MemoryMappedFile indexFile) {
        indexFile.setByteVolatile(6, (byte) 0);
    }

    /**
     * Begin index read. This method will wait for the writer to finish.
     *
     * @param indexFile
     * @return the time in millis as the lease of this index read.
     * @throws InterruptedException
     */
    public static long beginIndexRead(MemoryMappedFile indexFile) throws InterruptedException {
        int v = indexFile.getIntVolatile(6);
        int readerCount = (v & READER_COUNT_MASK) >> READER_COUNT_RIGHT_SHIFT_BITS;
        if (readerCount >= MAX_READER_COUNT) {
            throw new InterruptedException("Reaches the max concurrent read count.");
        }
        while ((v & RW_MASK) > 0 ||
                // cas ensures that reading rw flag and increasing reader count is atomic.
                indexFile.compareAndSwapInt(6, v, v + READER_COUNT_INC) == false) {
            // We failed to get read lock or increase reader count.
            if ((v & RW_MASK) > 0) {
                // if there is an existing writer, sleep for 10ms.
                Thread.sleep(10);
            }
            v = indexFile.getIntVolatile(6);
            readerCount = (v & READER_COUNT_MASK) >> READER_COUNT_RIGHT_SHIFT_BITS;
            if (readerCount >= MAX_READER_COUNT) {
                throw new InterruptedException("Reaches the max concurrent read count.");
            }
        }
        // return lease
        return System.currentTimeMillis();
    }

    public static long getReaderCount(MemoryMappedFile indexFile) {
        return (indexFile.getIntVolatile(6) & READER_COUNT_MASK) >> READER_COUNT_RIGHT_SHIFT_BITS;
    }

    public static boolean endIndexRead(MemoryMappedFile indexFile, long lease) {
        if (System.currentTimeMillis() - lease >= ZONE_READ_LEASE_MS) {
            return false;
        }
        int v = indexFile.getIntVolatile(6);
        // if reader count is already <= 0, nothing will be done.
        while ((v & READER_COUNT_MASK) > 0) {
            if (indexFile.compareAndSwapInt(6, v, v - READER_COUNT_INC)) {
                // if v is not changed and the reader count is successfully decreased, break.
                break;
            }
            v = indexFile.getIntVolatile(6);
        }
        return true;
    }
    
    public static void eliminateReaderCount(MemoryMappedFile indexFile) {
        indexFile.setByteVolatile(6, (byte) 0);
    }
}