/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.common.physical.io;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.storage.HDFS;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author guodong
 * @author hank
 */
public class PhysicalHDFSReader
        implements PhysicalReader
{
    private final HDFS hdfs;
    private final String path;
    private final FSDataInputStream rawReader;
    private final List<BlockWrapper> blocks;
    private static Comparator<BlockWrapper> comp;

    private class BlockWrapper
    {
        private long startOffset;
        private long blockSize;
        private ExtendedBlock block;

        public BlockWrapper(long startOffset, long blockSize)
        {
            this.startOffset = startOffset;
            this.blockSize = blockSize;
            this.block = null;
        }

        public BlockWrapper(long startOffset, long blockSize, ExtendedBlock block)
        {
            this.startOffset = startOffset;
            this.blockSize = blockSize;
            this.block = block;
        }

        public long getStartOffset()
        {
            return startOffset;
        }

        public long getBlockSize()
        {
            return blockSize;
        }

        public ExtendedBlock getBlock()
        {
            return block;
        }
    }

    static
    {
        // copied from {@link org.apache.hadoop.hdfs.protocol.LocatedBlocks#findBlock(long offset)}
        comp = (a, b) -> {
            // Returns 0 iff a is inside b or b is inside a
            long aBeg = a.getStartOffset();
            long bBeg = b.getStartOffset();
            long aEnd = aBeg + a.getBlockSize();
            long bEnd = bBeg + b.getBlockSize();
            if (aBeg <= bBeg && bEnd <= aEnd
                    || bBeg <= aBeg && aEnd <= bEnd)
                return 0; // one of the blocks is inside the other
            if (aBeg < bBeg)
                return -1; // a's left bound is to the left of the b's
            return 1;
        };
    }

    public PhysicalHDFSReader(Storage storage, String path) throws IOException
    {
        if (storage instanceof HDFS)
        {
            this.hdfs = (HDFS) storage;
        }
        else
        {
            throw new IOException("Storage is not HDFS.");
        }
        this.path = path;
        this.rawReader = (FSDataInputStream) hdfs.open(path);
        HdfsDataInputStream hdis = null;
        if (this.rawReader instanceof HdfsDataInputStream)
        {
            hdis = (HdfsDataInputStream) rawReader;
            try
            {
                List<LocatedBlock> locatedBlocks = hdis.getAllBlocks();
                this.blocks = new ArrayList<>();
                for (LocatedBlock block : locatedBlocks)
                {
                    this.blocks.add(
                            new BlockWrapper(block.getStartOffset(),
                                    block.getBlockSize(), block.getBlock()));
                }
                Collections.sort(this.blocks, comp);
            }
            catch (IOException e)
            {
                try
                {
                    this.rawReader.close();
                }
                catch (IOException e1)
                {
                    e.addSuppressed(e1);
                    throw e;
                }
                throw e;
            }
        }
        else
        {
            this.blocks = null;
        }
    }

    @Override
    public long getFileLength() throws IOException
    {
        return hdfs.getStatus(path).getLength();
    }

    public void seek(long desired) throws IOException
    {
        rawReader.seek(desired);
    }

    @Override
    public long readLong() throws IOException
    {
        return rawReader.readLong();
    }

    @Override
    public int readInt() throws IOException
    {
        return rawReader.readInt();
    }

    @Override
    public ByteBuffer readFully(int length) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        rawReader.readFully(buffer.array());
        return buffer;
    }

    @Override
    public void readFully(byte[] buffer) throws IOException
    {
        rawReader.readFully(buffer);
    }

    @Override
    public void readFully(byte[] buffer, int offset, int length) throws IOException
    {
        rawReader.readFully(buffer, offset, length);
    }

    /**
     * @return true if readAsync is supported.
     */
    @Override
    public boolean supportsAsync()
    {
        return false;
    }

    @Override
    public CompletableFuture<ByteBuffer> readAsync(long offset, int length) throws IOException
    {
        throw new IOException("Asynchronous read is not supported for HDFS.");
    }

    @Override
    public void close() throws IOException
    {
        rawReader.close();
    }

    @Override
    public String getPath()
    {
        return path;
    }

    @Override
    public String getName()
    {
        return new Path(path).getName();
    }

    /**
     * Block id of the current block that is been reading.
     *
     * @return
     * @throws IOException
     */
    @Override
    public long getBlockId() throws IOException
    {
        if (this.blocks != null)
        {
            BlockWrapper key = new BlockWrapper(this.rawReader.getPos(), 1);
            int i = Collections.binarySearch(blocks, key, comp);
            return this.blocks.get(i).getBlock().getBlockId();
        }
        else
        {
            throw new IOException("Failed to get blocks. This reader may be backed by a non-HdfsDataInputStream.");
        }
    }

    /**
     * Get the scheme of the backed physical storage.
     *
     * @return
     */
    @Override
    public Storage.Scheme getStorageScheme()
    {
        return hdfs.getScheme();
    }
}
