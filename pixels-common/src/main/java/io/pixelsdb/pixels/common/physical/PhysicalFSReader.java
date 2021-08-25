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
package io.pixelsdb.pixels.common.physical;

import io.pixelsdb.pixels.common.exception.FSException;
import io.pixelsdb.pixels.common.physical.impl.HDFS;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author guodong
 */
public class PhysicalFSReader
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

    public PhysicalFSReader(Storage storage, String path) throws IOException
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
    public int read(byte[] buffer) throws IOException
    {
        return rawReader.read(buffer);
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException
    {
        return rawReader.read(buffer, offset, length);
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
     * if this is not
     *
     * @return
     * @throws IOException
     */
    @Override
    public long getCurrentBlockId() throws IOException, FSException
    {
        if (this.blocks != null)
        {
            BlockWrapper key = new BlockWrapper(this.rawReader.getPos(), 1);
            int i = Collections.binarySearch(blocks, key, comp);
            return this.blocks.get(i).getBlock().getBlockId();
        }
        else
        {
            throw new FSException("Failed to get blocks. This reader may be backed by a non-HdfsDataInputStream.");
        }
    }
}
