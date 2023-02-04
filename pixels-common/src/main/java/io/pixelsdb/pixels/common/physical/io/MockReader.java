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
package io.pixelsdb.pixels.common.physical.io;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Storage;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class MockReader implements PhysicalReader
{
    private static final List<String> cacheIdxs = new ArrayList<>();
    private static final List<String> cacheKeys = new ArrayList<>();
    private static final Map<String, String> keyToIdxs = new HashMap<>();
    private static final Map<Long, Long> blkSizes = new HashMap<>();

    private static final Map<Long, String> blkToKeys = new HashMap<>();
    private static final Map<Long, String> blkToIdxs = new HashMap<>();

    static
    {
        try
        {
            // read the mock file
            BufferedReader br = new BufferedReader(new FileReader("dumpedCache.txt"));
            String line = br.readLine();
            String idxString = "";
            String keyString = "";
            while (line != null)
            {
                keyString = line.split(";")[1];
                String[] keyTokens = keyString.split("-");
                long blkId = Long.parseLong(keyTokens[0]);
                short rgId = Short.parseShort(keyTokens[1]);
                short colId = Short.parseShort(keyTokens[2]);
                idxString = line.split(";")[2];
                String[] idxTokens = idxString.split("-");
                long offset = Long.parseLong(idxTokens[0]);
                int length = Integer.parseInt(idxTokens[1]);

                if (blkSizes.containsKey(blkId))
                {
                    blkSizes.put(blkId, blkSizes.get(blkId) + length);
                } else
                {
                    blkSizes.put(blkId, (long) length);
                }
                cacheIdxs.add(idxString);
                cacheKeys.add(keyString);
                keyToIdxs.put(keyString, idxString);
                line = br.readLine();
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    private final long blockId;
    private final long length;
    private final Random ran;


    public MockReader(Storage storage, String path)
    {
        // path is a block id
        assert (storage.getScheme() == Storage.Scheme.mock);
        blockId = Long.parseLong(path);
        length = blkSizes.get(blockId);
        ran = new Random();
    }

    @Override
    public long getFileLength() throws IOException
    {
        return length;
    }

    @Override
    public void seek(long desired) throws IOException
    {

    }

    @Override
    public ByteBuffer readFully(int length) throws IOException
    {
        return null;
    }

    @Override
    public void readFully(byte[] buffer) throws IOException
    {
        Arrays.fill(buffer, (byte) ('A' + (ran.nextInt(26) % 26)));
    }

    @Override
    public void readFully(byte[] buffer, int offset, int length) throws IOException
    {

    }

    @Override
    public boolean supportsAsync()
    {
        return false;
    }

    @Override
    public CompletableFuture<ByteBuffer> readAsync(long offset, int length) throws IOException
    {
        return null;
    }

    @Override
    public long readLong() throws IOException
    {
        return 0;
    }

    @Override
    public int readInt() throws IOException
    {
        return 0;
    }

    @Override
    public void close() throws IOException
    {

    }

    @Override
    public String getPath()
    {
        return null;
    }

    @Override
    public String getName()
    {
        return null;
    }

    @Override
    public long getBlockId() throws IOException
    {
        return blockId;
    }

    @Override
    public Storage.Scheme getStorageScheme()
    {
        return null;
    }

    @Override
    public int getNumReadRequests()
    {
        return 0;
    }
}
