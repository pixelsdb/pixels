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

import java.util.List;

/**
 * @create 2024-02-17
 * @author alph00
 */
public class PixelsBucketToZoneMap 
{
    private static final Logger logger = LogManager.getLogger(PixelsBucketToZoneMap.class);
    MemoryMappedFile indexFile;
    long zoneNum;
    long startOfRwAndCounts;
    final long startOfHashNodeNum = 14;// store the number of nodes on hashcycle, it is used when scaling cache size.
    final long startOfMap = 18;
    final long sizeOfRwAndCount = 4;
    final long sizeOfBucketToZone = 4;
    final long sizeOfSlot = sizeOfRwAndCount + sizeOfBucketToZone;
    final long stepOfSlot = 3;

    PixelsBucketToZoneMap(MemoryMappedFile indexFile, long zoneNum) 
    {
        this.indexFile = indexFile;
        this.zoneNum = zoneNum;
        this.startOfRwAndCounts = this.startOfMap + this.sizeOfBucketToZone;
    }

    public void updateBucketZoneMap(long bucketId, int zoneId) 
    {
        indexFile.setIntVolatile(this.startOfMap + (bucketId << this.stepOfSlot), zoneId);
    }

    public boolean initialize() 
    {
        if (indexFile.getSize() < this.startOfMap + (this.zoneNum << this.stepOfSlot)) 
        {
            return false;
        }
        for (int i = 0; i < this.zoneNum; i++) 
        {
            indexFile.setIntVolatile(this.startOfMap + (i << this.stepOfSlot), i);
            indexFile.setIntVolatile(this.startOfRwAndCounts + (i << this.stepOfSlot), 0);
        }
        return true;
    }

    public boolean isMapFull(long num)
    {
        if (indexFile.getSize() < this.startOfMap + (num << this.stepOfSlot)) 
        {
            return true;
        }
        return false;
    }

    public int getBucketToZone(long bucketId) 
    {
        return indexFile.getIntVolatile(this.startOfMap + (bucketId << this.stepOfSlot));
    }

    public int getHashNodeNum()
    {
        return indexFile.getIntVolatile(this.startOfHashNodeNum);
    }

    public void setHashNodeNum(int hashNodeNum)
    {
        indexFile.setIntVolatile(this.startOfHashNodeNum, hashNodeNum);
    }

    public void close() 
    {
        try 
        {
            if (indexFile != null) 
            {
                indexFile.unmap();
            }
        } 
        catch (Exception e) 
        {
            logger.error("Failed to unmap index file", e);
        }
    }
}