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
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Zoned Pixels cache reader.
 * The previous non-zoned PixelsCacheReader has been moved as the PixelsZoneReader.
 *
 * @author alph00
 */
public class PixelsCacheReader implements AutoCloseable
{
    private static final Logger logger = LogManager.getLogger(PixelsCacheReader.class);
    private final PixelsLocator locator;
    private final List<PixelsZoneReader> zones;
    private final PixelsBucketToZoneMap bucketToZoneMap;

    private PixelsCacheReader(PixelsLocator builderLocator, List<PixelsZoneReader> builderZones, PixelsBucketToZoneMap bucketToZoneMap)
    {
        this.zones = builderZones;
        this.locator = builderLocator;
        this.bucketToZoneMap = bucketToZoneMap;
    }

    public static class Builder
    {
        private List<MemoryMappedFile> zoneCacheFiles;
        private List<MemoryMappedFile> zoneIndexFiles;
        private MemoryMappedFile globalIndexFile;
        /**
         * The number of zones in the cache, including the swap zones.
         */
        private int zoneNum = 0;
        /**
         * The number of swap zones.
         */
        private int swapZoneNum = 1;

        private Builder()
        {
        }

        /**
         * Set the cache files of the zones in the cache.
         * @param zoneCacheFiles the zone cache files, including those of the swap zones
         * @param swapZoneNum the number of swap zones in the cache, should be less than the number of zones
         * @return this builder
         */
        public PixelsCacheReader.Builder setCacheFiles(List<MemoryMappedFile> zoneCacheFiles, int swapZoneNum)
        {
            checkArgument(zoneCacheFiles != null && !zoneCacheFiles.isEmpty(),
                    "zoneCacheFiles is null or empty");
            checkArgument(swapZoneNum < zoneCacheFiles.size(),
                    "swapZoneNum must be less than the number of zoneCacheFiles");
            this.zoneCacheFiles = zoneCacheFiles;
            this.zoneNum = zoneCacheFiles.size();
            this.swapZoneNum = swapZoneNum;
            return this;
        }

        /**
         * Set the index files of the zones in the cache, and the global index file of the cache.
         * @param zoneIndexFiles the index files of the zones
         * @param globalIndexFile the global index file
         * @return this builder
         */
        public PixelsCacheReader.Builder setIndexFiles(List<MemoryMappedFile> zoneIndexFiles,
                                                       MemoryMappedFile globalIndexFile)
        {
            this.globalIndexFile = globalIndexFile;
            this.zoneIndexFiles = zoneIndexFiles;
            return this;
        }

        public PixelsCacheReader build()
        {
            List<PixelsZoneReader> builderZones = new ArrayList<>();
            for (int i = 0; i < zoneNum; i++)
            {
                builderZones.add(PixelsZoneReader.newBuilder().setZoneFile(
                        zoneCacheFiles.get(i)).setIndexFile(zoneIndexFiles.get(i)).build());
            }
            PixelsLocator builderLocator = new PixelsLocator(zoneNum - swapZoneNum);
            PixelsBucketToZoneMap bucketToZoneMap = new PixelsBucketToZoneMap(globalIndexFile, zoneNum);
            return new PixelsCacheReader(builderLocator, builderZones, bucketToZoneMap);
        }
    }

    public static PixelsCacheReader.Builder newBuilder()
    {
        return new PixelsCacheReader.Builder();
    }

    public ByteBuffer get(long blockId, short rowGroupId, short columnId, boolean direct)
    {
        // update the hashcycle in locator in the PixelsCacheReader
        int hashNodeNum = bucketToZoneMap.getHashNodeNum();
        int originalNodeNum = locator.getNodeNum();
        if (hashNodeNum != originalNodeNum) 
        {
            if (hashNodeNum > originalNodeNum) 
            {
                // expand
                String originZoneName  = zones.get(0).getZoneFile().getName();
                String baseZoneName    = originZoneName.substring(0, originZoneName.lastIndexOf('.'));
                String originIndexName  = zones.get(0).getIndexFile().getName();
                String baseIndexName    = originIndexName.substring(0, originIndexName.lastIndexOf('.'));
                long zoneSize = zones.get(0).getZoneFile().getSize();
                long indexSize = zones.get(0).getIndexFile().getSize();
                
                for (int i = originalNodeNum; i < hashNodeNum; i++) 
                {
                    int newZoneId = zones.size();// the new physical zone id is the size of the zone list
                    String newZoneLocation = baseZoneName + "." + newZoneId;
                    String newIndexLocation = baseIndexName + "." + newZoneId;
                    try 
                    {
                        zones.add(new PixelsZoneReader(newZoneLocation, newIndexLocation, zoneSize, indexSize));
                    } 
                    catch (Exception e) 
                    {
                        logger.warn("Failed to synchronize with writer expansion: could not create zone reader", e);
                        return null;
                    }
                    locator.addNode();
                }
                logger.info("CacheReader detected cache expansion: zoneNum from {} to {}", originalNodeNum, hashNodeNum);
            } 
            else if (hashNodeNum < originalNodeNum) 
            {
                // shrink
                int lastLazyZoneId = originalNodeNum - 1;
                locator.removeNode();
                zones.get(lastLazyZoneId).close();
                zones.remove(lastLazyZoneId);
                logger.info("CacheReader detected cache shrink: zoneNum from {} to {}", originalNodeNum, hashNodeNum);
            }
        }
        long bucketId = locator.getLocation(new PixelsCacheKey(blockId, rowGroupId, columnId));
        int zoneId = bucketToZoneMap.getBucketToZone(bucketId);
        if (zoneId < 0 || zoneId >= zones.size()) 
        {
            logger.warn("Invalid zone id: {}", zoneId);
            return null;
        }
        PixelsZoneReader zone = zones.get(zoneId);
        if (zone == null) 
        {
            logger.warn("Zone reader {} not initialized", zoneId);
            return null;
        }
        return zone.get(blockId, rowGroupId, columnId, direct);
    }

    public void close() 
    {
        try 
        {
            for (PixelsZoneReader zone : zones) 
            {
                zone.close();
            }
            if (bucketToZoneMap != null) 
            {
                bucketToZoneMap.close();
            }
            if (locator != null) 
            {
                locator.close();
            }
        } 
        catch (Exception e) 
        {
            e.printStackTrace();
        }
    }
}
