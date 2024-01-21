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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * pixels cache reader.
 *
 * @author alph00
 */
public class PixelsCacheReader implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(PixelsCacheReader.class);
    private final PixelsLocator locator;
    private final List<PixelsZoneReader> zones;
    private final PixelsBucketToZoneMap bucketToZoneMap;

    private PixelsCacheReader(PixelsLocator builderLocator, List<PixelsZoneReader> builderZones, PixelsBucketToZoneMap bucketToZoneMap) {
        this.zones = builderZones;
        this.locator = builderLocator;
        this.bucketToZoneMap = bucketToZoneMap;
    }

    public static class Builder {
        private List<MemoryMappedFile> zoneFiles;
        private List<MemoryMappedFile> indexFiles;
        private MemoryMappedFile globalIndexFile;
        private int zoneNum = 0;
        private int swapZoneNum = 1;

        private Builder() {
        }

        // we calculate zoneNum from zoneFiles including swap zone
        public PixelsCacheReader.Builder setCacheFile(List<MemoryMappedFile> zoneFiles) {
            this.zoneFiles = zoneFiles;
            if(zoneFiles != null) {
                zoneNum = zoneFiles.size();
            } else {
                zoneNum = 0;
            }
            return this;
        }

        public PixelsCacheReader.Builder setIndexFile(List<MemoryMappedFile> indexFiles) {
            this.indexFiles = indexFiles;
            return this;
        }

        public PixelsCacheReader.Builder setGlobalIndexFile(MemoryMappedFile globalIndexFile) {
            this.globalIndexFile = globalIndexFile;
            return this;
        }

        // now this function is not used, swapZoneNum is set to 1 by default
        public PixelsCacheReader.Builder setSwapZoneNum(int swapZoneNum) {
            this.swapZoneNum = swapZoneNum;
            return this;
        }

        public PixelsCacheReader build() {
            List<PixelsZoneReader> builderZones = new ArrayList<>();
            for (int i = 0; i < zoneNum; i++) {
                builderZones.add(PixelsZoneReader.newBuilder().setZoneFile(zoneFiles.get(i)).setIndexFile(indexFiles.get(i)).build());
            }
            PixelsLocator builderLocator = new PixelsLocator(zoneNum - swapZoneNum);
            PixelsBucketToZoneMap bucketToZoneMap = new PixelsBucketToZoneMap(globalIndexFile, zoneNum);
            return new PixelsCacheReader(builderLocator, builderZones, bucketToZoneMap);
        }
    }

    public static PixelsCacheReader.Builder newBuilder() {
        return new PixelsCacheReader.Builder();
    }

    public ByteBuffer get(long blockId, short rowGroupId, short columnId, boolean direct) {
        long bucketId = locator.getLocation(new PixelsCacheKey(blockId, rowGroupId, columnId));
        int zoneId = bucketToZoneMap.getBucketToZone(bucketId);
        if (zoneId < 0 || zoneId >= zones.size()) {
            logger.warn("Invalid zone id: {}", zoneId);
            return null;
        }
        PixelsZoneReader zone = zones.get(zoneId);
        if (zone == null) {
            logger.warn("Zone reader {} not initialized", zoneId);
            return null;
        }
        return zone.get(blockId, rowGroupId, columnId, direct);
    }

    public void close() {
        try {
            for (PixelsZoneReader zone : zones) {
                zone.close();
            }
            if (bucketToZoneMap != null) {
                bucketToZoneMap.close();
            }
            if (locator != null) {                
                locator.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
