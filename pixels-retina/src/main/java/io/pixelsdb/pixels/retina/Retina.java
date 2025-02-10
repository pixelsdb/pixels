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
package io.pixelsdb.pixels.retina;

import com.sun.jna.Platform;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.file.Paths;

/**
 * This class is used to manage the visibility of a row-group.
 * It provides methods to add visibility, delete record, and garbage collect.
 */
public class Retina implements AutoCloseable
{
    private static final Logger logger = LogManager.getLogger(Retina.class);
    static
    {
        String pixelsHome = System.getenv("PIXELS_HOME");
        if (pixelsHome == null || pixelsHome.isEmpty()) {
            throw new IllegalStateException("Environment variable PIXELS_HOME is not set");
        }

        if (!Platform.isLinux()) {
            logger.error("direct io is not supported on OS other than Linux");
        }
        String libPath = Paths.get(pixelsHome, "lib/libpixels-retina.so").toString();
        File libFile = new File(libPath);
        if (!libFile.exists()) {
            throw new IllegalStateException("libpixels-retina.so not found at " + libPath);
        }
        if (!libFile.canRead()) {
            throw new IllegalStateException("libpixels-retina.so is not readable at " + libPath);
        }
        System.load(libPath);
    }
    
    /**
     * Constructor creates C++ object and returns handle
     */
    private final long nativeHandle;

    public Retina(long rgRecordNum)
    {
        this.nativeHandle = createNativeObject(rgRecordNum);
    }

    @Override
    public void close()
    {
        if (this.nativeHandle != 0) {
            destroyNativeObject(this.nativeHandle);
        }
    }
    
    // native methods
    private native long createNativeObject(long rgRecordNum);
    private native void destroyNativeObject(long nativeHandle);
    private native void beginRowGroupRead(long nativeHandle);
    private native void endRowGroupRead(long nativeHandle);
    private native void deleteRecord(long rowId, long timestamp, long nativeHandle);
    private native void getVisibilityBitmap(long timestamp, long[] visibilityBitmap, long nativeHandle);
    private native void beginGarbageCollect(long timestamp, long nativeHandle);
    private native void endGarbageCollect(long nativeHandle);

    public void beginRowGroupRead()
    {
        beginRowGroupRead(this.nativeHandle);
    }

    public void endRowGroupRead()
    {
        endRowGroupRead(this.nativeHandle);
    }

    public void deleteRecord(long rowId, long timestamp)
    {
        deleteRecord(rowId, timestamp, this.nativeHandle);
    }

    public void getVisibilityBitmap(long timestamp, long[] visibilityBitmap)
    {
        getVisibilityBitmap(timestamp, visibilityBitmap, this.nativeHandle);
    }

    public void beginGarbageCollect(long timestamp)
    {
        beginGarbageCollect(timestamp, this.nativeHandle);
    }

    public void endGarbageCollect()
    {
        endGarbageCollect(this.nativeHandle);
    }
}
