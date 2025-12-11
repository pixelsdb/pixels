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
public class RGVisibility implements AutoCloseable
{
    private static final Logger logger = LogManager.getLogger(RGVisibility.class);
    static
    {
        String pixelsHome = System.getenv("PIXELS_HOME");
        if (pixelsHome == null || pixelsHome.isEmpty())
        {
            throw new IllegalStateException("Environment variable PIXELS_HOME is not set");
        }

        if (!Platform.isLinux())
        {
            logger.error("Direct io is not supported on OS other than Linux");
        }
        String libPath = Paths.get(pixelsHome, "lib/libpixels-retina.so").toString();
        File libFile = new File(libPath);
        if (!libFile.exists())
        {
            throw new IllegalStateException("libpixels-retina.so not found at " + libPath);
        }
        if (!libFile.canRead())
        {
            throw new IllegalStateException("libpixels-retina.so is not readable at " + libPath);
        }
        System.load(libPath);
    }

    /**
     * Constructor creates C++ object and returns handle.
     */
    private final long nativeHandle;

    public RGVisibility(long rgRecordNum)
    {
        this.nativeHandle = createNativeObject(rgRecordNum);
    }

    public RGVisibility(long rgRecordNum, long timestamp, long[] initialBitmap)
    {
        if (initialBitmap == null)
        {
            this.nativeHandle = createNativeObject(rgRecordNum);
        } else
        {
            this.nativeHandle = createNativeObjectInitialized(rgRecordNum, timestamp, initialBitmap);
        }
    }

    @Override
    public void close()
    {
        if (this.nativeHandle != 0)
        {
            destroyNativeObject(this.nativeHandle);
        }
    }

    // native methods
    private native long createNativeObject(long rgRecordNum);
    private native long createNativeObjectInitialized(long rgRecordNum, long timestamp, long[] bitmap);
    private native void destroyNativeObject(long nativeHandle);
    private native void deleteRecord(int rgRowOffset, long timestamp, long nativeHandle);
    private native long[] getVisibilityBitmap(long timestamp, long nativeHandle);
    private native void garbageCollect(long timestamp, long nativeHandle);

    public void deleteRecord(int rgRowOffset, long timestamp)
    {
        deleteRecord(rgRowOffset, timestamp, this.nativeHandle);
    }

    public long[] getVisibilityBitmap(long timestamp)
    {
        return getVisibilityBitmap(timestamp, this.nativeHandle);
    }

    public void garbageCollect(long timestamp)
    {
        garbageCollect(timestamp, this.nativeHandle);
    }
}
