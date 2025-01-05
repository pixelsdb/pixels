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

public class Visibility implements AutoCloseable
{
    private static final Logger logger = LogManager.getLogger(Visibility.class);
    static
    {
        String pixelsHome = System.getenv("PIXELS_HOME");
        if (pixelsHome == null || pixelsHome.isEmpty())
        {
            throw new IllegalStateException("Environment variable PIXELS_HOME is not set");
        }

        if (!Platform.isLinux())
        {
            logger.error("direct io is not supported on OS other than Linux");
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
     * Constructor creates C++ object and returns handle
     */
    private final long nativeHandle;

    /**
     * Constructor
     */
    public Visibility()
    {
        this.nativeHandle = createNativeObject();
    }

    @Override
    public void close() throws Exception
    {
        if (this.nativeHandle != 0)
        {
            destroyNativeObject(this.nativeHandle);
        }
    }

    // native methods
    private native void destroyNativeObject(long nativeHandle);
    private native long createNativeObject();
    public native void getVisibilityBitmap(long epochTs, long[] visibilityBitmap, long nativeHandle);
    public native void deleteRecord(int rowId, long epochTs, long nativeHandle);
    public native void createNewEpoch(long epochTs, long nativeHandle);
    public native void cleanEpochArrAndPatchArr(long epochTs, long nativeHandle);

    public long[] getVisibilityBitmap(long epochTs, long[] visibilityBitmap)
    {
        getVisibilityBitmap(epochTs, visibilityBitmap, this.nativeHandle);
        return visibilityBitmap;
    }

    public void deleteRecord(int rowId, long epochTs)
    {
        deleteRecord(rowId, epochTs, this.nativeHandle);
    }

    public void createNewEpoch(long epochTs)
    {
        createNewEpoch(epochTs, this.nativeHandle);
    }

    public void cleanEpochArrAndPatchArr(long epochTs)
    {
        cleanEpochArrAndPatchArr(epochTs, this.nativeHandle);
    }
}
