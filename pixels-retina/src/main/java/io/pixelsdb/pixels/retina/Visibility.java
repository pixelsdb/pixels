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
import java.util.concurrent.atomic.AtomicLong;

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
    private final AtomicLong nativeHandle = new AtomicLong();

    /**
     * Constructor
     */
    public Visibility()
    {
        this.nativeHandle.set(createNativeObject());
    }

    public Visibility(long timestamp, long[] visibilityBitmap)
    {
        this.nativeHandle.set(createNativeObject(timestamp, visibilityBitmap));
    }

    @Override
    public void close() throws Exception
    {
        if (this.nativeHandle.get() != 0)
        {
            destroyNativeObject(this.nativeHandle.get());
        }
    }

    // native methods
    private native long createNativeObject();
    private native long createNativeObject(long timestamp, long[] visibilityBitmap);
    private native void destroyNativeObject(long nativeHandle);
    public native void getVisibilityBitmap(long timestamp, long[] visibilityBitmap, long nativeHandle);
    public native void deleteRecord(int rowId, long timestamp, long nativeHandle);

    public void getVisibilityBitmap(long timestamp, long[] visibilityBitmap)
    {
        getVisibilityBitmap(timestamp, visibilityBitmap, this.nativeHandle.get());
    }

    public void deleteRecord(int rowId, long timestamp)
    {
        deleteRecord(rowId, timestamp, this.nativeHandle.get());
    }

    public void cleanUp(long timestamp)
    {
        long[] visibilityBitmap = new long[4];
        long oldNativeHandle = this.nativeHandle.get();
        getVisibilityBitmap(timestamp, visibilityBitmap, oldNativeHandle);
        long newNativeHandle = createNativeObject(timestamp, visibilityBitmap);
        nativeHandle.set(newNativeHandle);
        destroyNativeObject(oldNativeHandle);
    }
}
