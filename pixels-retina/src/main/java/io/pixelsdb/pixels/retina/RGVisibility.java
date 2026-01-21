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
    private final AtomicLong nativeHandle = new AtomicLong();

    public RGVisibility(long rgRecordNum)
    {
        this.nativeHandle.set(createNativeObject(rgRecordNum));
    }

    public RGVisibility(long rgRecordNum, long timestamp, long[] initialBitmap)
    {
        if (initialBitmap == null)
        {
            this.nativeHandle.set(createNativeObject(rgRecordNum));
        } else
        {
            this.nativeHandle.set(createNativeObjectInitialized(rgRecordNum, timestamp, initialBitmap));
        }
    }

    @Override
    public void close()
    {
        long handle = nativeHandle.getAndSet(0);
        if (handle != 0)
        {
            destroyNativeObject(handle);
        }
    }

    // native methods
    private native long createNativeObject(long rgRecordNum);
    private native long createNativeObjectInitialized(long rgRecordNum, long timestamp, long[] bitmap);
    private native void destroyNativeObject(long nativeHandle);
    private native void deleteRecord(int rgRowOffset, long timestamp, long nativeHandle);
    private native long[] getVisibilityBitmap(long timestamp, long nativeHandle);
    private native void garbageCollect(long timestamp, long nativeHandle);
    private static native long getNativeMemoryUsage();
    private static native long getRetinaTrackedMemoryUsage();
    private static native long getRetinaObjectCount();

    public void deleteRecord(int rgRowOffset, long timestamp)
    {
        long handle = nativeHandle.get();
        if (handle == 0) throw new IllegalStateException("RGVisibility is closed");
        deleteRecord(rgRowOffset, timestamp, handle);
    }

    public long[] getVisibilityBitmap(long timestamp)
    {
        long handle = this.nativeHandle.get();
        if (handle == 0)
        {
            throw new IllegalStateException("RGVisibility instance has been closed.");
        }
        long[] bitmap = getVisibilityBitmap(timestamp, handle);
        if (bitmap == null)
        {
            logger.warn("Native layer returned null bitmap for timestamp: {}", timestamp);
            return new long[0];
        }
        return bitmap;
    }

    public void garbageCollect(long timestamp)
    {
        long handle = this.nativeHandle.get();
        if (handle == 0)
        {
            throw new IllegalStateException("RGVisibility instance has been closed.");
        }

        garbageCollect(timestamp, handle);
    }

    /**
     * Retrieves the total number of bytes allocated by the native library.
     * This corresponds to the 'stats.allocated' metric in jemalloc.
     *
     * @return The number of bytes allocated.
     * @throws RuntimeException if jemalloc is disabled or fails to retrieve statistics.
     */
    public static long getMemoryUsage()
    {
        return handleMemoryMetric(getNativeMemoryUsage(), "Allocated Memory");
    }

    /**
     * Retrieves the total number of bytes currently tracked by RetinaBase.
     * This represents the net memory used by specific Retina business objects
     * (e.g., TileVisibility, DeleteIndexBlock).
     *
     * @return The number of tracked bytes.
     * @throws RuntimeException if the native library fails to retrieve statistics.
     */
    public static long getTrackedMemoryUsage()
    {
        return handleMemoryMetric(getRetinaTrackedMemoryUsage(), "Tracked Retina Memory");
    }

    public static long getRetinaTrackedObjectCount() {
        return handleMemoryMetric(getRetinaObjectCount(), "Retina Object Count");
    }

    /**
     * Translates native error codes into meaningful Java exceptions.
     * * Error Code Mapping:
     * -1: Feature disabled (ENABLE_JEMALLOC=OFF)
     * -2: Failed to refresh jemalloc epoch (internal cache update failed)
     * -3: Failed to read metric via mallctl
     *
     * @param result The value returned from the JNI layer.
     * @param metricName The name of the metric for the error message.
     * @return The valid metric value if non-negative.
     */
    private static long handleMemoryMetric(long result, String metricName)
    {
        if (result >= 0)
        {
            return result;
        }

        String errorMessage;
        switch ((int) result)
        {
            case -1:
                errorMessage = metricName + " monitoring is disabled. Build with -DENABLE_JEMALLOC=ON.";
                break;
            case -2:
                errorMessage = "Failed to refresh jemalloc epoch. Statistics might be stale or unreachable.";
                break;
            case -3:
                errorMessage = "Mallctl failed to read " + metricName + ". Check jemalloc configuration/prefix.";
                break;
            default:
                errorMessage = "An unexpected error occurred in native memory monitoring (Code: " + result + ")";
                break;
        }
        throw new RuntimeException(errorMessage);
    }
}
