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

package io.pixelsdb.pixels.core.retina;

public class Visibility implements AutoCloseable
{
    static
    {
        String pixelsHome = System.getenv("PIXELS_HOME");
        if (pixelsHome == null || pixelsHome.isEmpty()) {
            throw new IllegalStateException("Environment variable PIXELS_HOME is not set");
        }

        String osName = System.getProperty("os.name").toLowerCase();
        String libName;
        if (osName.contains("win")) {
            libName = "VisibilityNative.dll";
        } else if (osName.contains("nix") || osName.contains("nux") || osName.contains("aix")) {
            libName = "libVisibilityNative.so";
        } else if (osName.contains("mac")) {
            libName = "libVisibilityNative.dylib";
        } else {
            throw new IllegalStateException("Unsupported OS: " + osName);
        }

        String libPath = pixelsHome + "/lib/" + libName;
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
    public native long[] getReadableBitmap(int timestamp, long nativeHandle);
    public native void deleteRow(int timestamp, int rowId, long nativeHandle);

    public long[] getReadableBitmap(int timestamp)
    {
        return getReadableBitmap(timestamp, this.nativeHandle);
    }

    public void deleteRow(int timestamp, int rowId)
    {
        deleteRow(timestamp, rowId, this.nativeHandle);
    }
}
