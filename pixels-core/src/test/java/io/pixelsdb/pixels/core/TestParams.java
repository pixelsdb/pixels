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
package io.pixelsdb.pixels.core;

public class TestParams {
    public static String filePath = "";
    public static int rowNum = 10;

    public final static String schemaStr = "__table";
    public final static long blockSize = 1024;
    public final static int pixelStride = 16;

    public final static int rowGroupSize = 16;

    public final static short blockReplication = 4;
    public final static boolean blockPadding = true;
    public final static boolean encoding = true;

    public final static int compressionBlockSize = 16;
}
