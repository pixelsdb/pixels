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

import io.pixelsdb.pixels.core.encoding.EncodingLevel;

/**
 * Shared constants for pixels-core tests.
 */
public class TestParams
{
    /**
     * Placeholder for manual/IT tests that need an external path.
     */
    public static String filePath = "";

    public static int rowNum = 10;

    /**
     * Shared compact schema for tests that need a multi-column TypeDescription.
     */
    public static final String SIMPLE_SCHEMA =
            "struct<" +
                    "a:int," +
                    "b:float," +
                    "c:double," +
                    "d:timestamp," +
                    "e:boolean," +
                    "f:date," +
                    "g:time," +
                    "h:string," +
                    "i:decimal(18,2)," +
                    "j:decimal(38,10)" +
                    ">";

    public final static String schemaStr = SIMPLE_SCHEMA;

    public final static long blockSize = 1024 * 1024;
    public final static int pixelStride = 16;
    public final static int rowGroupSize = 64 * 1024;
    public final static short blockReplication = 1;
    public final static boolean blockPadding = true;
    public final static EncodingLevel encodingLevel = EncodingLevel.EL0;
    public final static int compressionBlockSize = 1;
}
