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
package io.pixelsdb.pixels.core.writer;

import io.pixelsdb.pixels.core.TypeDescription;

import java.nio.ByteOrder;

/**
 * Created at: 04/03/2022
 * Author: hank
 */
public class VarbinaryColumnWriter extends BinaryColumnWriter
{
    public VarbinaryColumnWriter(TypeDescription type, int pixelStride, boolean isEncoding, ByteOrder byteOrder)
    {
        super(type, pixelStride, isEncoding, byteOrder);
    }
}
