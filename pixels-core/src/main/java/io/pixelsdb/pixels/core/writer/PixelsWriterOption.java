/*
 * Copyright 2023 PixelsDB.
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

import io.pixelsdb.pixels.core.encoding.EncodingLevel;

import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2023-08-14
 */
public class PixelsWriterOption
{
    /**
     * The number of values in each pixel.
     */
    private int pixelStride;
    private EncodingLevel encodingLevel;
    private ByteOrder byteOrder;
    /**
     * Whether nulls positions in column are padded by arbitrary values and occupy storage and memory space.
     */
    private boolean nullsPadded;

    public PixelsWriterOption() { }

    public int getPixelStride()
    {
        return pixelStride;
    }

    public PixelsWriterOption pixelStride(int pixelStride)
    {
        this.pixelStride = pixelStride;
        return this;
    }

    public EncodingLevel getEncodingLevel()
    {
        return encodingLevel;
    }

    public PixelsWriterOption encodingLevel(EncodingLevel encodingLevel)
    {
        this.encodingLevel = encodingLevel;
        return this;
    }

    public ByteOrder getByteOrder()
    {
        return byteOrder;
    }

    public PixelsWriterOption byteOrder(ByteOrder byteOrder)
    {
        this.byteOrder = byteOrder;
        return this;
    }

    public boolean isNullsPadded()
    {
        return nullsPadded;
    }

    public PixelsWriterOption nullsPadded(boolean nullsPadded)
    {
        this.nullsPadded = nullsPadded;
        return this;
    }
}
