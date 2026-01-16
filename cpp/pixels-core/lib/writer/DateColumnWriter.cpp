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

#include "writer/DateColumnWriter.h"
#include "utils/BitUtils.h"

DateColumnWriter::DateColumnWriter(std::shared_ptr<TypeDescription> type,
                                   std::shared_ptr<PixelsWriterOption> writerOption) :
        ColumnWriter(type, writerOption)
{

}

int DateColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size)
{
    auto columnVector = std::static_pointer_cast<DateColumnVector>(vector);

    if (!columnVector)
    {
        throw std::invalid_argument("Invalid vector type");
    }

    int *values = columnVector->dates;
    EncodingUtils encodingUtils;

    for (int i = 0; i < size; i++)
    {
        isNull[curPixelIsNullIndex] = columnVector->isNull[i];
        curPixelEleIndex++;

        if (columnVector->isNull[i])
        {
            hasNull = true;
            if (nullsPadding)
            {
                encodingUtils.writeIntLE(outputStream, 0);
            }
        } else
        {
            if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN)
            {
                encodingUtils.writeIntLE(outputStream, values[i]);
            } else
            {
                encodingUtils.writeIntBE(outputStream, values[i]);
            }
        }

        if (curPixelEleIndex >= pixelStride)
        {
            newPixel();
        }
    }
    return outputStream->getWritePos();
}

bool DateColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption)
{
    return writerOption->isNullsPadding();
}

