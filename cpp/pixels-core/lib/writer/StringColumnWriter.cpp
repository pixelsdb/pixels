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

#include "writer/StringColumnWriter.h"

StringColumnWriter::StringColumnWriter(std::shared_ptr<TypeDescription> type,
                                       std::shared_ptr<PixelsWriterOption> writerOption) :
        ColumnWriter(type, writerOption), curPixelVector(pixelStride)
{
    encodingUtils = std::make_shared<EncodingUtils>();
    startsArray = std::make_shared<DynamicIntArray>();
}

int StringColumnWriter::write(std::shared_ptr<ColumnVector> vector, int length)
{
    auto columnVector = std::static_pointer_cast<BinaryColumnVector>(vector);

    if (!columnVector)
    {
        throw std::invalid_argument("Invalid vector type");
    }

    auto values = columnVector->str_vec;
    EncodingUtils encodingUtils;

    for (int i = 0; i < length; i++)
    {
        isNull[curPixelIsNullIndex] = columnVector->isNull[i];
        curPixelEleIndex++;

        if (columnVector->isNull[i])
        {
            hasNull = true;
            startsArray->add(startOffset);
        } else
        {
            int str_size = values[i].size();
            outputStream->putBytes((u_int8_t *) values[i].c_str(), str_size, startOffset);
            startsArray->add(startOffset);
            startOffset += str_size;
        }

        if (curPixelEleIndex >= pixelStride)
        {
            newPixel();
        }
    }
    return outputStream->getWritePos();
}

void StringColumnWriter::newPixels()
{
    ColumnWriter::newPixel();
}

void StringColumnWriter::writeCurPartWithoutDict(std::shared_ptr<PixelsWriterOption> writerOption,
                                                 std::vector<std::string> &values, int *vLens, int *vOffsets,
                                                 int curPartLength, int curPartOffset)
{
    for (int i = 0; i < curPartLength; i++)
    {
        curPixelEleIndex++;
        if (isNull[curPartOffset + i])
        {
            hasNull = true;
            if (nullsPadding)
            {
                // Padding with zero for null values
                startsArray->add(startOffset);
            }
        } else
        {
            // Write the actual data
            u_int8_t *temp_buffer = new u_int8_t[vLens[curPartOffset + i]];
            std::memcpy(temp_buffer, values[curPartOffset + i].c_str(), vLens[curPartOffset + i]);
            outputStream->putBytes(temp_buffer, vLens[curPartOffset + i], vOffsets[curPartOffset + i]);
            startsArray->add(startOffset);
            startOffset += vLens[curPartOffset + i];
            delete[] temp_buffer;
        }
    }
}

void StringColumnWriter::flush()
{
    ColumnWriter::flush();
    flushStarts();
}

void StringColumnWriter::flushStarts()
{
    int startsFieldOffset = outputStream->getWritePos();
    startsArray->add(startOffset);
    if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN)
    {
        for (int i = 0; i < startsArray->size(); i++)
        {
            encodingUtils->writeIntLE(outputStream, startsArray->get(i));
        }
    } else
    {
        for (int i = 0; i < startsArray->size(); i++)
        {
            encodingUtils->writeIntBE(outputStream, startsArray->get(i));
        }
    }
    startsArray->clear();
    std::shared_ptr<ByteBuffer> offsetBuffer = std::make_shared<ByteBuffer>(4);
    offsetBuffer->putInt(startsFieldOffset);
    outputStream->putBytes(offsetBuffer->getPointer(), offsetBuffer->getWritePos());
}

bool StringColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption)
{
    return writerOption->isNullsPadding();
}

void StringColumnWriter::close()
{
    ColumnWriter::close();
}

