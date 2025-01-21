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
    std::shared_ptr<PixelsWriterOption> writerOption):
    ColumnWriter(type,writerOption), curPixelVector(pixelStride)
{
    encodingUtils= std::make_shared<EncodingUtils>();
    runlengthEncoding = encodingLevel.ge(EncodingLevel::Level::EL2);
    if (runlengthEncoding)
    {
      encoder = std::make_unique<RunLenIntEncoder>();
    }
    startsArray=std::make_shared<DynamicIntArray>();
}

void StringColumnWriter::flush()
{
    ColumnWriter::flush();
    flushStarts();
}

void StringColumnWriter::flushStarts()
{
    size_t startsFieldOffset = outputStream->size();
    startsArray->add(startOffset);
    if(byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN)
    {
        for (int i = 0; i<startsArray->size(); i++)
        {
          encodingUtils->writeIntLE(outputStream,startsArray->get(i));
        }
    }
    else
    {
        for(int i=0;i<startsArray->size();i++)
        {
            encodingUtils->writeIntBE(outputStream,startsArray->get(i));
        }
    }
    startsArray->clear();
    std::shared_ptr<ByteBuffer> offsetBuffer=std::make_shared<ByteBuffer>(4);
    offsetBuffer->putInt(static_cast<int>(startsFieldOffset));
    outputStream->putBytes(offsetBuffer->getPointer(),offsetBuffer->getWritePos());
}